"""Ingestao da OpenF1 para o Google Cloud Storage (camada raw).

Executa uma "onda" por vez, em ordem de prioridade. A ingestao e
idempotente: arquivos ja existentes sao pulados, entao rodar o mesmo
comando duas vezes nao duplica dado e permite retomar apos falha.

Resultado vazio conta como sucesso e gera arquivo de zero linhas. E o que
mantem a idempotencia honesta: uma combinacao sem dado -- piloto que nao
rodou, endpoint que so existe em corrida -- fica registrada e nao volta a
ser consultada.

Ha dois modos:

  carga completa (padrao)  percorre todas as sessoes de todos os anos do
                           escopo. E o que se usa para o backfill.

  --incremental            usa a marca d'agua em _state/watermark.json para
                           considerar apenas sessoes que terminaram depois
                           da ultima execucao. Uma requisicao a /sessions
                           por ano resolve a descoberta; temporada antiga
                           nem chega a ser consultada. Ver
                           docs/ingestao-incremental.md.

Exemplos:
    python main.py --wave 1
    python main.py --wave 2 --years 2024 2025
    python main.py --wave 3 --local          # grava em ./data para testar
    python main.py --wave 2 --limit-sessions 1

    python main.py --wave 2 --incremental                 # rotina semanal
    python main.py --wave 2 --incremental --since 2026-08-01
    python main.py --wave 2 --refetch --session-keys 9472 # conserta um objeto
"""

from __future__ import annotations

import argparse
import logging
import sys
import time
import uuid
from datetime import datetime, timedelta, timezone
from typing import Any

import config
from openf1_client import AccessBlocked, OpenF1Client, OpenF1Error
from storage import (
    GCSStorage,
    LocalStorage,
    ManifestWriter,
    StateStore,
    Storage,
    manifest_prefix,
    object_path,
    state_path,
)

log = logging.getLogger("ingestao")


def _limite_de_carencia() -> str:
    """Sessao que terminou depois deste instante ainda nao esta consolidada."""
    return (datetime.now(timezone.utc) - timedelta(hours=config.GRACE_HOURS)).isoformat()


def _sessoes_do_endpoint(sessions: list[dict[str, Any]], endpoint: dict) -> list[dict[str, Any]]:
    """Aplica o filtro de corrida declarado no proprio endpoint.

    Vem por cima do filtro da onda. Evita gastar uma requisicao por sessao
    em endpoints que a API so publica em corrida.
    """
    if endpoint.get("only_races"):
        return [s for s in sessions if s.get("session_name") in config.RACE_SESSION_NAMES]
    if endpoint.get("skip_races"):
        return [s for s in sessions if s.get("session_name") not in config.RACE_SESSION_NAMES]
    return sessions


class Ingestor:
    def __init__(self, client: OpenF1Client, storage: Storage, prefix: str, run_id: str) -> None:
        self.client = client
        self.storage = storage
        self.prefix = prefix
        self.manifest = ManifestWriter(storage, prefix, run_id)
        self.state = StateStore(storage, prefix)
        self.stats = {
            "gravados": 0, "vazios": 0, "pulados": 0, "fora_da_marca": 0,
            "falhas": 0, "linhas": 0, "bytes": 0,
        }

    # -- unidade de trabalho ----------------------------------------------

    def ingest_one(
        self,
        endpoint: str,
        year: int,
        filters: dict[str, Any],
        session_key: int | None = None,
        driver_number: int | None = None,
        date_start: str | None = None,
        date_end: str | None = None,
        refresh: bool = False,
    ) -> None:
        path = object_path(self.prefix, endpoint, year, session_key, driver_number)

        # refresh=True sobrescreve. Usado pelas listas de nivel de ano
        # (meetings, sessions), que crescem a cada fim de semana e ficariam
        # congeladas na primeira gravacao, e pela flag --refetch.
        if not refresh and self.storage.exists(path):
            self.stats["pulados"] += 1
            log.debug("ja existe, pulando: %s", path)
            return

        started = time.monotonic()
        try:
            records = self.client.fetch(endpoint, filters, date_start, date_end)
        except OpenF1Error as exc:
            self.stats["falhas"] += 1
            log.error("falha em %s: %s", path, exc)
            self._log_manifest(endpoint, path, session_key, driver_number, 0, 0, "erro", str(exc))
            return

        size = self.storage.write_records(path, records)

        # Arquivo vazio ainda e checkpoint: a combinacao foi consultada e
        # nao tem dado. Nao conta como gravacao util nem como falha.
        if records:
            self.stats["gravados"] += 1
            self.stats["linhas"] += len(records)
            self.stats["bytes"] += size
            log.info(
                "%s -> %s linhas, %.1f KB, %.1fs",
                path, f"{len(records):,}".replace(",", "."), size / 1024, time.monotonic() - started,
            )
            status = "ok"
        else:
            self.stats["vazios"] += 1
            log.info("%s -> sem registros", path)
            status = "vazio"

        self._log_manifest(endpoint, path, session_key, driver_number, len(records), size, status, None)

    def _log_manifest(
        self,
        endpoint: str,
        path: str,
        session_key: int | None,
        driver_number: int | None,
        rows: int,
        size: int,
        status: str,
        error: str | None,
    ) -> None:
        self.manifest.add({
            "ingested_at": datetime.now(timezone.utc).isoformat(),
            "endpoint": endpoint,
            "object_path": path,
            "session_key": session_key,
            "driver_number": driver_number,
            "row_count": rows,
            "byte_size": size,
            "status": status,
            "error": error,
        })

    # -- onda ---------------------------------------------------------------

    def run_wave(
        self,
        wave: int,
        years: list[int],
        limit_sessions: int | None,
        *,
        incremental: bool = False,
        since: str | None = None,
        refetch: bool = False,
        session_keys: set[int] | None = None,
    ) -> None:
        spec = config.WAVES[wave]
        log.info("=== Onda %d: %s ===", wave, spec["nome"])

        marca = corte = None
        if incremental:
            marca = since or self.state.marca(wave)
            corte = _limite_de_carencia()
            log.info(
                "modo incremental | marca d'agua: %s | ignorando o que terminou depois de %s",
                marca or "(primeira execucao, sem marca)", corte,
            )

        # Maior date_end efetivamente considerado. Vira a nova marca d'agua
        # no fim, se a execucao nao tiver falhas.
        avanco: str | None = None

        for year in years:
            log.info("--- ano %d ---", year)
            sessions = self.client.sessions(year)
            if not sessions:
                log.warning("nenhuma sessao encontrada para %d", year)
                continue
            sessions.sort(key=lambda s: s.get("date_start") or "")

            sessions = _sessoes_do_endpoint(sessions, spec)

            if session_keys:
                sessions = [s for s in sessions if s["session_key"] in session_keys]

            if incremental:
                sessions = self._filtrar_por_marca(sessions, marca, corte)

            if limit_sessions:
                sessions = sessions[:limit_sessions]

            for s in sessions:
                fim = s.get("date_end")
                if fim and (avanco is None or fim > avanco):
                    avanco = fim

            for endpoint in spec["endpoints"]:
                name, scope = endpoint["name"], endpoint["scope"]

                if scope == "year":
                    # meetings e sessions sao listas que crescem a cada fim de
                    # semana. Em modo incremental precisam ser reescritas, senao
                    # o bucket guarda para sempre a foto do dia da primeira carga.
                    self.ingest_one(name, year, {"year": year}, refresh=incremental or refetch)
                    continue

                alvo = _sessoes_do_endpoint(sessions, endpoint)

                for index, session in enumerate(alvo, start=1):
                    session_key = session["session_key"]
                    log.info(
                        "[%s] sessao %d/%d - %s / %s",
                        name, index, len(alvo),
                        session.get("location"), session.get("session_name"),
                    )

                    if scope == "session":
                        self.ingest_one(
                            name, year, {"session_key": session_key}, session_key, refresh=refetch
                        )
                    elif scope == "session_driver":
                        self._ingest_by_driver(name, year, session, refresh=refetch)

        if incremental:
            self._avancar_marca(wave, avanco)

    def _filtrar_por_marca(
        self, sessions: list[dict[str, Any]], marca: str | None, corte: str
    ) -> list[dict[str, Any]]:
        """Reduz a lista de sessoes ao que e novo e ja esta consolidado.

        Duas condicoes, ambas sobre date_end:
          - depois da marca d'agua  -> nao foi ingerido ainda
          - antes do corte          -> a OpenF1 ja terminou de completar
        """
        antes = len(sessions)
        sem_data = [s for s in sessions if not s.get("date_end")]
        if sem_data:
            log.warning(
                "%d sessao(oes) sem date_end foram ignoradas no modo incremental: %s",
                len(sem_data), [s["session_key"] for s in sem_data],
            )

        restantes = [s for s in sessions if s.get("date_end")]
        if marca:
            restantes = [s for s in restantes if s["date_end"] > marca]
        recentes = [s for s in restantes if s["date_end"] >= corte]
        if recentes:
            log.info(
                "%d sessao(oes) aguardando a carencia de %dh: %s",
                len(recentes), config.GRACE_HOURS, [s["session_key"] for s in recentes],
            )
        restantes = [s for s in restantes if s["date_end"] < corte]

        self.stats["fora_da_marca"] += antes - len(restantes)
        log.info("marca d'agua reduziu %d sessoes para %d", antes, len(restantes))
        return restantes

    def _avancar_marca(self, wave: int, avanco: str | None) -> None:
        """Grava a nova marca d'agua, mas so se nada falhou.

        Avancar apos uma falha enterraria a sessao que falhou: ela ficaria
        atras da marca e nunca mais seria considerada. Nao avancar custa
        quase nada -- a proxima execucao reconsidera as mesmas sessoes, e o
        que ja foi gravado e pulado pela checagem de existencia.
        """
        if self.stats["falhas"]:
            log.warning(
                "marca d'agua NAO avancou: %d falha(s) nesta execucao. "
                "Rode de novo; o que ja foi gravado sera pulado.",
                self.stats["falhas"],
            )
            return
        if not avanco:
            log.info("marca d'agua inalterada: nenhuma sessao nova nesta execucao")
            return
        self.state.avancar(wave, avanco)

    def _ingest_by_driver(
        self, endpoint: str, year: int, session: dict[str, Any], refresh: bool = False
    ) -> None:
        """Telemetria: uma requisicao por piloto, com janela de tempo de reserva."""
        session_key = session["session_key"]
        try:
            drivers = self.client.drivers(session_key)
        except OpenF1Error as exc:
            self.stats["falhas"] += 1
            log.error("nao foi possivel listar pilotos da sessao %s: %s", session_key, exc)
            return

        numbers = sorted({d["driver_number"] for d in drivers if d.get("driver_number")})
        if not numbers:
            log.info("sessao %s nao lista pilotos", session_key)
            return

        for number in numbers:
            self.ingest_one(
                endpoint,
                year,
                {"session_key": session_key, "driver_number": number},
                session_key=session_key,
                driver_number=number,
                date_start=session.get("date_start"),
                date_end=session.get("date_end"),
                refresh=refresh,
            )


def build_storage(use_local: bool) -> Storage:
    if use_local:
        log.info("gravando localmente em %s", config.LOCAL_OUTPUT_DIR)
        return LocalStorage(config.LOCAL_OUTPUT_DIR)
    if config.GCS_BUCKET.endswith("trocar-aqui"):
        sys.exit("Defina a variavel de ambiente GCS_BUCKET antes de rodar.")
    log.info("gravando em gs://%s/%s", config.GCS_BUCKET, config.GCS_PREFIX)
    return GCSStorage(config.GCS_BUCKET)


def main() -> None:
    parser = argparse.ArgumentParser(description="Ingestao OpenF1 -> GCS")
    parser.add_argument("--wave", type=int, required=True, choices=sorted(config.WAVES))
    parser.add_argument("--years", type=int, nargs="+", default=config.YEARS)
    parser.add_argument("--local", action="store_true", help="grava em disco em vez do GCS")
    parser.add_argument("--limit-sessions", type=int, default=None, help="para testes rapidos")
    parser.add_argument("--verbose", action="store_true")

    inc = parser.add_argument_group("atualizacao incremental")
    inc.add_argument(
        "--incremental", action="store_true",
        help="ingere so o que e novo, usando a marca d'agua em _state/watermark.json",
    )
    inc.add_argument(
        "--since", metavar="DATA",
        help="ignora a marca d'agua gravada e usa esta data (ex.: 2026-08-01)",
    )
    inc.add_argument(
        "--refetch", action="store_true",
        help="rebusca e sobrescreve mesmo o que ja existe no bucket",
    )
    inc.add_argument(
        "--session-keys", type=int, nargs="+", metavar="SK",
        help="restringe a estas sessoes; use com --refetch para consertar um objeto",
    )
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(asctime)s %(levelname)-7s %(message)s",
        datefmt="%H:%M:%S",
    )

    if args.since and not args.incremental:
        sys.exit("--since so faz sentido junto com --incremental.")

    run_id = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S") + "-" + uuid.uuid4().hex[:6]
    log.info("run_id=%s", run_id)
    log.info("manifesto em %s/", manifest_prefix(config.GCS_PREFIX, run_id))
    if args.incremental:
        log.info("estado em %s", state_path(config.GCS_PREFIX))

    ingestor = Ingestor(OpenF1Client(), build_storage(args.local), config.GCS_PREFIX, run_id)

    started = time.monotonic()
    try:
        ingestor.run_wave(
            args.wave, args.years, args.limit_sessions,
            incremental=args.incremental,
            since=args.since,
            refetch=args.refetch,
            session_keys=set(args.session_keys) if args.session_keys else None,
        )
    except KeyboardInterrupt:
        log.warning("interrompido pelo usuario")
    except AccessBlocked as exc:
        log.error("%s", exc)
    finally:
        ingestor.manifest.flush()
        elapsed = time.monotonic() - started
        s = ingestor.stats
        log.info(
            "fim em %.1f min | gravados=%d vazios=%d pulados=%d fora_da_marca=%d falhas=%d "
            "| %s linhas | %.1f MB",
            elapsed / 60, s["gravados"], s["vazios"], s["pulados"], s["fora_da_marca"],
            s["falhas"], f"{s['linhas']:,}".replace(",", "."), s["bytes"] / 1024 / 1024,
        )


if __name__ == "__main__":
    main()
