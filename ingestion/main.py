"""Ingestao da OpenF1 para o Google Cloud Storage (camada raw).

Executa uma "onda" por vez, em ordem de prioridade. A ingestao e
idempotente: arquivos ja existentes sao pulados, entao rodar o mesmo
comando duas vezes nao duplica dado e permite retomar apos falha.

Exemplos:
    python main.py --wave 1
    python main.py --wave 2 --years 2024 2025
    python main.py --wave 3 --local          # grava em ./data para testar
    python main.py --wave 2 --limit-sessions 1
"""

from __future__ import annotations

import argparse
import json
import logging
import sys
import time
import uuid
from datetime import datetime, timezone
from typing import Any

import config
from openf1_client import OpenF1Client, OpenF1Error
from storage import GCSStorage, LocalStorage, Storage, manifest_path, object_path

log = logging.getLogger("ingestao")


class Ingestor:
    def __init__(self, client: OpenF1Client, storage: Storage, prefix: str, run_id: str) -> None:
        self.client = client
        self.storage = storage
        self.prefix = prefix
        self.manifest = manifest_path(prefix, run_id)
        self.stats = {"gravados": 0, "pulados": 0, "falhas": 0, "linhas": 0, "bytes": 0}

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
    ) -> None:
        path = object_path(self.prefix, endpoint, year, session_key, driver_number)

        if self.storage.exists(path):
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

        self.stats["gravados"] += 1
        self.stats["linhas"] += len(records)
        self.stats["bytes"] += size

        log.info(
            "%s -> %s linhas, %.1f KB, %.1fs",
            path, f"{len(records):,}".replace(",", "."), size / 1024, time.monotonic() - started,
        )
        self._log_manifest(endpoint, path, session_key, driver_number, len(records), size, "ok", None)

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
        entry = {
            "ingested_at": datetime.now(timezone.utc).isoformat(),
            "endpoint": endpoint,
            "object_path": path,
            "session_key": session_key,
            "driver_number": driver_number,
            "row_count": rows,
            "byte_size": size,
            "status": status,
            "error": error,
        }
        try:
            self.storage.append_line(self.manifest, json.dumps(entry, ensure_ascii=False))
        except Exception as exc:  # manifesto nao pode derrubar a ingestao
            log.warning("nao foi possivel gravar no manifesto: %s", exc)

    # -- onda ---------------------------------------------------------------

    def run_wave(self, wave: int, years: list[int], limit_sessions: int | None) -> None:
        spec = config.WAVES[wave]
        log.info("=== Onda %d: %s ===", wave, spec["nome"])

        for year in years:
            log.info("--- ano %d ---", year)
            sessions = self.client.sessions(year)
            sessions.sort(key=lambda s: s.get("date_start") or "")

            if spec.get("only_races"):
                sessions = [s for s in sessions if s.get("session_name") in config.RACE_SESSION_NAMES]
            elif spec.get("skip_races"):
                sessions = [s for s in sessions if s.get("session_name") not in config.RACE_SESSION_NAMES]

            if limit_sessions:
                sessions = sessions[:limit_sessions]

            for endpoint in spec["endpoints"]:
                name, scope = endpoint["name"], endpoint["scope"]

                if scope == "year":
                    self.ingest_one(name, year, {"year": year})
                    continue

                for index, session in enumerate(sessions, start=1):
                    session_key = session["session_key"]
                    log.info(
                        "[%s] sessao %d/%d - %s / %s",
                        name, index, len(sessions),
                        session.get("location"), session.get("session_name"),
                    )

                    if scope == "session":
                        self.ingest_one(name, year, {"session_key": session_key}, session_key)
                    elif scope == "session_driver":
                        self._ingest_by_driver(name, year, session)


    def _ingest_by_driver(self, endpoint: str, year: int, session: dict[str, Any]) -> None:
        """Telemetria: uma requisicao por piloto, com janela de tempo de reserva."""
        session_key = session["session_key"]
        try:
            drivers = self.client.drivers(session_key)
        except OpenF1Error as exc:
            self.stats["falhas"] += 1
            log.error("nao foi possivel listar pilotos da sessao %s: %s", session_key, exc)
            return

        numbers = sorted({d["driver_number"] for d in drivers if d.get("driver_number")})

        for number in numbers:
            self.ingest_one(
                endpoint,
                year,
                {"session_key": session_key, "driver_number": number},
                session_key=session_key,
                driver_number=number,
                date_start=session.get("date_start"),
                date_end=session.get("date_end"),
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
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(asctime)s %(levelname)-7s %(message)s",
        datefmt="%H:%M:%S",
    )

    run_id = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S") + "-" + uuid.uuid4().hex[:6]
    log.info("run_id=%s", run_id)

    ingestor = Ingestor(OpenF1Client(), build_storage(args.local), config.GCS_PREFIX, run_id)

    started = time.monotonic()
    try:
        ingestor.run_wave(args.wave, args.years, args.limit_sessions)
    except KeyboardInterrupt:
        log.warning("interrompido pelo usuario")
    finally:
        elapsed = time.monotonic() - started
        s = ingestor.stats
        log.info(
            "fim em %.1f min | gravados=%d pulados=%d falhas=%d | %s linhas | %.1f MB",
            elapsed / 60, s["gravados"], s["pulados"], s["falhas"],
            f"{s['linhas']:,}".replace(",", "."), s["bytes"] / 1024 / 1024,
        )


if __name__ == "__main__":
    main()
