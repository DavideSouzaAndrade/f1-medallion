"""Gerador de eventos: faz o replay de uma corrida real no Pub/Sub.

Le as voltas de uma corrida na silver_voltas e publica uma mensagem por
volta, na ordem em que as voltas terminaram, como se a corrida estivesse
acontecendo agora. Os intervalos entre eventos sao os da corrida real,
divididos pelo fator de aceleracao.

Com --injetar-invalidos, publica tambem eventos quebrados de proposito,
para demonstrar a limpeza e a dead-letter do Dataflow. Cada um leva o
atributo `injetado` com o tipo, o que permite conferir depois se todos
chegaram a DLQ.

Exemplos:
    python publisher.py --listar
    python publisher.py --session-key 11361 --limite 20 --dry-run
    python publisher.py                                  # ultima corrida, fator 30
    python publisher.py --session-key 11361 --fator 60 --injetar-invalidos 0.05
    python publisher.py --intervalo 1                    # 1 evento por segundo
"""

from __future__ import annotations

import argparse
import logging
import random
import sys
import time
import uuid
from concurrent import futures
from datetime import datetime, timezone
from typing import Any

import config
from evento import corromper, montar_evento, serializar, sortear_invalido
from fonte import FonteSilver

log = logging.getLogger("gerador")


def _momento(volta: dict[str, Any]) -> datetime | None:
    return volta.get("data_fim") or volta.get("data_inicio")


def _espera(anterior: dict[str, Any] | None, atual: dict[str, Any], fator: float, intervalo: float | None) -> float:
    """Quanto dormir antes de publicar `atual`."""
    if intervalo is not None:
        return intervalo
    if anterior is None:
        return 0.0
    t0, t1 = _momento(anterior), _momento(atual)
    if t0 is None or t1 is None:
        return 0.0
    real = max((t1 - t0).total_seconds(), 0.0)
    return min(real / fator, config.PAUSA_MAXIMA_S)


class Gerador:
    def __init__(self, dry_run: bool) -> None:
        self.dry_run = dry_run
        self.publicador = None
        self.topico = None
        if not dry_run:
            # Import tardio: o --dry-run roda sem a biblioteca do Pub/Sub.
            import google.auth
            from google.auth.transport.requests import Request
            from google.cloud import pubsub_v1

            # O publish envia lotes em varias threads. Com a credencial ainda
            # sem token, todas pedem um ao mesmo tempo na partida e estouram o
            # pool de conexoes HTTP ("Connection pool is full"). Buscar o
            # token uma vez antes evita a corrida.
            credenciais, _ = google.auth.default()
            credenciais.refresh(Request())
            self.publicador = pubsub_v1.PublisherClient(credentials=credenciais)
            self.topico = self.publicador.topic_path(config.PROJECT_ID, config.TOPICO)
        self.pendentes: list[futures.Future] = []
        self.stats = {"validos": 0, "invalidos": 0, "falhas": 0}

    def publicar(self, corpo: bytes, **atributos: str) -> None:
        if self.dry_run:
            print(corpo.decode("utf-8"), atributos or "")
            return
        futuro = self.publicador.publish(
            self.topico, corpo, schema_version=config.SCHEMA_VERSION, **atributos
        )
        futuro.add_done_callback(self._conferir)
        self.pendentes.append(futuro)

    def _conferir(self, futuro: futures.Future) -> None:
        # O publish e assincrono e agrupa mensagens em lote; uma falha so
        # aparece aqui. Sem este callback ela passaria em silencio.
        try:
            futuro.result()
        except Exception as exc:  # noqa: BLE001 -- qualquer erro de envio conta como falha
            self.stats["falhas"] += 1
            log.error("falha ao publicar: %s", exc)

    def aguardar(self) -> None:
        if self.pendentes:
            futures.wait(self.pendentes, timeout=60)


def listar(fonte: FonteSilver) -> None:
    print(f"{'session_key':>11}  {'data':10}  {'voltas':>6}  evento")
    for c in fonte.corridas(limite=15):
        print(f"{c['session_key']:>11}  {c['data']!s:10}  {c['voltas']:>6}  {c['nome_evento']} ({c['circuito']})")


def main() -> None:
    parser = argparse.ArgumentParser(description="Replay de uma corrida da Silver no Pub/Sub")
    parser.add_argument("--session-key", type=int, help="corrida a reproduzir; padrao: a mais recente")
    parser.add_argument("--listar", action="store_true", help="lista as corridas disponiveis e sai")
    parser.add_argument("--fator", type=float, default=config.FATOR_PADRAO,
                        help="aceleracao em relacao a corrida real (padrao: %(default)s)")
    parser.add_argument("--intervalo", type=float, metavar="S",
                        help="ignora o ritmo real e publica um evento a cada S segundos")
    parser.add_argument("--limite", type=int, help="publica so as primeiras N voltas")
    parser.add_argument("--injetar-invalidos", type=float, default=0.0, metavar="FRACAO",
                        help="fracao de eventos invalidos extras, de 0 a 1 (ex.: 0.05)")
    parser.add_argument("--semente", type=int, help="semente do sorteio dos invalidos, para repetir uma demo")
    parser.add_argument("--dry-run", action="store_true", help="imprime os eventos em vez de publicar")
    parser.add_argument("--verbose", action="store_true")
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(asctime)s %(levelname)-7s %(message)s",
        datefmt="%H:%M:%S",
    )

    if args.fator <= 0:
        sys.exit("--fator precisa ser maior que zero.")
    if not 0 <= args.injetar_invalidos <= 1:
        sys.exit("--injetar-invalidos e uma fracao entre 0 e 1.")

    fonte = FonteSilver()
    if args.listar:
        listar(fonte)
        return

    session_key = args.session_key
    if session_key is None:
        session_key = fonte.corridas(limite=1)[0]["session_key"]
    voltas = fonte.voltas(session_key)
    if not voltas:
        sys.exit(f"Nenhuma volta na Silver para a sessao {session_key}. Veja --listar.")
    if args.limite:
        voltas = voltas[: args.limite]

    replay_id = "replay-" + datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S") + "-" + uuid.uuid4().hex[:6]
    destino = "stdout (dry-run)" if args.dry_run else f"projects/{config.PROJECT_ID}/topics/{config.TOPICO}"
    ritmo = f"1 evento a cada {args.intervalo}s" if args.intervalo is not None else f"fator {args.fator:g}"
    log.info("replay_id=%s", replay_id)
    log.info("sessao %s: %d voltas, %s, para %s", session_key, len(voltas), ritmo, destino)

    gerador = Gerador(args.dry_run)
    sorteio = random.Random(args.semente)
    espera = 0.0 if args.dry_run else None
    anterior = None
    started = time.monotonic()
    try:
        for i, volta in enumerate(voltas, start=1):
            pausa = espera if espera is not None else _espera(anterior, volta, args.fator, args.intervalo)
            if pausa > 0:
                time.sleep(pausa)
            anterior = volta

            evento = montar_evento(volta, replay_id)
            gerador.publicar(serializar(evento))
            gerador.stats["validos"] += 1

            tipo = sortear_invalido(sorteio, args.injetar_invalidos)
            if tipo:
                gerador.publicar(corromper(evento, tipo), injetado=tipo)
                gerador.stats["invalidos"] += 1
                log.debug("invalido injetado: %s", tipo)

            if i % 50 == 0 or i == len(voltas):
                log.info("%d/%d | volta %s do piloto %s", i, len(voltas), volta["lap_number"], volta["driver_number"])
    except KeyboardInterrupt:
        log.warning("interrompido pelo usuario")
    finally:
        gerador.aguardar()
        s = gerador.stats
        log.info(
            "fim em %.1f s | validos=%d invalidos=%d falhas=%d",
            time.monotonic() - started, s["validos"], s["invalidos"], s["falhas"],
        )
        if s["falhas"]:
            sys.exit(1)


if __name__ == "__main__":
    main()
