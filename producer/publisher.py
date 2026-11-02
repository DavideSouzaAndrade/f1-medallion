"""Gerador de eventos: faz o replay de uma corrida real no Pub/Sub.

Le os registros brutos de uma corrida na Bronze e publica cada um como a
OpenF1 publicaria ao vivo (contrato em docs/contrato-openf1.md): mesmo JSON,
com _key e _id, um tipo de registro por atributo `topico`, e laps e stints
em varias versoes conforme sao preenchidos. Os intervalos entre mensagens
sao os da corrida real, divididos pelo fator de aceleracao.

Trocar este gerador por uma ponte que assina o MQTT da OpenF1 e repassa as
mensagens para o mesmo topico nao muda nada para quem consome.

Com --injetar-invalidos, publica tambem copias quebradas de proposito, para
demonstrar a dead-letter do Dataflow. Cada uma leva o atributo `injetado`
com o tipo do defeito.

Exemplos:
    python publisher.py --listar
    python publisher.py --session-key 11361 --minutos 2 --dry-run
    python publisher.py                                  # ultima corrida, fator 30
    python publisher.py --session-key 11361 --fator 60 --injetar-invalidos 0.01
"""

from __future__ import annotations

import argparse
import logging
import random
import sys
import time
import uuid
from collections import Counter
from concurrent import futures
from datetime import datetime, timedelta, timezone

import config
from corrupcao import corromper, sortear_invalido
from cronograma import Mensagem, montar
from fonte import FonteBronze

log = logging.getLogger("gerador")


class RelogioDoReplay:
    """Diz quando publicar cada mensagem, no ritmo da corrida acelerada.

    O alvo e calculado desde o inicio do replay, e nao como uma pausa entre
    duas mensagens: com ~27 mil mensagens, pequenos atrasos de cada sleep
    se somariam e o replay iria escorregando em relacao a corrida.
    """

    def __init__(self, fator: float, pausa_maxima: float) -> None:
        self.fator = fator
        self.pausa_maxima = pausa_maxima
        self.partida = time.monotonic()
        self.alvo = 0.0
        self.anterior: datetime | None = None

    def esperar(self, instante: datetime) -> None:
        if self.anterior is not None:
            real = max((instante - self.anterior).total_seconds(), 0.0)
            self.alvo += min(real / self.fator, self.pausa_maxima)
        self.anterior = instante
        falta = self.partida + self.alvo - time.monotonic()
        if falta > 0:
            time.sleep(falta)


class GeradorDeId:
    """O _id da OpenF1: milissegundos do relogio, forcados a sempre crescer."""

    def __init__(self) -> None:
        self.ultimo = 0

    def __call__(self) -> int:
        self.ultimo = max(time.time_ns() // 1_000_000, self.ultimo + 1)
        return self.ultimo


class Publicador:
    def __init__(self, dry_run: bool, replay_id: str) -> None:
        self.dry_run = dry_run
        self.replay_id = replay_id
        self.cliente = None
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
            self.cliente = pubsub_v1.PublisherClient(credentials=credenciais)
            self.topico = self.cliente.topic_path(config.PROJECT_ID, config.TOPICO)
        self.pendentes: list[futures.Future] = []
        self.falhas = 0

    def publicar(self, corpo: bytes, topico: str, session_key: int, **extras: str) -> None:
        atributos = {
            "topico": topico,
            "session_key": str(session_key),
            "fonte": "replay",
            "replay_id": self.replay_id,
            **extras,
        }
        if self.dry_run:
            print(atributos["topico"], corpo.decode("utf-8", errors="replace"),
                  {k: v for k, v in extras.items()} or "")
            return
        futuro = self.cliente.publish(self.topico, corpo, **atributos)
        futuro.add_done_callback(self._conferir)
        self.pendentes.append(futuro)

    def _conferir(self, futuro: futures.Future) -> None:
        # O publish e assincrono e agrupa mensagens em lote; uma falha so
        # aparece aqui. Sem este callback ela passaria em silencio.
        try:
            futuro.result()
        except Exception as exc:  # noqa: BLE001 -- qualquer erro de envio conta como falha
            self.falhas += 1
            log.error("falha ao publicar: %s", exc)

    def aguardar(self) -> None:
        if self.pendentes:
            futures.wait(self.pendentes, timeout=120)


def listar(fonte: FonteBronze) -> None:
    print(f"{'session_key':>11}  {'data':10}  {'voltas':>6}  evento")
    for c in fonte.corridas(limite=15):
        print(f"{c['session_key']:>11}  {c['data']!s:10}  {c['voltas']:>6}  {c['nome_evento']} ({c['circuito']})")


def _recortar(mensagens: list[Mensagem], largada: datetime, minutos: float | None) -> list[Mensagem]:
    if minutos is None:
        return mensagens
    limite = largada + timedelta(minutes=minutos)
    return [m for m in mensagens if m.instante <= limite]


def main() -> None:
    parser = argparse.ArgumentParser(description="Replay de uma corrida da Bronze no Pub/Sub, no formato da OpenF1")
    parser.add_argument("--session-key", type=int, help="corrida a reproduzir; padrao: a mais recente")
    parser.add_argument("--listar", action="store_true", help="lista as corridas disponiveis e sai")
    parser.add_argument("--fator", type=float, default=config.FATOR_PADRAO,
                        help="aceleracao em relacao a corrida real (padrao: %(default)s)")
    parser.add_argument("--minutos", type=float, metavar="N",
                        help="publica so os primeiros N minutos depois da largada")
    parser.add_argument("--injetar-invalidos", type=float, default=0.0, metavar="FRACAO",
                        help="fracao de copias quebradas extras, de 0 a 1 (ex.: 0.01)")
    parser.add_argument("--semente", type=int, help="semente do sorteio dos invalidos, para repetir uma demo")
    parser.add_argument("--dry-run", action="store_true", help="imprime as mensagens em vez de publicar, sem esperar")
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

    fonte = FonteBronze()
    if args.listar:
        listar(fonte)
        return

    session_key = args.session_key
    if session_key is None:
        session_key = fonte.corridas(limite=1)[0]["session_key"]
    registros = fonte.registros(session_key)
    if not registros["laps"]:
        sys.exit(f"Nenhuma volta na Bronze para a sessao {session_key}. Veja --listar.")

    cronograma = montar(registros)
    mensagens = _recortar(cronograma.mensagens, cronograma.largada, args.minutos)
    for topico, n in cronograma.descartados.items():
        log.warning("%d registro(s) de %s sem data ficaram fora do replay", n, topico)
    log.info("%d registros do pre-corrida ficaram fora (o replay comeca 1 min antes da largada)",
             cronograma.pre_corrida)

    replay_id = "replay-" + datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S") + "-" + uuid.uuid4().hex[:6]
    destino = "stdout (dry-run)" if args.dry_run else f"projects/{config.PROJECT_ID}/topics/{config.TOPICO}"
    duracao = (mensagens[-1].instante - mensagens[0].instante).total_seconds() / 60
    log.info("replay_id=%s", replay_id)
    log.info("sessao %s: %d mensagens, %.0f min de corrida, fator %g, para %s",
             session_key, len(mensagens), duracao, args.fator, destino)
    log.info("por topico: %s", dict(Counter(m.topico for m in mensagens)))

    publicador = Publicador(args.dry_run, replay_id)
    relogio = RelogioDoReplay(args.fator, config.PAUSA_MAXIMA_S)
    proximo_id = GeradorDeId()
    sorteio = random.Random(args.semente)
    invalidos: Counter = Counter()
    partida = time.monotonic()
    publicadas = 0
    try:
        for i, msg in enumerate(mensagens, start=1):
            if not args.dry_run:
                relogio.esperar(msg.instante)

            id_ = proximo_id()
            publicador.publicar(msg.serializar(id_), f"v1/{msg.topico}", session_key)
            publicadas += 1

            tipo = sortear_invalido(sorteio, args.injetar_invalidos)
            if tipo:
                corpo, topico = corromper(msg, proximo_id(), tipo)
                publicador.publicar(corpo, topico, session_key, injetado=tipo)
                invalidos[tipo] += 1

            if i % 2000 == 0 or i == len(mensagens):
                decorrido = (msg.instante - cronograma.largada).total_seconds() / 60
                log.info("%d/%d | %.0f min de corrida", i, len(mensagens), decorrido)
    except KeyboardInterrupt:
        log.warning("interrompido pelo usuario")
    finally:
        publicador.aguardar()
        log.info(
            "fim em %.1f s | publicadas=%d invalidas=%d %s falhas=%d",
            time.monotonic() - partida, publicadas, sum(invalidos.values()),
            dict(invalidos) if invalidos else "", publicador.falhas,
        )
        if publicador.falhas:
            sys.exit(1)


if __name__ == "__main__":
    main()
