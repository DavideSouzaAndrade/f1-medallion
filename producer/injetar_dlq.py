"""Publica, na hora, mensagens quebradas para a dead-letter do Dataflow.

Para a demonstracao: com o pipeline ligado, roda-se este script e, em
segundos, as mensagens aparecem em f1_stream.voltas_dlq, cada uma com o
motivo, e no grafico "Rejeitados por motivo" da tela Saude do BI.

Cada mensagem e uma volta real (a do exemplo de docs/contrato-openf1.md:
piloto 44, volta 23, GP da Italia de 2026) quebrada de um dos quatro jeitos
do contrato. Os defeitos sao os mesmos do --injetar-invalidos do
publisher.py (corrupcao.py); a diferenca e que aqui eles saem quando se
pede, um de cada, em vez de sorteados no meio da corrida.

Exemplos:
    python injetar_dlq.py                        # um de cada tipo
    python injetar_dlq.py --tipo json_quebrado   # so um
    python injetar_dlq.py --dry-run              # imprime sem publicar
"""

from __future__ import annotations

import argparse
import sys
from datetime import datetime, timezone
from typing import Callable

from corrupcao import TIPOS_INVALIDOS, corromper
from cronograma import Mensagem

# A volta do exemplo do contrato, como esta na Bronze.
VOLTA_EXEMPLO = {
    "meeting_key": 1293, "session_key": 11361, "driver_number": 44, "lap_number": 23,
    "date_start": "2026-09-06T14:10:15.650000+00:00",
    "duration_sector_1": 27.485, "duration_sector_2": 29.499, "duration_sector_3": 28.316,
    "i1_speed": 300, "i2_speed": 301, "is_pit_out_lap": False, "lap_duration": 85.3,
    "segments_sector_1": [None, 2048, 2048, 2048, 2048, 2048],
    "segments_sector_2": [2048, 2048, 2048, 2049, 2048, 2048, 2048],
    "segments_sector_3": [2048, 2048, 2048, 2048, 2048, 2048, 2048, 2048, 2048],
    "st_speed": 327,
}
CHAVE_EXEMPLO = "11361_23_44"

# O que mostrar depois, no BigQuery.
CONSULTA = """SELECT _ts_processamento, motivo, evento_bruto
FROM `pdm-davi-2026.f1_stream.voltas_dlq`
WHERE _ts_processamento > TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 10 MINUTE)
ORDER BY _ts_processamento DESC"""


def mensagens_quebradas(tipos: list[str], proximo_id: Callable[[], int]) -> list[tuple[str, bytes, str]]:
    """(tipo, corpo, atributo topico) de cada copia quebrada da volta do exemplo."""
    volta = Mensagem(instante=datetime.now(timezone.utc), topico="laps",
                     corpo=VOLTA_EXEMPLO, chave=CHAVE_EXEMPLO)
    return [(tipo, *corromper(volta, proximo_id(), tipo)) for tipo in tipos]


def main() -> None:
    parser = argparse.ArgumentParser(description="Mensagens quebradas para a DLQ, sob demanda")
    parser.add_argument("--tipo", choices=[*TIPOS_INVALIDOS, "todos"], default="todos",
                        help="qual defeito publicar (padrao: um de cada)")
    parser.add_argument("--dry-run", action="store_true", help="imprime as mensagens em vez de publicar")
    args = parser.parse_args()

    # Import tardio: o publisher traz o cliente do BigQuery e do Pub/Sub, e
    # mensagens_quebradas() precisa rodar offline no teste.
    from publisher import GeradorDeId, Publicador
    import config

    tipos = list(TIPOS_INVALIDOS) if args.tipo == "todos" else [args.tipo]
    replay_id = "demo-dlq-" + datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S")
    publicador = Publicador(args.dry_run, replay_id)
    for tipo, corpo, topico in mensagens_quebradas(tipos, GeradorDeId()):
        publicador.publicar(corpo, topico, VOLTA_EXEMPLO["session_key"], injetado=tipo)
    publicador.aguardar()

    if args.dry_run:
        return
    if publicador.falhas:
        sys.exit(f"{publicador.falhas} mensagem(ns) nao foram publicadas.")
    print(f"{len(tipos)} mensagem(ns) quebrada(s) publicada(s) em {config.TOPICO}: {', '.join(tipos)}")
    print("Com o Dataflow ligado, elas chegam a f1_stream.voltas_dlq em segundos. Para ver:\n")
    print(CONSULTA)


if __name__ == "__main__":
    main()
