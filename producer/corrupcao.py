"""Mensagens quebradas de proposito, para demonstrar a dead-letter do Dataflow.

O dado bruto ja traz defeitos reais ("+1 LAP" em campo numerico, campos
nulos, versoes repetidas), e esses o Dataflow trata como dado normal. Os
defeitos daqui sao os que nenhuma mensagem da OpenF1 traria; servem para
mostrar o que acontece com uma mensagem que nao da para aproveitar. Cada um
corresponde a um motivo de DLQ do contrato (docs/contrato-openf1.md).

A mensagem corrompida e uma copia extra: a original tambem e publicada, e a
corrida continua completa.
"""

from __future__ import annotations

import json
import random

from cronograma import Mensagem

TIPOS_INVALIDOS = ("json_quebrado", "sem_chave", "tipo_errado", "topico_desconhecido")

TOPICO_DESCONHECIDO = "v1/inexistente"


def sortear_invalido(sorteio: random.Random, fracao: float) -> str | None:
    if fracao <= 0 or sorteio.random() >= fracao:
        return None
    return sorteio.choice(TIPOS_INVALIDOS)


def corromper(msg: Mensagem, id_: int, tipo: str) -> tuple[bytes, str]:
    """Devolve (corpo, atributo topico) da copia quebrada."""
    topico = f"v1/{msg.topico}"
    doc = json.loads(msg.serializar(id_))

    if tipo == "json_quebrado":
        corpo = msg.serializar(id_)
        return corpo[: len(corpo) // 2], topico
    if tipo == "sem_chave":
        doc.pop("session_key", None)
    elif tipo == "tipo_errado":
        # Texto onde a OpenF1 sempre manda numero. Nao usa o formato "+1 LAP",
        # que e um texto legitimo da origem.
        campo = "driver_number" if "driver_number" in doc else "session_key"
        doc[campo] = "N/D"
    elif tipo == "topico_desconhecido":
        topico = TOPICO_DESCONHECIDO
    else:
        raise ValueError(f"tipo de defeito desconhecido: {tipo}")
    return json.dumps(doc, ensure_ascii=False, separators=(",", ":")).encode("utf-8"), topico
