"""Montagem do evento JSON a partir de uma volta da Silver.

Este modulo e a implementacao do contrato v1 do evento: os nomes, os tipos
e quais campos podem ser nulos estao no doc do plano da parte 2 e no schema
de definitions/stream/voltas.sqlx. Mudou aqui, muda la.

Funcoes puras, sem rede: dao para testar offline (test_evento.py).
"""

from __future__ import annotations

import json
import math
import random
import uuid
from datetime import datetime, timezone
from typing import Any

# Campos copiados da volta como vieram da Silver, na ordem do contrato.
CAMPOS_DA_VOLTA = (
    "session_key", "driver_number", "lap_number", "year", "circuito", "circuit_type",
    "data_inicio", "duracao_s", "setor_1_s", "setor_2_s", "setor_3_s",
    "composto", "stint_number", "idade_pneu", "volta_no_stint",
    "eh_saida_box", "eh_entrada_box",
    "gap_medio_a_frente", "temp_ar", "temp_pista", "umidade", "chuva",
)


def _iso_utc(instante: datetime) -> str:
    """ISO 8601 em UTC com milissegundos e sufixo Z, como no contrato."""
    return instante.astimezone(timezone.utc).isoformat(timespec="milliseconds").replace("+00:00", "Z")


def _valor_json(valor: Any) -> Any:
    if isinstance(valor, datetime):
        return _iso_utc(valor)
    # NaN nao e JSON valido: json.dumps escreveria NaN e o parse do outro
    # lado falharia. Na Silver ele significa ausencia de medida.
    if isinstance(valor, float) and math.isnan(valor):
        return None
    return valor


def montar_evento(volta: dict[str, Any], replay_id: str, agora: datetime | None = None) -> dict[str, Any]:
    """Uma volta da Silver vira um evento do contrato v1."""
    evento: dict[str, Any] = {
        "evento_id": str(uuid.uuid4()),
        "replay_id": replay_id,
        "ts_evento": _iso_utc(agora or datetime.now(timezone.utc)),
    }
    for campo in CAMPOS_DA_VOLTA:
        evento[campo] = _valor_json(volta.get(campo))
    return evento


def serializar(evento: dict[str, Any]) -> bytes:
    return json.dumps(evento, ensure_ascii=False, allow_nan=False).encode("utf-8")


# --- eventos invalidos -------------------------------------------------------
#
# Publicados de proposito para demonstrar a limpeza do Dataflow: cada tipo
# viola uma regra do contrato e deve terminar em f1_stream.voltas_dlq com o
# motivo correspondente. Partem de um evento valido para que so a regra
# testada esteja quebrada.

TIPOS_INVALIDOS = (
    "json_quebrado",
    "sem_session_key",
    "tipo_errado",
    "volta_zero",
    "duracao_negativa",
    "composto_invalido",
)


def corromper(evento: dict[str, Any], tipo: str) -> bytes:
    """Devolve o corpo da mensagem com a regra `tipo` violada."""
    ruim = dict(evento, evento_id=str(uuid.uuid4()))
    if tipo == "json_quebrado":
        return serializar(ruim)[:-20]
    if tipo == "sem_session_key":
        del ruim["session_key"]
    elif tipo == "tipo_errado":
        ruim["lap_number"] = "vinte e tres"
    elif tipo == "volta_zero":
        ruim["lap_number"] = 0
    elif tipo == "duracao_negativa":
        ruim["duracao_s"] = -abs(ruim.get("duracao_s") or 90.0)
    elif tipo == "composto_invalido":
        ruim["composto"] = "SUPERSOFT"
    else:
        raise ValueError(f"tipo de evento invalido desconhecido: {tipo}")
    return serializar(ruim)


def sortear_invalido(sorteio: random.Random, fracao: float) -> str | None:
    """Com probabilidade `fracao`, escolhe um tipo de evento invalido."""
    if fracao <= 0 or sorteio.random() >= fracao:
        return None
    return sorteio.choice(TIPOS_INVALIDOS)
