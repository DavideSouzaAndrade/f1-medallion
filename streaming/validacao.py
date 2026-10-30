"""Validacao e normalizacao do evento de volta, sem rede.

Este modulo e o lado do Dataflow do contrato v1: o producer/evento.py monta
o evento, este aqui o recebe e decide se ele entra em f1_stream.voltas ou em
f1_stream.voltas_dlq. Mudou o contrato, mudam os dois.

Funcoes puras de proposito: o pipeline Beam so as chama. Assim a regra de
negocio se testa offline, em segundos, sem subir worker nem tocar na nuvem
(test_validacao.py).
"""

from __future__ import annotations

import json
from datetime import datetime, timezone
from typing import Any

# --- o contrato v1, campo a campo --------------------------------------------
#
# Os NOT NULL do schema em definitions/stream/voltas.sqlx. Um evento sem um
# destes nao tem como ser gravado: vai para a DLQ.
CAMPOS_OBRIGATORIOS = (
    "evento_id", "replay_id", "ts_evento",
    "session_key", "driver_number", "lap_number", "year",
    "circuito", "circuit_type",
    "eh_saida_box", "eh_entrada_box",
)

CAMPOS_INT = (
    "session_key", "driver_number", "lap_number", "year",
    "stint_number", "idade_pneu", "volta_no_stint", "chuva",
)

CAMPOS_FLOAT = (
    "duracao_s", "setor_1_s", "setor_2_s", "setor_3_s",
    "gap_medio_a_frente", "temp_ar", "temp_pista", "umidade",
)

CAMPOS_BOOL = ("eh_saida_box", "eh_entrada_box")

CAMPOS_TEXTO = ("evento_id", "replay_id", "circuito", "circuit_type", "composto")

CAMPOS_TIMESTAMP = ("ts_evento", "data_inicio")

COMPOSTOS_VALIDOS = frozenset(
    {"SOFT", "MEDIUM", "HARD", "INTERMEDIATE", "WET", "UNKNOWN", "TEST_UNKNOWN"}
)

# So estes tres rendem previsao de degradacao: os de chuva tem outra fisica,
# e UNKNOWN/TEST_UNKNOWN nao identificam o pneu. Mesmo recorte da
# gold_degradacao_pneu.
COMPOSTOS_SECOS = frozenset({"SOFT", "MEDIUM", "HARD"})

# Ordem das colunas de f1_stream.voltas, menos as duas da parte 3
# (tempo_previsto_s e modelo_versao), que continuam nulas aqui.
CAMPOS_DA_TABELA = (
    "evento_id", "replay_id", "ts_evento",
    "session_key", "driver_number", "lap_number", "year",
    "circuito", "circuit_type",
    "data_inicio", "duracao_s", "setor_1_s", "setor_2_s", "setor_3_s",
    "composto", "stint_number", "idade_pneu", "volta_no_stint",
    "eh_saida_box", "eh_entrada_box",
    "gap_medio_a_frente", "temp_ar", "temp_pista", "umidade", "chuva",
    "apto_modelo", "_ts_processamento", "latencia_ms",
)

# --- motivos de rejeicao -----------------------------------------------------
#
# Texto identico ao documentado na coluna motivo de
# definitions/stream/voltas_dlq.sqlx: a consulta de diagnostico agrupa por
# este campo, e uma variacao de redacao viraria uma categoria a mais.
MOTIVO_JSON = "JSON invalido"
MOTIVO_AUSENTE = "campo obrigatorio ausente"
MOTIVO_TIPO = "tipo errado"
MOTIVO_VOLTA = "lap_number <= 0"
MOTIVO_DURACAO = "duracao_s <= 0"
MOTIVO_COMPOSTO = "composto fora da lista"
MOTIVO_BIGQUERY = "rejeitado pelo BigQuery"


class EventoInvalido(Exception):
    """Evento que nao respeita o contrato. A mensagem e o motivo da DLQ."""


# --- conversoes --------------------------------------------------------------


def _para_int(valor: Any, campo: str) -> int:
    # bool e subclasse de int em Python: sem esta guarda, True viraria 1 e um
    # eh_saida_box no lugar de lap_number passaria despercebido.
    if isinstance(valor, bool):
        raise EventoInvalido(MOTIVO_TIPO)
    if isinstance(valor, int):
        return valor
    # O JSON pode trazer 23.0 para um inteiro; 23.5 e outra coisa.
    if isinstance(valor, float) and valor.is_integer():
        return int(valor)
    raise EventoInvalido(MOTIVO_TIPO)


def _para_float(valor: Any, campo: str) -> float:
    if isinstance(valor, bool):
        raise EventoInvalido(MOTIVO_TIPO)
    if isinstance(valor, (int, float)):
        return float(valor)
    raise EventoInvalido(MOTIVO_TIPO)


def _para_bool(valor: Any, campo: str) -> bool:
    if isinstance(valor, bool):
        return valor
    raise EventoInvalido(MOTIVO_TIPO)


def _para_texto(valor: Any, campo: str) -> str:
    if isinstance(valor, str):
        return valor.strip()
    raise EventoInvalido(MOTIVO_TIPO)


def _para_timestamp(valor: Any, campo: str) -> datetime:
    """ISO 8601 do contrato para datetime com fuso.

    O gerador publica com sufixo Z; fromisoformat so passou a aceitar o Z no
    Python 3.11, e trocar por +00:00 funciona em qualquer versao.
    """
    if not isinstance(valor, str):
        raise EventoInvalido(MOTIVO_TIPO)
    texto = valor.strip()
    if texto.endswith("Z"):
        texto = texto[:-1] + "+00:00"
    try:
        instante = datetime.fromisoformat(texto)
    except ValueError:
        raise EventoInvalido(MOTIVO_TIPO) from None
    # Sem fuso explicito o contrato manda assumir UTC.
    if instante.tzinfo is None:
        instante = instante.replace(tzinfo=timezone.utc)
    return instante.astimezone(timezone.utc)


_CONVERSORES = (
    (CAMPOS_INT, _para_int),
    (CAMPOS_FLOAT, _para_float),
    (CAMPOS_BOOL, _para_bool),
    (CAMPOS_TEXTO, _para_texto),
    (CAMPOS_TIMESTAMP, _para_timestamp),
)


# --- regras ------------------------------------------------------------------


def eh_apto_para_modelo(evento: dict[str, Any]) -> bool:
    """A volta serve para o modelo de degradacao de pneu?

    Mesmo recorte da gold_degradacao_pneu. A volta inapta e gravada mesmo
    assim, apenas marcada: a filosofia da Silver e tratar sem filtrar, e
    quem consome decide.
    """
    if evento.get("composto") not in COMPOSTOS_SECOS:
        return False
    if any(evento.get(campo) is None for campo in ("duracao_s", "idade_pneu", "temp_pista")):
        return False
    return not (evento.get("eh_saida_box") or evento.get("eh_entrada_box"))


def decodificar(corpo: bytes) -> dict[str, Any]:
    """Corpo cru da mensagem para dicionario. Levanta EventoInvalido."""
    try:
        evento = json.loads(corpo.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError):
        raise EventoInvalido(MOTIVO_JSON) from None
    # "123" e um JSON valido que nao e um evento.
    if not isinstance(evento, dict):
        raise EventoInvalido(MOTIVO_JSON)
    return evento


def validar(evento: dict[str, Any], agora: datetime) -> dict[str, Any]:
    """Evento cru para linha de f1_stream.voltas.

    Levanta EventoInvalido com o motivo da DLQ na primeira regra violada. A
    ordem das checagens e a do contrato, e define qual motivo aparece quando
    o evento viola mais de uma.
    """
    # 1. obrigatorios presentes. None conta como ausente: a coluna e NOT NULL.
    for campo in CAMPOS_OBRIGATORIOS:
        if evento.get(campo) is None:
            raise EventoInvalido(MOTIVO_AUSENTE)

    # 2. tipos, com normalizacao de texto junto.
    linha: dict[str, Any] = {}
    for campos, converter in _CONVERSORES:
        for campo in campos:
            valor = evento.get(campo)
            linha[campo] = None if valor is None else converter(valor, campo)

    # 3. composto em maiusculas antes de conferir a lista: " medium " e
    #    MEDIUM, mas SUPERSOFT segue invalido.
    if linha["composto"] is not None:
        linha["composto"] = linha["composto"].upper()

    # 4. faixas de valor.
    if linha["lap_number"] <= 0:
        raise EventoInvalido(MOTIVO_VOLTA)
    if linha["duracao_s"] is not None and linha["duracao_s"] <= 0:
        raise EventoInvalido(MOTIVO_DURACAO)
    if linha["composto"] is not None and linha["composto"] not in COMPOSTOS_VALIDOS:
        raise EventoInvalido(MOTIVO_COMPOSTO)

    # 5. o que o Dataflow acrescenta.
    linha["apto_modelo"] = eh_apto_para_modelo(linha)
    linha["_ts_processamento"] = agora
    linha["latencia_ms"] = int(
        (agora - linha["ts_evento"]).total_seconds() * 1000
    )

    return {campo: linha.get(campo) for campo in CAMPOS_DA_TABELA}
