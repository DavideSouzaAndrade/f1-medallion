"""Leitura das mensagens brutas da OpenF1 (contrato v2), sem rede.

Este modulo e o lado do Dataflow do contrato descrito em
docs/contrato-openf1.md: recebe o corpo e os atributos de uma mensagem do
Pub/Sub e devolve um registro tipado, ou levanta MensagemInvalida com o
motivo que vai para f1_stream.voltas_dlq.

Ele NAO monta a volta -- isso e do montagem.py. Aqui so se decide se a
mensagem respeita o contrato e se tipa o que ela traz.

Funcoes puras de proposito: a regra se testa offline, em segundos, contra o
proprio producer/cronograma.py (test_montagem.py).
"""

from __future__ import annotations

import json
import re
from datetime import datetime, timezone
from typing import Any

# --- os sete topicos ---------------------------------------------------------
#
# Os que a silver_voltas precisa. O atributo `topico` da mensagem vem com o
# prefixo do endpoint ("v1/laps"); qualquer outro valor e mensagem fora do
# contrato, e e exatamente o que o gerador injeta como topico_desconhecido.
PREFIXO = "v1/"

TOPICOS = ("meetings", "sessions", "weather", "stints", "pit", "intervals", "laps")

# --- motivos de rejeicao -----------------------------------------------------
#
# Texto estavel: a consulta de diagnostico e o painel de saude do BI agrupam
# a DLQ por este campo, e uma variacao de redacao viraria uma categoria a
# mais. Os quatro primeiros correspondem aos tipos que o gerador injeta.
MOTIVO_JSON = "JSON invalido"
MOTIVO_AUSENTE = "campo obrigatorio ausente"
MOTIVO_TIPO = "tipo errado"
MOTIVO_TOPICO = "topico fora do contrato"
MOTIVO_SEM_CONTEXTO = "volta sem contexto da sessao"
MOTIVO_BIGQUERY = "rejeitado pelo BigQuery"


class MensagemInvalida(Exception):
    """Mensagem que nao respeita o contrato. A mensagem e o motivo da DLQ."""


# --- o que cada topico precisa trazer ----------------------------------------
#
# Sem um destes campos nao ha como usar o registro: ele nao casa com volta
# nenhuma. E o que o gerador remove em sem_chave.
OBRIGATORIOS: dict[str, tuple[str, ...]] = {
    "laps": ("session_key", "driver_number", "lap_number"),
    "stints": ("session_key", "driver_number", "stint_number", "lap_start", "lap_end"),
    "pit": ("session_key", "driver_number", "lap_number"),
    "intervals": ("session_key", "driver_number", "date"),
    "weather": ("session_key", "date"),
    "sessions": ("session_key", "year", "circuit_short_name"),
    # meetings nao tem session_key: e do nivel do fim de semana. A chave de
    # particionamento vem do atributo da mensagem, nao do corpo.
    "meetings": ("meeting_key",),
}

# Campo do JSON -> (nome no registro, conversor). Campos fora desta lista
# sao ignorados: o corpo bruto traz telemetria de segmento e velocidade que
# a silver_voltas nao usa.
_INT = "int"
_FLOAT = "float"
_BOOL = "bool"
_TEXTO = "texto"
_DATA = "data"
_GAP = "gap"

CAMPOS: dict[str, dict[str, tuple[str, str]]] = {
    "laps": {
        "session_key": ("session_key", _INT),
        "driver_number": ("driver_number", _INT),
        "lap_number": ("lap_number", _INT),
        "date_start": ("data_inicio", _DATA),
        "lap_duration": ("duracao_s", _FLOAT),
        "duration_sector_1": ("setor_1_s", _FLOAT),
        "duration_sector_2": ("setor_2_s", _FLOAT),
        "duration_sector_3": ("setor_3_s", _FLOAT),
        "is_pit_out_lap": ("eh_saida_box", _BOOL),
    },
    "stints": {
        "session_key": ("session_key", _INT),
        "driver_number": ("driver_number", _INT),
        "stint_number": ("stint_number", _INT),
        "lap_start": ("lap_start", _INT),
        "lap_end": ("lap_end", _INT),
        "compound": ("composto", _TEXTO),
        "tyre_age_at_start": ("idade_pneu_inicio", _INT),
    },
    "pit": {
        "session_key": ("session_key", _INT),
        "driver_number": ("driver_number", _INT),
        "lap_number": ("lap_number", _INT),
        "pit_duration": ("duracao_box_s", _FLOAT),
    },
    "intervals": {
        "session_key": ("session_key", _INT),
        "driver_number": ("driver_number", _INT),
        "date": ("data", _DATA),
        "interval": ("gap_frente_s", _GAP),
        "gap_to_leader": ("gap_lider_s", _GAP),
    },
    "weather": {
        "session_key": ("session_key", _INT),
        "date": ("data", _DATA),
        "air_temperature": ("temp_ar", _FLOAT),
        "track_temperature": ("temp_pista", _FLOAT),
        "humidity": ("umidade", _FLOAT),
        "rainfall": ("chuva", _INT),
    },
    "sessions": {
        "session_key": ("session_key", _INT),
        "year": ("year", _INT),
        "circuit_short_name": ("circuito", _TEXTO),
    },
    "meetings": {
        "meeting_key": ("meeting_key", _INT),
        "circuit_type": ("circuit_type", _TEXTO),
    },
}

# "+1 LAP" / "+2 LAPS" em campo de segundos: 1.237 ocorrencias em
# gap_to_leader numa corrida. Nao e tempo, e distancia em voltas.
_VOLTAS_ATRAS = re.compile(r"^\+(\d+)\s+LAPS?$", re.IGNORECASE)


# --- conversoes --------------------------------------------------------------


def _int(valor: Any) -> int:
    # bool e subclasse de int em Python: sem esta guarda, um True no lugar de
    # lap_number viraria 1 e passaria despercebido.
    if isinstance(valor, bool):
        raise MensagemInvalida(MOTIVO_TIPO)
    if isinstance(valor, int):
        return valor
    # O JSON pode trazer 23.0 para um inteiro; 23.5 e outra coisa.
    if isinstance(valor, float) and valor.is_integer():
        return int(valor)
    raise MensagemInvalida(MOTIVO_TIPO)


def _float(valor: Any) -> float:
    if isinstance(valor, bool):
        raise MensagemInvalida(MOTIVO_TIPO)
    if isinstance(valor, (int, float)):
        return float(valor)
    raise MensagemInvalida(MOTIVO_TIPO)


def _bool(valor: Any) -> bool:
    if isinstance(valor, bool):
        return valor
    raise MensagemInvalida(MOTIVO_TIPO)


def _texto(valor: Any) -> str:
    if isinstance(valor, str):
        return valor.strip()
    raise MensagemInvalida(MOTIVO_TIPO)


def _data(valor: Any) -> datetime:
    """ISO 8601 da OpenF1 para datetime com fuso.

    fromisoformat so passou a aceitar o sufixo Z no Python 3.11; trocar por
    +00:00 funciona em qualquer versao.
    """
    if not isinstance(valor, str):
        raise MensagemInvalida(MOTIVO_TIPO)
    texto = valor.strip()
    if texto.endswith("Z"):
        texto = texto[:-1] + "+00:00"
    try:
        instante = datetime.fromisoformat(texto)
    except ValueError:
        raise MensagemInvalida(MOTIVO_TIPO) from None
    # Sem fuso explicito, UTC: e o que a OpenF1 entrega.
    if instante.tzinfo is None:
        instante = instante.replace(tzinfo=timezone.utc)
    return instante.astimezone(timezone.utc)


def gap(valor: Any) -> tuple[float | None, int | None]:
    """Campo de gap para (segundos, voltas_atras).

    O dado bruto mistura numero e texto no mesmo campo. "+1 LAP" nao e
    tempo: o gap em segundos fica nulo e a distancia vira voltas_atras, como
    na silver_voltas.

    Texto em outro formato devolve (None, None) em vez de rejeitar a
    mensagem: perder uma amostra de gap custa uma casa decimal na media da
    volta, enquanto mandar o registro para a DLQ custaria a volta inteira. O
    tipo errado que o gerador injeta e em driver_number/session_key, campos
    de chave, onde a rejeicao e a resposta certa.
    """
    if valor is None:
        return (None, None)
    if isinstance(valor, bool):
        raise MensagemInvalida(MOTIVO_TIPO)
    if isinstance(valor, (int, float)):
        return (float(valor), None)
    if isinstance(valor, str):
        achado = _VOLTAS_ATRAS.match(valor.strip())
        return (None, int(achado.group(1))) if achado else (None, None)
    raise MensagemInvalida(MOTIVO_TIPO)


_CONVERSORES = {_INT: _int, _FLOAT: _float, _BOOL: _bool, _TEXTO: _texto, _DATA: _data}


# --- leitura da mensagem -----------------------------------------------------


def topico_de(atributos: dict[str, str]) -> str:
    """O atributo `topico` ("v1/laps") para o nome do endpoint ("laps")."""
    bruto = atributos.get("topico") or ""
    if not bruto.startswith(PREFIXO):
        raise MensagemInvalida(MOTIVO_TOPICO)
    nome = bruto[len(PREFIXO):]
    if nome not in TOPICOS:
        raise MensagemInvalida(MOTIVO_TOPICO)
    return nome


# O que o atributo `fonte` pode valer, segundo o contrato v2.
FONTE_REPLAY = "replay"
FONTE_AO_VIVO = "openf1"


def fonte_de(atributos: dict[str, str]) -> str:
    """De onde a mensagem veio: replay de corrida guardada, ou feed ao vivo.

    Procedencia e propriedade da linha, nao do transporte. O contrato v2
    carrega `fonte` em toda mensagem e o pipeline a descartava -- entao o
    BigQuery guardava o dado e perdia o contexto dele, e quem consome nao
    tinha como distinguir uma corrida reproduzida de uma acontecendo.

    Nao confundir com "esta ao vivo agora": isso e temporal, muda sozinho e
    se deriva de quando o ultimo evento chegou. Um replay encerrado continua
    sendo um replay; ele so deixa de estar em curso.
    """
    bruto = (atributos.get("fonte") or "").strip().lower()
    return bruto if bruto in (FONTE_REPLAY, FONTE_AO_VIVO) else FONTE_AO_VIVO


def chave_da_sessao(atributos: dict[str, str]) -> int:
    """A session_key do atributo, que e a chave de particionamento.

    Vem do atributo e nao do corpo porque meetings e do nivel do fim de
    semana e nao tem session_key no JSON. O gerador poe o atributo em toda
    mensagem, e uma ponte MQTT tambem poria: ele e o tema do topico.
    """
    try:
        return int(atributos["session_key"])
    except (KeyError, TypeError, ValueError):
        raise MensagemInvalida(MOTIVO_AUSENTE) from None


def decodificar(corpo: bytes) -> dict[str, Any]:
    """Corpo cru para dicionario. O json_quebrado do gerador morre aqui."""
    try:
        doc = json.loads(corpo.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError):
        raise MensagemInvalida(MOTIVO_JSON) from None
    # "123" e um JSON valido que nao e um registro.
    if not isinstance(doc, dict):
        raise MensagemInvalida(MOTIVO_JSON)
    return doc


def ler(topico: str, doc: dict[str, Any]) -> dict[str, Any]:
    """Registro bruto da OpenF1 para registro tipado.

    Levanta MensagemInvalida no primeiro problema. A ordem e a do contrato:
    campo ausente antes de tipo errado.
    """
    for campo in OBRIGATORIOS[topico]:
        if doc.get(campo) is None:
            raise MensagemInvalida(MOTIVO_AUSENTE)

    registro: dict[str, Any] = {"_topico": topico}

    # _id ordena as versoes do mesmo registro. Ausente (uma ponte que nao o
    # repasse) vale zero: a primeira versao vista ganha.
    bruto_id = doc.get("_id")
    registro["_id"] = _int(bruto_id) if bruto_id is not None else 0
    registro["_key"] = doc.get("_key")

    for origem, (destino, tipo) in CAMPOS[topico].items():
        valor = doc.get(origem)
        if tipo == _GAP:
            segundos, voltas = gap(valor)
            registro[destino] = segundos
            if voltas is not None:
                registro["voltas_atras"] = voltas
            continue
        registro[destino] = None if valor is None else _CONVERSORES[tipo](valor)

    return registro
