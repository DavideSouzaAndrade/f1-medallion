"""Configuracao da ingestao.

Tudo que muda entre ambientes vem de variavel de ambiente, com default
util para rodar local. Nada de credencial neste arquivo.
"""

from __future__ import annotations

import os

# --- destino ---------------------------------------------------------------

GCS_BUCKET = os.environ.get("GCS_BUCKET", "f1-raw-trocar-aqui")
GCS_PREFIX = os.environ.get("GCS_PREFIX", "openf1")

# Diretorio local; usado apenas quando --local e passado (testes).
LOCAL_OUTPUT_DIR = os.environ.get("LOCAL_OUTPUT_DIR", "./data")

# --- escopo ----------------------------------------------------------------

YEARS = [int(y) for y in os.environ.get("YEARS", "2023,2024,2025").split(",")]

# Sessoes de corrida: usadas para priorizar a telemetria na onda 3.
RACE_SESSION_NAMES = {"Race", "Sprint"}


# --- ondas de ingestao -----------------------------------------------------
#
# A ordem importa. As ondas 1 e 2 sao rapidas (~1h no total) e ja liberam
# o modelo de degradacao de pneu. A telemetria (ondas 3 e 4) alimenta o
# modelo de identificacao de piloto. A onda 5 e opcional.
#
#   scope="year"            -> uma requisicao por ano
#   scope="session"         -> uma requisicao por sessao
#   scope="session_driver"  -> uma requisicao por sessao e piloto

WAVES: dict[int, dict] = {
    1: {
        "nome": "metadados",
        "endpoints": [
            {"name": "meetings", "scope": "year"},
            {"name": "sessions", "scope": "year"},
            {"name": "drivers", "scope": "session"},
        ],
    },
    2: {
        "nome": "dados de volta e contexto",
        "endpoints": [
            {"name": "laps", "scope": "session"},
            {"name": "stints", "scope": "session"},
            {"name": "pit", "scope": "session"},
            {"name": "weather", "scope": "session"},
            {"name": "position", "scope": "session"},
            {"name": "race_control", "scope": "session"},
            {"name": "session_result", "scope": "session"},
        ],
    },
    3: {
        "nome": "telemetria das corridas",
        "endpoints": [
            {"name": "car_data", "scope": "session_driver"},
        ],
        "only_races": True,
    },
    4: {
        "nome": "telemetria de treinos e classificacao",
        "endpoints": [
            {"name": "car_data", "scope": "session_driver"},
        ],
        "skip_races": True,
    },
    5: {
        "nome": "posicao no circuito (opcional)",
        "endpoints": [
            {"name": "location", "scope": "session_driver"},
        ],
    },
}
