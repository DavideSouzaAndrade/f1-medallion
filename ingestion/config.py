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

# A OpenF1 cobre de 2023 em diante. A temporada corrente e gratuita e
# acessivel sem autenticacao como qualquer outra -- nao ha motivo para
# deixa-la de fora.
YEARS = [int(y) for y in os.environ.get("YEARS", "2023,2024,2025,2026").split(",")]

# Sessoes de corrida: usadas para priorizar a telemetria na onda 3 e para
# pular endpoints que a API so publica em corrida.
RACE_SESSION_NAMES = {"Race", "Sprint"}


# --- ondas de ingestao -----------------------------------------------------
#
# A ordem importa. As ondas 1 e 2 sao rapidas e ja liberam o modelo de
# degradacao de pneu. A telemetria (ondas 3 e 4) alimenta o modelo de
# identificacao de piloto. As ondas 5 e 6 sao complementares.
#
#   scope="year"            -> uma requisicao por ano
#   scope="session"         -> uma requisicao por sessao
#   scope="session_driver"  -> uma requisicao por sessao e piloto
#
# only_races / skip_races valem tanto no nivel da onda quanto no de um
# endpoint isolado. No nivel do endpoint servem para nao gastar requisicao
# em combinacao que a API nunca preenche: intervals, overtakes e as duas
# tabelas de campeonato so existem em corrida.

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
            # Gap ao lider a cada amostra. E o preditor de ar sujo do modelo
            # de pneu: andar atras de outro carro degrada composto mais rapido,
            # e sem esta tabela o modelo nao tem como enxergar isso.
            # ~30 mil linhas por corrida, uma unica requisicao.
            {"name": "intervals", "scope": "session", "only_races": True},
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
    6: {
        "nome": "contexto adicional",
        "endpoints": [
            # Grid de largada: as linhas carregam o session_key da
            # classificacao, entao a consulta por sessao acerta na quali.
            {"name": "starting_grid", "scope": "session"},
            {"name": "team_radio", "scope": "session"},
            {"name": "overtakes", "scope": "session", "only_races": True},
            {"name": "championship_drivers", "scope": "session", "only_races": True},
            {"name": "championship_teams", "scope": "session", "only_races": True},
        ],
    },
}
