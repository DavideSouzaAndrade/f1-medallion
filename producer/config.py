"""Configuracao do gerador de eventos.

Tudo que muda entre ambientes vem de variavel de ambiente, com default
apontando para o projeto do grupo. Nada de credencial neste arquivo: local
usa as Application Default Credentials, no Cloud Run usa a service account.
"""

from __future__ import annotations

import os

PROJECT_ID = os.environ.get("PROJECT_ID", "pdm-davi-2026")

# Destino: o topico criado por setup/setup_streaming.sh.
TOPICO = os.environ.get("TOPICO", "f1-openf1")

# Origem do replay: a Bronze, que guarda cada registro exatamente como a
# OpenF1 entregou. O gerador le daqui e publica como se a corrida estivesse
# acontecendo agora.
DATASET_BRONZE = os.environ.get("DATASET_BRONZE", "f1_bronze")

# Os endpoints que a silver_voltas usa, na ordem em que saem quando duas
# mensagens caem no mesmo instante: o contexto (evento, sessao, pneu) antes
# da volta que depende dele.
TOPICOS = ("meetings", "sessions", "weather", "stints", "pit", "intervals", "laps")

# --- ritmo do replay ---------------------------------------------------------

# Quantas vezes mais rapido que a corrida real. Uma corrida de ~80 min com
# fator 30 vira ~3 min de mensagens: cabe na demonstracao ao vivo.
FATOR_PADRAO = float(os.environ.get("FATOR", "30"))

# Teto para a espera entre duas mensagens. Bandeira vermelha para a corrida
# por 30 min ou mais; sem teto, o replay ficaria minutos parado no meio da
# apresentacao.
PAUSA_MAXIMA_S = float(os.environ.get("PAUSA_MAXIMA_S", "2"))
