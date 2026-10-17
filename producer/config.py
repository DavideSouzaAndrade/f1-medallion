"""Configuracao do gerador de eventos.

Tudo que muda entre ambientes vem de variavel de ambiente, com default
apontando para o projeto do grupo. Nada de credencial neste arquivo: local
usa as Application Default Credentials, no Cloud Run usa a service account.
"""

from __future__ import annotations

import os

PROJECT_ID = os.environ.get("PROJECT_ID", "pdm-davi-2026")

# Destino: o topico criado por setup/setup_streaming.sh.
TOPICO = os.environ.get("TOPICO", "f1-voltas")

# Origem do replay: a Silver do batch. O gerador le daqui e publica como se
# a corrida estivesse acontecendo agora.
DATASET_SILVER = os.environ.get("DATASET_SILVER", "f1_silver")

# Versao do contrato do evento. Vai como atributo da mensagem para que o
# Dataflow saiba qual schema esperar sem abrir o corpo.
SCHEMA_VERSION = "1"

# --- ritmo do replay ---------------------------------------------------------

# Quantas vezes mais rapido que a corrida real. Uma corrida de ~80 min com
# fator 30 vira ~3 min de eventos: cabe na demonstracao ao vivo.
FATOR_PADRAO = float(os.environ.get("FATOR", "30"))

# Teto para a espera entre dois eventos. Bandeira vermelha para a corrida
# por 30 min ou mais; sem teto, o replay ficaria minutos parado no meio da
# apresentacao.
PAUSA_MAXIMA_S = float(os.environ.get("PAUSA_MAXIMA_S", "2"))
