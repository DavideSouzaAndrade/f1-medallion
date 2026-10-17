#!/usr/bin/env bash
#
# Publica o gerador de eventos como Cloud Run Job. Rodar de novo depois de
# mudar o codigo atualiza o Job com a imagem nova.
#
# Pre-requisito: setup/setup_streaming.sh (topico e permissao de publicar).
#
# Uso, a partir da raiz do repositorio:
#   export PROJECT_ID="pdm-davi-2026"
#   bash producer/deploy.sh

set -euo pipefail

PROJECT_ID="${PROJECT_ID:?defina PROJECT_ID}"
RUN_REGION="${RUN_REGION:-us-central1}"

JOB_NAME="f1-gerador"
SA_EMAIL="f1-pipeline@${PROJECT_ID}.iam.gserviceaccount.com"
PASTA="$(cd "$(dirname "$0")" && pwd)"

echo ">> projeto: ${PROJECT_ID} | job: ${JOB_NAME} | regiao: ${RUN_REGION}"

# --max-retries=0 de proposito: uma nova tentativa recomecaria o replay do
# inicio e publicaria de novo voltas que ja estavam no topico. Melhor falhar
# e decidir na mao do que duplicar o stream em silencio.
#
# --task-timeout=2h cobre uma corrida inteira em tempo real (--fator 1),
# que e o caso mais lento possivel.
gcloud run jobs deploy "${JOB_NAME}" \
  --project="${PROJECT_ID}" \
  --source="${PASTA}" \
  --region="${RUN_REGION}" \
  --service-account="${SA_EMAIL}" \
  --set-env-vars="PROJECT_ID=${PROJECT_ID},TOPICO=f1-voltas" \
  --task-timeout=2h \
  --max-retries=0 \
  --memory=512Mi \
  --labels=projeto=f1-medallion,camada=stream

cat <<EOF

Pronto.

Replay da corrida mais recente, fator 30 (~3 min):
  gcloud run jobs execute ${JOB_NAME} --region=${RUN_REGION}

Escolhendo corrida, ritmo e eventos invalidos:
  gcloud run jobs execute ${JOB_NAME} --region=${RUN_REGION} \\
    --args="--session-key,11361,--fator,60,--injetar-invalidos,0.05"

Acompanhar:
  gcloud run jobs executions list --job=${JOB_NAME} --region=${RUN_REGION}
EOF
