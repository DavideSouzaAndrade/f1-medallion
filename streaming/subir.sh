#!/usr/bin/env bash
#
# Sobe o pipeline de streaming no Dataflow.
#
# O job fica de pe ate alguem derruba-lo: o Dataflow cobra por hora de
# worker ligado, tenha mensagem ou nao. Nao esqueca do streaming/parar.sh.
#
# Uso:
#   bash streaming/subir.sh              # 1 worker, e2-standard-2
#   MAX_WORKERS=3 bash streaming/subir.sh

set -euo pipefail

RAIZ="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
PYTHON="${PYTHON:-${RAIZ}/.venv/bin/python}"

PROJECT_ID="${PROJECT_ID:-pdm-davi-2026}"
REGIAO="${REGIAO:-us-central1}"
MAQUINA="${MAQUINA:-e2-standard-2}"
MAX_WORKERS="${MAX_WORKERS:-1}"

if [[ ! -x "${PYTHON}" ]]; then
  echo "ERRO: interpretador nao encontrado em ${PYTHON}." >&2
  echo "Defina PYTHON ou crie o venv e instale streaming/requirements.txt." >&2
  exit 1
fi

# Um job de streaming com o mesmo nome ja de pe significa cobranca dobrada
# consumindo a mesma assinatura. Melhor avisar do que somar dois workers.
echo ">> procurando job de streaming ja em execucao"
EM_EXECUCAO=$(gcloud dataflow jobs list \
  --project="${PROJECT_ID}" \
  --region="${REGIAO}" \
  --status=active \
  --filter="name~^f1-voltas-" \
  --format="value(id,name)" 2>/dev/null || true)

if [[ -n "${EM_EXECUCAO}" ]]; then
  echo "   ja existe job ativo:"
  echo "${EM_EXECUCAO}" | sed 's/^/     /'
  echo "   derrube com streaming/parar.sh antes de subir outro." >&2
  exit 1
fi

echo ">> lancando (projeto ${PROJECT_ID}, regiao ${REGIAO}, ate ${MAX_WORKERS} worker)"
cd "${RAIZ}/streaming"
"${PYTHON}" pipeline.py \
  --runner DataflowRunner \
  --regiao "${REGIAO}" \
  --maquina "${MAQUINA}" \
  --max-workers "${MAX_WORKERS}"

cat <<EOF

Lancado. Leva de 2 a 4 minutos ate o primeiro evento ser gravado: o
Dataflow ainda precisa provisionar a VM e instalar as dependencias.

Acompanhar:
  gcloud dataflow jobs list --region=${REGIAO} --status=active
  https://console.cloud.google.com/dataflow/jobs?project=${PROJECT_ID}

Publicar eventos:
  gcloud run jobs execute f1-gerador --region=${REGIAO} \\
    --args="--limite,50,--injetar-invalidos,0.1"

Derrubar (NAO ESQUECA -- cobra por hora ligado):
  bash streaming/parar.sh
EOF
