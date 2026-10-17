#!/usr/bin/env bash
# Exporta o modelo BQML para o GCS e valida o artefato contra ML.PREDICT.
# Uso: bash model/exportar.sh   (padrões já apontam para pdm-davi-2026.f1_gold)
set -euo pipefail
cd "$(dirname "$0")"

BQ_MODEL="${BQ_MODEL:-pdm-davi-2026.f1_gold.modelo_degradacao_pneu}"
TABELA="${TABELA:-pdm-davi-2026.f1_gold.gold_degradacao_pneu}"
BUCKET="${BUCKET:-f1-raw-pdm-davi-2026}"
VERSAO="${VERSAO:-v1}"
DESTINO="gs://$BUCKET/modelos/degradacao_pneu/$VERSAO/"
PROJECT="${BQ_MODEL%%.*}"

echo ">> tipo do modelo:"
bq show --model --format=prettyjson "${PROJECT}:${BQ_MODEL#*.}" | grep -m1 '"modelType"' || true

echo ">> EXPORT MODEL -> $DESTINO"
bq query --project_id "$PROJECT" --use_legacy_sql=false \
  "EXPORT MODEL \`$BQ_MODEL\` OPTIONS (URI = '$DESTINO')"

echo ">> conteúdo exportado:"
gcloud storage ls -r "$DESTINO"

if [ -n "${TABELA:-}" ]; then
  echo ">> validando o artefato contra ML.PREDICT"
  python3 validar_export.py --bq-model "$BQ_MODEL" --tabela "$TABELA" --uri "$DESTINO"
else
  echo ">> (defina TABELA=... para validar o artefato contra ML.PREDICT)"
fi
