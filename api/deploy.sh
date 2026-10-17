#!/usr/bin/env bash
# Deploy da API no Cloud Run.
#   bash api/deploy.sh            -> min-instances=0 (padrão, sem custo parado)
#   DEMO=1 bash api/deploy.sh     -> min-instances=1 (dia da apresentação: sem partida a frio)
set -euo pipefail
cd "$(dirname "$0")"

PROJECT="${PROJECT:-pdm-davi-2026}"
REGION="${REGION:-us-central1}"
SERVICE="${SERVICE:-f1-api}"
SA_NAME="${SA_NAME:-f1-api}"
BUCKET="${BUCKET:-f1-raw-pdm-davi-2026}"
VERSAO="${VERSAO:-v1}"
MODEL_URI="${MODEL_URI:-gs://$BUCKET/modelos/degradacao_pneu/$VERSAO/}"
BQ_MODEL="${BQ_MODEL:-pdm-davi-2026.f1_gold.modelo_degradacao_pneu}"
BACKEND="${BACKEND:-auto}"        # auto | tensorflow | bqml
MIN_INST=$([ "${DEMO:-0}" = "1" ] && echo 1 || echo 0)

echo ">> projeto=$PROJECT região=$REGION serviço=$SERVICE min-instances=$MIN_INST"
gcloud services enable run.googleapis.com cloudbuild.googleapis.com \
  artifactregistry.googleapis.com bigquery.googleapis.com --project "$PROJECT"

# conta de serviço própria, papéis mínimos: ler o artefato no GCS e rodar ML.PREDICT
SA="$SA_NAME@$PROJECT.iam.gserviceaccount.com"
gcloud iam service-accounts describe "$SA" --project "$PROJECT" >/dev/null 2>&1 || \
  gcloud iam service-accounts create "$SA_NAME" --project "$PROJECT" --display-name "F1 API (Cloud Run)"
gcloud storage buckets add-iam-policy-binding "gs://$BUCKET" \
  --member "serviceAccount:$SA" --role roles/storage.objectViewer --quiet >/dev/null
for ROLE in roles/bigquery.jobUser roles/bigquery.dataViewer; do
  gcloud projects add-iam-policy-binding "$PROJECT" --member "serviceAccount:$SA" \
    --role "$ROLE" --condition=None --quiet >/dev/null
done

gcloud run deploy "$SERVICE" \
  --project "$PROJECT" --region "$REGION" \
  --source . \
  --service-account "$SA" \
  --allow-unauthenticated \
  --memory 2Gi --cpu 1 --cpu-boost \
  --min-instances "$MIN_INST" --max-instances 3 \
  --concurrency 40 --timeout 60 \
  --labels "projeto=f1-medallion,parte=2" \
  --set-env-vars "BACKEND=$BACKEND,MODEL_URI=$MODEL_URI,BQ_MODEL=$BQ_MODEL,GCP_PROJECT=$PROJECT,MODELO_NOME=f1-degradacao-pneu@$VERSAO"

URL=$(gcloud run services describe "$SERVICE" --project "$PROJECT" --region "$REGION" --format 'value(status.url)')
echo ">> API no ar: $URL"
echo ">> teste:     bash api/testar.sh $URL"
