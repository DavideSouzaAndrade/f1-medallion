#!/usr/bin/env bash
#
# Provisiona a infraestrutura do projeto no Google Cloud.
# Idempotente: pode rodar mais de uma vez sem quebrar.
#
# Uso:
#   export PROJECT_ID="f1-medallion-xyz"
#   export BUCKET_NAME="f1-raw-xyz"
#   bash setup/setup_gcp.sh

set -euo pipefail

PROJECT_ID="${PROJECT_ID:?defina PROJECT_ID}"
BUCKET_NAME="${BUCKET_NAME:?defina BUCKET_NAME}"

# A regiao do bucket e a localizacao dos datasets precisam ser compativeis.
# US (multi-regiao) + us-central1 e a combinacao que aproveita o free tier.
BQ_LOCATION="${BQ_LOCATION:-US}"
GCS_REGION="${GCS_REGION:-us-central1}"
RUN_REGION="${RUN_REGION:-us-central1}"

SA_NAME="f1-pipeline"
SA_EMAIL="${SA_NAME}@${PROJECT_ID}.iam.gserviceaccount.com"
JOB_NAME="f1-ingestao"

echo ">> projeto: ${PROJECT_ID} | bucket: ${BUCKET_NAME} | BQ: ${BQ_LOCATION}"
gcloud config set project "${PROJECT_ID}"

echo ">> habilitando APIs"
gcloud services enable \
  storage.googleapis.com \
  bigquery.googleapis.com \
  run.googleapis.com \
  cloudbuild.googleapis.com \
  artifactregistry.googleapis.com \
  dataform.googleapis.com

echo ">> bucket"
if ! gcloud storage buckets describe "gs://${BUCKET_NAME}" &>/dev/null; then
  gcloud storage buckets create "gs://${BUCKET_NAME}" \
    --location="${GCS_REGION}" \
    --uniform-bucket-level-access
else
  echo "   ja existe"
fi

echo ">> datasets do BigQuery (uma camada por dataset)"
for ds in bronze silver gold; do
  if ! bq --location="${BQ_LOCATION}" show --dataset "${PROJECT_ID}:${ds}" &>/dev/null; then
    bq --location="${BQ_LOCATION}" mk --dataset \
      --description="Camada ${ds} da arquitetura medallion" \
      "${PROJECT_ID}:${ds}"
  else
    echo "   ${ds} ja existe"
  fi
done

echo ">> service account"
if ! gcloud iam service-accounts describe "${SA_EMAIL}" &>/dev/null; then
  gcloud iam service-accounts create "${SA_NAME}" \
    --display-name="Pipeline F1 Medallion"
fi

# A criacao da service account e assincrona no IAM. Sem esperar a
# propagacao, o add-iam-policy-binding abaixo falha com
# INVALID_ARGUMENT: "Service account ... does not exist".
echo ">> aguardando propagacao da service account"
for _ in $(seq 1 30); do
  if gcloud iam service-accounts describe "${SA_EMAIL}" &>/dev/null; then
    break
  fi
  sleep 2
done
if ! gcloud iam service-accounts describe "${SA_EMAIL}" &>/dev/null; then
  echo "ERRO: a service account ${SA_EMAIL} nao propagou em 60s." >&2
  exit 1
fi

for role in roles/storage.objectAdmin roles/bigquery.dataEditor roles/bigquery.jobUser; do
  gcloud projects add-iam-policy-binding "${PROJECT_ID}" \
    --member="serviceAccount:${SA_EMAIL}" \
    --role="${role}" \
    --condition=None \
    --quiet >/dev/null
done

echo ">> Cloud Run Job de ingestao"
gcloud run jobs deploy "${JOB_NAME}" \
  --source=ingestion \
  --region="${RUN_REGION}" \
  --service-account="${SA_EMAIL}" \
  --set-env-vars="GCS_BUCKET=${BUCKET_NAME}" \
  --task-timeout=24h \
  --max-retries=3 \
  --memory=1Gi

cat <<EOF

Pronto.

Rodar a ingestao (uma onda por vez):
  gcloud run jobs execute ${JOB_NAME} --region=${RUN_REGION} --args="--wave,1"
  gcloud run jobs execute ${JOB_NAME} --region=${RUN_REGION} --args="--wave,2"

Acompanhar:
  gcloud run jobs executions list --job=${JOB_NAME} --region=${RUN_REGION}

Nao esqueca do alerta de orcamento em Billing > Budgets & alerts.
EOF
