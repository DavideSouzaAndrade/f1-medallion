#!/usr/bin/env bash
# Publica o front no Cloud Run (nginx servindo o build do Vite).
#   bash bi-frontend/deploy.sh
# Pré-requisito: a API de BI já no ar (bash bi-api/deploy.sh). Depois, libere a URL
# do front no CORS da API:  CORS_ORIGINS=<url do front> bash bi-api/deploy.sh
set -euo pipefail
cd "$(dirname "$0")"

PROJECT="${PROJECT:-pdm-davi-2026}"
REGION="${REGION:-us-central1}"
SERVICE="${SERVICE:-f1-bi-frontend}"
API_SERVICE="${API_SERVICE:-f1-bi-api}"

API_URL=$(gcloud run services describe "$API_SERVICE" --project "$PROJECT" --region "$REGION" --format 'value(status.url)')
echo ">> API de BI: $API_URL"
echo "VITE_API_URL=$API_URL" > .env.production

gcloud run deploy "$SERVICE" \
  --project "$PROJECT" --region "$REGION" \
  --source . \
  --allow-unauthenticated \
  --memory 256Mi --cpu 1 --min-instances 0 --max-instances 2 \
  --labels "projeto=f1-medallion,parte=frontend"

FRONT=$(gcloud run services describe "$SERVICE" --project "$PROJECT" --region "$REGION" --format 'value(status.url)')
echo ">> front no ar: $FRONT"
echo ">> libere no CORS da API:  CORS_ORIGINS=$FRONT bash ../bi-api/deploy.sh"
