#!/usr/bin/env bash
# Deploy da API de BI no Cloud Run.
#   bash bi-api/deploy.sh                          -> min-instances=0
#   DEMO=1 bash bi-api/deploy.sh                   -> min-instances=1 (dia da apresentação)
#   CORS_ORIGINS=https://meu-front.web.app bash bi-api/deploy.sh   -> troca a lista padrão do CORS
# Sem CORS_ORIGINS, libera o dev local e as duas URLs de produção do front.
set -euo pipefail
cd "$(dirname "$0")"

PROJECT="${PROJECT:-pdm-davi-2026}"
REGION="${REGION:-us-central1}"
SERVICE="${SERVICE:-f1-bi-api}"
SA_NAME="${SA_NAME:-f1-bi-api}"
FRONT_SERVICE="${FRONT_SERVICE:-f1-bi-frontend}"
MIN_INST=$([ "${DEMO:-0}" = "1" ] && echo 1 || echo 0)

echo ">> projeto=$PROJECT região=$REGION serviço=$SERVICE min-instances=$MIN_INST"

# O padrão do CORS precisa incluir o front de produção. O --set-env-vars lá embaixo troca TODAS as
# variáveis do serviço: com padrão só localhost, redeployar só a API apagava a lista que o deploy do
# front grava, e o navegador bloqueava todas as telas ("Não foi possível falar com a API de BI"),
# com a API respondendo normalmente por curl. Todo serviço do Cloud Run responde em duas URLs (a do
# número do projeto e a com hash); a lista é a mesma que o bi-frontend/deploy.sh grava.
if [ -z "${CORS_ORIGINS:-}" ]; then
  NUMERO=$(gcloud projects describe "$PROJECT" --format 'value(projectNumber)')
  FRONT_URL=$(gcloud run services describe "$FRONT_SERVICE" --project "$PROJECT" --region "$REGION" \
    --format 'value(status.url)' 2>/dev/null || true)   # vazio no primeiro deploy, antes do front existir
  CORS_ORIGINS="http://localhost:5173,https://$FRONT_SERVICE-$NUMERO.$REGION.run.app${FRONT_URL:+,$FRONT_URL}"
fi
echo ">> CORS liberado para: $CORS_ORIGINS"
gcloud services enable run.googleapis.com cloudbuild.googleapis.com \
  artifactregistry.googleapis.com bigquery.googleapis.com --project "$PROJECT"

# Conta própria, só leitura: consultar o BigQuery e ler as tabelas. Não escreve em nada.
SA="$SA_NAME@$PROJECT.iam.gserviceaccount.com"
gcloud iam service-accounts describe "$SA" --project "$PROJECT" >/dev/null 2>&1 || \
  gcloud iam service-accounts create "$SA_NAME" --project "$PROJECT" --display-name "F1 BI API (Cloud Run)"
for ROLE in roles/bigquery.jobUser roles/bigquery.dataViewer; do
  gcloud projects add-iam-policy-binding "$PROJECT" --member "serviceAccount:$SA" \
    --role "$ROLE" --condition=None --quiet >/dev/null
done

# --timeout 3600: o SSE é uma conexão longa; o EventSource do navegador reconecta ao fim dela.
# --max-instances 1: o difusor guarda o cursor do stream em memória; uma instância basta e
# evita consultas duplicadas ao BigQuery.
gcloud run deploy "$SERVICE" \
  --project "$PROJECT" --region "$REGION" \
  --source . \
  --service-account "$SA" \
  --allow-unauthenticated \
  --memory 512Mi --cpu 1 \
  --min-instances "$MIN_INST" --max-instances 1 \
  --concurrency 200 --timeout 3600 \
  --labels "projeto=f1-medallion,parte=frontend" \
  --set-env-vars "^@^GCP_PROJECT=$PROJECT@CORS_ORIGINS=$CORS_ORIGINS"

URL=$(gcloud run services describe "$SERVICE" --project "$PROJECT" --region "$REGION" --format 'value(status.url)')
echo ">> API de BI no ar: $URL"
echo ">> teste: curl $URL/api/health"
