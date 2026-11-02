#!/usr/bin/env bash
# Publica o front no Cloud Run (nginx servindo o build do Vite).
#   bash bi-frontend/deploy.sh
# Pré-requisito: a API de BI já no ar (bash bi-api/deploy.sh).
set -euo pipefail
cd "$(dirname "$0")"

PROJECT="${PROJECT:-pdm-davi-2026}"
REGION="${REGION:-us-central1}"
SERVICE="${SERVICE:-f1-bi-frontend}"
API_SERVICE="${API_SERVICE:-f1-bi-api}"

API_URL=$(gcloud run services describe "$API_SERVICE" --project "$PROJECT" --region "$REGION" --format 'value(status.url)')
echo ">> API de BI: $API_URL"
# O Vite embute VITE_API_URL no build. O arquivo precisa subir junto com o código: ver .gcloudignore.
echo "VITE_API_URL=$API_URL" > .env.production

gcloud run deploy "$SERVICE" \
  --project "$PROJECT" --region "$REGION" \
  --source . \
  --allow-unauthenticated \
  --memory 256Mi --cpu 1 --min-instances 0 --max-instances 2 \
  --labels "projeto=f1-medallion,parte=frontend"

FRONT=$(gcloud run services describe "$SERVICE" --project "$PROJECT" --region "$REGION" --format 'value(status.url)')
echo ">> front no ar: $FRONT"

# Confere que a URL da API entrou no build (sem ela o front chamaria a si mesmo, sem erro visível).
# Com retentativa: logo apos o deploy o Cloud Run ainda pode estar roteando
# para a revisao antiga, e uma checagem de uma tentativa so acusa falha que
# nao existe. Aviso que grita lobo treina todo mundo a ignorar aviso.
ACHOU=0
for tentativa in 1 2 3 4 5 6; do
  JS=$(curl -s --max-time 30 "$FRONT/" | grep -o 'assets/[^"]*\.js' | head -1)
  if [ -n "$JS" ] && curl -s --max-time 60 "$FRONT/$JS" | grep -q "${API_URL#https://}"; then
    ACHOU=1; break
  fi
  echo "   conferindo o build publicado (tentativa $tentativa)..."
  sleep 10
done
if [ "$ACHOU" = "1" ]; then
  echo ">> ok: o front aponta para a API"
else
  echo "!! ATENÇÃO: a URL da API NÃO entrou no build do front. Veja o bi-frontend/.gcloudignore." >&2
  exit 1
fi

# Todo serviço do Cloud Run responde em duas URLs (a do número do projeto e a com hash). A API só
# aceita as origens que listarmos, então liberamos as duas.
NUMERO=$(gcloud projects describe "$PROJECT" --format 'value(projectNumber)')
ORIGENS="http://localhost:5173,$FRONT,https://$SERVICE-$NUMERO.$REGION.run.app"
echo ">> liberando no CORS da API: $ORIGENS"
gcloud run services update "$API_SERVICE" --project "$PROJECT" --region "$REGION" \
  --update-env-vars "^@^CORS_ORIGINS=$ORIGENS" >/dev/null
echo ">> pronto. Abra: https://$SERVICE-$NUMERO.$REGION.run.app"
