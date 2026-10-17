#!/usr/bin/env bash
#
# Provisiona a parte de streaming do projeto: topico, assinaturas e o
# dataset onde o Dataflow grava. Complementa o setup_gcp.sh, que cuida do
# batch. Idempotente: pode rodar mais de uma vez sem quebrar.
#
# As tabelas do dataset NAO sao criadas aqui: ficam em definitions/stream/,
# no Dataform, junto com o resto do SQL do projeto.
#
# Uso:
#   export PROJECT_ID="pdm-davi-2026"
#   bash setup/setup_streaming.sh

set -euo pipefail

PROJECT_ID="${PROJECT_ID:?defina PROJECT_ID}"
BQ_LOCATION="${BQ_LOCATION:-US}"

TOPICO="f1-voltas"
ASSINATURA_DATAFLOW="f1-voltas-dataflow"
ASSINATURA_DEBUG="f1-voltas-debug"
DATASET="f1_stream"

SA_EMAIL="f1-pipeline@${PROJECT_ID}.iam.gserviceaccount.com"

echo ">> projeto: ${PROJECT_ID} | topico: ${TOPICO} | dataset: ${DATASET}"
gcloud config set project "${PROJECT_ID}"

echo ">> habilitando APIs"
gcloud services enable pubsub.googleapis.com bigquery.googleapis.com

echo ">> topico"
if ! gcloud pubsub topics describe "${TOPICO}" &>/dev/null; then
  gcloud pubsub topics create "${TOPICO}" \
    --labels=projeto=f1-medallion,camada=stream
else
  echo "   ja existe"
fi

# Duas assinaturas sobre o mesmo topico, porque o Pub/Sub entrega uma copia
# de cada mensagem para cada assinatura. A do Dataflow e a de producao. A de
# debug existe para inspecionar mensagens com `gcloud pubsub subscriptions
# pull` sem roubar eventos do pipeline: um pull com ack na assinatura do
# Dataflow consumiria a mensagem e ela nunca chegaria ao BigQuery.
#
# --expiration-period=never: por padrao uma assinatura sem uso por 31 dias e
# apagada. O projeto vai ate dezembro, com semanas paradas entre as entregas.
echo ">> assinatura do Dataflow"
if ! gcloud pubsub subscriptions describe "${ASSINATURA_DATAFLOW}" &>/dev/null; then
  gcloud pubsub subscriptions create "${ASSINATURA_DATAFLOW}" \
    --topic="${TOPICO}" \
    --ack-deadline=60 \
    --message-retention-duration=7d \
    --expiration-period=never \
    --labels=projeto=f1-medallion,camada=stream
else
  echo "   ja existe"
fi

# Retencao curta: e so para olhar mensagens, nao deve acumular semanas de
# eventos que ninguem vai ler.
echo ">> assinatura de debug"
if ! gcloud pubsub subscriptions describe "${ASSINATURA_DEBUG}" &>/dev/null; then
  gcloud pubsub subscriptions create "${ASSINATURA_DEBUG}" \
    --topic="${TOPICO}" \
    --ack-deadline=10 \
    --message-retention-duration=1d \
    --expiration-period=never \
    --labels=projeto=f1-medallion,camada=stream
else
  echo "   ja existe"
fi

# Dataset proprio para o stream, separado de f1_silver: na apresentacao a
# fronteira entre o que chegou em lote e o que chegou em tempo real precisa
# estar visivel ja pelo nome do dataset.
echo ">> dataset do BigQuery"
if ! bq --location="${BQ_LOCATION}" show --dataset "${PROJECT_ID}:${DATASET}" &>/dev/null; then
  bq --location="${BQ_LOCATION}" mk --dataset \
    --description="Camada de streaming do pipeline F1 Medallion: voltas publicadas no Pub/Sub e gravadas pelo Dataflow" \
    "${PROJECT_ID}:${DATASET}"
else
  echo "   ${DATASET} ja existe"
fi

# O gerador roda como Cloud Run Job com a mesma service account do batch, e
# precisa publicar no topico. O papel e dado no topico, nao no projeto: a SA
# nao ganha permissao de publicar em nenhum outro topico.
echo ">> permissao de publicacao para a service account"
gcloud pubsub topics add-iam-policy-binding "${TOPICO}" \
  --member="serviceAccount:${SA_EMAIL}" \
  --role="roles/pubsub.publisher" \
  --quiet >/dev/null

cat <<EOF

Pronto.

Criar as tabelas do stream (Dataform, tag "stream"):
  definitions/stream/voltas.sqlx e voltas_dlq.sqlx

Publicar um evento de teste:
  gcloud pubsub topics publish ${TOPICO} --message='{"evento_id": "teste"}' --attribute=schema_version=1

Ler sem consumir do pipeline:
  gcloud pubsub subscriptions pull ${ASSINATURA_DEBUG} --auto-ack --limit=5
EOF
