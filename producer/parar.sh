#!/usr/bin/env bash
# Para o replay em andamento: cancela a execucao do Cloud Run Job f1-gerador
# que ainda nao terminou.
#
# Serve para o replay longo da apresentacao (uma corrida em tempo real, com
# --fator 1, que dura ~1h30): ele roda ate alguem mandar parar. Disparado pela
# DAG f1_pipeline_fim_a_fim, o cancelamento faz a tarefa disparar_gerador
# falhar (sem nova tentativa, de proposito), e a drenar_dataflow, que roda
# sempre, desliga o Dataflow em seguida. Nao e preciso parar o Dataflow a mao.
#
# Uso:
#   bash producer/parar.sh

set -euo pipefail

PROJECT_ID="${PROJECT_ID:-pdm-davi-2026}"
REGIAO="${REGIAO:-us-central1}"
JOB="f1-gerador"

# Execucao em andamento = sem completionTime. O filtro do gcloud nao testa
# existencia de campo de data, entao o recorte e feito aqui.
em_andamento=$(gcloud run jobs executions list --job="$JOB" --region="$REGIAO" \
  --project="$PROJECT_ID" --limit=20 \
  --format="value(metadata.name,status.completionTime)" | awk -F'\t' '$2 == "" {print $1}')

if [[ -z "$em_andamento" ]]; then
  echo "Nenhuma execucao do $JOB em andamento. Nada a parar."
  exit 0
fi

for execucao in $em_andamento; do
  echo ">> cancelando $execucao"
  gcloud run jobs executions cancel "$execucao" --region="$REGIAO" --project="$PROJECT_ID" --quiet
done

echo
echo "Replay interrompido. Se ele foi disparado pela DAG, o Airflow drena o"
echo "Dataflow sozinho; conferir em alguns minutos com:"
echo "  gcloud dataflow jobs list --region=$REGIAO --status=active"
