#!/usr/bin/env bash
#
# Derruba o pipeline de streaming no Dataflow.
#
# Por padrao usa drain, nao cancel: o drain para de ler a assinatura e
# deixa terminar o que ja esta em voo, entao nenhum evento ja lido fica
# sem ser gravado. O cancel mata na hora e perde o que estava em memoria --
# as mensagens correspondentes nao foram confirmadas e voltam para a
# assinatura, mas o drain e mais limpo.
#
# Mensagens publicadas com o job parado ficam retidas 7 dias na assinatura
# e sao consumidas quando ele voltar.
#
# O drain NAO e instantaneo: o Dataflow reavalia o estado do job a cada 10
# minutos ("Job is in state JOB_STATE_DRAINING, will check again in 10m0s"),
# e a cobranca so termina quando as VMs sao liberadas. Com ESPERAR=1 o
# script fica aqui ate o job sair da lista de ativos -- e assim que a DAG
# do Airflow confirma que nao ha mais worker ligado.
#
# Uso:
#   bash streaming/parar.sh              # pede o drain e sai
#   ESPERAR=1 bash streaming/parar.sh    # pede e espera o job sumir
#   MODO=cancel bash streaming/parar.sh  # mata na hora

set -euo pipefail

PROJECT_ID="${PROJECT_ID:-pdm-davi-2026}"
REGIAO="${REGIAO:-us-central1}"
MODO="${MODO:-drain}"
ESPERAR="${ESPERAR:-0}"
# Teto da espera. O drain costuma levar de 2 a 12 minutos; 20 min cobrem
# com folga e evitam uma tarefa pendurada para sempre no Airflow.
ESPERA_MAXIMA_S="${ESPERA_MAXIMA_S:-1200}"

if [[ "${MODO}" != "drain" && "${MODO}" != "cancel" ]]; then
  echo "ERRO: MODO deve ser drain ou cancel, nao '${MODO}'." >&2
  exit 1
fi

echo ">> procurando jobs ativos do streaming"
JOBS=$(gcloud dataflow jobs list \
  --project="${PROJECT_ID}" \
  --region="${REGIAO}" \
  --status=active \
  --filter="name~^f1-openf1-" \
  --format="value(id)")

if [[ -z "${JOBS}" ]]; then
  echo "   nenhum job ativo. Nada a fazer, e nada sendo cobrado."
  exit 0
fi

for id in ${JOBS}; do
  echo ">> ${MODO} em ${id}"
  gcloud dataflow jobs "${MODO}" "${id}" \
    --project="${PROJECT_ID}" \
    --region="${REGIAO}"
done

if [[ "${ESPERAR}" == "1" ]]; then
  echo ">> esperando o job sair da lista de ativos (ate ${ESPERA_MAXIMA_S}s)"
  limite=$(( $(date +%s) + ESPERA_MAXIMA_S ))
  while [[ "$(date +%s)" -lt "${limite}" ]]; do
    restantes=$(gcloud dataflow jobs list \
      --project="${PROJECT_ID}" --region="${REGIAO}" --status=active \
      --filter="name~^f1-openf1-" --format="value(id)" 2>/dev/null || true)
    if [[ -z "${restantes}" ]]; then
      echo "   nenhum worker ligado. A cobranca parou."
      exit 0
    fi
    sleep 30
  done
  echo "   o job nao saiu da lista no prazo -- confira no console." >&2
  exit 1
fi

cat <<EOF

Pedido enviado. O drain leva alguns minutos ate o job chegar em DRAINED e
as VMs serem liberadas -- a cobranca so termina ai.

Conferir:
  gcloud dataflow jobs list --region=${REGIAO} --status=active

Se a lista voltar vazia, nao ha mais worker ligado.
EOF
