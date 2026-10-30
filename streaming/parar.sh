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
# Uso:
#   bash streaming/parar.sh            # drain
#   MODO=cancel bash streaming/parar.sh

set -euo pipefail

PROJECT_ID="${PROJECT_ID:-pdm-davi-2026}"
REGIAO="${REGIAO:-us-central1}"
MODO="${MODO:-drain}"

if [[ "${MODO}" != "drain" && "${MODO}" != "cancel" ]]; then
  echo "ERRO: MODO deve ser drain ou cancel, nao '${MODO}'." >&2
  exit 1
fi

echo ">> procurando jobs ativos do streaming"
JOBS=$(gcloud dataflow jobs list \
  --project="${PROJECT_ID}" \
  --region="${REGIAO}" \
  --status=active \
  --filter="name~^f1-voltas-" \
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

cat <<EOF

Pedido enviado. O drain leva alguns minutos ate o job chegar em DRAINED e
as VMs serem liberadas -- a cobranca so termina ai.

Conferir:
  gcloud dataflow jobs list --region=${REGIAO} --status=active

Se a lista voltar vazia, nao ha mais worker ligado.
EOF
