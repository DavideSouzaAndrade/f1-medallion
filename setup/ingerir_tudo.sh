#!/usr/bin/env bash
#
# Executa a ingestao completa: ondas 1 a 6, para cada ano do escopo.
#
# Roda uma execucao por vez e espera cada uma terminar. Isso e obrigatorio,
# nao preferencia: cada processo aplica seu proprio limitador de 25 req/min,
# mas todos compartilham o limite de 30 req/min da OpenF1. Duas execucoes
# simultaneas se derrubam mutuamente com HTTP 429.
#
# A ingestao e idempotente -- o checkpoint e a existencia do objeto no GCS.
# Se o script for interrompido, basta roda-lo de novo: o que ja foi gravado
# e pulado e a execucao retoma de onde parou.
#
# Uso:
#   export PROJECT_ID="..."
#   export BUCKET_NAME="..."
#   bash setup/ingerir_tudo.sh
#
# Sao dezenas de horas no total. Rode dentro de tmux ou screen:
#   tmux new -s f1 'bash setup/ingerir_tudo.sh 2>&1 | tee ingestao.log'

set -euo pipefail

PROJECT_ID="${PROJECT_ID:?defina PROJECT_ID}"
RUN_REGION="${RUN_REGION:-us-central1}"
JOB_NAME="${JOB_NAME:-f1-ingestao}"

# Ondas em ordem de prioridade. A 1 e a 2 liberam o modelo de pneu; a 3
# libera o modelo de piloto; 4, 5 e 6 sao volume e contexto adicional.
WAVES="${WAVES:-1 2 3 4 5 6}"
YEARS="${YEARS:-2023 2024 2025 2026}"

gcloud config set project "${PROJECT_ID}" >/dev/null

# --- guardas antes de comecar ----------------------------------------------

echo ">> verificando execucoes em andamento"
em_curso=$(gcloud run jobs executions list \
  --job="${JOB_NAME}" --region="${RUN_REGION}" \
  --filter="status.completionTime=null" --format="value(name)" 2>/dev/null || true)
if [[ -n "${em_curso}" ]]; then
  echo "ERRO: ja existe execucao em andamento: ${em_curso}" >&2
  echo "Cancele antes de continuar, ou as duas vao competir pelo rate limit:" >&2
  echo "  gcloud run jobs executions cancel ${em_curso} --region=${RUN_REGION}" >&2
  exit 1
fi

# A OpenF1 bloqueia o acesso global -- inclusive dados historicos -- para
# usuarios nao autenticados enquanto ha sessao de F1 ao vivo. Comeca a
# ingestao so se a API estiver respondendo.
echo ">> verificando disponibilidade da API"
codigo=$(curl -s -o /dev/null -w '%{http_code}' -m 30 "https://api.openf1.org/v1/sessions?year=2024")
if [[ "${codigo}" != "200" ]]; then
  echo "ERRO: a OpenF1 respondeu HTTP ${codigo}." >&2
  echo "Se for 401, ha sessao de F1 ao vivo. Aguarde o fim da sessao." >&2
  exit 1
fi

# --- execucao ---------------------------------------------------------------

echo ">> publicando a versao atual do codigo no Cloud Run Job"
gcloud run jobs deploy "${JOB_NAME}" \
  --source=ingestion \
  --region="${RUN_REGION}" \
  --quiet

total=0
for wave in ${WAVES}; do
  for year in ${YEARS}; do
    total=$((total + 1))
    echo
    echo "=============================================================="
    echo ">> onda ${wave} | ano ${year} | inicio $(date -u +%H:%M:%SZ)"
    echo "=============================================================="

    # Uma execucao por onda e ano: nenhuma se aproxima do teto de 24 h do
    # Cloud Run, e uma falha custa apenas o reprocessamento de um ano.
    if ! gcloud run jobs execute "${JOB_NAME}" \
        --region="${RUN_REGION}" \
        --args="--wave,${wave},--years,${year}" \
        --wait; then
      echo "AVISO: onda ${wave}/${year} terminou com erro. Seguindo." >&2
      echo "       Rode o script de novo ao final para reprocessar o que faltou." >&2
    fi
  done
done

cat <<EOF

Concluido: ${total} execucoes.

Conferir o volume por endpoint:
  gcloud storage du -s "gs://\${BUCKET_NAME}/openf1/*"

Conferir falhas no manifesto:
  gcloud storage cat "gs://\${BUCKET_NAME}/openf1/_manifest/**/*.jsonl" \\
    | grep '"status":"erro"' | head

Rode este script novamente para reprocessar o que tiver falhado -- o que ja
foi gravado e pulado.
EOF
