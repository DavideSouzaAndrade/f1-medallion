#!/usr/bin/env bash
# Teste ao vivo da API + latência (prints para os slides).
# Uso: bash api/testar.sh https://f1-api-xxxx-uc.a.run.app [n_requisicoes]
set -euo pipefail
URL="${1:?passe a URL do Cloud Run}"; N="${2:-20}"
H="Content-Type: application/json"
VOLTA='{"composto":"MEDIUM","idade_pneu":20,"volta_no_stint":20,"volta_corrida":23,"gap_medio_a_frente":3.328,"temp_pista":53.8,"temp_ar":32.4,"umidade":37.5,"circuito":"Monza","circuit_type":"Permanent"}'

echo "== GET /health"; curl -s "$URL/health"; echo; echo

echo "== POST /predict (1 volta: Hamilton, volta 23, Monza 2026, MEDIUM com 20 voltas)"
curl -s -X POST "$URL/predict" -H "$H" -d "{\"instancias\":[$VOLTA]}"; echo; echo

echo "== POST /predict em lote: o mesmo pneu envelhecendo (degradação)"
LOTE=$(for IDADE in 2 10 20 30; do
  echo "$VOLTA" | sed "s/\"idade_pneu\":20/\"idade_pneu\":$IDADE/;s/\"volta_no_stint\":20/\"volta_no_stint\":$IDADE/"
done | paste -sd, -)
curl -s -X POST "$URL/predict" -H "$H" -d "{\"instancias\":[$LOTE]}"; echo; echo

echo "== Entrada inválida (composto fora da lista) -> espera 422"
curl -s -o /dev/null -w "HTTP %{http_code}\n" -X POST "$URL/predict" -H "$H" \
  -d '{"instancias":[{"composto":"SUPERSOFT","idade_pneu":-1}]}'; echo

echo "== Latência: $N requisições (tempo total do cliente, inclui rede)"
for i in $(seq "$N"); do
  curl -s -o /dev/null -w "%{time_total}\n" -X POST "$URL/predict" -H "$H" -d "{\"instancias\":[$VOLTA]}"
done | sort -n | awk '{a[NR]=$1*1000} END {
  printf "p50 = %.0f ms | p95 = %.0f ms | máx = %.0f ms\n", a[int(NR*0.5)], a[int(NR*0.95)], a[NR] }'
echo "(latência só do modelo, no servidor: cabeçalho X-Latencia-ms)"
curl -s -D - -o /dev/null -X POST "$URL/predict" -H "$H" -d "{\"instancias\":[$VOLTA]}" | grep -i x-latencia
