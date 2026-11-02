#!/usr/bin/env bash
#
# Audita o que o Dataflow gravou, comparando com o lote quando possível.
#
# Não é teste de unidade: aqui o alvo é o dado em produção, depois de uma
# execução. Os testes offline provam que a regra está certa; isto prova que
# o que chegou ao BigQuery respeita a regra.
#
# Uso:
#   bash streaming/auditar.sh                      # a última execução
#   bash streaming/auditar.sh replay-2026...-abc   # uma específica
#
# Sai com código 1 se qualquer verificação obrigatória falhar.

set -uo pipefail

PROJECT_ID="${PROJECT_ID:-pdm-davi-2026}"
REPLAY="${1:-}"

if [[ -z "${REPLAY}" ]]; then
  REPLAY=$(bq query --project_id="${PROJECT_ID}" --nouse_legacy_sql --format=csv \
    'SELECT replay_id FROM `pdm-davi-2026.f1_stream.voltas`
     GROUP BY 1 ORDER BY MAX(_ts_processamento) DESC LIMIT 1' 2>/dev/null | tail -1)
  [[ -n "${REPLAY}" ]] || { echo "nenhuma execucao em f1_stream.voltas" >&2; exit 1; }
fi
echo ">> auditando ${REPLAY}"

SAIDA=$(bq query --project_id="${PROJECT_ID}" --nouse_legacy_sql --format=csv "
WITH s AS (
  SELECT * FROM \`${PROJECT_ID}.f1_stream.voltas\` WHERE replay_id = '${REPLAY}'
),
b AS (
  SELECT * FROM \`${PROJECT_ID}.f1_silver.silver_voltas\`
  WHERE session_key IN (SELECT DISTINCT session_key FROM s)
),
j AS (SELECT s.* EXCEPT(fonte), s.fonte, b.duracao_s AS dur_lote, b.composto AS comp_lote,
             b.idade_pneu AS idade_lote, b.temp_pista AS temp_lote,
             b.gap_medio_a_frente AS gap_lote
      FROM s LEFT JOIN b USING (session_key, driver_number, lap_number))
SELECT 'evento_id duplicado' AS checagem,
       CAST(COUNT(*) - COUNT(DISTINCT evento_id) AS STRING) AS valor,
       IF(COUNT(*) = COUNT(DISTINCT evento_id), 'ok', 'FALHA') AS veredito FROM s
UNION ALL SELECT 'chave natural duplicada',
  CAST(COUNT(*) - COUNT(DISTINCT FORMAT('%d-%d-%d', session_key, driver_number, lap_number)) AS STRING),
  IF(COUNT(*) = COUNT(DISTINCT FORMAT('%d-%d-%d', session_key, driver_number, lap_number)), 'ok', 'FALHA') FROM s
UNION ALL SELECT 'coluna NOT NULL vazia',
  CAST(COUNTIF(evento_id IS NULL OR replay_id IS NULL OR ts_evento IS NULL OR year IS NULL
            OR circuito IS NULL OR circuit_type IS NULL OR apto_modelo IS NULL) AS STRING),
  IF(COUNTIF(evento_id IS NULL OR year IS NULL OR circuito IS NULL OR apto_modelo IS NULL) = 0, 'ok', 'FALHA') FROM s
UNION ALL SELECT 'lap_number <= 0', CAST(COUNTIF(lap_number <= 0) AS STRING),
  IF(COUNTIF(lap_number <= 0) = 0, 'ok', 'FALHA') FROM s
UNION ALL SELECT 'duracao_s fora de faixa',
  CAST(COUNTIF(duracao_s IS NOT NULL AND (duracao_s <= 0 OR duracao_s > 7200)) AS STRING),
  IF(COUNTIF(duracao_s IS NOT NULL AND (duracao_s <= 0 OR duracao_s > 7200)) = 0, 'ok', 'FALHA') FROM s
UNION ALL SELECT 'idade_pneu negativa', CAST(COUNTIF(idade_pneu < 0) AS STRING),
  IF(COUNTIF(idade_pneu < 0) = 0, 'ok', 'FALHA') FROM s
UNION ALL SELECT 'apta sem previsao', CAST(COUNTIF(apto_modelo AND tempo_previsto_s IS NULL) AS STRING),
  IF(COUNTIF(apto_modelo AND tempo_previsto_s IS NULL) = 0, 'ok', 'FALHA') FROM s
UNION ALL SELECT 'previsao em volta inapta', CAST(COUNTIF(NOT apto_modelo AND tempo_previsto_s IS NOT NULL) AS STRING),
  IF(COUNTIF(NOT apto_modelo AND tempo_previsto_s IS NOT NULL) = 0, 'ok', 'FALHA') FROM s
UNION ALL SELECT 'procedencia ausente', CAST(COUNTIF(fonte IS NULL) AS STRING),
  IF(COUNTIF(fonte IS NULL) = 0, 'ok', 'FALHA') FROM s
UNION ALL SELECT 'setores nao somam a duracao (>1s)',
  FORMAT('%d de %d', COUNTIF(setor_1_s IS NOT NULL AND setor_2_s IS NOT NULL AND setor_3_s IS NOT NULL
         AND duracao_s IS NOT NULL AND ABS(setor_1_s + setor_2_s + setor_3_s - duracao_s) > 1),
         COUNTIF(setor_1_s IS NOT NULL AND duracao_s IS NOT NULL)),
  IF(COUNTIF(setor_1_s IS NOT NULL AND setor_2_s IS NOT NULL AND setor_3_s IS NOT NULL
      AND duracao_s IS NOT NULL AND ABS(setor_1_s + setor_2_s + setor_3_s - duracao_s) > 1) = 0, 'ok', 'FALHA') FROM s
-- Comparacoes com o lote: so as voltas COMPLETAS. A volta cortada pelo fim
-- do replay nao tem duracao, e o lote tem -- divergir ali e esperado.
UNION ALL SELECT 'duracao diferente do lote',
  FORMAT('%d de %d', COUNTIF(dur_lote IS NOT NULL AND duracao_s IS NOT NULL AND duracao_s != dur_lote),
         COUNTIF(dur_lote IS NOT NULL AND duracao_s IS NOT NULL)),
  IF(COUNTIF(dur_lote IS NOT NULL AND duracao_s IS NOT NULL AND duracao_s != dur_lote) = 0, 'ok', 'FALHA') FROM j
UNION ALL SELECT 'composto diferente do lote',
  FORMAT('%d de %d', COUNTIF(duracao_s IS NOT NULL AND COALESCE(composto,'-') != COALESCE(comp_lote,'-')),
         COUNTIF(duracao_s IS NOT NULL)),
  IF(COUNTIF(duracao_s IS NOT NULL AND COALESCE(composto,'-') != COALESCE(comp_lote,'-')) = 0, 'ok', 'FALHA') FROM j
UNION ALL SELECT 'temp_pista diferente do lote',
  FORMAT('%d de %d', COUNTIF(duracao_s IS NOT NULL AND COALESCE(temp_pista,-1) != COALESCE(temp_lote,-1)),
         COUNTIF(duracao_s IS NOT NULL)),
  IF(COUNTIF(duracao_s IS NOT NULL AND COALESCE(temp_pista,-1) != COALESCE(temp_lote,-1)) = 0, 'ok', 'FALHA') FROM j
ORDER BY checagem" 2>/dev/null)

echo "$SAIDA" | tail -n +2 | awk -F, '{printf "  %-36s %-14s %s\n", $1, $2, $3}'
if echo "$SAIDA" | tail -n +2 | grep -q "FALHA"; then
  echo ">> HOUVE FALHA na auditoria." >&2
  exit 1
fi
echo ">> tudo ok"
