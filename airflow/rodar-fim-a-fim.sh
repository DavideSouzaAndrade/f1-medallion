#!/bin/bash
# Roda o fim a fim pelo Airflow da VM e acompanha, sem SSH: entra por um
# startup script e despeja o progresso no console serial.
set -uo pipefail
marca() { echo "### F1-DAG [$(date -u +%H:%M:%S)] $*"; }

cd /opt/f1-medallion/airflow || { marca "FALHA: airflow nao provisionado"; exit 1; }
cmp() { docker compose exec -T scheduler "$@"; }

marca "atualizando o repositorio para o ultimo commit do ramo"
RAMO=parte3/frente-b-dataflow
REPO=DavideSouzaAndrade/f1-medallion
rm -rf /tmp/f1 && mkdir -p /tmp/f1
if curl -fsSL "https://api.github.com/repos/${REPO}/tarball/${RAMO}" | tar xz -C /tmp/f1 --strip-components=1; then
  # Copia sobre o que existe, preservando airflow/logs (volume do container).
  rm -rf /tmp/f1/airflow/logs
  cp -r /tmp/f1/. /opt/f1-medallion/
  # O container e uid 50000, e o Beam escreve no diretorio do setup.py ao
  # empacotar o job. Sem isto o subir_dataflow da permissao negada.
  chown -R 50000:0 /opt/f1-medallion
  chmod -R u+rwX /opt/f1-medallion
  marca "repositorio atualizado"
else
  marca "aviso: nao atualizei o repositorio, seguindo com o que esta na VM"
fi

marca "reiniciando o scheduler para reler as DAGs e o compose"
docker compose up -d 2>&1 | tail -3
sleep 20

marca "despausando as DAGs"
cmp airflow dags unpause f1_pipeline_fim_a_fim 2>&1 | tail -1
cmp airflow dags unpause f1_saude_pipeline 2>&1 | tail -1

EXEC="fim-a-fim-$(date -u +%Y%m%dT%H%M%S)"
marca "disparando ${EXEC}"
cmp airflow dags trigger f1_pipeline_fim_a_fim \
  --run-id "$EXEC" \
  --conf '{"session_key":"11361","minutos":"45","injetar_invalidos":"0.01"}' 2>&1 | tail -3

marca "acompanhando (ate 45 min)"
anterior=""
for _ in $(seq 1 180); do
  linhas=$(cmp airflow tasks states-for-dag-run f1_pipeline_fim_a_fim "$EXEC" 2>/dev/null \
    | awk 'NR>2 && NF {printf "%s=%s ", $2, $4}')
  estado=$(cmp airflow dags list-runs -d f1_pipeline_fim_a_fim --no-backfill 2>/dev/null \
    | awk -v e="$EXEC" '$0 ~ e {print $3; exit}')
  if [[ "$linhas $estado" != "$anterior" ]]; then
    marca "  ${estado:-?} | ${linhas}"
    anterior="$linhas $estado"
  fi
  case "$estado" in
    success) marca "DAG CONCLUIDA COM SUCESSO"; break ;;
    failed)  marca "DAG FALHOU"; break ;;
  esac
  sleep 15
done

marca "logs das tarefas que falharam, se houver:"
for t in aquecer_f1_api subir_dataflow esperar_dataflow_pronto disparar_gerador esperar_stream_saudavel drenar_dataflow; do
  st=$(cmp airflow tasks state f1_pipeline_fim_a_fim "$t" --map-index -1 2>/dev/null | tail -1)
  if [[ "$st" == "failed" ]]; then
    marca "  --- $t ---"
    cmp bash -lc "tail -25 /opt/airflow/logs/dag_id=f1_pipeline_fim_a_fim/run_id=${EXEC}/task_id=${t}/attempt=*.log" 2>&1 | sed 's/^/    /'
  fi
done
marca "FIM A FIM ENCERRADO"
