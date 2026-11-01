"""DAG de vigilancia: olha a saude do pipeline a cada 5 minutos.

Separada da DAG principal de proposito. A principal custa hora de worker do
Dataflow e roda sob demanda; esta nao custa nada alem de duas requisicoes
HTTP e serve para o painel de saude ter historico -- quando a apresentacao
perguntar "e se cair?", a resposta esta nas execucoes desta DAG.

As duas tarefas usam o status HTTP como resultado, nao o corpo: a API de BI
devolve 503 quando alguma checagem falha, e o sensor marca a tarefa como
falha sozinho (bi-api/AIRFLOW.md).

Enquanto nao houver Dataflow de pe, esta DAG falha -- e o comportamento
certo: ela mede se o stream esta vivo, e ele nao esta.
"""

from __future__ import annotations

from datetime import datetime, timedelta

from airflow import DAG
from airflow.providers.http.sensors.http import HttpSensor

with DAG(
    "f1_saude_pipeline",
    description="Vigia a API de BI e a saude do stream",
    start_date=datetime(2026, 11, 1),
    schedule="*/5 * * * *",
    catchup=False,
    # Sem acumular execucoes: o que interessa e o agora, nao a fila.
    max_active_runs=1,
    default_args={"owner": "frente-b", "retries": 0},
    tags=["f1", "saude"],
    doc_md=__doc__,
) as dag:

    # A API de BI responde e o BigQuery responde.
    api_de_bi_viva = HttpSensor(
        task_id="api_de_bi_viva",
        http_conn_id="f1_bi_api",
        endpoint="api/health",
        poke_interval=30,
        timeout=120,
        mode="reschedule",
    )

    # A API do modelo responde. Um GET simples: nao vale aquecer o modelo a
    # cada 5 minutos, isso e tarefa da DAG principal, antes da demonstracao.
    api_do_modelo_viva = HttpSensor(
        task_id="api_do_modelo_viva",
        http_conn_id="f1_api",
        endpoint="health",
        poke_interval=30,
        timeout=120,
        mode="reschedule",
    )

    # As quatro checagens do pipeline: stream recente, latencia p95, DLQ e
    # predicoes. O limite de idade e mais frouxo que na DAG principal porque
    # aqui nao ha replay em curso garantido.
    pipeline_saudavel = HttpSensor(
        task_id="pipeline_saudavel",
        http_conn_id="f1_bi_api",
        endpoint="api/pipeline/status",
        request_params={"max_idade_s": 300},
        poke_interval=30,
        timeout=int(timedelta(minutes=4).total_seconds()),
        mode="reschedule",
    )

    [api_de_bi_viva, api_do_modelo_viva] >> pipeline_saudavel
