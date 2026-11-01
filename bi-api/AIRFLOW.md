# Integração com o Airflow

A API de BI oferece dois endpoints pensados para uma DAG. Os dois respondem em JSON e usam o
**status HTTP** como resultado: `HttpSensor` e `HttpOperator` marcam a tarefa como falha sozinhos,
sem interpretar o corpo.

| Endpoint | 200 quando | 503 quando |
|---|---|---|
| `GET /api/health` | a API e o BigQuery respondem | o BigQuery não responde |
| `GET /api/pipeline/status` | todas as checagens passam | alguma checagem falha |

## `/api/pipeline/status`

Parâmetros (todos opcionais; o padrão serve para a demonstração):

| Parâmetro | Padrão | Checagem que ele ajusta |
|---|---|---|
| `max_idade_s` | 300 | `stream_recente`: idade máxima do último evento |
| `max_p95_ms` | 10000 | `latencia_p95`: latência p95 das últimas 24 h |
| `max_dlq_pct` | 30 | `dlq`: % de rejeitados sobre o que chegou (o gerador injeta inválidos de propósito) |
| `sempre_200` | false | devolve 200 mesmo com falha (usado pela tela) |

Resposta:

```json
{"status": "falha", "verificado_em": "2026-10-31T16:00:00Z",
 "checagens": [
   {"nome": "stream_recente", "estado": "falha",    "detalhe": "último evento há 1041058 s (limite 300 s)"},
   {"nome": "latencia_p95",   "estado": "falha",    "detalhe": "sem eventos nas últimas 24 h"},
   {"nome": "dlq",            "estado": "ok",       "detalhe": "42 rejeitados, 19.8% do que chegou (limite 30%)"},
   {"nome": "predicoes",      "estado": "pendente", "detalhe": "nenhuma volta com previsão ainda"}]}
```

`predicoes` é só informativa: fica `pendente` enquanto o Dataflow não anexa a previsão e nunca
derruba o status.

## Exemplo de DAG

Pré-requisito: uma *connection* HTTP no Airflow (`f1_bi_api`) com o host da API de BI
(`https://f1-bi-api-....run.app`).

```python
from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.http.sensors.http import HttpSensor

with DAG("f1_saude_pipeline", start_date=datetime(2026, 11, 1), schedule="*/5 * * * *", catchup=False) as dag:
    # Depois das tarefas que disparam o gerador e o Dataflow: espera o stream ficar saudável.
    esperar_stream = HttpSensor(
        task_id="esperar_stream_saudavel",
        http_conn_id="f1_bi_api",
        endpoint="api/pipeline/status",
        request_params={"max_idade_s": 120},
        poke_interval=15,
        timeout=int(timedelta(minutes=10).total_seconds()),
        mode="reschedule",
    )
```

## Disparar o gerador pela DAG

O gerador é o Cloud Run Job `f1-gerador` (região `us-central1`). Argumentos (ver
`producer/README.md`, branch `parte3/gerador-openf1`):

| Argumento | Efeito |
|---|---|
| `--session-key` | corrida a reproduzir (padrão: a mais recente da Bronze; ex.: `11361`) |
| `--fator` | aceleração do replay (padrão 30: a corrida inteira em ~4 min) |
| `--minutos N` | só os N primeiros minutos depois da largada (bom para testes) |
| `--injetar-invalidos F` | fração de cópias quebradas para exercitar a DLQ |

```python
from airflow.providers.google.cloud.operators.cloud_run import CloudRunExecuteJobOperator

disparar_gerador = CloudRunExecuteJobOperator(
    task_id="disparar_gerador",
    project_id="pdm-davi-2026",
    region="us-central1",
    job_name="f1-gerador",
    overrides={"container_overrides": [{"args": ["--session-key", "11361", "--minutos", "5"]}]},
)
```

> O nome dos parâmetros do operador depende da versão do provider do Google; confira a que o
> Airflow do time usa. O gerador não devolve o `replay_id` ao Airflow, então a DAG não deve
> esperar "o replay X": use o `esperar_stream_saudavel` acima, que olha se **há** evento recente.

## Ordem sugerida na DAG principal

```
disparar_gerador ─┐
subir_dataflow ───┴─▶ esperar_stream_saudavel ─▶ checar f1-api (Cloud Run) ─▶ drain_dataflow
```

1. `subir_dataflow` primeiro ou em paralelo: o job leva ~3 min para o primeiro evento e a
   assinatura `f1-openf1-dataflow` retém as mensagens, então publicar antes não perde nada.
2. `esperar_stream_saudavel` (o `HttpSensor` acima) passa quando o último evento do stream tem
   menos de `max_idade_s` segundos. Enquanto o Dataflow novo não existir, ele falha: é o
   comportamento certo.
3. `drain_dataflow` ao final: o job cobra por hora ligado, tenha mensagem ou não.

## O que o Airflow não precisa fazer pelo front

A tela de tempo real não depende do Airflow: ela lê o stream por SSE. O Airflow só dispara e
vigila o pipeline; o que ele produz no BigQuery o front já mostra sozinho.
