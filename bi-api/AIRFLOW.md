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

Ordem sugerida na DAG principal: `executar f1-gerador` → `subir Dataflow` →
`esperar_stream_saudavel` → (parte 3) checar o Cloud Run da API de predição → `drain` do Dataflow
ao final da demonstração.

## O que o Airflow não precisa fazer pelo front

A tela de tempo real não depende do Airflow: ela lê o stream por SSE. O Airflow só dispara e
vigila o pipeline; o que ele produz no BigQuery o front já mostra sozinho.
