# API de BI (front-end analítico)

Serve ao front o que está no BigQuery. O navegador **só fala com esta API**: nunca com o
BigQuery, e sem credencial da GCP. É um serviço Cloud Run separado da `f1-api` (que serve o
modelo), com conta de serviço só de leitura (`bigquery.jobUser` + `bigquery.dataViewer`).

```
f1_silver.silver_* / vw_voltas_unificada ─┐
f1_gold.gold_degradacao_pneu              ├─▶ bi-api (FastAPI, Cloud Run) ─▶ bi-frontend (navegador)
f1_stream.voltas / voltas_dlq  ───────────┘        └─ /api/stream/sse (Server-Sent Events)
```

## Rotas

| Rota | O que devolve | Tabela |
|---|---|---|
| `GET /api/health` | `{status, modo}`; **503** se o BigQuery não responde | — |
| `GET /api/pipeline/status` | checagens do pipeline para o Airflow (**503** se alguma falha) — ver [AIRFLOW.md](AIRFLOW.md) | `f1_stream.voltas`, `voltas_dlq` |
| `GET /api/sessoes?year=` | corridas com dados | `silver_sessoes` |
| `GET /api/sessoes/{key}/pilotos` | pilotos da corrida | `silver_pilotos` |
| `GET /api/voltas?session_key=&driver_number=&origem=` | voltas, com a coluna `origem` (`batch`/`stream`) | `vw_voltas_unificada` |
| `GET /api/degradacao?session_key=&circuito=` | tempo médio por composto × idade do pneu | `gold_degradacao_pneu` |
| `GET /api/comparativo/origem` | volume batch × stream | `vw_voltas_unificada` |
| `GET /api/stream/resumo` | volume, latência p50/p95, DLQ por motivo, vazão por minuto | `f1_stream.voltas`, `voltas_dlq` |
| `GET /api/stream/replays` | execuções do gerador vistas no stream | `f1_stream.voltas` |
| `GET /api/stream/eventos?desde=&limite=` | eventos ao vivo por polling (alternativa ao SSE) | `f1_stream.voltas` |
| `GET /api/stream/sse` | eventos ao vivo por SSE | `f1_stream.voltas` |
| `GET /api/previsto-real?session_key=&replay_id=` | previsto × real, MAE e viés | `f1_stream.voltas` |

Documentação interativa em `/docs`.

## Tempo real (SSE)

Uma única tarefa no servidor consulta `f1_stream.voltas` a cada `STREAM_POLL_S` (3 s) e empurra
o que é novo a todos os clientes: cem abas custam o mesmo que uma. Ela só roda enquanto há
alguém conectado, usa o cursor `_ts_processamento` e descarta repetidos por `evento_id`
(o Pub/Sub entrega pelo menos uma vez). A latência total é a do pipeline (~2 s) mais o ciclo
de consulta.

Mensagens (`event:` do SSE): `volta` (um evento), `ok` (batimento a cada consulta) e `erro`
(a consulta ao BigQuery falhou; a tarefa tenta de novo sozinha).

Não usa assinatura própria do Pub/Sub, de propósito: evita estado em memória atrelado a uma
assinatura e roubo de mensagens do Dataflow. Por isso o serviço roda com `--max-instances 1`.

## Previsto × real

`tempo_previsto_s` e `duracao_s` saem da **mesma linha** de `f1_stream.voltas`, então já vêm
casados pela chave (`session_key`, `driver_number`, `lap_number`). Enquanto o Dataflow não
preenche `tempo_previsto_s` (parte 3), `/api/previsto-real` devolve `metricas.voltas = 0` e
`aguardando_predicao` com o número de voltas aptas esperando.

> Se na parte 3 a previsão for gravada em **outra tabela** em vez de em `f1_stream.voltas`,
> só `repo_bq.previsto_real` muda: junte pela mesma chave.

## Como executar

```bash
pip install -r requirements-dev.txt

# sem GCP: dados simulados (a API e o front avisam "DADOS SIMULADOS")
MOCK=1 uvicorn main:app --port 8080

# com GCP real
gcloud auth application-default login
python validar_sql.py          # dry run: confere tabelas/colunas de TODAS as consultas, sem custo
uvicorn main:app --port 8080

pytest                         # contrato das rotas e o SSE, em modo mock

bash deploy.sh                 # Cloud Run (DEMO=1: min-instances=1)
```

## Configuração (variáveis de ambiente)

| Variável | Padrão | Efeito |
|---|---|---|
| `GCP_PROJECT` | `pdm-davi-2026` | projeto do BigQuery |
| `MOCK` | `0` | `1` usa dados simulados |
| `CORS_ORIGINS` | `http://localhost:5173` | origens do front, separadas por vírgula. O `deploy.sh` já passa o localhost e as duas URLs do front de produção |
| `STREAM_POLL_S` | `3` | ciclo da consulta que alimenta o SSE |
| `CACHE_S` | `60` | cache do histórico (o stream nunca é cacheado) |
| `BQ_MAX_BYTES` | 10 GB | teto de bytes lidos por consulta |
| `MOCK_PREDICOES` | `1` | `0` simula a parte 2 (previsão nula) para testar o estado "aguardando" |

## Estado de verificação

- Testado: rotas e SSE no modo mock (`pytest`), e o fluxo HTTP/SSE com `uvicorn` + front.
- **Ainda não executado contra o BigQuery real** (as consultas foram escritas a partir dos
  `.sqlx` do repo). Rode `python validar_sql.py` antes do primeiro deploy.
