# API do modelo de degradação de pneu (parte 2)

Serve o modelo `f1_gold.modelo_degradacao_pneu` (BigQuery ML, regressão linear) no Cloud Run,
seguindo o contrato v1 combinado com o gerador (A) e o Dataflow (B).

**URL:** https://f1-api-418113305910.us-central1.run.app (documentação interativa em `/docs`)

## Fluxo

```
definitions/ml/modelo_degradacao_pneu.sqlx  --(Dataform, model_registry='vertex_ai')-->  Vertex AI Model Registry
        |                                                                                 (f1-degradacao-pneu)
        +-- model/exportar.sh: EXPORT MODEL --> gs://f1-raw-pdm-davi-2026/modelos/degradacao_pneu/v1/  (SavedModel)
                                                        |
                                    api/ (FastAPI + TensorFlow, Cloud Run) carrega UMA vez no startup
```

## Contrato

```http
POST /predict
{"instancias": [{"composto": "MEDIUM", "idade_pneu": 20, "volta_no_stint": 20, "volta_corrida": 23,
                 "gap_medio_a_frente": 3.328, "temp_pista": 53.8, "temp_ar": 32.4, "umidade": 37.5,
                 "circuito": "Monza", "circuit_type": "Permanent"}]}

-> {"modelo": "f1-degradacao-pneu@v1", "predicoes": [{"tempo_previsto_s": 86.321}]}
```

- Até 500 instâncias por chamada; a resposta mantém a ordem.
- `gap_medio_a_frente` nulo vira `99.0`, igual ao `IFNULL` do treino.
- `composto` e `circuit_type` validados contra as listas do contrato; entrada inválida → HTTP 422.
- `GET /health` → `{"status": "ok", "modelo": "...", "backend": "tensorflow"}`.
- Latência do modelo no servidor no cabeçalho `X-Latencia-ms`.

`BACKEND=auto` (padrão) usa o SavedModel e, se ele não carregar, cai para `ML.PREDICT`
no BigQuery (plano B, 1–2 s). O `/health` mostra qual está ativo.

## Como executar (Cloud Shell)

```bash
pip install --user tensorflow-cpu google-cloud-bigquery google-cloud-storage
bash model/exportar.sh            # EXPORT MODEL + compara o artefato com ML.PREDICT
bash api/deploy.sh                # Cloud Run, 2 GiB, conta de serviço f1-api, min-instances=0
DEMO=1 bash api/deploy.sh         # dia da apresentação: min-instances=1 (sem partida a frio)
bash api/testar.sh <URL>          # health, 1 volta, lote, erro 422 e latência p50/p95
```

`model/avaliar.sql`: `ML.EVALUATE`, erro por composto e `ML.TRAINING_INFO`.

## Resultados medidos (17/10/2026)

| Medida | Valor |
|---|---|
| Artefato exportado vs. ML.PREDICT (50 voltas) | diferença máx. 0,000 s |
| ML.PREDICT, 50 voltas | 2.215 ms |
| SavedModel em memória, 50 voltas | 45 ms |
| Latência do modelo na API (servidor) | 4,6 ms |
| Latência para o cliente, p50 / p95 | 89 ms / 256 ms |

## Decisões

- **Sem Vertex Endpoint:** cobraria por hora; o Cloud Run escala a zero e o modelo roda em memória.
- **Conta de serviço própria (`f1-api`):** só leitura no bucket e permissão de consulta no BigQuery.
- **Região:** `us-central1`, a mesma do bucket e do Registry.
- **`volta_no_stint` igual à do treino:** o gerador calcula a feature com o mesmo filtro da Gold
  (só voltas limpas do stint), evitando training-serving skew. Conferido em 5.052 voltas de 5 corridas.
