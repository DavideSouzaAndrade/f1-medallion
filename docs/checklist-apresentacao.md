# Roteiro de apresentação

Duas listas: o que fazer **na véspera**, com calma, e o que fazer **na hora**,
com o professor olhando. A separação é o ponto — nada que possa falhar
deveria acontecer ao vivo.

Tudo que custa dinheiro aparece marcado com 💰 e tem a linha de desligar.

---

## Na véspera

### 1. Ligar a instância garantida da API 💰

```bash
gcloud run services update f1-api --region=us-central1 --min-instances=1
```

A API carrega um SavedModel do TensorFlow: 33 s de container, e o Cloud Run
pode levar mais de um minuto só para alocar instância. A DAG aquece a API
antes de publicar e absorve isso, mas na véspera é barato ter as duas
proteções.

Custa ~US$ 1,20 por dia ligada. **Desligar depois da apresentação:**

```bash
gcloud run services update f1-api --region=us-central1 --min-instances=0
```

Conferir que está quente (deve responder em menos de 1 s):

```bash
curl -s -o /dev/null -w "%{http_code} em %{time_total}s\n" \
  -X POST https://f1-api-ytz2c7b2wq-uc.a.run.app/predict \
  -H "Content-Type: application/json" \
  -d '{"instancias":[{"composto":"MEDIUM","idade_pneu":20,"volta_no_stint":20,
       "volta_corrida":23,"gap_medio_a_frente":3.3,"temp_pista":53.8,
       "temp_ar":32.4,"umidade":37.5,"circuito":"Monza","circuit_type":"Permanent"}]}'
```

### 2. Ligar a VM do Airflow 💰

```bash
gcloud compute instances start f1-airflow --zone=us-central1-a
```

O `restart: unless-stopped` do compose religa os contêineres no boot, e o
volume do Postgres preserva o histórico de execuções. Custa ~US$ 0,80 por
dia ligada. **Desligar depois:**

```bash
gcloud compute instances stop f1-airflow --zone=us-central1-a
```

Conferir que o scheduler voltou (sem SSH, pelo console serial):

```bash
gcloud compute instances get-serial-port-output f1-airflow \
  --zone=us-central1-a | grep 'command("/bin/bash")' | tail -10
```

### 3. Rodar o fim a fim uma vez, inteiro

Disparar a DAG `f1_pipeline_fim_a_fim` e deixar terminar. Serve para três
coisas: aquecer tudo, deixar dado fresco no BI, e descobrir qualquer
problema **com um dia de sobra**.

### 4. Conferir que o BI tem o que mostrar

```sql
SELECT origem, COUNT(*) AS voltas
FROM `pdm-davi-2026.f1_silver.vw_voltas_unificada`
GROUP BY origem;
```

Os dois lados precisam ter linha: `batch` é o histórico do Dataform,
`stream` é o que o Dataflow gravou. **É essa consulta que mostra a relação
entre lote e streaming que o Trabalho 2 pede.**

```sql
-- previsto × real na mesma linha, que é o que o Trabalho 3 cobra
SELECT lap_number, driver_number, duracao_s, tempo_previsto_s,
       ROUND(tempo_previsto_s - duracao_s, 2) AS erro_s, modelo_versao
FROM `pdm-davi-2026.f1_stream.voltas`
WHERE tempo_previsto_s IS NOT NULL
ORDER BY _ts_processamento DESC LIMIT 10;
```

### 5. Abrir o front e clicar em tudo

`https://f1-bi-frontend-ytz2c7b2wq-uc.a.run.app` — as quatro telas:
Histórico, Ao vivo, Previsto × Real, Saúde.

### 6. Garantir que não há job do Dataflow de pé 💰

```bash
gcloud dataflow jobs list --region=us-central1 --status=active
```

Lista vazia = ninguém pagando. Se tiver algo:
`ESPERAR=1 bash streaming/parar.sh`

---

## Na hora

### Antes de começar a falar

```bash
# nada de pé do dia anterior
gcloud dataflow jobs list --region=us-central1 --status=active

# a API está quente
curl -s -o /dev/null -w "%{http_code} em %{time_total}s\n" \
  https://f1-api-ytz2c7b2wq-uc.a.run.app/health
```

### A demonstração ao vivo

**Disparar a DAG e falar enquanto ela roda.** A sequência leva ~12 minutos,
e cada tarefa dá assunto:

| tarefa | o que contar enquanto roda |
|---|---|
| `aquecer_f1_api` | por que ela existe: 33 s de SavedModel contra 10 s de timeout do Dataflow |
| `subir_dataflow` | por que antes do gerador, e não em paralelo |
| `esperar_dataflow_pronto` | `Running` é o estado do job, não do worker |
| `disparar_gerador` | o replay no fator 30: 2 h de corrida em 4 min |
| `esperar_stream_saudavel` | as quatro checagens do painel de saúde |
| `drenar_dataflow` | `ALL_DONE`: a tarefa que roda mesmo se tudo falhar, porque o job cobra por hora |

**Enquanto o gerador publica**, abrir a tela *Ao vivo* do front: as voltas
aparecem com `tempo_previsto_s` preenchido. É a prova de que o Dataflow
chamou a API durante o processamento — os 35% da nota.

**Se alguém perguntar "e se a API cair?"**: a volta é gravada com previsão
nula e o painel a mostra como pendente. O evento nunca se perde. Está
testado em `streaming/test_predicao.py`, nos cinco modos de falha.

### Depois da apresentação 💰

```bash
ESPERAR=1 bash streaming/parar.sh
gcloud compute instances stop f1-airflow --zone=us-central1-a
gcloud run services update f1-api --region=us-central1 --min-instances=0
gcloud dataflow jobs list --region=us-central1 --status=active   # tem que vir vazio
```

---

## Se der errado ao vivo

| sintoma | causa provável | o que fazer |
|---|---|---|
| voltas aparecem sem `tempo_previsto_s` | API fria ou fora | `curl .../health`; a DAG continua, as voltas entram sem previsão |
| `esperar_stream_saudavel` falhando | o gerador não publicou, ou o Dataflow não subiu | olhar `disparar_gerador` e `subir_dataflow` no Airflow |
| nenhuma volta chegando | job do Dataflow não está consumindo | `gcloud dataflow jobs list --region=us-central1 --status=active` |
| `subir_dataflow` falha com "já existe job ativo" | sobrou job da véspera | `ESPERAR=1 bash streaming/parar.sh` e disparar de novo |
| DLQ com muita coisa | normal: o gerador injeta 1% de propósito | mostrar como recurso, não como defeito |

**A carta na manga:** se o ao vivo falhar, o BigQuery tem as execuções
anteriores. `SELECT ... FROM f1_stream.voltas WHERE tempo_previsto_s IS NOT
NULL` mostra previsão e tempo real na mesma linha, com `replay_id` e
`_ts_processamento` provando quando foi gravado. A demonstração não depende
de dar tudo certo na hora.

---

## Números para ter na ponta da língua

| | |
|---|---|
| voltas conferidas contra o lote | **127 de 127** iguais em duração, composto, idade de pneu, temperatura e gap |
| duplicatas | zero (`evento_id` determinístico) |
| latência mediana, Pub/Sub → BigQuery, predição incluída | **4,15 s** |
| latência p95 | 7,33 s |
| erro do modelo em volta de corrida normal | **1,32 s** sobre 88,5 s (1,5%) |
| mensagens por corrida | ~27.000, ~110/s no fator 30 |
| mensagens por volta emitida | ~26 |
| custo de uma rodada completa | ~US$ 0,04 |
| DirectRunner × Dataflow, latência mediana | 60 s × 4,15 s (**14×**) |
