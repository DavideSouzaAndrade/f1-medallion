# F1 Medallion — pipeline de telemetria de Fórmula 1 no GCP

Arquitetura de dados em nuvem (GCP) com dados reais de Fórmula 1:

- **Parte 1:** pipeline batch em Arquitetura Medallion, com modelos treinados
  em BigQuery ML.
- **Parte 2:** pipeline de streaming que traz voltas em tempo real e uma API
  que serve o modelo.
- **Parte 3:** o Dataflow chama a API durante o processamento, o Airflow
  orquestra tudo e um front-end de BI mostra previsto × real.

Para entender o sistema inteiro, comece por
**[docs/arquitetura.md](docs/arquitetura.md)**.

**Fonte:** [OpenF1 API](https://openf1.org) — telemetria a ~3,7 Hz por piloto,
temporadas 2023 em diante.

**Volume:** ~400 milhões de registros brutos (car_data + location),
aproximadamente 60 GB em JSON não comprimido.

**Equipe:** Davi Andrade, Frederico Relvas, Gustavo Novack e Henrick de Souza.

**No ar, no Cloud Run:** o modelo como API REST em [f1-api-418113305910.us-central1.run.app/docs](https://f1-api-418113305910.us-central1.run.app/docs) e o front de BI em [f1-bi-frontend-418113305910.us-central1.run.app](https://f1-bi-frontend-418113305910.us-central1.run.app).

---

## Arquitetura

```
BATCH — histórico e treino (parte 1)
  OpenF1 API → Cloud Run Job f1-ingestao → GCS (NDJSON bruto)
    → f1_bronze → f1_silver → f1_gold → BigQuery ML (2 modelos)

SERVING — o modelo como API (parte 2)
  BigQuery ML → Vertex AI Model Registry
             → EXPORT MODEL (GCS) → Cloud Run f1-api  (POST /predict)

STREAMING — voltas em tempo real (partes 2 e 3)
  Cloud Run Job f1-gerador → Pub/Sub f1-openf1 → Dataflow ──→ f1_stream.voltas
  (replay da Bronze no         (mensagens brutas)   │   │      (volta + previsão)
   formato da OpenF1 ao vivo)                       │   └────→ f1_stream.voltas_dlq
                                                    └─ POST /predict → f1-api
  f1_silver.vw_voltas_unificada = silver_voltas (batch) ∪ f1_stream.voltas (stream)

BI — consumo (parte 3)
  BigQuery → Cloud Run f1-bi-api → Cloud Run f1-bi-frontend

ORQUESTRAÇÃO (parte 3)
  Airflow numa VM: aquece a API → sobe o Dataflow → dispara o gerador
                   → confere a saúde → drena o Dataflow
```

O batch guarda o histórico e treina o modelo; o streaming traz as voltas
novas; a view unificada é onde os dois se encontram, com a coluna `origem`
dizendo o caminho de cada volta.

O gerador publica as mensagens exatamente como a OpenF1 entrega ao vivo, um
registro bruto por mensagem; trocá-lo pela OpenF1 de verdade não muda nada
para quem consome.

O Dataflow faz três coisas sobre essas mensagens brutas, numa passagem só:

1. **Monta a volta** — junta as 4 versões de `laps` que se substituem, o
   `stint`, o `pit`, o clima vigente e a média de gap dos ~21 `intervals`
   medidos durante ela. É o mesmo trabalho que a `silver_voltas` faz em
   lote, agora com as mensagens chegando uma a uma e fora de ordem.
2. **Chama a API de predição** no Cloud Run, ainda durante o processamento,
   e grava a previsão na **mesma linha** da volta — é o que permite ao BI
   mostrar previsto e real sem join nenhum.
3. **Separa o que não dá para aproveitar** em `f1_stream.voltas_dlq`, com o
   motivo.

Conferido na corrida inteira (GP da Itália de 2026, 1.055 voltas), volta a
volta contra a `silver_voltas`: as 1.052 voltas completas saíram iguais em
`duracao_s`, `composto` e `temp_pista`, sem duplicata. Mediana de latência
de 1,70 s do Pub/Sub ao BigQuery, predição incluída, e erro médio do modelo
de 1,13 s em voltas de ~88 s.

O **Airflow** orquestra tudo: aquece a API, sobe o job, dispara o replay,
vigia a saúde e drena o job no fim.

---

## Estrutura do repositório

```
├── ingestion/     ingestão batch da OpenF1 para o GCS (Cloud Run Job)
├── definitions/   SQL do Dataform: bronze, silver, gold, ml e stream
├── includes/      catálogo dos endpoints usado pelo Dataform
├── producer/      gerador de eventos: replay de corrida no Pub/Sub
├── streaming/     pipeline Dataflow: monta a volta, chama a predição, grava no BigQuery
├── airflow/       orquestração: DAGs e o Airflow containerizado
├── model/         exportação e avaliação do modelo
├── api/           API de predição no Cloud Run
├── bi-api/        API de BI: serve o BigQuery e o stream (SSE) ao front
├── bi-frontend/   front-end analítico (React): histórico, ao vivo, previsto × real
├── setup/         provisionamento do GCP (batch e streaming)
└── docs/          documentação de apoio (ver abaixo)
```

## Documentação

| Documento | Conteúdo |
|-----------|----------|
| **[docs/arquitetura.md](docs/arquitetura.md)** | **Comece por aqui.** O sistema inteiro: o caminho do dado de ponta a ponta, as três frentes, camada por camada, as decisões e os resultados medidos |
| [docs/contrato-openf1.md](docs/contrato-openf1.md) | **Entrada do streaming:** as mensagens no Pub/Sub, no formato da OpenF1 ao vivo, as versões de cada registro e o que o dado bruto traz de defeito |
| [docs/contrato-evento.md](docs/contrato-evento.md) | **Saída do streaming:** a volta montada pelo Dataflow, campos e regras do que é gravado em `f1_stream.voltas` |
| [docs/perfil-dados-bronze.md](docs/perfil-dados-bronze.md) | Campo a campo dos 18 endpoints na Bronze e a proposta de estrutura da Silver |
| [docs/analise-exploratoria.md](docs/analise-exploratoria.md) | Qualidade dos dados medida na Bronze e a decisão de tratamento que cada achado implica |
| [docs/ingestao-incremental.md](docs/ingestao-incremental.md) | Como a ingestão se atualiza com as corridas novas sem varrer tudo |
| [docs/caso-carencia-reordenacao.md](docs/caso-carencia-reordenacao.md) | Os bugs de ordenação achados no streaming, a comparação Mac M4 × Dataflow e as otimizações |
| [streaming/README.md](streaming/README.md) | O pipeline atual: como rodar, as decisões e os resultados medidos |
| [airflow/README.md](airflow/README.md) | A orquestração, a receita da VM e por que não Cloud Composer |
| [docs/bastidores/](docs/bastidores/README.md) | Registros internos da equipe: planos, passagens entre frentes, roteiro da apresentação |

---

## Como executar

Cada componente tem um README próprio com os detalhes.

**Batch** — [ingestion/README.md](ingestion/README.md)

```bash
bash setup/setup_gcp.sh
gcloud run jobs execute f1-ingestao --region=us-central1 --args="--wave,2"
```

As camadas e os modelos são executados pelo Dataform: repositório
`F1 Medallion` na GCP, ligado a este GitHub.

**Gerador de eventos** — [producer/README.md](producer/README.md)

```bash
bash setup/setup_streaming.sh
npx @dataform/cli@3.0.0 run --tags stream
bash producer/deploy.sh
gcloud run jobs execute f1-gerador --region=us-central1
```

**Pipeline de streaming** — [streaming/README.md](streaming/README.md)

```bash
python streaming/test_montagem.py                     # montagem da volta, offline
python streaming/test_predicao.py                     # chamada à API, offline
bash streaming/subir.sh                               # sobe no Dataflow
bash streaming/parar.sh                               # drena e desliga
```

O job de streaming cobra por hora ligado — não escala a zero como o Cloud
Run. O `parar.sh` é o que fecha a torneira.

**Orquestração** — [airflow/README.md](airflow/README.md)

```bash
cd airflow
export GCLOUD_CONFIG="$HOME/.config/gcloud"     # o gcloud do container usa a sua conta
mkdir -p logs && docker compose up -d --build    # interface em http://localhost:8080
```

Na interface, a DAG `f1_pipeline_fim_a_fim` roda o streaming de ponta a
ponta (da API ao drain do Dataflow); a `f1_saude_pipeline` vigia tudo a cada
5 minutos. A mesma imagem roda na VM `f1-airflow` (receita no README).

**API de predição** — [api/README.md](api/README.md)

```bash
curl -X POST https://f1-api-418113305910.us-central1.run.app/predict \
  -H "Content-Type: application/json" \
  -d '{"instancias": [{"composto": "MEDIUM", "idade_pneu": 20, "volta_no_stint": 20,
       "volta_corrida": 23, "gap_medio_a_frente": 3.328, "temp_pista": 53.8,
       "temp_ar": 32.4, "umidade": 37.5, "circuito": "Monza", "circuit_type": "Permanent"}]}'
```

**Front-end de BI** — [bi-api/README.md](bi-api/README.md) · [bi-frontend/README.md](bi-frontend/README.md)

```bash
cd bi-api && MOCK=1 uvicorn main:app --port 8080    # sem GCP, dados simulados
cd bi-frontend && npm install && npm run dev         # http://localhost:5173
bash bi-api/deploy.sh && bash bi-frontend/deploy.sh  # Cloud Run
```

---

## Licença e atribuição

Dados fornecidos pela [OpenF1](https://openf1.org), licenciados sob
CC BY-NC-SA 4.0. OpenF1 é um projeto não oficial, sem associação com as
empresas de Fórmula 1. F1, FORMULA 1 e marcas relacionadas são marcas
registradas da Formula One Licensing B.V. Este projeto tem finalidade
exclusivamente acadêmica.
