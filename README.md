# F1 Medallion — pipeline de telemetria de Fórmula 1 no GCP

Arquitetura de dados em nuvem (GCP) com dados reais de Fórmula 1: um
pipeline batch em Arquitetura Medallion com modelos treinados em BigQuery ML
(parte 1), um pipeline de streaming que traz voltas em tempo real e uma API
que serve o modelo (parte 2).

**Fonte:** [OpenF1 API](https://openf1.org) — telemetria a ~3,7 Hz por piloto,
temporadas 2023 em diante.

**Volume:** ~400 milhões de registros brutos (car_data + location),
aproximadamente 60 GB em JSON não comprimido.

---

## Arquitetura

```
BATCH — histórico e treino (parte 1)
  OpenF1 API → Cloud Run Job f1-ingestao → GCS (NDJSON bruto)
    → f1_bronze → f1_silver → f1_gold → BigQuery ML (2 modelos)

STREAMING — voltas em tempo real (parte 2)
  Cloud Run Job f1-gerador → Pub/Sub f1-voltas → Dataflow → f1_stream.voltas
                                                          └→ f1_stream.voltas_dlq (rejeitados)
  f1_silver.vw_voltas_unificada = silver_voltas (batch) ∪ f1_stream.voltas (stream)

SERVING — predição sob demanda (parte 2)
  BigQuery ML → Vertex AI Model Registry
             → EXPORT MODEL (GCS) → Cloud Run f1-api ← POST /predict
```

O batch guarda o histórico e treina o modelo; o streaming traz as voltas
novas; a view unificada é onde os dois se encontram, com a coluna `origem`
dizendo o caminho de cada volta.

O Dataflow é a peça do meio do streaming: um job Apache Beam que fica de pé
consumindo a assinatura, valida cada evento contra o contrato v1, marca o que
serve ao modelo e grava — a volta boa em `f1_stream.voltas`, a rejeitada em
`f1_stream.voltas_dlq` com o motivo.

---

## Estrutura do repositório

```
├── ingestion/     ingestão batch da OpenF1 para o GCS (Cloud Run Job)
├── definitions/   SQL do Dataform: bronze, silver, gold, ml e stream
├── includes/      catálogo dos endpoints usado pelo Dataform
├── producer/      gerador de eventos: replay de corrida no Pub/Sub
├── streaming/     pipeline Dataflow: Pub/Sub → validação → BigQuery, com DLQ
├── model/         exportação e avaliação do modelo
├── api/           API de predição no Cloud Run
├── setup/         provisionamento do GCP (batch e streaming)
└── docs/          documentação de apoio (ver abaixo)
```

## Documentação

| Documento | Conteúdo |
|-----------|----------|
| [docs/perfil-dados-bronze.md](docs/perfil-dados-bronze.md) | Campo a campo dos 18 endpoints na Bronze e a proposta de estrutura da Silver |
| [docs/analise-exploratoria.md](docs/analise-exploratoria.md) | Qualidade dos dados medida na Bronze e a decisão de tratamento que cada achado implica |
| [docs/ingestao-incremental.md](docs/ingestao-incremental.md) | Como a ingestão se atualiza com as corridas novas sem varrer tudo |
| [docs/contrato-evento.md](docs/contrato-evento.md) | Formato do evento de volta no Pub/Sub: campos, tipos e o que vai para a DLQ |
| [docs/plano-frente-b.md](docs/plano-frente-b.md) | Plano e decisões do pipeline Dataflow: desenho, ordem da validação e custo |

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
python streaming/test_validacao.py                    # contrato, offline
cd streaming && python pipeline.py --runner DirectRunner --duracao 120
bash streaming/subir.sh                               # sobe no Dataflow
bash streaming/parar.sh                               # drena e desliga
```

O job de streaming cobra por hora ligado — não escala a zero como o Cloud
Run. O `parar.sh` é o que fecha a torneira.

**API de predição** — [api/README.md](api/README.md)

```bash
curl -X POST https://f1-api-418113305910.us-central1.run.app/predict \
  -H "Content-Type: application/json" \
  -d '{"instancias": [{"composto": "MEDIUM", "idade_pneu": 20, "volta_no_stint": 20,
       "volta_corrida": 23, "gap_medio_a_frente": 3.328, "temp_pista": 53.8,
       "temp_ar": 32.4, "umidade": 37.5, "circuito": "Monza", "circuit_type": "Permanent"}]}'
```

---

## Licença e atribuição

Dados fornecidos pela [OpenF1](https://openf1.org), licenciados sob
CC BY-NC-SA 4.0. OpenF1 é um projeto não oficial, sem associação com as
empresas de Fórmula 1. F1, FORMULA 1 e marcas relacionadas são marcas
registradas da Formula One Licensing B.V. Este projeto tem finalidade
exclusivamente acadêmica.
