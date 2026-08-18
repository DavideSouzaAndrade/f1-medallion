# F1 Medallion — pipeline de telemetria de Fórmula 1 no BigQuery

Arquitetura de dados em nuvem (GCP) processando telemetria de Fórmula 1 com
Arquitetura Medallion e modelos preditivos treinados inteiramente em SQL com
BigQuery ML.

**Fonte:** [OpenF1 API](https://openf1.org) — telemetria a ~3,7 Hz por piloto,
temporadas 2023 em diante.

**Volume:** ~400 milhões de registros brutos (car_data + location),
aproximadamente 60 GB em JSON não comprimido.

---

## Arquitetura

```
OpenF1 API
    │  Cloud Run Job (Python) — ingestão em ondas, idempotente
    ▼
GCS  gs://<bucket>/openf1/<endpoint>/year=/session_key=/*.json.gz
    │  tabelas externas (Hive partitioning)
    ▼
BRONZE   dados brutos, sem transformação de negócio
    │  tipagem, deduplicação, as-of join telemetria ↔ volta ↔ stint ↔ clima
    ▼
SILVER   telemetria limpa e conformada, uma linha por amostra
    │  agregação por piloto e volta
    ▼
GOLD     tabela de features (~150 mil linhas)
    │
    ├──▶ modelo 1: regressão de tempo de volta (degradação de pneu)
    └──▶ modelo 2: classificação de piloto pela assinatura de pilotagem
```

O volume vive em bronze e silver. A camada gold é deliberadamente pequena —
é o que mantém o treino dentro da cota gratuita do BigQuery ML e o que
caracteriza a arquitetura funcionando corretamente.

---

## Estrutura do repositório

```
├── ingestion/          Cliente da API e orquestrador da ingestão
│   ├── openf1_client.py    rate limit, retry, quebra por janela de tempo
│   ├── storage.py          escrita NDJSON.gz em GCS ou disco, checkpoint
│   ├── config.py           bucket, anos e definição das ondas
│   ├── main.py             CLI
│   └── Dockerfile
├── setup/
│   └── setup_gcp.sh    provisiona bucket, datasets, SA e Cloud Run Job
├── dataform/           definições SQL orquestradas (bronze → silver → gold → ml)
├── sql/                cópia plana dos .sql para leitura
└── docs/               apresentação e diagramas
```

---

## Como rodar

### 1. Provisionar o GCP

```bash
export PROJECT_ID="f1-medallion-xyz"
export BUCKET_NAME="f1-raw-xyz"
bash setup/setup_gcp.sh
```

### 2. Testar a ingestão localmente

```bash
cd ingestion
pip install -r requirements.txt
python main.py --wave 1 --years 2024 --local --limit-sessions 2
```

Os arquivos aparecem em `./data/openf1/...`.

### 3. Ingestão completa, em ondas

As ondas são executadas em ordem de prioridade. As duas primeiras levam
cerca de uma hora e já liberam todo o trabalho do modelo de pneu.

| Onda | Conteúdo | Duração aprox. | Necessária para |
|------|----------|----------------|-----------------|
| 1 | meetings, sessions, drivers | minutos | tudo |
| 2 | laps, stints, pit, weather, position, race_control | ~1 h | modelo de pneu |
| 3 | car_data das corridas | ~4 h | modelo de piloto |
| 4 | car_data de treinos e classificação | ~8 h | volume adicional |
| 5 | location | ~8 h | opcional |

```bash
gcloud run jobs execute f1-ingestao --region=us-central1 --args="--wave,1"
gcloud run jobs execute f1-ingestao --region=us-central1 --args="--wave,2"
```

A ingestão é **idempotente**: o checkpoint é a existência do objeto no GCS.
Rodar o mesmo comando duas vezes não duplica dado e retoma de onde parou.

---

## Decisões de projeto

**Por que salvar o bruto no GCS antes de processar.** Separa falha de
ingestão de falha de transformação. Um erro no SQL da silver custa segundos
para corrigir; sem a cópia bruta, custaria horas de redownload.

**Por que NDJSON comprimido e não Parquet.** Parquet exige definir schema e
tipar os campos — isso é trabalho da camada silver, não da ingestão. A
bronze deve ser cópia fiel da origem.

**Por que particionamento Hive.** `year=` e `session_key=` no caminho fazem o
BigQuery podar partições automaticamente: uma consulta filtrada por sessão
não lê os demais arquivos, o que mantém o consumo dentro do 1 TB/mês gratuito.

**Por que ondas.** Permite que o time de SQL comece a trabalhar após ~1 hora
de ingestão, em vez de esperar o download completo. Também define uma ordem
de corte segura caso o prazo aperte.

---

## Limites da API

Tier gratuito da OpenF1: 3 requisições por segundo e 30 por minuto. O cliente
usa 25/min com intervalo mínimo de 0,5 s entre chamadas, e faz retry com
backoff exponencial em 429 e 5xx. Respostas grandes demais são quebradas
recursivamente em janelas de tempo menores.

---

## Licença e atribuição

Dados fornecidos pela [OpenF1](https://openf1.org), licenciados sob
CC BY-NC-SA 4.0. OpenF1 é um projeto não oficial, sem associação com as
empresas de Fórmula 1. F1, FORMULA 1 e marcas relacionadas são marcas
registradas da Formula One Licensing B.V. Este projeto tem finalidade
exclusivamente acadêmica.
