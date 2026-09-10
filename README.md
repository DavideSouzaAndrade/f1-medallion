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
│   ├── includes/           catálogo dos endpoints
│   └── definitions/bronze/ tabelas externas + camada Bronze incremental
└── docs/
    ├── ingestao-incremental.md   como o pipeline se atualiza sem varrer tudo
    └── perfil-dados-bronze.md    campo a campo dos 18 endpoints + plano da Silver
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
| 2 | laps, stints, pit, weather, position, race_control, session_result, intervals | ~1 h | modelo de pneu |
| 3 | car_data das corridas | ~4 h | modelo de piloto |
| 4 | car_data de treinos e classificação | ~8 h | volume adicional |
| 5 | location | ~8 h | opcional |
| 6 | starting_grid, team_radio, overtakes, championship_drivers, championship_teams | ~30 min | contexto |

As seis ondas cobrem os 18 endpoints publicados pela OpenF1. `intervals`
entra na onda 2 porque o gap ao carro da frente é preditor direto de
degradação de pneu — andar em ar sujo destrói composto mais rápido.

```bash
gcloud run jobs execute f1-ingestao --region=us-central1 --args="--wave,1"
gcloud run jobs execute f1-ingestao --region=us-central1 --args="--wave,2"
```

A ingestão é **idempotente**: o checkpoint é a existência do objeto no GCS.
Rodar o mesmo comando duas vezes não duplica dado e retoma de onde parou.

### 4. Atualizar com as corridas novas

O backfill acima varre todas as sessões de todas as temporadas. Para a
rotina semanal existe o modo incremental, que usa uma marca d'água para
considerar apenas o que terminou depois da última execução:

```bash
python main.py --wave 2 --incremental
```

Uma requisição a `/sessions` por ano resolve a descoberta; temporada antiga
nem chega a ser consultada. Sessões que terminaram há menos de 24 h ficam
para a execução seguinte, porque a OpenF1 ainda está completando os dados.

Detalhes em [docs/ingestao-incremental.md](docs/ingestao-incremental.md).

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
backoff exponencial em 429 e 5xx.

Os demais códigos têm significados distintos e são tratados como tais:

| Código | Significado | Tratamento |
|--------|-------------|------------|
| 404 | `No results found` — combinação válida, sem dado | resultado vazio; grava arquivo de zero linhas e entra no checkpoint |
| 422 | resposta grande demais (teto medido ~250 mil registros / ~40 MB) | quebra recursiva por janela de tempo |
| 401 | sessão de F1 ao vivo bloqueia o acesso global sem autenticação | aborta; aguardar o fim da sessão |

Tratar 404 como falha seria um erro caro: a combinação nunca entraria no
checkpoint e voltaria a ser consultada em toda execução, indefinidamente.

A janela usada na quebra recebe folga de 2 h antes e 1 h depois do horário
agendado da sessão. O `date_start` de `/sessions` é o horário nominal, mas a
telemetria começa bem antes — em Bahrein 2024, 36% das amostras de `car_data`
estão antes dele. Sem a folga, toda quebra descartaria esse terço em silêncio.

---

## Licença e atribuição

Dados fornecidos pela [OpenF1](https://openf1.org), licenciados sob
CC BY-NC-SA 4.0. OpenF1 é um projeto não oficial, sem associação com as
empresas de Fórmula 1. F1, FORMULA 1 e marcas relacionadas são marcas
registradas da Formula One Licensing B.V. Este projeto tem finalidade
exclusivamente acadêmica.
