# Relatório de implementação — Pipeline F1 Medallion

Documentação do processo de construção do pipeline de dados, da definição do
escopo até os modelos. Registra o que foi feito, os comandos executados, os
problemas encontrados e as decisões tomadas em cada etapa.

**Projeto GCP:** `pdm-davi-2026`
**Repositório:** https://github.com/DavideSouzaAndrade/f1-medallion
**Período:** agosto a setembro de 2026

**Datasets:** `f1_bronze`, `f1_silver`, `f1_gold`. O prefixo é deliberado —
o projeto GCP é compartilhado com outras disciplinas, e nomes genéricos de
camada colidiriam com dados de terceiros.

---

## 1. Escolha do domínio e da fonte

O requisito de "dados massivos" orientou a escolha. Fontes clássicas de
Fórmula 1 (Ergast, datasets do Kaggle) contêm resultados e voltas desde 1950,
totalizando poucas centenas de milhares de linhas — volume insuficiente.

A alternativa adotada foi a **telemetria**, disponível pela API pública
[OpenF1](https://openf1.org). O endpoint `car_data` fornece velocidade, RPM,
marcha, acelerador, freio e DRS a uma taxa de amostragem de aproximadamente
3,7 Hz por piloto, o que gera milhões de registros por fim de semana de GP.

### Características da API

| Aspecto | Situação |
|---------|----------|
| Autenticação | Não exigida para dados históricos (2023+) |
| Custo | Gratuito no plano Community |
| Limite de requisições | 3/s e 30/min |
| Paginação | Inexistente — o controle de volume é feito por filtros |
| Formato | JSON ou CSV |
| Licença | CC BY-NC-SA 4.0 |

Uma limitação relevante foi descoberta durante a execução e está documentada
na seção 5.

---

## 2. Provisionamento da infraestrutura

### Recursos criados

- Bucket `f1-raw-pdm-davi-2026` (região `us-central1`)
- Datasets BigQuery `bronze`, `silver` e `gold` (localização `US`)
- Service account `f1-pipeline@pdm-davi-2026.iam.gserviceaccount.com`
- Cloud Run Job `f1-ingestao`

A região foi fixada no início do projeto. Bucket e datasets precisam ser
compatíveis: o BigQuery não realiza joins entre regiões distintas, e
transferência entre regiões gera custo adicional.

### Script de provisionamento

Todo o provisionamento foi automatizado em `setup/setup_gcp.sh`, escrito de
forma idempotente — pode ser executado repetidamente sem erro, o que permite
que qualquer integrante do grupo reproduza o ambiente.

```bash
export PROJECT_ID="pdm-davi-2026"
export BUCKET_NAME="f1-raw-pdm-davi-2026"
bash setup/setup_gcp.sh
```

O script habilita as APIs necessárias, cria bucket e datasets, provisiona a
service account com os papéis `storage.objectAdmin`, `bigquery.dataEditor` e
`bigquery.jobUser`, e publica o Cloud Run Job.

### Problema encontrado: propagação da service account

A primeira execução falhou:

```
ERROR: (gcloud.projects.add-iam-policy-binding) INVALID_ARGUMENT:
Service account f1-pipeline@pdm-davi-2026.iam.gserviceaccount.com does not exist.
```

**Causa:** a criação da service account é assíncrona no IAM. O script tentou
atribuir os papéis antes da propagação concluir.

**Correção:** laço de espera após a criação, verificando a existência da conta
antes de prosseguir. A segunda execução completou sem erro — comportamento
esperado de um script idempotente.

### Controle de custo

Orçamento configurado em US$ 20 com alertas em 50%, 90% e 100%, criado antes
de qualquer processamento. O crédito de teste da conta é de US$ 300, válido
por 90 dias.

---

## 3. Ingestão

### Arquitetura da solução

A ingestão foi implementada como um Cloud Run Job em Python. Ferramentas de
orquestração visual foram avaliadas e descartadas:

| Ferramenta | Motivo da rejeição |
|-----------|--------------------|
| Cloud Composer (Airflow) | ~US$ 300/mês apenas pelo ambiente ativo |
| Dataflow / Dataproc | Processamento ocorre no BigQuery; cluster ocioso gera custo |
| Data Transfer Service | Não consome APIs REST arbitrárias |
| Cloud Functions | Limite de tempo de execução insuficiente |

O Cloud Run Job aceita timeout de até 24 horas e executa código arbitrário,
atendendo aos requisitos de rate limiting, retry e checkpoint.

### Componentes

**`ingestion/openf1_client.py`** — cliente HTTP com:

- Limitador duplo: 25 requisições por minuto e intervalo mínimo de 0,5 s
  entre chamadas (margem de segurança sobre os limites de 3/s e 30/min)
- Retry com backoff exponencial em HTTP 429 e 5xx, até 5 tentativas
- Quebra recursiva por janela de tempo quando a resposta é grande demais
- Codificação manual da query string: datas da OpenF1 contêm `+`, que em
  query string seria interpretado como espaço

**`ingestion/storage.py`** — escrita em NDJSON comprimido:

- Conversão do array JSON da API para um objeto por linha, formato que o
  BigQuery carrega nativamente
- Particionamento Hive no caminho: `year=/session_key=/`
- Nenhum campo é renomeado, convertido ou filtrado

**`ingestion/main.py`** — orquestrador com execução por ondas.

### Estratégia de ondas

A ingestão foi dividida em ondas por prioridade, permitindo que o trabalho
de modelagem começasse antes do download completo.

| Onda | Endpoints | Requisições | Duração |
|------|-----------|-------------|---------|
| 1 | meetings, sessions, drivers | ~250 | minutos |
| 2 | laps, stints, pit, weather, position, race_control, session_result | ~1.750 | ~70 min |
| 3 | car_data (corridas) | ~1.000 | ~45 min |
| 4 | car_data (treinos e classificação) | ~4.000 | ~3 h |
| 5 | location | ~5.000 | ~3,5 h |
| 6 | starting_grid, team_radio, overtakes, championship_drivers, championship_teams | ~340 | ~15 min |

`intervals` passou para a onda 2, por ser preditor direto de degradação: o
gap ao carro da frente mede ar sujo, que destrói composto mais rápido.

As ondas 1 e 2 são suficientes para o modelo de degradação de pneu, que não
depende de telemetria. A onda 3 habilita o modelo de identificação de piloto.

As seis ondas cobrem os **18 endpoints** publicados pela OpenF1. A versão
inicial cobria 12; a revisão descrita na seção 10 acrescentou os outros seis.

### Idempotência

O checkpoint da ingestão é a **existência do objeto no GCS**: antes de cada
requisição, o script verifica se o arquivo já foi gravado e o pula em caso
afirmativo.

Isso produz três propriedades:

1. Execuções interrompidas retomam de onde pararam
2. Executar o mesmo comando duas vezes não duplica dados
3. Contorna o limite de tempo de execução: o trabalho pode ser dividido em
   várias execuções sucessivas

A propriedade foi verificada na prática: uma segunda execução da onda 2
reportou `pulados=7, gravados=0`.

### Manifesto

Cada requisição gera um registro em `_manifest/run_id=.../log.jsonl` com
endpoint, caminho do objeto, contagem de linhas, bytes, status e mensagem de
erro. Permite auditar a ingestão sem inspecionar o bucket arquivo a arquivo.

### Comandos de execução

```bash
gcloud run jobs execute f1-ingestao --region=us-central1 --args="--wave,1"
gcloud run jobs execute f1-ingestao --region=us-central1 --args="--wave,2"
gcloud run jobs execute f1-ingestao --region=us-central1 --args="--wave,3"
gcloud run jobs execute f1-ingestao --region=us-central1 --args="--wave,4"
gcloud run jobs execute f1-ingestao --region=us-central1 --args="--wave,5"
```

Acompanhamento:

```bash
gcloud run jobs executions list --job=f1-ingestao --region=us-central1
gcloud storage ls -r gs://$BUCKET_NAME/openf1/car_data/ | grep -c json.gz
gcloud storage du -s gs://$BUCKET_NAME/openf1/
```

### Resultado

Escopo final: temporadas **2024, 2025 e 2026**, os 18 endpoints.

- 12.538 objetos no bucket, 1,76 GB comprimidos
- 9.824 arquivos de telemetria em 2024–2025, mais 5.542 em 2026
- Nenhum erro registrado no manifesto das execuções de 2026

A ingestão de 2026 levou 6h20 nas seis ondas, contra as ~13 h estimadas.

---

## 4. Problemas encontrados durante a execução

### 4.1 Execuções simultâneas

Duas execuções da onda 4 foram disparadas acidentalmente. Os dados não foram
corrompidos — o checkpoint por existência de objeto impede duplicação. O
problema é o rate limit: cada processo aplica seu próprio limitador de 25
req/min, mas ambos compartilham o limite de 30 req/min da API, resultando em
HTTP 429 para as duas execuções.

**Procedimento adotado:** cancelar a execução mais recente e verificar antes
de disparar qualquer onda:

```bash
gcloud run jobs executions list --job=f1-ingestao \
  --region=us-central1 --filter="status.completionTime=null"
```

### 4.2 Bloqueio global durante sessão ao vivo

A tentativa de executar a onda 6 retornou HTTP 401:

```json
{"detail": "Live F1 session in progress. Global API access
(including past sessions) is restricted to authenticated users
until the session ends."}
```

**Causa:** durante uma sessão de F1 ao vivo, a OpenF1 restringe o acesso à
API **inteira** para usuários não autenticados — inclusive dados históricos.
O comportamento é mais amplo do que a documentação sugere, que menciona
restrição apenas à janela da sessão corrente.

**Impacto no projeto:** nenhum sobre a demonstração, já que o pipeline lê do
GCS e do BigQuery, não da API. Afeta apenas novas ingestões.

**Recomendação:** verificar o calendário da F1 antes de qualquer ingestão
adicional. Na data da apresentação (11/09/2026), o pipeline não depende da
API.

---

## 5. Camada Bronze

### Decisão: uma tabela por endpoint

A Bronze preserva a estrutura da origem. A API expõe endpoints com
granularidades distintas — `weather` é uma leitura por minuto, `laps` é uma
volta de um piloto, `car_data` é uma amostra a 3,7 Hz. Consolidá-los em uma
tabela única exigiria inventar uma estrutura inexistente na fonte, o que
seria transformação, papel da camada Silver.

Há também motivo de desempenho: tabelas separadas permitem que consultas a
`stints` (poucos milhares de linhas) não atravessem `car_data` (dezenas de
milhões).

### Decisão: payload preservado como STRING

A Bronze não interpreta o conteúdo. Cada linha do NDJSON é armazenada
integralmente em uma coluna `payload` do tipo STRING, acompanhada de colunas
de governança. A tipagem e o parse dos campos são responsabilidade da Silver.

Essa abordagem torna a camada resiliente: mudanças de schema na origem, ou
campos com tipo variável, não quebram a carga.

### Estrutura das tabelas

| Coluna | Tipo | Origem |
|--------|------|--------|
| `id_registro` | STRING | MD5 do payload |
| `year` | INT64 | Particionamento Hive do caminho |
| `session_key` | INT64 | Particionamento Hive do caminho |
| `payload` | STRING | Linha do NDJSON, íntegra |
| `_arquivo_origem` | STRING | Pseudo-coluna `_FILE_NAME` |
| `_data_ingestao_bronze` | TIMESTAMP | `CURRENT_TIMESTAMP()` na carga |

O `id_registro` usa MD5 do payload em vez de UUID: a carga passa a ser
determinística, e registros idênticos na origem recebem o mesmo
identificador, o que facilita a deduplicação na Silver.

`year` e `session_key` estão no *caminho* do arquivo, não dentro do JSON.
São recuperados por Hive partitioning e transformados em colunas
consultáveis — sem isso, seria impossível relacionar tabelas por sessão.

### Implementação

A carga ocorre em duas etapas, executadas por endpoint:

**Etapa 1 — Staging.** Tabela externa que lê cada linha do NDJSON como uma
STRING. Declarada com formato CSV, delimitador `\x01` (caractere ausente do
JSON) e sem caractere de aspas, o que impede o BigQuery de interpretar o
conteúdo.

**Etapa 2 — Materialização.** `CREATE TABLE AS SELECT` que adiciona as
colunas de governança, particiona por data de ingestão e clusteriza por
`session_key`. A staging é descartada em seguida: é intermediário técnico,
não camada.

A materialização em tabela nativa (em vez de manter tabelas externas) foi
escolhida por desempenho: arquivos gzip não são divisíveis, impedindo leitura
paralela. Como Silver e Gold consultarão essas tabelas repetidamente, o custo
de armazenamento (centavos por mês) compensa amplamente.

---

## 6. Estrutura do repositório

```
f1-medallion/
├── README.md
├── relatorio.md                  Este documento
├── ingestion/
│   ├── openf1_client.py          Rate limit, retry, semântica dos códigos HTTP
│   ├── storage.py                NDJSON.gz, checkpoint, manifesto, marca d'água
│   ├── config.py                 Ondas, anos, carência
│   ├── main.py                   CLI, incluindo o modo incremental
│   ├── test_incremental.py       34 verificações, sem rede
│   └── Dockerfile
├── setup/
│   ├── setup_gcp.sh              Provisionamento idempotente
│   └── ingerir_tudo.sh           Backfill por onda e ano
├── dataform/
│   ├── workflow_settings.yaml
│   ├── includes/endpoints.js     Catálogo dos 18 endpoints
│   └── definitions/
│       ├── bronze/               18 externas + 18 tabelas, geradas em JS
│       ├── silver/               4 tabelas tipadas
│       ├── gold/                 1 tabela por modelo
│       └── ml/                   2 modelos em BigQuery ML
└── docs/
    ├── ingestao-incremental.md   Marca d'água e atualização sem varredura
    ├── analise-exploratoria.md   9 blocos de EDA, 13 decisões de tratamento
    └── perfil-dados-bronze.md    Campo a campo dos 18 endpoints
```

## 7. Custos observados

| Item | Custo estimado |
|------|----------------|
| Armazenamento GCS (1,76 GB comprimidos) | ~US$ 0,04/mês |
| Armazenamento BigQuery | ~US$ 0,50/mês |
| Consultas | Dentro do 1 TB/mês gratuito |
| BigQuery ML | Dentro dos 10 GB/mês gratuitos |
| Cloud Run | Dentro do free tier |
| Tráfego de entrada (download da API) | Gratuito |

Total projetado abaixo de US$ 5, contra US$ 300 de crédito disponível.

**Riscos de custo identificados:** consultas sem filtro de partição nas
tabelas de telemetria, uso de BigQuery Editions em vez de on-demand, e
agendamentos deixados ativos por engano.

---

## 8. Camadas Silver e Gold

### Silver: tratar sem filtrar

A Silver tipa, deduplica e resolve o relacionamento temporal, mas **não
descarta uma linha sequer**. Ela expõe colunas de diagnóstico e deixa o
recorte para a Gold — assim uma decisão errada de filtro não se propaga
para todos os modelos.

| Tabela | Linhas | Papel |
|--------|-------:|-------|
| `silver_sessoes` | 377 | dimensão: corrida, teste, cancelada, futura |
| `silver_pilotos` | 7.733 | dimensão no grão (sessão, piloto) |
| `silver_voltas` | 190.579 | volta com pneu, box, clima e tráfego resolvidos |
| `silver_telemetria` | 165.272.123 | amostra a 3,7 Hz com a volta resolvida |

`silver_telemetria` ocupa 17 GB contra 51,6 GB da Bronze equivalente, com o
mesmo número de linhas: trocar o `payload` STRING por colunas tipadas
comprime muito melhor.

### Gold: uma tabela por modelo

| Tabela | Linhas | Modelo |
|--------|-------:|--------|
| `gold_degradacao_pneu` | 63.572 | regressão do tempo de volta |
| `gold_assinatura_piloto` | 74.577 | classificação do piloto |

O afunilamento é o que caracteriza a arquitetura funcionando: 326 milhões
de linhas brutas viram 138 mil linhas de treino.

### O *as-of join*

O problema central da Silver é ligar três granularidades: amostra a 3,7 Hz,
volta, e leitura de clima por minuto. A solução é definir a janela de cada
volta e casar por intervalo:

```sql
COALESCE(
  LEAD(date_start) OVER (PARTITION BY session_key, driver_number ORDER BY lap_number),
  TIMESTAMP_ADD(date_start, INTERVAL CAST(lap_duration * 1000 AS INT64) MILLISECOND)
) AS data_fim
```

A janela da última volta **não** fica aberta. Se ficasse, absorveria o
retorno ao box após a bandeirada — medido a 29 km/h de média — inflando
artificialmente a última volta de cada piloto.

Medido: dentro do período de voltas, **100% das amostras casam**. As 36% que
não casam são o carro na garagem antes da primeira volta cronometrada, a 18
km/h de média.

---

## 9. Modelos

Ambos treinados em BigQuery ML, em SQL, dentro da cota gratuita — 10 MB
faturados cada.

### Modelo 1 — Degradação de pneu (`LINEAR_REG`)

| Métrica | Valor |
|---------|------:|
| R² | **0,9553** |
| Erro absoluto médio | 1,505 s |
| Mediana do erro | 1,121 s |

Regressão linear e não árvore, de propósito: o objetivo é **interpretar o
coeficiente**, não maximizar acurácia.

| Variável | Coeficiente (s) |
|----------|----------------:|
| `temp_ar` | +0,1521 |
| `volta_corrida` | **−0,0467** |
| `umidade` | +0,0357 |
| `temp_pista` | +0,0293 |
| `gap_medio_a_frente` | +0,0120 |
| `volta_no_stint` | +0,0075 |
| `idade_pneu` | +0,0019 |

O resultado confirma quantitativamente o que a análise exploratória tinha
apontado: **`volta_corrida` tem coeficiente negativo** — o carro fica 0,047 s
mais rápido a cada volta, porque queima combustível. Esse efeito é cerca de
**25 vezes maior** que o da idade do pneu e tem sinal contrário. Sem ele no
modelo, a degradação sairia invertida.

**Ressalva honesta:** o R² de 0,955 vem principalmente do controle de
circuito, do combustível e da temperatura, não da degradação. E
`idade_pneu` e `volta_no_stint` são fortemente colineares — a idade é
`tyre_age_at_start + volta_no_stint − 1` —, então os dois dividem o mesmo
efeito, o que espalha o coeficiente entre eles. O efeito combinado é
~0,0094 s por volta de stint, abaixo do que se observa na prática em F1.
Separar melhor os dois exigiria tratar `tyre_age_at_start` como termo
próprio.

### Modelo 2 — Assinatura de pilotagem (`LOGISTIC_REG` multiclasse)

| Métrica | Valor |
|---------|------:|
| Acurácia | **0,1079** |
| ROC AUC | 0,7457 |
| Precisão | 0,1493 |
| Recall | 0,1550 |
| Acaso (28 classes) | 0,0357 |
| Baseline de maioria | 0,0518 |

O modelo é **3× melhor que o acaso** e 2× melhor que chutar sempre o piloto
mais frequente, e o ROC AUC de 0,746 indica discriminação real. Mas 10,8%
de acurácia é fraco em termos absolutos.

O cuidado central foi evitar vazamento: `driver_number`, `equipe` e
`nome_completo` ficaram **fora** das features. Incluir `equipe` reduziria o
problema de 28 para 14 opções e daria acurácia alta sem responder nada.

Três caminhos para melhorar, em ordem de retorno esperado:

1. **Agregar várias voltas por amostra.** Uma assinatura de 10 voltas é
   muito mais identificável que uma volta isolada.
2. **Features por setor** em vez de médias da volta inteira: o estilo se
   revela na frenagem de curvas específicas, não na média do circuito.
3. **Boosted tree** no lugar da regressão logística.

---

## 9. Próximas etapas

1. Avaliação dos dois modelos e ajuste de features
2. Agendamento da atualização incremental via Cloud Scheduler
3. Apresentação e ensaios cronometrados

## 10. Atribuição

Dados fornecidos pela [OpenF1](https://openf1.org), licenciados sob
CC BY-NC-SA 4.0. OpenF1 é um projeto não oficial, sem associação com as
empresas de Fórmula 1. F1, FORMULA 1 e marcas relacionadas são marcas
registradas da Formula One Licensing B.V. Este projeto tem finalidade
exclusivamente acadêmica.
