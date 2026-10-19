# Plano da frente B — pipeline Dataflow

Registro das decisões e do passo a passo da parte do streaming que lê o
Pub/Sub e grava no BigQuery. O formato das mensagens está em
[contrato-evento.md](contrato-evento.md); este documento é o *como*.

---

## O lugar da frente B

```
Fred (frente A)                    frente B                     Davi (frente C)
───────────────                    ────────                     ───────────────
Cloud Run Job f1-gerador
  lê silver_voltas
  monta o evento v1
  publica  ──────────▶ Pub/Sub f1-voltas
                          │
                          ├─ f1-voltas-dataflow ──────────▶ [ ESTE PIPELINE ]
                          │                                    │ valida
                          └─ f1-voltas-debug                   │ normaliza
                             (inspeção manual)                 │ enriquece
                                                               ├──▶ f1_stream.voltas
                                                               └──▶ f1_stream.voltas_dlq
                                                                     │
                                         f1_silver.vw_voltas_unificada
                                                                     
                                                  parte 3 ─ ─▶ API /predict
```

As duas pontas estão congeladas pelo contrato. A liberdade da frente B está
só na implementação.

## Por que Dataflow e não outra coisa

| | Dataform | Dataflow |
|---|---|---|
| Linguagem | SQL | Python ou Java (Apache Beam) |
| Onde processa | dentro do BigQuery | em VMs que o serviço provisiona |
| Modo | roda e termina | fica ligado, consumindo a assinatura |
| Papel aqui | constrói bronze → silver → gold | lê do Pub/Sub e grava no BigQuery |

O Pub/Sub tem um modo nativo (*BigQuery subscription*) que grava direto na
tabela sem Dataflow nenhum, e custaria zero de worker. Foi descartado porque
não valida nem enriquece: não daria para calcular `apto_modelo`, nem
`latencia_ms`, nem separar a DLQ por motivo — que é justamente o que o
contrato pede.

O job de streaming **não escala a zero**: cobra por hora de worker ligado,
tenha mensagem ou não. Daí os scripts de subir e parar.

## Decisões

| Item | Escolha | Porquê |
|---|---|---|
| Runner | Dataflow, Beam em Python | pipeline próprio, código versionado no repo |
| Leitura | assinatura `f1-voltas-dataflow` | a de produção; a `-debug` é para inspeção manual |
| Escrita | `WriteToBigQuery`, `STREAMING_INSERTS` | o Storage Write API não aceita a coluna `_ts_processamento` — ver *Achados da implementação* |
| Disposição | `CREATE_NEVER` + `WRITE_APPEND` | a tabela é do Dataform; o Dataflow nunca cria nem recria |
| Workers | `--max-num-workers=1`, máquina pequena | controle de custo |
| Ciclo de vida | `drain` para parar | termina o que está em voo antes de desligar |

**Service account `f1-dataflow`**, separada da `f1-pipeline` do batch: se o
pipeline errar o alvo de uma escrita, o estrago fica numa identidade que é
só da frente B. O projeto é compartilhado com outras disciplinas.

## Desenho do pipeline

```
ReadFromPubSub (bytes crus da assinatura)
        │
        ▼
ParDo(validar)  ─── saída 'dlq' ──▶ {evento_bruto, motivo, _ts_processamento}
        │                                        │
   saída principal                               ▼
        │                              WriteToBigQuery(voltas_dlq)
        ▼
  evento normalizado
  + apto_modelo
  + _ts_processamento
  + latencia_ms
        │
        ▼
WriteToBigQuery(f1_stream.voltas)
        │
        └── FAILED_ROWS ──▶ voltas_dlq (motivo "rejeitado pelo BigQuery")
```

O ramo `FAILED_ROWS` existe porque uma linha pode passar na validação e
ainda assim o BigQuery recusá-la — um `NOT NULL` que escapou, por exemplo.
Sem esse ramo ela desapareceria em silêncio.

### Ordem da validação

A ordem determina o `motivo` gravado. As strings são exatamente as do
`definitions/stream/voltas_dlq.sqlx`, para que a consulta de diagnóstico
agrupe corretamente.

| # | Checagem | `motivo` |
|---|----------|----------|
| 1 | o corpo é JSON válido | `JSON inválido` |
| 2 | campos obrigatórios presentes | `campo obrigatório ausente` |
| 3 | tipos corretos | `tipo errado` |
| 4 | `lap_number > 0` | `lap_number <= 0` |
| 5 | `duracao_s > 0` quando presente | `duracao_s <= 0` |
| 6 | composto na lista, **após** normalizar | `composto fora da lista` |

A normalização vem antes do passo 6 de propósito: `" medium "` deve virar
`MEDIUM` e passar; `SUPERSOFT` em maiúscula continua inválido.

O gerador marca os eventos quebrados com o atributo Pub/Sub `injetado`.
**Esse atributo não entra na validação** — serve só para conferir, depois,
se os seis tipos chegaram à DLQ. A validação olha só o conteúdo.

### Campos acrescentados

- `apto_modelo` — composto em (SOFT, MEDIUM, HARD), `duracao_s`,
  `idade_pneu` e `temp_pista` presentes, e volta fora do box. A volta
  inapta **é gravada mesmo assim**, como na Silver: trata, não filtra.
- `_ts_processamento` — instante em que o Dataflow processou.
- `latencia_ms` — `_ts_processamento` menos `ts_evento`.

`tempo_previsto_s` e `modelo_versao` ficam nulos: são da parte 3, quando o
Dataflow passar a chamar a API.

## Estrutura dos arquivos

```
streaming/
├── validacao.py        funções puras, sem rede — espelha producer/evento.py
├── pipeline.py         o pipeline Beam e a CLI
├── test_validacao.py   testes offline dos seis tipos inválidos
├── requirements.txt
├── subir.sh            lança o job no Dataflow
├── parar.sh            drena o job
└── README.md
```

A lógica fica separada do pipeline pelo mesmo motivo que o Fred separou
`evento.py` de `publisher.py`: função pura se testa offline, de graça, em
segundos.

## Fases

Ordenadas para que o máximo de defeito apareça antes de ligar um worker pago.

| Fase | O que | Custo |
|------|-------|-------|
| 0 | branch, API do Dataflow, service account, papéis, pasta de staging | zero |
| 1 | `validacao.py` + testes offline | zero |
| 2 | pipeline no DirectRunner, na máquina local, contra o Pub/Sub e o BigQuery reais | zero de Dataflow |
| 3 | job no Dataflow, consumindo eventos novos em tempo real | passa a cobrar |
| 4 | `subir.sh`, `parar.sh`, `streaming/README.md` e os marcadores do README da raiz | zero |

A fase 2 é a que de-arrisca o projeto: o pipeline inteiro roda de verdade,
lendo a assinatura e gravando nas tabelas, sem nenhum worker provisionado.

## Custo

| Cenário | Custo |
|---|---|
| Fases 0 a 2 | nada de Dataflow |
| Job ligado, 1 worker pequeno | ~US$ 1,20 a 2,00 por dia |
| Demonstração de 1 hora | ~US$ 0,08 |

Com o job parado, as mensagens ficam retidas 7 dias na assinatura. Ao
religar, o pipeline consome o acumulado — o que é conveniente para a
demonstração: publica-se antes e mostra-se o job esvaziando a fila.

## Estado da infraestrutura

Verificado em 19/10/2026, projeto `pdm-davi-2026`:

| Recurso | Situação |
|---|---|
| `dataflow.googleapis.com` | habilitada |
| SA `f1-dataflow@pdm-davi-2026.iam.gserviceaccount.com` | criada |
| Papéis da SA | `dataflow.worker`, `pubsub.subscriber`, `editor` |
| `gs://f1-raw-pdm-davi-2026/dataflow/` | criada, para staging e temp |
| Tópico `f1-voltas` e as duas assinaturas | existem |
| `f1_stream.voltas` e `voltas_dlq` | criadas pelo Dataform, vazias |
| Rede | VPC `default`; a org policy permite IP externo, que é o padrão dos workers |
| Beam | 2.77.0 em Python 3.14.3 — o Dataflow suporta 2.73 a 2.77 em Python 3.10 a 3.14 |

> O `roles/editor` na service account é mais amplo do que esta frente
> precisa: dá escrita em todo o projeto compartilhado. Foi mantido por
> decisão do time. Para apertar, basta trocá-lo por `bigquery.dataEditor` e
> `storage.objectAdmin` e escopar o `pubsub.subscriber` na assinatura.


---

## Achados da implementação

Defeitos que só apareceram ao executar, e que justificam a fase 2 existir
antes da 3. Nenhum deles seria visível numa revisão de código.

**O Storage Write API não aceita coluna com underscore.** Era a escolha
inicial, por ser a API nova e mais barata. O Beam monta um `NamedTuple` do
Python a partir do schema, e `namedtuple` proíbe nome de campo começando
com `_`: a coluna `_ts_processamento`, exigida pelo contrato, derruba a
montagem do pipeline com *"Field names cannot start with an underscore"*.
Renomeá-la quebraria o contrato, o schema do Dataform e a
`vw_voltas_unificada`. Trocado por `STREAMING_INSERTS`, que manda JSON e
não se importa com o nome da coluna. Cobra cerca de US$ 0,05 por GB
inserido, fração de centavo neste volume.

**O worker não receberia o `validacao.py`.** O Dataflow envia apenas o
arquivo principal. Sem o `setup.py` ao lado, empacotando o módulo, o job
sobe e morre no primeiro evento com `ModuleNotFoundError`.

**Um venv de Python 3.12 em diante não traz `setuptools`.** O Beam o usa
para construir o pacote do `setup.py` ao lançar o job. Sem ele o lançamento
falha antes de provisionar qualquer VM — de graça, mas sem explicar o
motivo com clareza. `setuptools` e `build` entraram no `requirements.txt`.

**O `DirectRunner` do Beam 2.77 tenta o Prism primeiro.** O Prism não
implementa leitura de Pub/Sub: falha, registra um traceback e só então o
Beam volta ao runner Python. Funciona, mas enche o log de erro que não é
erro. Pedir `BundleBasedDirectRunner` diretamente pula o desvio.

**`wait_until_finish(duration=...)` não existe no DirectRunner**, apesar de
a assinatura comum aos runners sugerir que sim, e o `cancel()` avisa na
própria docstring que não espera os workers desligarem — a leitura do
Pub/Sub continua e o processo não termina. A opção `--duracao` cancela e,
após uma carência, encerra o processo na marra. É conveniência de teste
local; no Dataflow quem desliga é o `parar.sh`.

## Resultados medidos

**Fase 2**, fila de 106 eventos acumulada pelo gerador, consumida no
DirectRunner sem provisionar worker:

| Medida | Valor |
|---|---|
| Eventos processados | 106 — nenhum perdido |
| Gravados em `voltas` | 85, sendo 81 aptos ao modelo |
| Rejeitados para a DLQ | 21, cobrindo os seis tipos injetados |
| `evento_id` duplicado | nenhum |
| Linhas sem `_ts_processamento` | nenhuma |

As quatro voltas inaptas chegaram sem composto, sem duração e sem idade de
pneu: foram gravadas mesmo assim e marcadas, como manda o contrato.

A DLQ por motivo, confirmando que a validação distingue os seis casos:

| Motivo | Eventos |
|---|---:|
| JSON inválido | 8 |
| `duracao_s <= 0` | 6 |
| campo obrigatório ausente | 3 |
| `lap_number <= 0` | 2 |
| composto fora da lista | 1 |
| tipo errado | 1 |

**Fase 3**, no Dataflow, 1 worker `e2-standard-2` com Streaming Engine:

| Medida | Valor |
|---|---|
| Partida a frio até o primeiro evento gravado | ~3 minutos |
| Latência com o job quente — mediana | 2,2 s |
| p95 / máximo | 2,7 s / 2,8 s |
| Mínimo observado | 117 ms |
| Segundo lote | 72 mensagens publicadas, 72 gravadas |

A latência vem do campo `latencia_ms` da própria tabela, medida do
`publish` no Pub/Sub à linha no BigQuery. O primeiro lote após uma partida
a frio carrega o tempo de boot do worker: numa demonstração, convém subir o
job alguns minutos antes de publicar.

O `drain` foi exercitado ao fim: o job sai de `Running`, as VMs são
liberadas e a cobrança termina aí.
