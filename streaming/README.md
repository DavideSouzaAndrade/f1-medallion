# Pipeline de streaming no Dataflow (parte 2)

Lê as voltas publicadas no Pub/Sub pelo gerador, aplica o contrato v1 e
grava no BigQuery: a volta válida em `f1_stream.voltas`, o evento rejeitado
em `f1_stream.voltas_dlq` junto com o motivo.

```
Pub/Sub f1-voltas-dataflow
      │
      ▼
  validar  ──── rejeitado ──▶ f1_stream.voltas_dlq  (evento cru + motivo)
      │
  normalizar, marcar apto_modelo, medir latência
      │
      ▼
  f1_stream.voltas  ──── recusado pelo BigQuery ──▶ voltas_dlq
```

O formato das mensagens está em
[docs/contrato-evento.md](../docs/contrato-evento.md); as decisões de
projeto, em [docs/plano-frente-b.md](../docs/plano-frente-b.md).

## Arquivos

| Arquivo | Papel |
|---------|-------|
| [`validacao.py`](validacao.py) | o contrato em código: tipos, regras, `apto_modelo`. Funções puras, sem rede |
| [`pipeline.py`](pipeline.py) | o grafo Beam e a CLI. Só liga as peças |
| [`test_validacao.py`](test_validacao.py) | 43 verificações offline, usando o gerador de verdade |
| [`setup.py`](setup.py) | empacota `validacao.py` para o worker |
| [`subir.sh`](subir.sh) / [`parar.sh`](parar.sh) | ligar e desligar o job |

## Como executar

### Testar o contrato, offline

```bash
python streaming/test_validacao.py
```

Não toca na nuvem, roda em menos de um segundo. Monta os eventos com o
próprio `producer/evento.py`: se o gerador e a validação divergirem do
contrato, a falha aparece aqui.

### Rodar na máquina, contra o Pub/Sub real

```bash
pip install -r streaming/requirements.txt
cd streaming && python pipeline.py --runner DirectRunner --duracao 120
```

O pipeline inteiro roda local, lendo a assinatura e gravando nas tabelas de
verdade — **sem provisionar worker, sem custo de Dataflow**. É a forma de
validar antes de ligar a máquina paga.

### Subir no Dataflow

```bash
bash streaming/subir.sh          # 1 worker, e2-standard-2
bash streaming/parar.sh          # drain
```

O job leva de 2 a 4 minutos até gravar o primeiro evento: o Dataflow ainda
provisiona a VM e instala as dependências.

**O job cobra por hora ligado, tenha mensagem ou não** — não escala a zero
como o Cloud Run. O `parar.sh` usa `drain`, que para de ler a assinatura e
deixa terminar o que está em voo.

Com o job parado, as mensagens ficam retidas 7 dias na assinatura e são
consumidas quando ele voltar. Para uma demonstração, publique antes e suba
o job: dá para ver a fila sendo esvaziada.

### Publicar eventos

```bash
gcloud run jobs execute f1-gerador --region=us-central1 \
  --args="--limite,50,--injetar-invalidos,0.1"
```

## Como conferir

```sql
-- o que entrou e quanto serve ao modelo
SELECT COUNT(*) AS linhas, COUNTIF(apto_modelo) AS aptas,
       COUNT(DISTINCT evento_id) AS eventos, ROUND(AVG(latencia_ms)) AS latencia_ms
FROM `pdm-davi-2026.f1_stream.voltas`;

-- rejeitados, por motivo
SELECT motivo, COUNT(*) AS eventos
FROM `pdm-davi-2026.f1_stream.voltas_dlq`
GROUP BY motivo ORDER BY eventos DESC;

-- batch e stream lado a lado
SELECT origem, COUNT(*) AS voltas
FROM `pdm-davi-2026.f1_silver.vw_voltas_unificada`
GROUP BY origem;
```

## Decisões

**`STREAMING_INSERTS` e não Storage Write API.** O Storage Write do Beam
monta um `NamedTuple` do Python a partir do schema, e `namedtuple` proíbe
nome de campo começando com underscore. A coluna `_ts_processamento`, que o
contrato exige, derruba a construção do pipeline com *"Field names cannot
start with an underscore"*. Renomeá-la quebraria o contrato, o schema do
Dataform e a `vw_voltas_unificada`. O `insertAll` manda JSON e não se
importa com o nome; cobra ~US$ 0,05 por GB, fração de centavo aqui.

**O schema é lido da tabela, não declarado em Python.** Declará-lo aqui
seria uma segunda fonte da verdade ao lado de
`definitions/stream/voltas.sqlx`, e as duas divergiriam na primeira coluna
nova. A leitura acontece ao lançar o job, na máquina que lança — se a
tabela não existir, falha ali, antes de provisionar nada.

**`CREATE_NEVER`.** As tabelas são do Dataform. Se faltar a tabela, o certo
é falhar, não criar uma sem partição nem cluster.

**Entrega pelo menos uma vez, deduplicação na leitura.** O mesmo
`evento_id` pode ser gravado duas vezes; a `vw_voltas_unificada` resolve com
`QUALIFY ROW_NUMBER()`. Garantir exatamente-uma-vez na escrita custaria mais
para resolver um problema que já estava resolvido.

**Duas saídas num `ParDo` só.** A mensagem é lida uma vez e cada evento sai
por exatamente um lado — válido ou DLQ. Duas transformações separadas leriam
tudo duas vezes.

**O ramo `FAILED_ROWS` também vai para a DLQ.** Uma linha pode passar na
validação e ainda assim o BigQuery recusá-la. Sem esse ramo ela sumiria em
silêncio, que é o pior desfecho: o evento não apareceria em lugar nenhum.

**Exceção inesperada vira DLQ, não travamento.** Se o erro for meu e não do
evento, o Dataflow reentregaria a mensagem indefinidamente e o pipeline
pararia num único evento. Melhor registrar e seguir.

**A validação ignora o atributo `injetado`.** O gerador marca os eventos
quebrados com ele, mas usá-lo seria trapaça: a validação olha só o conteúdo.
O atributo serve para conferir, depois, se os seis tipos chegaram à DLQ.

**`BundleBasedDirectRunner` no teste local.** O `DirectRunner` do Beam 2.77
tenta primeiro o Prism, que não implementa leitura de Pub/Sub: ele falha,
loga um traceback e só então volta para o runner Python. Pedir o
`BundleBased` direto pula o desvio.

## Resultados medidos (19/10/2026)

**Local, DirectRunner** — fila de 106 eventos acumulada pelo gerador:

| Medida | Valor |
|---|---|
| Eventos processados | 106 — nenhum perdido |
| Gravados em `voltas` | 85, sendo 81 aptos ao modelo |
| Rejeitados para a DLQ | 21, cobrindo os **6** tipos injetados |
| `evento_id` duplicado | nenhum |
| Linhas sem `_ts_processamento` | nenhuma |

As 4 voltas inaptas chegaram sem composto, duração e idade de pneu: foram
gravadas mesmo assim e marcadas, como manda o contrato.

**No Dataflow** — 1 worker `e2-standard-2`, Streaming Engine:

| Medida | Valor |
|---|---|
| Partida a frio até o primeiro evento gravado | ~3 min (provisionar VM e instalar dependências) |
| Latência com o job quente — mediana | **2,2 s** |
| p95 / máximo | 2,7 s / 2,8 s |
| Mínimo observado | 117 ms |
| Eventos do segundo lote | 72 publicados, 72 gravados (60 voltas + 12 DLQ) |

A latência é o `latencia_ms` da própria tabela: do `publish` no Pub/Sub à
linha no BigQuery. O primeiro lote depois de uma partida a frio carrega o
tempo de boot do worker — para uma demonstração, suba o job alguns minutos
antes de publicar.
