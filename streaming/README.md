# Pipeline de streaming no Dataflow (partes 2 e 3)

Lê o dado bruto da OpenF1 publicado no Pub/Sub, **monta a volta** — o mesmo
trabalho que a `silver_voltas` faz em lote — e grava no BigQuery: a volta em
`f1_stream.voltas`, a mensagem rejeitada em `f1_stream.voltas_dlq` junto com
o motivo.

```
Pub/Sub f1-openf1-dataflow   (7 topicos brutos, fora de ordem, ~27 mil msg/corrida)
      │
      ▼
  classificar  ──── fora do contrato ──▶ f1_stream.voltas_dlq
      │                                   (mensagem crua + motivo)
      │  (session_key, registro tipado)
      ▼
  montar  ── estado por sessao: dedup por _key/_id, janela da volta,
      │      stint, clima as-of, media de gap
      │
      │  volta fechada + carencia
      ▼
  f1_stream.voltas  ──── recusado pelo BigQuery ──▶ voltas_dlq
```

O formato das mensagens está em
[docs/contrato-openf1.md](../docs/contrato-openf1.md). As decisões de
projeto, em [docs/plano-frente-b.md](../docs/plano-frente-b.md), e o relato
do bug de reordenação em
[docs/caso-carencia-reordenacao.md](../docs/caso-carencia-reordenacao.md).

## Por que montar a volta aqui

No contrato v1 o Pub/Sub transportava a volta pronta e este pipeline só a
validava. No v2 ele transporta o dado bruto, exatamente como a OpenF1
entrega ao vivo, e **a montagem da volta sai do Dataform e vem para cá**.

Uma volta nasce de ~26 mensagens: 4 versões de `v1/laps` que se substituem
(só a última tem `lap_duration`), 1 de `v1/stints`, até 1 de `v1/pit`, e
~21 de `v1/intervals` que entram na média de gap. Elas chegam fora de ordem
e podem chegar repetidas.

O Dataform continua dono do lote e do histórico; o Dataflow faz o caminho ao
vivo. É a relação entre lote e streaming que a
[`vw_voltas_unificada`](../definitions/stream/vw_voltas_unificada.sqlx)
expõe numa consulta só.

## Arquivos

| Arquivo | Papel |
|---------|-------|
| [`contrato.py`](contrato.py) | o contrato em código: os 7 tópicos, tipagem, motivos de DLQ. Funções puras, sem rede |
| [`montagem.py`](montagem.py) | a `silver_voltas` em streaming: estado da sessão, dedup, janela, stint, clima, gap |
| [`pipeline.py`](pipeline.py) | o grafo Beam e a CLI. Só liga as peças |
| [`test_montagem.py`](test_montagem.py) | verificações offline, usando o gerador de verdade |
| [`setup.py`](setup.py) | empacota `contrato.py` e `montagem.py` para o worker |
| [`subir.sh`](subir.sh) / [`parar.sh`](parar.sh) | ligar e desligar o job |

## Como executar

### Testar a montagem, offline

```bash
python streaming/test_montagem.py
```

Não toca na nuvem, roda em menos de um segundo. Monta a corrida com o
próprio `producer/cronograma.py`: se o gerador e a montagem divergirem do
contrato, a falha aparece aqui.

O teste reproduz a **forma** do tempo real, e não só o conteúdo:
reordenação dentro de uma janela deslizante (como o Pub/Sub reordena) e
colheita intercalada (como o timer do Beam dispara). Rodar tudo e conferir
no fim passava com o código errado — ver
[docs/caso-carencia-reordenacao.md](../docs/caso-carencia-reordenacao.md).

### Rodar na máquina, contra o Pub/Sub real

```bash
pip install -r streaming/requirements.txt
cd streaming && python pipeline.py --runner DirectRunner --duracao 300
```

O pipeline inteiro roda local, lendo a assinatura e gravando nas tabelas de
verdade — **sem provisionar worker, sem custo de Dataflow**. É a forma de
validar antes de ligar a máquina paga.

### Subir no Dataflow

```bash
bash streaming/subir.sh          # 1 worker, e2-standard-2
bash streaming/parar.sh          # drain
```

O job se chama `f1-openf1-<data>`; o prefixo `f1-openf1-` é estável e é por
ele que o `subir.sh`, o `parar.sh` e o Airflow identificam o job.

O job leva de 2 a 4 minutos até gravar a primeira volta: o Dataflow ainda
provisiona a VM e instala as dependências.

**O job cobra por hora ligado, tenha mensagem ou não** — não escala a zero
como o Cloud Run. O `parar.sh` usa `drain`, que para de ler a assinatura e
deixa terminar o que está em voo.

Com o job parado, as mensagens ficam retidas 7 dias na assinatura e são
consumidas quando ele voltar.

**Suba o Dataflow antes do gerador.** `year`, `circuito` e `circuit_type`
são `NOT NULL` e vêm de `v1/sessions` e `v1/meetings`, que o gerador publica
1 minuto antes da largada. Com o job já de pé, o contexto chega primeiro.

### Publicar o replay

```bash
gcloud run jobs execute f1-gerador --region=us-central1 \
  --args="--session-key,11361,--minutos,45,--injetar-invalidos,0.01"
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

**A validação ignora o atributo `injetado`.** O gerador marca as mensagens
quebradas com ele, mas usá-lo seria trapaça: a validação olha só o conteúdo.
O atributo serve para conferir, depois, se os quatro tipos chegaram à DLQ.

**Uma chave de estado por `session_key`, não por piloto.** O Beam processa
uma chave num worker só, então isto serializa a corrida inteira num core. O
ganho é que clima e contexto da sessão (`year`, `circuito`, `circuit_type`)
valem para todos os pilotos: separá-los por piloto exigiria um *side input*
atualizável em streaming, que no Beam Python é frágil. O custo é folgado —
são ~110 mensagens/s no fator 30, contra as dezenas de milhares/s que o
`DoFn` sustenta. Para várias corridas ao mesmo tempo, o particionamento por
`session_key` já dá o paralelismo.

**O estado não guarda intervals nem voltas emitidas.** Intervals são 80% do
volume (21.955 por corrida) e servem a uma coluna só. O buffer guarda apenas
o que ainda pode cair numa volta aberta — ~2 voltas por piloto — e é podado
a cada emissão. Com isso o estado da sessão fica em ordem de KB, que é o que
sustenta a chave única por sessão.

**O que decide a volta é a emissão, não a chegada.** Clima vigente, média de
gap e fechamento da volta são todos resolvidos no momento de emitir, por
timestamp. Decidir na chegada tornava as três coisas dependentes da ordem de
entrega, que o Pub/Sub não garante — foi a raiz de três bugs de uma vez
([relato](../docs/caso-carencia-reordenacao.md)).

**Duas carências, não uma.** A volta é emitida 3 s depois de a própria
versão final chegar (`GRACA_S`). O início da volta seguinte só marca uma
carência longa, de 30 s (`GRACA_SEM_TEMPO_S`), que existe para as 3 voltas
por corrida que nunca ganham `lap_duration`. A premissa: **a carência tem
que ser maior que o atraso de reordenação** — a margem medida em produção é
de ~16x.

**`evento_id` determinístico, não UUID.** É
`{replay_id}:{session_key}:{driver}:{lap}`. No v1 cada mensagem era uma
volta e um UUID servia; no v2 uma volta nasce de ~26 mensagens e não existe
"a" mensagem. Além disso o UUID aleatório fazia a deduplicação da
`vw_voltas_unificada` nunca deduplicar nada, porque uma reentrega geraria
outro id.

**Volta já emitida não ressuscita.** Cada piloto lembra as voltas que já
saíram, então uma reentrega tardia não grava uma segunda linha. É um
conjunto e não uma marca d'água: com marca d'água, uma volta que saísse fora
de ordem barraria todas as anteriores, e elas se perderiam em silêncio.

**Volta sem contexto da sessão espera, e só depois vai para a DLQ.**
`year` e `circuito` são `NOT NULL`. Mandar a volta para a DLQ porque a
mensagem de contexto ficou dois segundos atrás seria perder dado por nada:
a volta espera até o prazo de abandono.

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
