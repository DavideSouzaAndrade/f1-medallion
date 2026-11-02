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
[docs/contrato-openf1.md](../docs/contrato-openf1.md), e o da volta gravada
em [docs/contrato-evento.md](../docs/contrato-evento.md). As decisões de
projeto estão na seção [Decisões](#decisões), e o relato do bug de
reordenação em
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
| [`auditar.sh`](auditar.sh) | confere o que foi gravado, contra as regras e contra o lote |

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

### Auditar o que foi gravado

```bash
bash streaming/auditar.sh                      # a última execução
bash streaming/auditar.sh replay-2026...-abc   # uma específica
```

Os testes offline provam que a **regra** está certa; a auditoria prova que
o que chegou ao BigQuery **respeita** a regra. São coisas diferentes, e só
a segunda pega erro de escrita, de schema ou de deduplicação.

Treze verificações, entre integridade (`evento_id` duplicado, chave natural
duplicada, `NOT NULL` vazia), faixas (`lap_number`, `duracao_s`,
`idade_pneu`), coerência (soma dos setores contra a duração), contrato do
modelo (apta sem previsão, previsão em volta inapta) e comparação com a
`silver_voltas` da mesma corrida.

A comparação com o lote olha **só as voltas completas**: a volta cortada
pelo fim do replay não tem duração e o lote tem, e divergir ali é esperado,
não defeito.

Sai com código 1 se qualquer verificação falhar — dá para pendurar numa
tarefa do Airflow.

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

**O drain não é instantâneo.** Medido: o Dataflow reavalia o estado do job a
cada 10 minutos — o log do worker diz literalmente *"Job is in state
JOB_STATE_DRAINING, will check again in 10m0s"* — e a cobrança só termina
quando as VMs são liberadas. Dois caminhos:

```bash
bash streaming/parar.sh             # pede o drain e sai
ESPERAR=1 bash streaming/parar.sh   # pede e espera o job sumir da lista
MODO=cancel bash streaming/parar.sh # mata na hora, para o que não pode esperar
```

A DAG usa `ESPERAR=1`, para que "a DAG terminou" signifique "paramos de
pagar" — e para que a execução seguinte não esbarre num job ainda ativo.

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

**`apto_modelo` é o recorte inteiro da Gold, inclusive o corte de 1,15x a
mediana.** Separar "apta ao modelo" de "dentro da faixa" foi um erro: a
`gold_degradacao_pneu` trata o filtro como um só, e o modelo nunca viu uma
volta de safety car. Unificar consertou três coisas de uma vez — a volta sem
previsão passou a ser só a que a API não atendeu (antes, 43 de 106 voltas
apareciam como predição pendente no painel do BI sem nada estar errado),
`volta_no_stint` passou a numerar o mesmo conjunto que a Gold numera, e a
predição deixou de precisar de um campo auxiliar viajando com a linha.

**A linha guarda de onde veio, mas não se ainda está chegando.** O contrato
v2 carrega o atributo `fonte` (`replay` | `openf1`) em toda mensagem, e o
pipeline o descartava — o BigQuery guardava o dado e perdia o contexto
dele. Agora é a coluna `fonte`.

Procedência e atividade são coisas diferentes e só uma pertence à linha:

| | o que é | onde mora | muda? |
|---|---|---|---|
| procedência | replay ou feed real | coluna `fonte` | nunca |
| atividade | acontecendo agora ou acabou | derivado de `MAX(ts_evento)` | sozinha |

Um replay encerrado **continua sendo replay** — ele só deixa de estar em
curso. Gravar atividade numa coluna criaria dado que mente com o passar do
tempo: uma volta gravada às 23:00 estava em curso às 23:00 e é histórico às
23:10, sem que nada nela mude. Quem deriva isso é a API de BI, contra
`EM_CURSO_S`.

**Volta sem contexto da sessão espera, e só depois vai para a DLQ.**
`year` e `circuito` são `NOT NULL`. Mandar a volta para a DLQ porque a
mensagem de contexto ficou dois segundos atrás seria perder dado por nada:
a volta espera até o prazo de abandono.

**`BundleBasedDirectRunner` no teste local.** O `DirectRunner` do Beam 2.77
tenta primeiro o Prism, que não implementa leitura de Pub/Sub: ele falha,
loga um traceback e só então volta para o runner Python. Pedir o
`BundleBased` direto pula o desvio.

## Resultados medidos (01/11/2026)

Corrida 11361 (GP da Itália 2026), 45 minutos de replay no fator 30, 3.637
mensagens, 1% corrompidas de propósito.

### Corretude — comparado volta a volta com a `silver_voltas`

| coluna | voltas iguais ao lote |
|---|---:|
| `duracao_s` | **127/127** |
| `composto` | **127/127** |
| `idade_pneu` | **127/127** |
| `temp_pista` | **127/127** |
| `gap_medio_a_frente` | **127/127** |
| `eh_saida_box` | **127/127** |
| `data_inicio` | **127/127** |
| `eh_entrada_box` | 106/127 |

| | |
|---|---|
| linhas / `evento_id` distintos | **127 / 127** — zero duplicatas |
| voltas sem contexto da sessão | 0 |
| DLQ | 40, exatamente as 40 injetadas, nos 4 motivos do contrato |

As 21 divergências de `eh_entrada_box` não têm correção: esta corrida teve
bandeira vermelha na volta 3, e os registros de `v1/pit` dessas paradas têm
data **2.137 s depois da largada** — meia hora depois da volta terminar. Ao
vivo é impossível saber. É o argumento para a DAG de reprocessamento em
lote existir.

### Latência

Do `publish_time` da versão final da volta no Pub/Sub até a linha gravada no
BigQuery, incluindo a chamada à API de predição.

| | M4 local (`DirectRunner`) | **Dataflow + Streaming Engine** |
|---|---:|---:|
| mínimo | 4,10 s | **2,68 s** |
| mediana | ~60 s | **4,60 s** |
| p95 | 97,0 s | 8,22 s |
| máximo | 99,0 s | 8,87 s |

O `DirectRunner` é um processo único que intercala processar elementos e
disparar timers; enquanto mastiga uma rajada de 1.325 `intervals`, o timer
que devia disparar em 3 s dispara 60 s depois. O Streaming Engine mantém
estado e timers fora do worker e os dispara no prazo.

**E ele não foi só mais rápido: foi mais correto.** A corretude da montagem
depende da carência ser maior que o atraso de reordenação, e com o runner
lento o atraso que importa passa a ser o da fila interna dele. No
`DirectRunner` o mesmo replay deu 110/118 em `temp_pista` e 105/118 em
`gap`. Detalhe em
[docs/caso-carencia-reordenacao.md](../docs/caso-carencia-reordenacao.md).

### Predição

| | |
|---|---|
| latência da API quente | 260–344 ms |
| partida a frio | 37 s de container (33 s carregando o SavedModel), e o Cloud Run levou mais de 1 min só para alocar instância |
| erro em volta de corrida normal | **1,32 s** sobre 88,5 s (1,5%) |
| erro na volta de largada parada | 6,73 s |

A partida a frio não cabe no timeout de leitura de 10 s do pipeline — é por
isso que a DAG aquece a API **antes** de disparar o gerador, e é por isso
que o dia da apresentação pede `DEMO=1 bash api/deploy.sh`.

### Corrida inteira, pelo Airflow, na nuvem (02/11/2026)

A corrida 11361 completa (111 minutos, fator 10 → ~14 min de replay),
disparada pela API REST do Airflow na VM.

| | |
|---|---|
| voltas / `evento_id` distintos | **1.055 / 1.055** — a corrida inteira, zero duplicata |
| voltas completas iguais ao lote | **1.052 / 1.052** em `duracao_s`, `composto`, `temp_pista` |
| soma dos setores vs duração | **1.003 / 1.003** dentro de 1 s |
| voltas aptas / com previsão | **938 / 938** — zero aguardando |
| latência mediana / p95 | **1,70 s** / 3,35 s |
| erro do modelo (MAE) | **1,13 s** sobre voltas de ~88 s (1,3%) |
| DLQ | 265 injetadas, nos 4 motivos do contrato |
| procedência gravada | `replay` em 1.055 de 1.055 |

A latência caiu 2,4× em relação à execução anterior (4,15 s), e o motivo é
o ritmo: a 32 mensagens/s a reordenação cabe na carência de 1 s da volta
completa, o que não acontecia a 95 mensagens/s. **Replay mais lento é
replay mais correto e mais rápido de processar** — o oposto do que a
intuição sugere.

Auditoria completa: `bash streaming/auditar.sh`.

### Execução anterior, recortada em 45 minutos (01/11/2026, 21:11 UTC)

A mesma corrida, disparada pela DAG `f1_pipeline_fim_a_fim` num Airflow
containerizado numa `e2-medium` do Compute Engine — não da máquina de
desenvolvimento.

| | |
|---|---|
| voltas / `evento_id` distintos | **149 / 149** — zero duplicatas |
| voltas completas iguais ao lote | **127 / 127** em `duracao_s`, `composto`, `idade_pneu`, `temp_pista` |
| voltas aptas / aguardando predição | 43 / **0** |
| erro do modelo (MAE) | 4,09 s, e **1,32 s** na volta de corrida normal |
| latência mediana / p95 | **5,02 s** / 6,87 s |

As 22 voltas que sobram são a **última de cada piloto**, com `duracao_s` e
`latencia_ms` nulos. Não é falha: o replay foi cortado em 45 minutos de
corrida, no meio dessas voltas. O stream grava o que sabe — a volta começou
e não terminou — e a latência fica nula porque nunca houve `publish_time`
de versão final para medir contra. São as duas correções do dia visíveis
juntas: antes do `ABANDONO_S` cair de 600 s para 120 s, essas voltas nem
eram emitidas, porque o pipeline drenava antes do prazo.

### Custo

Cada rodada de medição custou cerca de **US$ 0,04**: 1 worker
`e2-standard-2` por ~20 minutos, mais Streaming Engine e inserções no
BigQuery. O gerador é um Cloud Run Job e escala a zero.

O drain leva de 2 a 12 minutos (o Dataflow reavalia o estado a cada 10) e a
cobrança só termina quando as VMs saem — por isso a DAG espera.
