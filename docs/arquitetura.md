# Arquitetura do F1 Medallion

Este documento descreve o sistema inteiro: o que ele faz, como as peças se
encaixam, e por que cada decisão foi tomada. Os documentos existentes
descrevem cada componente; este descreve o **conjunto**.

---

## O que o sistema faz

Reproduz uma corrida de Fórmula 1 como se ela estivesse acontecendo agora,
monta cada volta em tempo real a partir do dado bruto, pede ao modelo uma
previsão do tempo daquela volta, e grava previsão e tempo real na mesma
linha — para que se possa comparar os dois enquanto a corrida corre.

Em paralelo, o histórico de 245 mil voltas de 495 sessões (2023–2026) é
processado em lote, e é dele que o modelo é treinado.

A relação entre os dois caminhos é o ponto do trabalho, não um detalhe: o
lote dá o histórico e o modelo; o streaming dá o agora; a view
`vw_voltas_unificada` mostra os dois no mesmo formato.

---

## O caminho do dado, de ponta a ponta

```
                    OpenF1 (API pública)
                            │
          ┌─────────────────┴─────────────────┐
          │ LOTE                              │ TEMPO REAL
          ▼                                   ▼
   Cloud Run Job                       Cloud Run Job
   f1-ingestao                         f1-gerador
   (JSON → GCS)                        (replay da Bronze → Pub/Sub)
          │                                   │
          ▼                                   ▼
   GCS particionado                    Pub/Sub  f1-openf1
   year=/session_key=                  7 tópicos brutos, ~27 mil msg/corrida
          │                                   │
          ▼                                   ▼
   BigQuery: tabelas externas          Dataflow  f1-openf1-<data>
   f1_bronze.bronze_*                  (Beam Python, Streaming Engine)
          │                                   │
          ▼                                   │  monta a volta
   Dataform                                   │  ~26 mensagens → 1 linha
   silver_* → gold_*                          │
          │                                   ├──► POST /predict ──┐
          ▼                                   │    Cloud Run       │
   BigQuery ML                                │    f1-api          │
   modelo_degradacao_pneu                     │◄───────────────────┘
          │                                   │    previsão anexada
          ▼                                   ▼
   Vertex AI Model Registry            f1_stream.voltas    f1_stream.voltas_dlq
          │                                   │
          ▼                                   │
   EXPORT MODEL → GCS                         │
          │                                   │
          └────► Cloud Run f1-api ◄───────────┘
                                              │
                    f1_silver.vw_voltas_unificada
                    (lote + stream no mesmo formato)
                                              │
                                              ▼
                        Cloud Run  f1-bi-api  →  f1-bi-frontend

            Tudo orquestrado por Airflow numa VM e2-medium
```

---

## As três frentes

O trabalho foi dividido em três frentes com contratos escritos entre elas.
Cada contrato é um documento, não uma conversa.

| frente | responsável | entrega | contrato que publica |
|---|---|---|---|
| **A — gerador** | Fred | replay da corrida no Pub/Sub | [`contrato-openf1.md`](contrato-openf1.md) |
| **B — Dataflow** | Gustavo | monta a volta, prevê, orquestra | [`contrato-evento.md`](contrato-evento.md), tabelas de `f1_stream` |
| **C — BI e modelo** | Davi | API de predição, API de BI, front | [`bi-api/AIRFLOW.md`](../bi-api/AIRFLOW.md) |

O que torna isso funcional: **as frentes se testam contra o contrato da
outra, não contra a implementação**. O teste do Dataflow monta a corrida
chamando o `producer/cronograma.py` do gerador — se as duas pontas
divergirem, a falha aparece na máquina de quem desenvolve, antes de
qualquer deploy.

---

## Camada por camada

### Bronze — o dado como a origem entregou

JSON da OpenF1 em GCS, particionado por `year=` e `session_key=`, exposto
ao BigQuery como tabela externa. **Nada é transformado aqui.** Um campo
numérico que veio como `"+1 LAP"` continua `"+1 LAP"`.

A razão é auditoria: quando uma transformação parecer errada, é preciso
poder voltar ao que a origem disse.

### Silver — tratado, sem filtrar

Uma linha por `(sessão, piloto, volta)`, com o contexto resolvido: qual
pneu, qual clima, quanto tráfego, entrou no box?

A regra da camada: **tratar sem filtrar.** A volta sob safety car é
marcada, não removida — `razao_mediana_sessao` diz o quanto ela destoa, e
quem consome decide. Remover aqui tiraria de quem vem depois a chance de
decidir diferente.

O trabalho pesado está em `silver_voltas`: ela resolve a janela temporal de
cada volta, casa o stint com o desempate de pneu fantasma, faz o *as-of
join* do clima, e agrega o gap médio dos `intervals` medidos durante a
volta. 245.298 linhas.

### Gold — recortado para um fim

`gold_degradacao_pneu` é a tabela de treino: só volta limpa de corrida, com
pneu seco, fora do box e dentro de 1,15× a mediana da sessão. 85.982 linhas
de 245.298 — o recorte descarta dois terços, e é isso que faz o modelo
funcionar.

O corte de 1,15 é mais frouxo que o 1,07 da Silver de propósito: o corte
apertado descartaria justamente as voltas mais degradadas, que são o
fenômeno que se quer modelar.

### ML — regressão linear, e isso é uma escolha

`LINEAR_REG` do BigQuery ML sobre idade de pneu, posição no stint, volta da
corrida, composto, gap médio, temperatura e circuito.

Linear e não uma árvore porque **o coeficiente é o produto**: o que
interessa é quanto a degradação custa por volta de pneu, e um número
interpretável vale mais aqui que meio ponto de erro.

A variável `volta_corrida` não é enfeite: sem ela, a regressão atribui ao
pneu um efeito que é do tanque de combustível, e o sinal chega a inverter.

### Stream — o agora

`f1_stream.voltas` tem o mesmo grão da `silver_voltas` e é escrita pelo
Dataflow. O Dataform **não a recria**: as definições usam
`CREATE TABLE IF NOT EXISTS`, porque uma tabela normal do Dataform é
reconstruída a cada execução e apagaria tudo que o streaming gravou.

---

## O caminho em tempo real, em detalhe

### O que trafega

O Pub/Sub transporta o **dado bruto da OpenF1**, não a volta pronta. Sete
tópicos: `laps`, `stints`, `weather`, `intervals`, `pit`, `sessions`,
`meetings`. ~27 mil mensagens por corrida.

Uma volta nasce de ~26 mensagens: 4 versões de `v1/laps` que se substituem
(só a última tem `lap_duration`), 1 de `v1/stints`, até 1 de `v1/pit`, e
~21 de `v1/intervals`.

Elas chegam **fora de ordem** e podem chegar **repetidas**. O Pub/Sub não
garante nenhuma das duas coisas.

### Como a volta é montada

Um `DoFn` com estado, chaveado por `session_key`:

1. **Deduplica** por `_key`, ficando com a versão de maior `_id`.
2. **Acumula** o contexto: stints, clima, medições de gap.
3. **Fecha** a volta quando a versão final chega, espera uma carência curta
   pelos atrasados, e emite.
4. **Poda** o que não serve mais — o estado da corrida inteira cabe em
   ordem de KB.

Três decisões sustentam o desenho:

**Uma chave por sessão, não por piloto.** Clima e contexto da sessão valem
para todos os pilotos; separá-los exigiria um *side input* atualizável em
streaming, frágil no Beam Python. O custo é serializar a corrida num core —
folgado, contra ~32 mensagens/s.

**O estado não guarda intervals.** São 80% do volume e servem a uma coluna.
O buffer guarda só o que ainda pode cair numa volta aberta.

**O que decide a volta é a emissão, não a chegada.** Clima vigente, média
de gap e fechamento são resolvidos no momento de emitir, por timestamp.
Decidir na chegada tornava os três dependentes da ordem de entrega — foi a
raiz de três bugs de uma vez
([relato](caso-carencia-reordenacao.md)).

### A predição, dentro do pipeline

Quando a volta fecha, ainda no Dataflow, o pipeline chama `/predict` no
Cloud Run e grava a previsão **na mesma linha**. É isso que permite ao BI
mostrar previsto e real sem join.

O modelo prevê o tempo **da própria volta** a partir das features dela, não
da seguinte — então `tempo_previsto_s` e `duracao_s` na mesma linha falam
da mesma volta.

Cuidados que a rede impõe: sessão HTTP reusada por worker, timeout curto,
poucas tentativas, e **falha nunca perde a volta** — ela é gravada com
previsão nula.

---

## Procedência e atividade: duas coisas diferentes

Esta distinção aparece em toda a stack e vale isolar.

| | o que é | onde mora | muda? |
|---|---|---|---|
| **Procedência** | replay ou feed real | coluna `fonte` na linha | nunca |
| **Atividade** | acontecendo agora ou acabou | derivado de `MAX(ts_evento)` | sozinha, com o tempo |

Um replay encerrado **continua sendo replay** — ele só deixa de estar em
curso. Gravar atividade numa coluna criaria dado que mente com o passar do
tempo: uma volta gravada às 23:00 estava em curso às 23:00 e é histórico às
23:10, sem que nada nela mude.

---

## Orquestração

Airflow containerizado numa VM `e2-medium`, com duas DAGs:

**`f1_pipeline_fim_a_fim`** (sob demanda) — aquece a API, sobe o Dataflow,
espera os workers, dispara o replay, verifica a saúde, drena o job.

Duas arestas vêm de medição, não de preferência:

- **Aquecer a API antes de tudo.** Ela carrega um SavedModel do TensorFlow:
  33 s de container, mais até 2 min para o Cloud Run alocar instância. O
  timeout de leitura do Dataflow é 10 s.
- **Subir o Dataflow antes do gerador.** A assinatura retém mensagens, mas
  o backlog entregue de uma vez passa da carência e degrada a média de gap.

**`f1_saude_pipeline`** (a cada 5 min) — vigia as duas APIs e as quatro
checagens do pipeline. Não exige stream ao vivo: cobrar frescor de minutos
numa DAG de 5 em 5 minutos a faria falhar o tempo todo, e um monitor que
sempre alarma é pior que não ter monitor.

**Por que não Cloud Composer:** ele é este mesmo Airflow sobre GKE
gerenciado, a US$ 300–400/mês com o cluster ligado 24 h. A carga aqui é uma
DAG de seis tarefas — o paralelismo de dados está no Dataflow. Numa VM
desligada entre demonstrações: ~US$ 1–3/mês.

---

## Resultados medidos

Corrida 11361 (GP da Itália 2026, 111 minutos), comparada volta a volta com
a `silver_voltas` da mesma corrida.

Corrida completa, 111 minutos, reproduzida a fator 10 (~14 min de replay):

| medida | valor |
|---|---|
| voltas processadas | **1.055** — a corrida inteira |
| `evento_id` duplicado | **zero** |
| voltas completas iguais ao lote | **1.052 / 1.052** em duração, composto e temperatura |
| soma dos setores vs duração | **1.003 / 1.003** dentro de 1 s |
| voltas aptas / com previsão | **938 / 938** |
| latência mediana, Pub/Sub → BigQuery (predição inclusa) | **1,70 s** |
| latência p95 | 3,35 s |
| erro do modelo (MAE) | **1,13 s** sobre voltas de ~88 s (1,3%) |
| DLQ | 265 mensagens injetadas, 4 motivos, nenhum evento perdido |
| custo de uma execução completa | ~US$ 0,06 |

E uma comparação que vale como lição:

| | Mac M4 local (`DirectRunner`) | Dataflow + Streaming Engine |
|---|---:|---:|
| latência mediana | ~60 s | **4,15 s** |
| colunas iguais ao lote | 105–118 de 118 | **127 de 127** |

O runner lento não só atrasou o resultado — **mudou o resultado**. A
corretude da montagem depende de a carência ser maior que o atraso de
reordenação, e com o runner saturado o atraso que importa passa a ser o da
fila interna dele.

---

## Limites conhecidos

**`eh_entrada_box` sob bandeira vermelha.** Nesta corrida os registros de
`v1/pit` das paradas têm data 2.137 s depois da largada — meia hora depois
da volta terminar. Ao vivo o dado não existe ainda. Não tem correção em
streaming; é o papel de uma DAG de reprocessamento em lote.

**`razao_mediana_sessao` ao vivo.** O lote divide pela mediana da corrida
inteira; ao vivo só existe a mediana das voltas recebidas. O pipeline usa a
corrente, e exclui as voltas anômalas da amostra — sem isso, quatro voltas
anormais em seis envenenam a mediana.

**Modelo linear.** Captura a tendência, não a interação entre composto e
temperatura. Escolha consciente: interpretabilidade sobre meio ponto de
erro.

**Uma corrida por vez no estado.** O particionamento por `session_key` dá
paralelismo entre corridas, mas dentro de uma corrida o processamento é
serial. Folgado para 32 msg/s; precisaria mudar para dezenas de corridas
simultâneas.

---

## Onde está cada coisa

| diretório | o quê |
|---|---|
| [`ingestion/`](../ingestion/) | ingestão em lote da OpenF1 para o GCS |
| [`definitions/`](../definitions/) | SQL do Dataform: bronze, silver, gold, ml, stream |
| [`producer/`](../producer/) | gerador: replay da corrida no Pub/Sub |
| [`streaming/`](../streaming/) | pipeline Dataflow: monta, prevê, grava |
| [`model/`](../model/) | exportação e avaliação do modelo |
| [`api/`](../api/) | API de predição no Cloud Run |
| [`bi-api/`](../bi-api/) | API de BI: serve BigQuery e o stream por SSE |
| [`bi-frontend/`](../bi-frontend/) | front analítico em React |
| [`airflow/`](../airflow/) | DAGs e o Airflow containerizado |
| [`setup/`](../setup/) | provisionamento do GCP |

### Documentos

| documento | para quê |
|---|---|
| [`contrato-openf1.md`](contrato-openf1.md) | o que trafega no Pub/Sub |
| [`contrato-evento.md`](contrato-evento.md) | o que o Dataflow grava |
| [`caso-carencia-reordenacao.md`](caso-carencia-reordenacao.md) | os bugs de ordenação, local × nuvem, as otimizações |
| [`bastidores/`](bastidores/README.md) | registros internos da equipe: planos, passagens entre frentes, roteiro da apresentação |
| [`analise-exploratoria.md`](analise-exploratoria.md) | qualidade do dado medida na Bronze |
| [`perfil-dados-bronze.md`](perfil-dados-bronze.md) | campo a campo dos 18 endpoints |
