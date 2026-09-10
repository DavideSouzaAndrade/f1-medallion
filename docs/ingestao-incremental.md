# Atualização incremental

Como atualizar o bucket com as corridas novas **sem varrer todas as
temporadas de novo**.

---

## O problema

A carga completa funciona assim:

```
para cada ano (2023, 2024, 2025, 2026)
    para cada uma das ~123 sessões do ano
        para cada endpoint
            o arquivo já existe no bucket?  →  se sim, pula
```

São mais de 12 mil perguntas ao Cloud Storage, toda vez que você roda. E
você acaba perguntando pela centésima vez sobre a corrida de Mônaco de
2024, que terminou há dois anos e nunca mais vai mudar.

Isso é aceitável para o backfill inicial. Não é aceitável para uma rotina
semanal — e piora a cada temporada que entra no escopo.

## Por que o `year=` do caminho não resolve

Os arquivos ficam assim:

```
openf1/laps/year=2024/session_key=9472/data.json.gz
             ↑
      ano da TEMPORADA, não da ingestão
```

`year=2024` é a temporada de Fórmula 1, não a data em que o arquivo foi
baixado. Ele agrupa milhares de objetos, e saber que algo é de 2026 **não
diz se é novo** — 2026 tem 131 sessões, das quais a maioria já está no
bucket. Nada no caminho registra "isto chegou depois daquilo".

E mesmo que registrasse, perguntar ao bucket "o que eu já tenho?" exigiria
listar os 12 mil objetos. Pior: responderia a pergunta errada. Você não
quer saber o que **tem**, quer saber o que **existe na API e você ainda
não pegou**.

---

## A solução: marca d'água

Uma marca d'água é só isto: **uma data guardada num arquivo, dizendo "já
cuidei de tudo que terminou antes disto"**. Um marcador de página.

Fica em `openf1/_state/watermark.json`:

```json
{
  "atualizado_em": "2026-09-15T06:00:00+00:00",
  "ondas": {
    "2": {"ate": "2026-09-13T15:00:00+00:00"},
    "6": {"ate": "2026-09-13T15:00:00+00:00"}
  }
}
```

É por onda, porque cada onda avança sozinha — você pode estar em dia na
onda 2 e atrasado na 3.

### Por que funciona

Porque **toda sessão tem data de fim**, e a API entrega a lista inteira de
datas numa única requisição:

```
GET /v1/sessions?year=2026     →  131 sessões, cada uma com date_start e date_end
```

Uma sessão que terminou **antes** da marca não pode ser novidade. Então
ela nem entra na conversa: não se pergunta ao bucket sobre ela, não se
pergunta à API, não se faz nada.

---

## Exemplo, passo a passo

**Domingo, 6 de setembro.** Você roda pela primeira vez. A última sessão
realizada foi Zandvoort, em 23 de agosto. Ao terminar, o código grava:

```
marca = 2026-08-23T15:00
```

**Madri acontece de 11 a 13 de setembro** — cinco sessões novas.

**Segunda, 15 de setembro.** Você roda de novo:

| Passo | O que acontece | Custo |
|---|---|---|
| 1 | `GET /sessions?year=2026` | **1 requisição** |
| 2 | Filtra `date_end > 2026-08-23T15:00` | zero — é filtro em memória |
| 3 | Sobram **5 sessões** (as de Madri) | — |
| 4 | Ingere só essas 5 | ~40 requisições |
| 5 | Grava `marca = 2026-09-13T15:00` | 1 escrita |

As outras 126 sessões de 2026 e as 369 de 2023–2025 **não foram tocadas**.
Nenhuma chamada ao Cloud Storage, nenhuma à API. Não porque estão em outro
`year=`, mas porque a data delas é anterior à marca.

---

## As duas travas

O sistema tem duas proteções, e elas respondem perguntas diferentes:

| | Marca d'água | Existência do objeto |
|---|---|---|
| Pergunta | *sobre o que devo perguntar?* | *isto eu já baixei?* |
| Onde consulta | lista da API (1 requisição) | o bucket (1 chamada por arquivo) |
| Quando corta | antes de tudo | dentro do que sobrou |

A marca reduz o universo de 495 sessões para 5. A checagem de existência
continua atuando dentro dessas 5, como rede de segurança.

---

## Carência de 24 horas

A OpenF1 continua completando os dados por um tempo depois que a sessão
acaba. Se você ingerir a corrida às 17h05 de domingo, grava dado pela
metade — e como o arquivo passa a existir, **nunca mais seria rebuscado**.
O erro ficaria permanente e silencioso.

Por isso o filtro tem dois lados:

```
date_end > marca            (é novo)
date_end < agora − 24 h     (já está consolidado)
```

Sessões que caem na carência aparecem no log e são pegas na execução
seguinte. Ajustável pela variável de ambiente `GRACE_HOURS`.

---

## A marca não avança se algo falhar

Se a execução tiver **qualquer** falha, a marca fica onde está.

Parece conservador, mas é praticamente de graça: a próxima execução
reconsidera as mesmas poucas sessões, e tudo que já foi gravado é pulado
pela checagem de existência — algumas dezenas de chamadas ao bucket.

A alternativa — avançar até a última sessão bem-sucedida — abriria um
buraco silencioso. Se a sessão de segunda falha e a de terça funciona,
avançar até terça enterra a de segunda para sempre, porque ela passa a
ficar atrás da marca.

---

## As listas de nível de ano são reescritas

`meetings` e `sessions` são um arquivo por temporada, e essas listas
**crescem** a cada fim de semana. Se ficassem sujeitas ao checkpoint por
existência, congelariam na primeira gravação — o bucket guardaria para
sempre a foto do dia da carga inicial.

Em modo incremental esses dois endpoints são sempre sobrescritos. São 2
requisições por ano.

---

## Como usar

```bash
# rotina: ingere só o que é novo
python main.py --wave 2 --incremental

# ignora a marca gravada e reconsidera a partir de uma data
python main.py --wave 2 --incremental --since 2026-08-01

# conserta um objeto específico, sobrescrevendo
python main.py --wave 2 --refetch --session-keys 9472
```

### Primeira execução

Não existe marca ainda. Nesse caso o modo incremental se comporta como
varredura completa daquela onda e grava a marca ao final. A primeira roda
é lenta; todas as seguintes são rápidas. Não precisa configurar nada.

### Cuidado: backfill de temporada antiga não usa `--incremental`

A marca d'água é uma data, e ela só anda para frente. Se a marca da onda 2
está em dezembro de 2025 e você tentar trazer a temporada de **2023** com
`--incremental`, todas as sessões de 2023 serão descartadas pelo filtro —
elas terminaram antes da marca.

Isso é o comportamento correto para atualização, e errado para backfill.
Para trazer uma temporada que ficou para trás, rode em modo normal:

```bash
python main.py --wave 2 --years 2023          # backfill: sem --incremental
```

Regra prática: **`--incremental` é para o que vem depois; modo normal é
para o que ficou para trás.** O backfill não mexe na marca d'água.

### O que aparece no log

```
modo incremental | marca d'agua: 2026-08-23T15:00:00+00:00 | ignorando o que terminou depois de ...
marca d'agua reduziu 131 sessoes para 5
1 sessao(oes) aguardando a carencia de 24h: [11402]
marca d'agua da onda 2 avancou para 2026-09-13T15:00:00+00:00
fim em 1.2 min | gravados=38 vazios=2 pulados=0 fora_da_marca=126 falhas=0
```

`fora_da_marca` é quantas sessões o filtro descartou. Numa rotina saudável
esse número é alto e `gravados` é baixo.

---

## Agendando no GCP

O serviço para isso é o **Cloud Scheduler**, o cron gerenciado do Google
Cloud. Uma tarefa semanal dispara o Cloud Run Job:

```
Cloud Scheduler  (segunda, 06:00)
      │ dispara
      ▼
Cloud Run Job f1-ingestao --args="--wave,2,--incremental"
      │ 1 requisição de descoberta por ano + só o que é novo
      ▼
GCS (objetos novos apenas)
```

Segunda-feira é boa escolha: cobre a corrida de domingo com folga sobre a
carência de 24 h.

Vale agendar uma onda por horário, com intervalo entre elas — duas
execuções simultâneas compartilham o limite de 30 req/min da OpenF1 e se
derrubam com HTTP 429.

---

## E a camada Bronze

O mesmo raciocínio se aplica do outro lado. Se a Bronze for carregada com
`CREATE OR REPLACE TABLE AS SELECT` sobre a tabela externa inteira, ela
reprocessa 100% dos arquivos toda vez — e aí não adianta a ingestão ser
incremental.

A Bronze precisa inserir apenas os `session_key` que ainda não estão lá.
A coluna `_data_ingestao_bronze` serve de marca d'água desse lado.
