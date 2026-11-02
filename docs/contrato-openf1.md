# Contrato das mensagens brutas da OpenF1 (v2)

O acordo entre quem publica dados de corrida no Pub/Sub e o Dataflow que os
consome. A regra que define tudo aqui: **a mensagem é igual à que a OpenF1
entrega ao vivo**. Hoje quem publica é o gerador (`producer/`), fazendo o
replay de uma corrida guardada na Bronze; no futuro pode ser uma ponte que
assina o MQTT da OpenF1 e repassa as mensagens. Para o Dataflow, as duas
fontes são indistinguíveis.

A v1 ([contrato-evento.md](contrato-evento.md)) descrevia uma volta já
montada, no formato da Silver. Ela deixa de ser o que trafega no Pub/Sub e
passa a ser o que o Dataflow produz depois de montar a volta.

Fontes: documentação da OpenF1 ([openf1.org/docs](https://openf1.org/docs),
[tempo real](https://openf1.org/auth.html)), o código de ingestão ao vivo da
própria OpenF1 ([br-g/openf1](https://github.com/br-g/openf1), pasta
`ingestor_livetiming/core/processing/collections`) e os dados da Bronze do GP
da Itália de 2026 (`session_key` 11361).

---

## Como a OpenF1 entrega ao vivo

- MQTT (`mqtt.openf1.org:8883`) ou WebSocket, só no plano pago (Sponsor,
  €9,90/mês). Os dados chegam ~3 s depois do evento real.
- Um tópico por endpoint: `v1/laps`, `v1/stints`, `v1/intervals`...
- Cada mensagem é o JSON do REST com dois campos a mais:
  - `_key`: identifica o registro. **Mesmo `_key` no mesmo tópico = nova
    versão do mesmo registro**, que substitui a anterior.
  - `_id`: inteiro sempre crescente; ordena as mensagens.
- `_key` é a chave única do registro unida por `_`, com datas em
  milissegundos desde 1970 (ex.: `"1744370476603_31"` para `location`).

## A mensagem no Pub/Sub

- Tópico `f1-openf1`; o Dataflow lê da assinatura `f1-openf1-dataflow`. A
  `f1-openf1-debug` serve para inspecionar sem roubar mensagens.
- Corpo: o JSON da OpenF1, intacto, com `_key` e `_id`. Nenhum campo é
  renomeado, tipado ou calculado.
- Atributos (metadados de transporte, fora do corpo):

| Atributo | Exemplo | Para quê |
|----------|---------|----------|
| `topico` | `v1/laps` | Qual endpoint; equivale ao tópico MQTT |
| `session_key` | `11361` | Filtrar e particionar sem abrir o JSON |
| `fonte` | `replay` ou `openf1` | Distinguir simulação de dado ao vivo |
| `replay_id` | `replay-20261031T...-1a2b3c` | Separar dois replays da mesma corrida (só no replay) |
| `injetado` | `json_quebrado` | Só em mensagens corrompidas de propósito |

- O Pub/Sub entrega **pelo menos uma vez** e **não garante ordem**. A mesma
  mensagem pode chegar duas vezes, e a volta pode chegar antes do stint dela.

## Tópicos usados

Só os sete de que a `silver_voltas` precisa. Volume medido numa corrida
(GP da Itália de 2026, 2 h):

| Tópico | Registros na Bronze | `_key` | Quando é publicado |
|--------|--------------------:|--------|--------------------|
| `v1/meetings` | 1 | `meeting_key` | No início do replay |
| `v1/sessions` | 1 | `session_key` | No início do replay |
| `v1/weather` | 176 | `date` | Em `date` (~1 por minuto) |
| `v1/intervals` | 21.955 | `date_driver` | Em `date` (~a cada 4 s por piloto) |
| `v1/laps` | 1.055 | `session_lap_driver` | Várias versões por volta (ver abaixo) |
| `v1/stints` | 52 | `session_stint_driver` | Ao começar e a cada volta do stint |
| `v1/pit` | 30 | `session_lap_driver` | Em `date`, uma vez |

Ao todo, ~27 mil mensagens por corrida. No fator 30, ~4 min e ~110
mensagens/s.

O replay começa 1 minuto antes da largada (o início da primeira volta). O
pré-corrida da Bronze (~1 h de clima e intervalos do grid) fica de fora,
menos o último registro de clima, para as primeiras voltas não chegarem sem
temperatura.

### Como laps e stints mudam durante a volta

É assim que a OpenF1 monta esses registros ao vivo, e o gerador reproduz:

**`v1/laps`**: a volta nasce quando o piloto cruza a linha e vai sendo
preenchida. Cada linha abaixo é uma mensagem com o mesmo `_key`
`11361_23_44`:

| Momento | O que já vem preenchido |
|---------|-------------------------|
| `date_start` | `lap_number`, `date_start`, `is_pit_out_lap` |
| + setor 1 | `duration_sector_1`, `segments_sector_1`, `i1_speed` |
| + setor 2 | `duration_sector_2`, `segments_sector_2`, `i2_speed` |
| fim da volta | `duration_sector_3`, `segments_sector_3`, `st_speed`, `lap_duration` |

A versão final é o registro que está na Bronze:

```json
{"meeting_key":1293,"session_key":11361,"driver_number":44,"lap_number":23,
 "date_start":"2026-09-06T14:10:15.650000+00:00",
 "duration_sector_1":27.485,"duration_sector_2":29.499,"duration_sector_3":28.316,
 "i1_speed":300,"i2_speed":301,"is_pit_out_lap":false,"lap_duration":85.3,
 "segments_sector_1":[null,2048,2048,2048,2048,2048],
 "segments_sector_2":[2048,2048,2048,2049,2048,2048,2048],
 "segments_sector_3":[2048,2048,2048,2048,2048,2048,2048,2048,2048],
 "st_speed":327,"_key":"11361_23_44","_id":1761921015650}
```

**`v1/stints`**: o stint nasce na troca de pneu com `lap_end = lap_start` e
recebe uma nova versão a cada volta concluída, com `lap_end` avançando. O
`lap_end` final da Bronze (53 no exemplo abaixo) só existe no fim da corrida;
publicá-lo no início seria entregar o futuro.

```json
{"meeting_key":1293,"session_key":11361,"stint_number":2,"driver_number":44,
 "lap_start":4,"lap_end":23,"compound":"MEDIUM","tyre_age_at_start":0,
 "_key":"11361_2_44","_id":1761921101000}
```

## Datas

Os campos de data do corpo (`date`, `date_start`) ficam com o valor original
da corrida: é o dado bruto. O momento em que o replay publicou fica no
`publish_time` do próprio Pub/Sub. A latência do pipeline se mede por ele.

No replay, o `_id` é o instante da publicação em milissegundos, forçado a
crescer de uma mensagem para a outra, como a OpenF1 faz.

## O que o dado bruto traz e o Dataflow precisa tratar

Medido na corrida 11361. Isto é o tratamento de verdade, não injetado:

| Achado | Onde | Tratamento (o mesmo da Silver) |
|--------|------|--------------------------------|
| Texto em campo numérico: `"+1 LAP"`, `"+2 LAPS"` | `intervals.gap_to_leader` (1.237), `intervals.interval` (2) | Não é segundos: o gap vira nulo e o `N` vira "voltas atrás" |
| Gap nulo | `intervals.interval` (144), `gap_to_leader` (81) | Fica fora da média de gap da volta |
| `lap_duration` nulo | `laps` (3) | Volta sem tempo; não vai para o modelo |
| Setor 1 nulo | `laps` (51, 21 deles em saída do box) | Setor ausente; a volta segue se tiver `lap_duration` |
| Composto `UNKNOWN` ou ausente | `stints` (não nesta corrida; existe em outras) | Stint fantasma; não vai para o modelo |
| Registro reenviado (mesmo `_key`) | `laps`, `stints` | Ficar com a versão de maior `_id` |
| Fora de ordem | todos | A volta pode chegar antes do stint; esperar um pouco antes de montar |

Para a demonstração da DLQ, o gerador continua podendo corromper uma fração
das mensagens (`--injetar-invalidos`), com o atributo `injetado`:

| Tipo | O que o gerador faz | Motivo esperado na DLQ |
|------|---------------------|------------------------|
| `json_quebrado` | Corta o JSON no meio | JSON inválido |
| `sem_chave` | Remove `session_key` ou `driver_number` | Campo obrigatório ausente |
| `tipo_errado` | Troca um número por texto (fora dos `"+N LAP"` reais) | Tipo inválido |
| `topico_desconhecido` | `topico = v1/inexistente` | Tópico fora do contrato |

## O que o Dataflow entrega

A partir dessas mensagens, o Dataflow monta a volta (stint, clima e gap médio
de cada volta concluída) e produz o registro descrito na
[v1](contrato-evento.md): mesmos campos e regras, e mesma tabela
`f1_stream.voltas`. É isso que mantém a estrutura do streaming igual à da
`silver_voltas`.

Uma diferença inevitável em relação ao batch: a Silver calcula
`razao_mediana_sessao` com a mediana da corrida inteira. Ao vivo, ninguém
sabe a mediana da corrida antes de ela acabar; o Dataflow só pode usar a
mediana das voltas recebidas até o momento.
