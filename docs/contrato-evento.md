# Contrato da volta montada (saída do streaming)

O formato da volta que o Dataflow produz e grava em `f1_stream.voltas`: uma
linha por volta concluída de um piloto, com as mesmas features da
`silver_voltas` e da API de predição. Mudanças aqui mudam também o schema de
[`definitions/stream/voltas.sqlx`](../definitions/stream/voltas.sqlx).

A entrada do streaming é outro contrato: as mensagens brutas da OpenF1 no
Pub/Sub ([contrato-openf1.md](contrato-openf1.md), o contrato v2). O
Dataflow monta esta volta a partir delas: `laps` dá a volta, `stints` o
pneu, `weather` o clima, `intervals` o gap médio dentro da janela da volta,
`pit` a entrada no box e `sessions`/`meetings` o circuito.

Na parte 2 este formato era a própria mensagem no Pub/Sub (o contrato v1);
por isso algumas descrições antigas ainda o chamam assim.

Tipos e valores foram conferidos nos dados reais da `silver_voltas`.

---

## Identificação

- `evento_id` = `{replay_id}:{session_key}:{driver_number}:{lap_number}`
  (ex.: `replay-20261031T150908-dab46d:11361:44:23`). Determinístico: se o
  Dataflow gravar a mesma volta duas vezes, a view unificada deduplica. Um
  UUID não serviria: uma volta nasce de ~26 mensagens, e uma reentrega
  geraria outro id.
- `replay_id` vem do atributo da mensagem; `openf1` quando a fonte é a OpenF1
  ao vivo.
- `ts_evento` é o `publish_time` da versão final da volta, a que traz
  `lap_duration`: o momento em que a volta ficou completa no Pub/Sub. Se a
  versão final nunca chega, vale o `publish_time` do início da volta
  seguinte; sem nenhum dos dois, o instante de processamento, e aí
  `latencia_ms` fica nula.

Exemplo com dados reais (piloto 44, volta 23, GP da Itália de 2026):

```json
{"evento_id": "replay-20261031T150908-dab46d:11361:44:23",
 "replay_id": "replay-20261031T150908-dab46d", "fonte": "replay",
 "ts_evento": "2026-10-31T15:12:01.123Z",
 "session_key": 11361, "driver_number": 44, "lap_number": 23, "year": 2026,
 "circuito": "Monza", "circuit_type": "Permanent",
 "data_inicio": "2026-09-06T14:10:15.650Z",
 "duracao_s": 85.3, "setor_1_s": 27.485, "setor_2_s": 29.499, "setor_3_s": 28.316,
 "composto": "MEDIUM", "stint_number": 2, "idade_pneu": 20, "volta_no_stint": 20,
 "eh_saida_box": false, "eh_entrada_box": false,
 "gap_medio_a_frente": 3.328,
 "temp_ar": 32.4, "temp_pista": 53.8, "umidade": 37.5, "chuva": 0}
```

## Campos

| Campo | Tipo (BigQuery) | Obrigatório | Regra |
|-------|-----------------|-------------|-------|
| `evento_id` | STRING | sim | `{replay_id}:{session_key}:{driver_number}:{lap_number}`; chave de deduplicação |
| `replay_id` | STRING | sim | Atributo da mensagem; separa dois replays da mesma corrida (`openf1` ao vivo) |
| `fonte` | STRING | não | `replay` (corrida reproduzida da Bronze) ou `openf1` (feed ao vivo). É **procedência**, não estado: um replay encerrado continua sendo replay. Se a execução está em curso se deriva de `MAX(ts_evento)`, não desta coluna |
| `ts_evento` | TIMESTAMP | sim | `publish_time` da versão final da volta, em UTC (ver Identificação); define a partição |
| `session_key`, `driver_number`, `year` | INT64 | sim | — |
| `lap_number` | INT64 | sim | Maior que 0 |
| `circuito` | STRING | sim | — |
| `circuit_type` | STRING | sim | `Permanent`, `Temporary - Street` ou `Temporary - Road` |
| `data_inicio` | TIMESTAMP | não | Início original da volta na corrida |
| `duracao_s` | FLOAT64 | não | Maior que 0 quando presente; nulo em ~0,4% das voltas de corrida |
| `setor_1_s`, `setor_2_s`, `setor_3_s` | FLOAT64 | não | — |
| `composto` | STRING | não | `SOFT`, `MEDIUM`, `HARD`, `INTERMEDIATE`, `WET`, `UNKNOWN`, `TEST_UNKNOWN` |
| `stint_number`, `idade_pneu` | INT64 | não | `idade_pneu` ≥ 0 |
| `volta_no_stint` | INT64 | não | Calculada como na Gold: posição entre as voltas limpas do stint. Volta que não entra na Gold recebe a posição que teria se fosse limpa. Nula só sem `stint_number` |
| `eh_saida_box`, `eh_entrada_box` | BOOL | sim | — |
| `gap_medio_a_frente` | FLOAT64 | não | 0,0 para o líder da volta (~5%); nulo quando não houve medição (~0,8%) |
| `temp_ar`, `temp_pista`, `umidade` | FLOAT64 | não | — |
| `chuva` | INT64 | não | 0 ou 1 — inteiro, como na Silver, não booleano |

## O que o Dataflow faz com cada volta

- **Rejeita para `f1_stream.voltas_dlq`**, com o `motivo`:
  - a mensagem bruta que não dá para ler: `JSON invalido`,
    `campo obrigatorio ausente`, `tipo errado` ou `topico fora do contrato`
    (ver [contrato-openf1.md](contrato-openf1.md));
  - a volta que não tem como ser montada: `volta sem contexto da sessao`,
    quando `v1/sessions` nunca chegou (`year`, `circuito` e `circuit_type`
    são obrigatórios e não são inventados);
  - a linha que o BigQuery recusar: `rejeitado pelo BigQuery`.
- **Grava em `f1_stream.voltas` a volta válida mesmo que imprópria para o
  modelo**, marcando `apto_modelo = false` — a mesma filosofia da Silver,
  que trata sem filtrar. `apto_modelo` é o recorte inteiro da
  `gold_degradacao_pneu`: composto seco (SOFT, MEDIUM ou HARD), `duracao_s`,
  `idade_pneu` e `temp_pista` presentes, volta fora do box e dentro de 1,15×
  a mediana da sessão. Ao vivo a mediana da corrida inteira ainda não existe,
  então vale a mediana das voltas recebidas até o momento.
- **Normaliza**: textos sem espaços nas pontas; gap em texto (`"+1 LAP"`)
  vira nulo e fica fora da média de gap.
- **Acrescenta**: `_ts_processamento` e `latencia_ms` (`_ts_processamento`
  menos `ts_evento`; nula quando o `ts_evento` é o instante de
  processamento, ver Identificação).
- **Prevê**: na volta apta, chama `POST /predict` da API e preenche
  `tempo_previsto_s` e `modelo_versao`. Se a API falhar, a volta é gravada
  mesmo assim, com a previsão nula. A volta inapta não recebe previsão.
