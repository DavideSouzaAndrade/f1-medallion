# Contrato do evento de volta (v1)

O acordo entre as três peças do streaming: o gerador (`producer/`), que
publica; o Dataflow, que valida e grava; e a API de predição, que consome as
mesmas features. Qualquer mudança aqui muda também
[`producer/evento.py`](../producer/evento.py) e o schema de
[`definitions/stream/voltas.sqlx`](../definitions/stream/voltas.sqlx).

Tipos e valores foram conferidos nos dados reais da `silver_voltas`.

---

## A mensagem no Pub/Sub

- Tópico `f1-voltas`; o Dataflow lê da assinatura `f1-voltas-dataflow`.
- Corpo: JSON UTF-8, um evento por mensagem. Um evento é **uma volta
  concluída** de um piloto.
- Atributo `schema_version = "1"`. Eventos inválidos publicados de propósito
  pelo gerador levam também o atributo `injetado` com o tipo do defeito.
- O Pub/Sub entrega **pelo menos uma vez**: o mesmo `evento_id` pode chegar
  duas vezes. A deduplicação é feita na leitura, pela view unificada.

Exemplo com dados reais (piloto 44, volta 23, GP da Itália de 2026):

```json
{"evento_id": "c1a7e0b2-3f4d-4b8e-9a61-2d5f7e8c9b10",
 "replay_id": "replay-20261017T154210-1a2b3c",
 "ts_evento": "2026-10-17T15:42:10.123Z",
 "session_key": 11361, "driver_number": 44, "lap_number": 23, "year": 2026,
 "circuito": "Monza", "circuit_type": "Permanent",
 "data_inicio": "2026-09-06T14:10:15.000Z",
 "duracao_s": 85.3, "setor_1_s": 27.485, "setor_2_s": 29.499, "setor_3_s": 28.316,
 "composto": "MEDIUM", "stint_number": 2, "idade_pneu": 20, "volta_no_stint": 20,
 "eh_saida_box": false, "eh_entrada_box": false,
 "gap_medio_a_frente": 3.328,
 "temp_ar": 32.4, "temp_pista": 53.8, "umidade": 37.5, "chuva": 0}
```

## Campos

| Campo | Tipo (BigQuery) | Obrigatório | Regra |
|-------|-----------------|-------------|-------|
| `evento_id` | STRING | sim | UUID por mensagem; chave de deduplicação |
| `replay_id` | STRING | sim | Uma execução do gerador; separa dois replays da mesma corrida |
| `ts_evento` | TIMESTAMP | sim | ISO 8601 em UTC, hora da publicação; define a partição |
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

## O que o Dataflow faz com cada evento

- **Rejeita para `f1_stream.voltas_dlq`**, com o `motivo`: JSON inválido,
  campo obrigatório ausente, tipo errado, `lap_number` ≤ 0, `duracao_s` ≤ 0,
  `composto` fora da lista.
- **Grava em `f1_stream.voltas` a volta válida mesmo que imprópria para o
  modelo**, marcando `apto_modelo = false` — a mesma filosofia da Silver,
  que trata sem filtrar. `apto_modelo` exige composto seco (SOFT, MEDIUM ou
  HARD), `duracao_s`, `idade_pneu` e `temp_pista` presentes, e volta fora do
  box.
- **Normaliza**: composto em maiúsculas, textos sem espaços nas pontas.
- **Acrescenta**: `_ts_processamento` e `latencia_ms` (`_ts_processamento`
  menos `ts_evento`).
- `tempo_previsto_s` e `modelo_versao` ficam nulos na parte 2; a partir da
  parte 3 o Dataflow os preenche chamando a API.

## Os tipos de evento inválido do gerador

Cada um viola uma única regra, partindo de um evento válido:

| Tipo (`injetado`) | Defeito | Motivo esperado na DLQ |
|-------------------|---------|------------------------|
| `json_quebrado` | corpo truncado | JSON inválido |
| `sem_session_key` | campo removido | campo obrigatório ausente |
| `tipo_errado` | `lap_number` como texto | tipo errado |
| `volta_zero` | `lap_number = 0` | `lap_number` ≤ 0 |
| `duracao_negativa` | `duracao_s` negativo | `duracao_s` ≤ 0 |
| `composto_invalido` | `composto = "SUPERSOFT"` | composto fora da lista |
