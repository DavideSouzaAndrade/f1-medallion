# Contrato da volta montada (v1)

O formato da volta que o Dataflow produz e grava em `f1_stream.voltas`: uma
linha por volta concluída de um piloto, com as mesmas features da
`silver_voltas` e da API de predição. Mudanças aqui mudam também o schema de
[`definitions/stream/voltas.sqlx`](../definitions/stream/voltas.sqlx).

Até a parte 2 este era o formato da própria mensagem no Pub/Sub. Agora o
Pub/Sub carrega as mensagens brutas da OpenF1
([contrato-openf1.md](contrato-openf1.md)), e o Dataflow monta esta volta a
partir delas: `laps` dá a volta, `stints` o pneu, `weather` o clima,
`intervals` o gap médio dentro da janela da volta e `sessions`/`meetings` o
circuito.

Tipos e valores foram conferidos nos dados reais da `silver_voltas`.

---

## Identificação

- `evento_id` = `replay_id` + `:` + o `_key` da volta (ex.:
  `replay-20261031T150908-dab46d:11361_23_44`). Determinístico: se o
  Dataflow gravar a mesma volta duas vezes, a view unificada deduplica.
- `replay_id` vem do atributo da mensagem; `openf1` quando a fonte é a OpenF1
  ao vivo.
- `ts_evento` é o `publish_time` da versão final da volta, a que traz
  `lap_duration`: o momento em que a volta ficou completa no Pub/Sub.

Exemplo com dados reais (piloto 44, volta 23, GP da Itália de 2026):

```json
{"evento_id": "replay-20261031T150908-dab46d:11361_23_44",
 "replay_id": "replay-20261031T150908-dab46d",
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
| `evento_id` | STRING | sim | `replay_id:_key` da volta; chave de deduplicação |
| `replay_id` | STRING | sim | Atributo da mensagem; separa dois replays da mesma corrida (`openf1` ao vivo) |
| `ts_evento` | TIMESTAMP | sim | `publish_time` da versão final da volta, em UTC; define a partição |
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

- **Rejeita para `f1_stream.voltas_dlq`**, com o `motivo`: a mensagem bruta
  que não dá para ler (ver [contrato-openf1.md](contrato-openf1.md)) e a
  volta montada que viola uma regra deste contrato (`lap_number` ≤ 0,
  `duracao_s` ≤ 0, `composto` fora da lista).
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
