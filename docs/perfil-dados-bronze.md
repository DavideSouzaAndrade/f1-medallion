# Perfil dos dados na camada Bronze

Levantamento de campo por campo dos 18 endpoints, feito consultando a Bronze
em 07/09/2026, e sugestões de estrutura para a Silver.

**Estado no momento do levantamento:** 18 tabelas, ~235 milhões de linhas,
70,6 GB. Temporadas 2024 e 2025 completas; 2026 em ingestão (ondas 1 e 2
concluídas, telemetria em andamento), então as contagens de 2026 estão
defasadas.

Todos os números aqui foram medidos, não estimados. Onde a amostra foi
parcial, está indicado.

---

# Parte 1 — O que temos em cada endpoint

## Como ler

Cada linha da Bronze é um registro JSON íntegro na coluna `payload`, mais as
colunas de governança (`id_registro`, `year`, `session_key`, `driver_number`
quando aplicável, `_arquivo_origem`, `_data_ingestao_bronze`).

"nulo %" é a fração de registros em que o campo existe mas vem vazio.

---

## Grupo 1 — Catálogo (dimensões)

### `meetings` — 77 linhas · um fim de semana de GP

| Campo | Tipo | Nulo | Observação |
|---|---|---|---|
| `meeting_key` | INT64 | 0% | chave, 77 distintos |
| `meeting_name` | STRING | 0% | "Bahrain Grand Prix", "Pre-Season Testing" |
| `meeting_official_name` | STRING | 0% | nome comercial com patrocinador |
| `location`, `country_name`, `country_code`, `country_key` | STRING/INT64 | 0% | 30 locais, 21 países |
| `circuit_key`, `circuit_short_name` | INT64/STRING | 0% | 26 circuitos |
| `circuit_type` | STRING | 0% | **3 valores**: `Permanent`, `Temporary - Street`, `Temporary - Road` |
| `country_flag`, `circuit_info_url`, `circuit_image` | STRING | 0% | URLs de mídia |
| `gmt_offset` | STRING | 0% | `"03:00:00"` — texto, não intervalo |
| `date_start`, `date_end` | ISO8601 | 0% | com offset `+00:00` |
| `year` | INT64 | 0% | 2024–2026 |
| `is_cancelled` | BOOL | 0% | tem `True` |

`circuit_type` é interessante para os modelos: rua e permanente degradam
pneu de forma diferente.

### `sessions` — 377 linhas · uma sessão

| Campo | Tipo | Nulo | Observação |
|---|---|---|---|
| `session_key` | INT64 | 0% | chave |
| `session_type` | STRING | 0% | **3**: `Practice`, `Qualifying`, `Race` |
| `session_name` | STRING | 0% | **10**: `Practice 1/2/3`, `Qualifying`, `Sprint Qualifying`, `Sprint`, `Race`, `Day 1/2/3` |
| `date_start`, `date_end` | ISO8601 | 0% | **horário agendado**, não real |
| `meeting_key`, `circuit_key`, `country_*`, `location`, `gmt_offset`, `year` | — | 0% | repetidos de `meetings` |
| `is_cancelled` | BOOL | 0% | |

**Atenção:** `date_start` é o horário nominal. A telemetria começa até 1 h
antes (medido: mediana 14 min, máximo 59 min). Não use como fronteira de
sessão.

`session_key` **não é monotônico no tempo**: a sessão 9832 é de dezembro/2025
e a 10015 é de abril/2025. Não sirva de marca d'água.

### `drivers` — 7.683 linhas · um piloto por sessão

| Campo | Tipo | Nulo | Observação |
|---|---|---|---|
| `driver_number` | INT64 | 0% | **41 distintos** em 2024–2026 |
| `full_name`, `first_name`, `last_name`, `broadcast_name`, `name_acronym` | STRING | 0% | **37 pilotos distintos** |
| `team_name` | STRING | 0% | **13 equipes** ao longo dos 3 anos |
| `team_colour` | STRING | 0% | hex sem `#`; **30 distintos** — muda de ano para ano |
| `headshot_url` | STRING | **4,0%** | |
| `country_code` | STRING | **18,1%** | |

41 números para 37 nomes: piloto troca de número entre temporadas, e piloto
de teste aparece em treino livre. **A chave natural é `(session_key,
driver_number)`, não o número sozinho.**

---

## Grupo 2 — Por volta

### `laps` — 182.247 linhas · uma volta de um piloto

| Campo | Tipo | Nulo | Observação |
|---|---|---|---|
| `lap_number` | INT64 | 0% | |
| `date_start` | ISO8601 | 0,1% | início da volta |
| `lap_duration` | FLOAT64 | **3,1%** | segundos |
| `duration_sector_1` | FLOAT64 | **13,7%** | bem mais nulo que os outros dois |
| `duration_sector_2` | FLOAT64 | 1,0% | |
| `duration_sector_3` | FLOAT64 | 1,1% | |
| `i1_speed` | INT64 | **11,9%** | km/h no ponto de medição 1 |
| `i2_speed` | INT64 | 0,7% | |
| `st_speed` | INT64 | 6,0% | *speed trap* |
| `is_pit_out_lap` | BOOL | 0% | |
| `segments_sector_1/2/3` | ARRAY&lt;INT64&gt; | ~1,3% | **tamanho variável: 6 a 11 elementos** |

Os `segments_*` são arrays de códigos de minissetor de tamanho diferente por
circuito. Precisam de tratamento explícito na Silver — ou desnormalizar em
linhas, ou manter como `ARRAY<INT64>`, ou descartar.

`duration_sector_1` nulo em 13,7% é bem acima dos outros setores. Provável
volta de saída de box, onde o setor 1 é parcial.

### `stints` — 26.478 linhas · um período com o mesmo jogo de pneu

| Campo | Tipo | Nulo | Observação |
|---|---|---|---|
| `stint_number` | INT64 | 0% | |
| `lap_start`, `lap_end` | INT64 | 0,8% | intervalo **fechado** de voltas |
| `compound` | STRING | 0,1% | **6 valores**: `SOFT`, `MEDIUM`, `HARD`, `INTERMEDIATE`, `WET`, `TEST_UNKNOWN` |
| `tyre_age_at_start` | INT64 | 0% | voltas que o pneu já tinha |

**Esta é a tabela central do modelo de degradação.** `tyre_age_at_start +
(lap_number - lap_start)` dá a idade do pneu em qualquer volta.

### `pit` — 24.782 linhas · uma passagem pelo pit lane

| Campo | Tipo | Nulo | Observação |
|---|---|---|---|
| `date`, `lap_number` | — | 0% | |
| `pit_duration` | FLOAT64 | 18,9% | |
| `lane_duration` | FLOAT64 | 18,9% | |
| `stop_duration` | — | **95,8%** | preenchido em apenas **1.038 de 24.782** |

Duas armadilhas medidas:

- **`pit_duration` e `lane_duration` são iguais em 19.314 de 24.782 linhas
  (78%).** Não são campos independentes na maior parte dos casos.
- **`stop_duration` é praticamente sempre nulo.** Não dá para usar como
  tempo de parada.

---

## Grupo 3 — Resultado e classificação

### `session_result` — 5.045 linhas · resultado de um piloto numa sessão

| Campo | Tipo | Nulo | Observação |
|---|---|---|---|
| `position` | INT64 | 2,5% | |
| `number_of_laps` | INT64 | 0,1% | |
| `dnf`, `dns`, `dsq` | BOOL | 0% | |
| `duration` | FLOAT64 | 8,2% | |
| `points` | FLOAT64 | — | **presente em só 24% dos registros** (só corrida pontua) |
| `gap_to_leader` | **misto** | 3,3% | ver abaixo |

### `starting_grid` — 1.179 linhas · grid de largada

`position`, `driver_number`, `lap_duration` (tempo da classificação, 3,6%
nulo). As linhas carregam o `session_key` **da classificação**, não da
corrida — atenção ao juntar com a corrida.

### `championship_drivers` (1.272) e `championship_teams` (600)

`position_start`, `position_current`, `points_start`, `points_current`. Dão
o campeonato antes e depois de cada corrida. Só existem em corridas.

---

## Grupo 4 — Séries temporais

### `car_data` — 115.045.079 linhas · amostra a ~3,7 Hz por piloto

| Campo | Tipo | Faixa observada | Observação |
|---|---|---|---|
| `date` | ISO8601 | — | milissegundos |
| `speed` | INT64 | 0–300+ | km/h |
| `rpm` | INT64 | 0–~12.000 | |
| `n_gear` | INT64 | 0–8 | **e valores impossíveis, ver abaixo** |
| `throttle` | INT64 | 0–104 | |
| `brake` | INT64 | 0, 100, 104 | |
| `drs` | INT64 | 0,1,2,3,8,10,12,14 | **código, não booleano** |

**Três problemas medidos** (sessão 9472, 696.380 linhas):

1. **O valor 104 é sentinela de "sem telemetria".** `throttle=104` e
   `brake=104` ocorrem juntos em **51.685 linhas (7,4%)**, e nessas linhas a
   **velocidade média é exatamente 0,0** contra 136,9 nas demais. Não é
   dado real: é preenchimento. Precisa ser filtrado, senão o modelo de
   assinatura de pilotagem aprende ruído.

2. **`n_gear` assume valores impossíveis** — até 75, num carro de 8 marchas.
   São 54 linhas (0,01%), raro mas presente.

3. **`drs` é um código**, não liga/desliga. Os valores 0 e 1 significam
   desativado; 8 indica elegível; 10, 12 e 14 indicam ativado. Tratar como
   booleano perde informação e erra.

### `location` — 118.010.252 linhas · posição na pista a ~3,7 Hz

`x`, `y`, `z` em INT64 (unidade proprietária, não metros). Mesmo `date` de
`car_data`, mas **os timestamps não coincidem exatamente** — as duas séries
têm cadência própria.

### `intervals` — 1.247.052 linhas · gap ao líder e ao carro da frente

| Campo | Tipo | Nulo | Observação |
|---|---|---|---|
| `interval` | FLOAT64 | 1,1% | gap ao carro da frente |
| `gap_to_leader` | **misto** | 0,8% | ver abaixo |

**Armadilha de tipo, medida:** `gap_to_leader` mistura número e texto. Em
**97.396 de 1.247.052 linhas (7,8%)** o valor é `"+1 LAP"`, `"+2 LAPS"` etc.
O mesmo vale para `session_result.gap_to_leader` (227 de 5.045, 4,5%).

Um `CAST(... AS FLOAT64)` direto devolve NULL nessas linhas **em silêncio** —
justo os carros dobrados, que são os mais degradados. Precisa de tratamento
explícito: separar em `gap_segundos FLOAT64` e `voltas_atras INT64`.

### `position` — 230.070 linhas · posição na corrida ao longo do tempo

`date`, `driver_number`, `position`. Simples e limpo, 0% nulo.

### `weather` — 36.003 linhas · uma leitura por minuto por sessão

`air_temperature`, `track_temperature`, `humidity`, `pressure`, `wind_speed`,
`wind_direction`, `rainfall`. Todos 0% nulo.

`rainfall` é INT64 e nas sessões amostradas só assume 0 — provavelmente
0/1, mas não confirmado com chuva de verdade.

---

## Grupo 5 — Eventos

### `race_control` — 18.086 linhas · bandeiras, safety car, incidentes

| Campo | Nulo | Observação |
|---|---|---|
| `category` | 0% | **6 valores**: `Flag`, `Other`, `SessionStatus`, … |
| `message` | 0% | texto livre, 686 distintos |
| `flag` | **52,0%** | `BLUE`, `CLEAR`, `GREEN`, `YELLOW`, … (8) |
| `scope` | **52,0%** | `Track`, `Sector`, `Driver` |
| `lap_number` | **56,0%** | |
| `driver_number` | **81,3%** | |
| `sector` | **81,5%** | |
| `qualifying_phase` | **88,4%** | |

Os nulos altos não são defeito: a tabela mistura tipos de mensagem com
campos diferentes. É um caso clássico de tabela esparsa — na Silver, ou
separar por `category`, ou assumir a esparsidade.

### `overtakes` — 12.351 linhas

`overtaking_driver_number`, `overtaken_driver_number`, `position`, `date`.
Só em corridas. A documentação avisa que pode estar incompleto.

### `team_radio` — 9.456 linhas

`recording_url` e `date`. Áudio, não texto — só útil com transcrição.
Cobertura irregular: a F1 não libera rádio de toda sessão.

---

# Parte 2 — Sugestões para a Silver

## Princípio

A Bronze é cópia fiel. A Silver é onde o dado vira **utilizável**: tipado,
deduplicado, conformado e com o contexto temporal resolvido. Ela não deve
agregar — isso é Gold.

## O problema central: três granularidades diferentes

| Granularidade | Tabelas | Volume |
|---|---|---|
| Amostra (~3,7 Hz) | `car_data`, `location`, `intervals` | 234 M linhas |
| Volta | `laps`, `stints`, `pit` | 233 mil |
| Minuto | `weather` | 36 mil |
| Sessão | `session_result`, `starting_grid`, `championship_*` | 8 mil |

Ligar isso exige *as-of join*: para cada amostra de telemetria, descobrir em
que volta e em que stint ela caiu. O BigQuery não tem `AS OF JOIN` nativo;
faz-se com janela sobre `date`.

**A chave é fazer isso uma vez só na Silver**, não em toda consulta da Gold.

## Estrutura proposta: 6 tabelas

### 1. `silver_sessoes` — dimensão · ~377 linhas

`sessions` + `meetings` desnormalizados. Uma linha por sessão com circuito,
país, tipo, datas tipadas como TIMESTAMP, `circuit_type`, `is_cancelled`.

Acrescentar duas colunas derivadas úteis: `eh_corrida` (BOOL) e
`janela_real_inicio`/`fim` — os extremos reais da telemetria, não o
agendado.

### 2. `silver_pilotos` — dimensão · ~7.700 linhas

Uma linha por `(session_key, driver_number)`. É a granularidade certa
porque número e equipe mudam entre temporadas. Resolve os 41 números para
37 pilotos.

### 3. `silver_voltas` — fato · ~182 mil linhas

**A tabela mais importante do projeto.** Uma linha por `(session_key,
driver_number, lap_number)`, juntando:

- `laps`: durações, velocidades, `is_pit_out_lap`
- `stints`: composto e **idade do pneu na volta** = `tyre_age_at_start + (lap_number - lap_start)`
- `pit`: marcação de volta de entrada/saída de box
- `weather`: leitura vigente no início da volta (as-of)
- `intervals`: gap ao carro da frente agregado na volta — **o proxy de ar sujo**

Derivar aqui: `volta_valida` (exclui pit in/out, safety car, bandeira
amarela via `race_control`), e `ritmo_relativo` contra a melhor volta da
sessão.

Esta tabela sozinha alimenta o modelo de degradação de pneu.

### 4. `silver_telemetria` — fato · ~115 M linhas

Uma linha por amostra de `car_data`, tipada, **com `lap_number` e
`stint_number` já resolvidos** pelo as-of join.

Decisões a tomar:
- **Filtrar o sentinela 104.** Sugiro coluna `amostra_valida BOOL` em vez de
  excluir — preserva a Bronze fiel e deixa a escolha para a Gold.
- **`drs` decodificado** em `drs_ativo BOOL` + `drs_codigo INT64`.
- **`n_gear`** fora de 0–8 vira NULL.
- **Juntar `location`?** As duas séries têm timestamps próprios. Sugiro
  **não** juntar: manter `silver_posicao` separada e unir só se a Gold
  precisar. Um as-of join de 115 M contra 118 M linhas é caro e nenhum dos
  dois modelos pede isso hoje.

### 5. `silver_clima` — fato · 36 mil linhas

`weather` tipado, mais `session_key` e a leitura anterior/posterior para
interpolação, se for útil.

### 6. `silver_eventos` — fato · ~30 mil linhas

`race_control` e `overtakes` unificados num formato comum
`(session_key, date, tipo_evento, driver_number, detalhe)`. Resolve a
esparsidade concentrando as diferenças numa coluna de detalhe.

## Decisões de tipagem que precisam ser explícitas

| Campo | Problema | Sugestão |
|---|---|---|
| `gap_to_leader` | 7,8% são texto `"+1 LAP"` | duas colunas: `gap_segundos FLOAT64`, `voltas_atras INT64` |
| `throttle`/`brake` = 104 | 7,4% são sentinela | coluna `amostra_valida BOOL` |
| `drs` | código de 8 valores | `drs_ativo BOOL` + `drs_codigo INT64` |
| `n_gear` > 8 | 0,01% impossível | NULL |
| `stop_duration` | 95,8% nulo | descartar |
| `pit_duration` vs `lane_duration` | iguais em 78% | manter uma, documentar |
| `segments_sector_*` | array de tamanho variável | manter `ARRAY<INT64>` ou descartar |
| `gmt_offset` | STRING `"03:00:00"` | converter para INTERVAL ou minutos INT64 |
| datas | ISO com offset | TIMESTAMP; guardar tudo em UTC |

## Perguntas em aberto

1. **`location` entra na Silver?** 118 M linhas e nenhum dos dois modelos
   usa hoje. Sugiro tipar e deixar parada, sem as-of join.
2. **Voltas de treino e classificação entram no modelo de pneu?** A
   degradação em stint curto de classificação é outro fenômeno.
3. **Dias de teste de pré-temporada** (`Day 1/2/3`, 9 h cada) entram? Carro
   e programa são diferentes de fim de semana de GP.
4. **`team_radio`** vale transcrever, ou fica de fora do escopo?

## Ordem sugerida de implementação

1. `silver_sessoes` e `silver_pilotos` — dimensões, rápidas, destravam o resto
2. `silver_voltas` — entrega o modelo de degradação inteiro
3. `silver_telemetria` — a mais cara; entrega o modelo de piloto
4. `silver_clima` e `silver_eventos` — complementares
5. `silver_posicao` (location) — só se for necessário

As duas primeiras mais a terceira já permitem começar a Gold.
