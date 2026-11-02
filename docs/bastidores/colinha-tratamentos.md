# Tratamentos Silver e Gold

---

## SILVER

**Princípio:** tipa, deduplica, corrige e **marca**. 

**4 tabelas:** `silver_sessoes` (377) · `silver_pilotos` (7.733) ·
`silver_voltas` (245 mil) · `silver_telemetria` (216 mi)

### Tratamentos

| # | Tratamento | Detalhe |
|---|---|---|
| 1 | **Deduplicação** | por `id_registro` (MD5 do payload) — a API repetiu 156 registros |
| 2 | **Tipagem** | `payload` STRING → colunas tipadas |
| 3 | **Campo de tipo misto** | `gap_to_leader` vinha número **ou** texto `"+1 LAP"` (10,4%) → 2 colunas: `gap_segundos` + `voltas_atras` |
| 4 | **Sentinela 104** | `throttle`/`brake` = 104 é "sem telemetria" → **anula os 2 campos, mantém a linha** |
| 5 | **Valor impossível** | `n_gear` fora de 0–8 → NULL (origem trazia até 75) |
| 6 | **Código, não booleano** | `drs` → `drs_codigo` + `drs_aberto` (10/12/14 = aberto), NULL preservado |
| 7 | **Stint fantasma** | composto `UNKNOWN`, 1 volta, idade 0 → `stint_valido = false` |
| 8 | **Sobreposição de stint** | 1 volta casava com 2 stints → duplicava. Agora 1 por volta + `stint_ambiguo` |
| 9 | **As-of join** | cada amostra recebe volta e stint; cada volta recebe clima e gap |
| 10 | **Janela da última volta** | fecha em `date_start + lap_duration` (senão absorve retorno ao box) |
| 11 | **Formato** | `gmt_offset` texto `"03:00:00"` → minutos; datas → TIMESTAMP UTC |
| 12 | **Dimensão de pilotos** | união de **todas** as fontes, não só `drivers` (67 fatos ficariam órfãos) |

### Colunas de diagnóstico (marcam, não filtram)

`volta_limpa` · `tem_tempo` · `eh_saida_box` · `eh_entrada_box` ·
`razao_mediana_sessao` · `em_pista` · `telemetria_sentinela` · `stint_valido`

### Detalhando o item 9 — as-of join

**O problema:** três granularidades que não casam por chave.

| Granularidade | Tabelas | Volume |
|---|---|---:|
| Amostra (~3,7 Hz) | `car_data`, `location`, `intervals` | 438 mi |
| Volta | `laps`, `stints`, `pit` | 296 mil |
| Minuto | `weather` | 48 mil |

Não existe chave comum — só **tempo**. Casar exige perguntar *"que volta
estava em curso neste instante?"*.

**A solução:** transformar cada evento numa janela de validade e casar por
intervalo.

```sql
-- fim da volta = início da próxima (LEAD)
COALESCE(LEAD(date_start) OVER (PARTITION BY session_key, driver_number
                                ORDER BY lap_number),
         date_start + lap_duration) AS data_fim
```

Depois é só `t.data >= j.data_inicio AND t.data < j.data_fim`.

**Resultado medido:** 100% das amostras dentro do período de voltas casam.
Os 36% que não casam são o carro na garagem antes da 1ª volta cronometrada,
a 18 km/h de média — ficam com `lap_number` NULL, que é a verdade.

**Por que na Silver e não na Gold:** resolvido uma vez, serve os dois
modelos. Na Gold seria refeito a cada consulta.

---

## GOLD — filtra e deriva features

**Princípio:** cada modelo recorta o que precisa. Mesma Silver, filtros
diferentes.

### `gold_degradacao_pneu` — 86 mil linhas

**Filtros**
- só corrida (86,7% de aproveitamento vs ~55% em treino/quali)
- sem teste de pré-temporada
- `stint_valido`, tem tempo, composto seco (SOFT/MEDIUM/HARD)
- **sem** voltas de entrada e saída de box
- `razao_mediana_sessao <= 1.15` — corte **frouxo** de propósito: o apertado
  (1,07) descartaria as voltas mais degradadas, que são o fenômeno

**Features derivadas**
- `volta_no_stint` — aquecimento do pneu (primeiras voltas são lentas)
- **`volta_corrida`** — proxy de **combustível**. Sem ela a regressão atribui
  ao pneu um efeito que é do tanque, e inverte o sinal
- `voltas_atras` → `COALESCE(0)`

### `gold_assinatura_piloto` — 101 mil linhas

**Filtros**
- volta identificada, `em_pista`, só corrida
- **`NOT telemetria_sentinela`** — aqui **descarta**, porque as features
  *são* acelerador e freio
- `amostras_validas >= 100`

**Features derivadas** — telemetria 3,7 Hz agregada por volta
- `pct_acelerador_pleno` (throttle ≥ 99), `pct_freando`
- **`suavidade_acelerador`** — desvio padrão da *variação* entre amostras (`LAG`)
- `trocas_marcha`, `vel_media/max/desvio`, `rpm_medio`, `pct_drs_aberto`

**Cuidado central: vazamento**
`driver_number`, `equipe` e `nome_completo` ficam **fora**. `equipe`
reduziria 28 pilotos para 14 opções. `circuito` fica — é contexto, não
identidade.

