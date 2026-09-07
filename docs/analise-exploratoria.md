# Análise exploratória da camada Bronze

Levantamento sistemático para decidir os tratamentos da Silver. Cada achado
aqui foi medido por consulta, não estimado, e traz a decisão que ele implica.

**Base:** 18 tabelas, ~330 milhões de linhas, temporadas 2024–2026.
**Início:** 07/09/2026.

Documento em construção — vai sendo preenchido conforme a análise avança.

---

## 1. Duplicatas exatas

`id_registro` é o MD5 do payload, então linhas com o mesmo id são registros
JSON idênticos.

| Tabela | Linhas | Distintas | Duplicatas | % |
|---|---:|---:|---:|---:|
| `weather` | 36.003 | 35.915 | **88** | 0,244% |
| `position` | 230.070 | 230.002 | **68** | 0,030% |
| `laps` | 182.247 | 182.247 | 0 | — |
| `stints` | 26.478 | 26.478 | 0 | — |
| `pit` | 24.782 | 24.782 | 0 | — |
| `intervals` | 1.629.087 | 1.629.087 | 0 | — |
| `drivers`, `sessions`, `session_result`, `race_control` | — | — | 0 | — |

### Origem

Investigado: **as duplicatas vêm do mesmo arquivo** (`COUNT(DISTINCT
_arquivo_origem) = 1` em todos os 88 e 68 grupos), e cada uma aparece
exatamente 2×.

Não é falha da ingestão nem carga dupla — foi a própria API da OpenF1 que
devolveu o mesmo registro repetido dentro de uma única resposta.

Exemplo (weather, sessão 9921):
```json
{"date":"2025-08-01T11:16:22.256000+00:00","session_key":9921,
 "air_temperature":25.8,"track_temperature":43.2,"humidity":39.0, ...}
```

> **Decisão para a Silver:** deduplicar por `id_registro` em todas as
> tabelas. O custo é desprezível e protege contra o problema voltar em
> ingestões futuras.

---

## 2. Chaves naturais

Testado se a chave de negócio de cada tabela identifica uma linha
unicamente.

| Tabela | Chave candidata | Colisões |
|---|---|---:|
| `laps` | (sessão, piloto, volta) | **0** |
| `stints` | (sessão, piloto, stint) | **0** |
| `session_result` | (sessão, piloto) | **0** |
| `drivers` | (sessão, piloto) | **0** |
| `weather` | (sessão, data) | 88 |
| `position` | (sessão, piloto, data) | 68 |

As colisões de `weather` e `position` são **exatamente** as duplicatas do
item 1 — mesmos 88 e 68 registros.

> **Conclusão:** depois de deduplicar por `id_registro`, todas as chaves
> naturais acima são únicas e podem ser usadas como chave primária na
> Silver.

---

## 3. Integridade referencial

Testado se toda chave estrangeira dos fatos existe nas dimensões.

| Tabela | Linhas | Sessão órfã | Piloto não listado na sessão |
|---|---:|---:|---:|
| `laps` | 182.247 | **0** | **0** |
| `intervals` | 1.629.087 | **0** | **0** |
| `session_result` | 6.443 | **0** | **0** |
| `starting_grid` | 1.575 | **0** | **0** |
| `stints` | 26.478 | **0** | 50 (0,189%) |
| `pit` | 24.782 | **0** | 8 (0,032%) |
| `position` | 230.070 | **0** | 9 (0,004%) |

**Zero sessões órfãs em todas as tabelas** — a dimensão de sessões cobre
100% dos fatos.

Os pilotos órfãos aparecem em dois padrões distintos, investigados no item
seguinte.

> **Decisão:** usar `LEFT JOIN` dos fatos para a dimensão de pilotos, nunca
> `INNER JOIN`. E construir `silver_pilotos` a partir da união dos números
> observados em todas as fontes, não só do endpoint `drivers`.

---

## 4. Stints fantasma

O caso mais relevante encontrado até aqui, porque afeta diretamente o modelo
de degradação de pneu.

### O sintoma

Em Spielberg 2025 (sessões 9949, 9950 e 9951), as fontes discordam sobre
quantos pilotos participaram:

| Sessão | `drivers` | `laps` | `car_data` | `stints` |
|---|---:|---:|---:|---:|
| 9949 | 20 | 20 | 20 | **32** |
| 9950 | 20 | 20 | 20 | **38** |
| 9951 | 20 | 20 | 20 | **32** |

Três fontes concordam em 20; `stints` tem quase o dobro.

### A causa

Os registros excedentes são todos iguais em forma:

```json
{"session_key":9950,"stint_number":1,"driver_number":33,
 "lap_start":1,"lap_end":1,"compound":"UNKNOWN","tyre_age_at_start":0}
```

Contra um stint legítimo da mesma sessão:

```json
{"session_key":9950,"stint_number":1,"driver_number":1,
 "lap_start":1,"lap_end":9,"compound":"HARD","tyre_age_at_start":0}
```

São **stints fantasma**: a API emite um registro vazio — composto
desconhecido, uma única volta, pneu com idade zero — para pilotos que não
participaram da sessão.

### Distribuição dos compostos em toda a base

| Composto | Linhas | Sessões | Voltas médias por stint |
|---|---:|---:|---:|
| `SOFT` | 12.957 | 291 | 4,8 |
| `MEDIUM` | 8.683 | 271 | 8,3 |
| `HARD` | 3.643 | 193 | 13,4 |
| `INTERMEDIATE` | 751 | 27 | 7,8 |
| `WET` | 151 | 8 | 5,1 |
| `TEST_UNKNOWN` | 132 | 4 | 6,2 |
| **NULL** | 93 | 10 | 7,1 |
| **UNKNOWN** | 68 | 16 | 2,8 |

Sanidade confirmada: a duração média do stint cresce com a dureza do
composto — SOFT 4,8 → MEDIUM 8,3 → HARD 13,4 voltas. O dado é fisicamente
coerente.

> **Decisão para a Silver:** marcar como inválido o stint com `compound` em
> (`UNKNOWN`, NULL) — 161 registros. Não excluir da Silver, que deve
> permanecer geral, mas expor uma coluna `stint_valido BOOL` para a Gold do
> modelo de pneu filtrar.
>
> `TEST_UNKNOWN` (132 registros, 4 sessões) é composto real de teste de
> pré-temporada, não fantasma — tratar à parte.

---

## 5. Qualidade dos tempos de volta

`lap_duration` é o alvo do modelo de degradação, então merece o exame mais
cuidadoso.

### Panorama geral (182.247 voltas)

| Métrica | Valor |
|---|---:|
| Sem tempo registrado | 7.065 (3,9%) |
| Abaixo de 50 s | **0** |
| Acima de 300 s | **11.251 (6,2%)** |
| Mínimo | 60,4 s |
| Mediana | 96,0 s |
| Percentil 99 | 934,2 s |
| Máximo | **3.510,5 s** (58 minutos) |
| Voltas de saída de box | 24.724 (13,6%) |
| Soma dos setores diverge do total em > 0,5 s | 140 (0,08%) |

Duas leituras:

- **Não há voltas impossivelmente rápidas.** Zero abaixo de 50 s, mínimo de
  60,4 s. O piso do dado é confiável.
- **A cauda superior é lixo.** Uma volta de 58 minutos é o carro parado na
  garagem ou uma bandeira vermelha, não uma volta. O percentil 99 em 934 s
  mostra que o 1% superior é inteiramente artefato.

Os setores são consistentes: em apenas 140 de 182 mil voltas a soma
`s1+s2+s3` diverge de `lap_duration` em mais de meio segundo.

### A sujeira se concentra em treino e classificação

| Tipo | Voltas | Sem tempo | Acima de 300 s | Saída de box | Mediana |
|---|---:|---:|---:|---:|---:|
| **Race** | 74.788 | **0,37%** | **0,15%** | 3,1% | 88,7 s |
| Practice | 84.346 | 5,4% | 9,3% | 18,5% | 100,9 s |
| Qualifying | 23.113 | 9,8% | 14,3% | 29,3% | 105,5 s |

A diferença é enorme e faz sentido físico: em treino e classificação o carro
entra e sai do box o tempo todo, e fica parado na garagem entre as
tentativas. Em corrida ele roda continuamente.

**88% das voltas acima de 300 s são voltas de saída de box**, então a flag
`is_pit_out_lap` já captura a maior parte do problema.

### Quanto sobra com um filtro de volta limpa

Filtro testado: tem tempo registrado, não é saída de box, não é entrada de
box (derivada do endpoint `pit`), e o tempo é no máximo 107% da mediana da
sessão.

| Tipo | Total | Com tempo | Sem box | **Limpa** | Aproveitamento |
|---|---:|---:|---:|---:|---:|
| **Race** | 74.788 | 74.511 | 70.131 | **64.833** | **86,7%** |
| Practice | 84.346 | 79.833 | 67.088 | 45.582 | 54,0% |
| Qualifying | 23.113 | 20.838 | 15.507 | 12.927 | 55,9% |

> **Decisões para a Silver e a Gold:**
>
> - A Silver expõe as colunas de diagnóstico — `tem_tempo`, `eh_saida_box`,
>   `eh_entrada_box`, `razao_mediana_sessao` — sem filtrar nada. Ela
>   permanece geral.
> - A Gold do modelo de pneu filtra. **Recomendo restringir a corridas**:
>   86,7% de aproveitamento contra ~55% nas outras, e o fenômeno de
>   degradação em stint longo só existe em corrida mesmo. Isso dá
>   **~64.800 voltas limpas** para treino, volume mais que suficiente.
> - `is_pit_out_lap` vem do endpoint `laps`, mas **a volta de entrada no box
>   precisa ser derivada** do endpoint `pit` — não existe flag equivalente.

---

## 6. `intervals`: o campo de tipo misto

`intervals` é o preditor de ar sujo do modelo de pneu — andar atrás de outro
carro degrada o composto mais rápido. É também a tabela com a armadilha de
tipagem mais perigosa da base.

### O problema

`gap_to_leader` e `interval` chegam ora como número, ora como texto:

| Campo | Nulo | Numérico | **Texto** |
|---|---:|---:|---:|
| `gap_to_leader` | 8.717 | 1.451.241 | **169.129 (10,4%)** |
| `interval` | 18.773 | 1.610.240 | 74 |

Em 1.629.087 linhas. O texto tem a forma `+1 LAP`, `+2 LAPS`, até `+17 LAPS`
— quando o carro está uma ou mais voltas atrás.

### Por que é perigoso

Um `CAST(gap_to_leader AS FLOAT64)` direto devolve NULL nessas 169 mil
linhas, **em silêncio**. E não é uma perda aleatória: são exatamente os
carros dobrados — os mais lentos, os que mais sofreram degradação. O modelo
perderia justamente o extremo do fenômeno que tenta prever.

### Verificação do padrão

Testadas as **169.533 ocorrências de texto** nas três colunas afetadas
(`intervals.gap_to_leader`, `intervals.interval`,
`session_result.gap_to_leader`) contra a expressão `^\+\d+ LAPS?$`:

```
textos totais       169.533
casam com o padrão  169.533
fora do padrão            0
```

Cobertura total, sem exceção escondida. O parse pode ser feito com segurança.

> **Decisão para a Silver:** desmembrar cada campo misto em duas colunas
> tipadas, nunca em uma só.
>
> ```sql
> SAFE_CAST(v AS FLOAT64)                                   AS gap_segundos,
> SAFE_CAST(REGEXP_EXTRACT(v, r'^\+(\d+) LAPS?$') AS INT64) AS voltas_atras
> ```
>
> As duas são mutuamente exclusivas: quando uma está preenchida, a outra é
> NULL. Uma terceira coluna `esta_dobrado BOOL` deixa o filtro explícito
> para a Gold.

---

## 7. Telemetria (`car_data`)

São 160 milhões de linhas e 36,8 GB, então a análise foi feita sobre uma
amostra de **8 corridas espalhadas por 2024, 2025 e 2026** — 5.663.341
linhas, 164 arquivos de piloto — materializada em
`f1_bronze._amostra_car_data`.

### O valor 104 é sentinela, não leitura

`throttle` e `brake` assumem o valor 104 simultaneamente em **689.454
linhas (12,17%)**. Detalhando o que o carro estava fazendo nessas linhas:

| Situação | Linhas | % do sentinela | Vel. média | RPM médio |
|---|---:|---:|---:|---:|
| Parado, motor desligado | 589.314 | 85,5% | 0,0 | 0 |
| Parado, motor ligado | 85.445 | 12,4% | 0,0 | 1.021 |
| Rodando devagar (< 50 km/h) | 12.974 | 1,9% | 3,3 | 634 |
| **Rodando rápido** | **1.721** | 0,25% | **185,8** | **10.421** |

O achado que muda o tratamento: **em 1.721 linhas o carro está genuinamente
em pista a ~186 km/h, com 10.421 rpm e marchas de 1 a 6** — telemetria real
em que apenas `throttle` e `brake` vieram corrompidos.

> **Decisão:** o sentinela invalida **os dois campos, não a linha**. Anular
> `throttle` e `brake` quando valem 104, preservando `speed`, `rpm`,
> `n_gear` e `drs`, que continuam válidos. Uma coluna auxiliar
> `em_pista BOOL` (`speed > 0 OR rpm > 0`) dá o filtro que a Gold precisa.
>
> Descartar a linha inteira seria mais simples e estaria errado.

### `drs` é um código de estado, e os dados provam

Cruzando cada código com a velocidade e o acelerador médios:

| Código | % | Vel. média | Throttle médio | Interpretação |
|---|---:|---:|---:|---|
| 0 | 53,8% | 135 | 41 | desativado |
| NULL | **23,3%** | 113 | 33 | ausente |
| 1 | 19,8% | 59 | 17 | desativado, baixa velocidade |
| 8 | 1,5% | 167 | 55 | elegível (zona de detecção) |
| **12** | 1,5% | **288** | **99** | **aberto** |
| **14** | 0,1% | 247 | 99 | **aberto** |
| **10** | 0,01% | 250 | 90 | **aberto** |
| 2, 3, 9, 13, 15 | < 0,05% | baixa | baixo | resíduo |

Os códigos 10, 12 e 14 aparecem sempre a alta velocidade e acelerador quase
pleno — exatamente onde o DRS é usado. Os códigos 0 e 1 dominam o resto.
Isso confirma a semântica pela evidência, não só pela documentação.

Atenção ao **NULL em 23,3%**: quase um quarto das amostras não tem DRS.

> **Decisão:** expor `drs_codigo INT64` (valor cru) e `drs_aberto BOOL`
> (`drs IN (10,12,14)`), com NULL preservado como NULL — e não convertido
> para `false`, que afirmaria algo que o dado não diz.

### Faixas dos campos, excluído o sentinela

| Campo | Mín | Máx | Anomalias |
|---|---:|---:|---|
| `speed` | 0 | 363 km/h | nenhuma acima de 400 |
| `rpm` | 0 | 13.566 | nenhuma acima de 15.000 |
| `n_gear` | 0 | **75** | **184 linhas acima de 8** |

Fora do padrão do sentinela existem ainda 49 linhas com `throttle > 100` e
1.628 com `brake` fora de {0, 100}.

Há também **6.221 linhas com o carro em movimento e `rpm = 0`** — falha
pontual de sensor.

> **Decisões:** `n_gear` fora de 0–8 vira NULL (184 linhas na amostra);
> `throttle` fora de 0–100 e `brake` fora de {0,100} viram NULL. Nenhuma
> linha é descartada na Silver — a decisão de filtrar é da Gold.

---

## 8. Viabilidade do *as-of join*

A Silver precisa dizer, para cada amostra de telemetria a 3,7 Hz, em que
volta ela caiu. Testado sobre a amostra de 5,66 M linhas.

### Método

`laps` traz `date_start` de cada volta. A janela de uma volta vai do seu
`date_start` até o `date_start` da volta seguinte do mesmo piloto:

```sql
LEAD(date_start) OVER (PARTITION BY session_key, driver_number ORDER BY lap_number)
```

### Resultado

| Onde a amostra cai | Amostras | % | Casaram | Vel. média |
|---|---:|---:|---:|---:|
| Antes da 1ª volta | 2.063.692 | 36,4% | 0 | 18,2 km/h |
| **Dentro do período de voltas** | **3.074.557** | **54,3%** | **100%** | **173,3 km/h** |
| Na última volta ou depois | 525.092 | 9,3% | 100% | 29,4 km/h |

**Dentro do período de voltas o casamento é de 100%** — nenhuma amostra
órfã. O método funciona.

Os 36,4% que não casam não são falha: é o carro na garagem e no pit lane
antes da primeira volta cronometrada, a 18 km/h de média. É a mesma janela
pré-sessão que já tínhamos medido na ingestão.

### Um detalhe de desenho que a análise revelou

A janela da última volta fica **aberta** (`LEAD` devolve NULL), então ela
absorve tudo o que vem depois — inclusive o retorno ao box após a bandeirada,
a 29,4 km/h de média. Isso inflaria artificialmente a última volta de cada
piloto.

> **Decisão:** fechar a janela da última volta em
> `date_start + lap_duration`, e não deixá-la aberta. Amostras posteriores
> ficam com `lap_number` NULL, que é a verdade.

### Erro cometido e corrigido

Na primeira execução desta análise eu listei a sessão 11340 no filtro, mas a
amostra continha a 11327. Isso criou uma categoria falsa de "piloto sem
volta nenhuma" com 390 mil amostras (6,9%), que na verdade era uma sessão
inteira cujas voltas eu não havia carregado. Verificado depois: **não existe
nenhum piloto com telemetria e sem voltas** em toda a base.

---

# Resumo das decisões para a Silver

Consolidado do que a análise determinou. Cada linha tem o achado que a
justifica.

| # | Tratamento | Justificativa | Volume afetado |
|---|---|---|---|
| 1 | Deduplicar por `id_registro` em todas as tabelas | API devolveu registros repetidos na mesma resposta | 156 linhas |
| 2 | `LEFT JOIN` para a dimensão de pilotos, nunca `INNER` | 67 fatos com piloto não listado em `drivers` | 67 linhas |
| 3 | Dimensão de pilotos = união dos números observados | pilotos de teste (241–247) não aparecem em `drivers` | — |
| 4 | `stint_valido = compound NOT IN (UNKNOWN, NULL)` | stints fantasma com 1 volta e composto desconhecido | 161 linhas |
| 5 | Desmembrar `gap_to_leader` em `gap_segundos` + `voltas_atras` | 10,4% é texto `+N LAPS`; CAST direto perde os dobrados | 169.533 linhas |
| 6 | Anular `throttle`/`brake` quando valem 104, **sem descartar a linha** | 1.721 amostras têm telemetria real com só esses 2 campos corrompidos | 12,2% da telemetria |
| 7 | `drs_codigo` + `drs_aberto`, NULL preservado | código de estado, não booleano; 23,3% ausente | — |
| 8 | `n_gear` fora de 0–8 vira NULL | fisicamente impossível | 0,003% |
| 9 | Colunas de diagnóstico de volta, sem filtrar | Silver é geral; filtro é da Gold | — |
| 10 | Derivar `eh_entrada_box` do endpoint `pit` | `laps` só tem a flag de saída | — |
| 11 | Fechar a janela da última volta em `date_start + lap_duration` | janela aberta absorve o retorno ao box | 9,3% da telemetria |
| 12 | Descartar `stop_duration` | 95,8% nulo | — |
| 13 | Datas para TIMESTAMP em UTC; `gmt_offset` para minutos INT64 | vêm como texto ISO com offset | — |

## O que a Silver **não** faz

Não filtra linha, não agrega e não decide o que é volta válida. Ela tipa,
deduplica, desmembra campos mistos, resolve o *as-of join* e **expõe
colunas de diagnóstico**. Quem escolhe o recorte é a Gold, e cada modelo
escolhe o seu.
