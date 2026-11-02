# O caso da carência: quando o teste verde esconde o bug

Registro de um problema achado na frente B (Dataflow) em 01/11/2026, do
sintoma à correção. Vale como caso de apresentação porque o erro não estava
no código que falhou — estava no **teste que passou**.

## O contexto em uma frase

No contrato v2, o Pub/Sub deixa de transportar a volta pronta e passa a
transportar o dado bruto da OpenF1. Montar a volta — o que a `silver_voltas`
fazia em SQL, sobre a corrida inteira — passa a ser feito em streaming, com
as mensagens chegando uma a uma e fora de ordem.

Uma volta nasce de ~26 mensagens: 4 versões de `v1/laps` que se substituem,
1 de `v1/stints`, até 1 de `v1/pit`, e ~21 de `v1/intervals` que entram na
média de gap.

## O sintoma

76 verificações offline passando. Primeira execução contra o Pub/Sub real,
comparada volta a volta com a `silver_voltas` da mesma corrida:

| coluna | voltas iguais ao lote |
|---|---|
| `data_inicio` | 64/64 |
| `eh_saida_box` | 64/64 |
| `duracao_s` | **53/64** |
| `composto` | **46/64** |
| `idade_pneu` | **46/64** |
| `temp_pista` | **21/64** |

E 77 linhas gravadas para 64 voltas distintas — 13 duplicatas.

## O diagnóstico

Três causas distintas, que só se separaram olhando as linhas uma por uma.

### 1. O gatilho de emissão estava no lugar errado

No lote, a janela de uma volta termina no início da seguinte — é o `LEAD` da
`silver_voltas`. Eu li isso como "a volta está pronta quando a seguinte
começa" e usei como gatilho de emissão, com 3 segundos de carência.

Está errado, e por um motivo que só o ao vivo expõe: **o início da volta
seguinte fecha a janela dos intervals, não a volta**. A versão final da volta
N — a única que traz `lap_duration` — pode chegar *depois* da primeira versão
da volta N+1, porque o Pub/Sub não garante ordem. Quando isso acontecia, a
volta N era emitida sem tempo, sem composto e sem idade de pneu.

E aí vinha a duplicata: a versão final chegava atrasada, recriava a volta no
estado, e ela era emitida uma segunda vez — agora completa. Como a
deduplicação do BI usa `ORDER BY _ts_processamento` e fica com a **primeira**
linha, o resultado que sobrevivia era justamente o incompleto.

Correção: a volta é emitida quando a **própria** versão final chega
(carência curta, 3 s). O início da volta seguinte só marca uma carência longa
(30 s), que existe para as 3 voltas por corrida que nunca ganham
`lap_duration` — abandonos e entradas de box.

### 2. O clima era resolvido na chegada, não na emissão

Eu tirava um retrato do clima no instante em que a volta nascia. Mas a
leitura de clima vigente pode chegar depois da volta que a usa. Resultado:
56,3 °C onde o lote dizia 55,7 °C — a leitura seguinte, não a vigente.

Correção: guardar as leituras com a data de medição e resolver o *as-of* na
emissão, exatamente como o `LEFT JOIN` do lote faz. São ~176 leituras por
corrida: cabe no estado sem pensar.

### 3. O mesmo erro, na média de gap

Idêntico em espírito: eu atribuía cada medição de gap à volta aberta no
momento da **chegada**. A medição que chegasse antes da volta que a contém ia
para a volta anterior, ou se perdia.

Correção: guardar as medições com a data e calcular a média sobre a janela
da volta na emissão. O cuidado aqui é que intervals são 80% do volume
(21.955 por corrida): guardar tudo seria inviável. A poda resolve — depois de
cada emissão, descarta-se o que nenhuma volta aberta pode mais usar, e o
buffer fica em ~2 voltas por piloto.

## O que estava errado no teste

Esta é a parte que vale contar.

O teste offline montava a corrida com o próprio gerador, empurrava **todas**
as mensagens pelo estado e só então colhia as voltas. Ele até embaralhava a
ordem — e passava.

Passava porque colher no fim esconde o problema: na hora da colheita, todas as
mensagens já estavam no estado, então nunca havia dado faltando. **Em produção
o timer dispara continuamente, no meio da chegada das mensagens.**

A correção do teste foi combinar duas coisas que eu tinha separadas:

1. **reordenação local** — embaralhar dentro de uma janela deslizante, que é
   como o Pub/Sub reordena de verdade. Embaralhar a corrida inteira inverte a
   causalidade e testa um cenário que não existe.
2. **colheita intercalada** — colher depois de *cada* mensagem, como o timer
   do Beam faz.

Com as duas juntas, o desenho antigo falhava em 6 de 15 execuções e o novo em
0 de 15.

## A premissa que o caso revelou

Variando o intervalo entre mensagens no teste, dá para medir a margem:

| deslocamento máximo da reordenação | execuções divergentes |
|---:|---:|
| 0,2 s | 0/15 |
| 2,0 s | 0/15 |
| 4,0 s | 1/15 |
| 10,0 s | 5/15 |

A regra é simples e agora está escrita no código: **a carência precisa ser
maior que o atraso de reordenação.** A carência é de 3 s; em produção, a 110
mensagens/s do fator 30, 20 mensagens de deslocamento são 0,18 s — margem de
~16×.

E a degradação é graciosa: acima da carência o pipeline perde o *valor* de
uma coluna, nunca a volta. Isso também virou verificação.

## Uma divergência que não é bug

`eh_entrada_box` na volta 3 desta corrida: o lote diz `true` para 21 pilotos,
o streaming diz `false`. Investigando, a corrida teve **bandeira vermelha** na
volta 3 — todos foram ao box, e a volta 4 durou 1.954 segundos.

Os registros de `v1/pit` dessas paradas têm data de **2.137 segundos depois
da largada**: 36 minutos. A volta 3 terminou aos ~5 minutos. Ou seja, ao vivo
é *impossível* saber que aquela volta teve entrada no box — o dado só existe
meia hora depois.

Isso não se corrige; documenta-se. É a diferença real entre lote e streaming,
e é o argumento para a DAG de reprocessamento em lote existir: ela volta e
completa o que o ao vivo não podia saber.

## O que fica

- Teste que passa não é teste que cobre. O que faltava não era mais
  verificação — era **reproduzir a forma do tempo real**: chegada fora de
  ordem *e* emissão contínua.
- Comparar com o lote foi o que achou tudo. A `silver_voltas` funcionou como
  oráculo: 64 voltas conferidas coluna por coluna.
- Três bugs diferentes com a mesma raiz: **decidir na chegada o que devia ser
  decidido na emissão.** Clima, gap e fechamento da volta, todos pelo mesmo
  motivo.

---

# Parte 2: o runner local mentiu sobre o desempenho

O mesmo pipeline, a mesma corrida (11361, 45 minutos de replay, 3.637
mensagens), medido nos dois ambientes no mesmo dia.

**Local**: Apple M4, 10 núcleos, 16 GB, macOS 26.6.2, Python 3.14.3,
`BundleBasedDirectRunner`.
**Nuvem**: Dataflow, 1 worker `e2-standard-2` (2 vCPU, 8 GB), Streaming
Engine ligado.

## Latência

`latencia_ms` mede do `publish_time` da versão final da volta no Pub/Sub até
a linha gravada no BigQuery.

| | M4 local | Dataflow | fator |
|---|---:|---:|---:|
| mínimo | 4,10 s | **2,68 s** | 1,5× |
| mediana | ~60 s | **4,60 s** | **13×** |
| p95 | 97,0 s | **8,22 s** | 12× |
| máximo | 99,0 s | **8,87 s** | 11× |

O M4 é uma máquina mais rápida que a `e2-standard-2` em núcleo único, e
perdeu por uma ordem de magnitude. A diferença não é de CPU.

## Corretude — e aqui está o ponto

Comparação volta a volta com a `silver_voltas` da mesma corrida:

| coluna | M4 local | Dataflow |
|---|---:|---:|
| `duracao_s` | 118/118 | **127/127** |
| `composto` | 116/118 | **127/127** |
| `idade_pneu` | 116/118 | **127/127** |
| `temp_pista` | 110/118 | **127/127** |
| `gap_medio_a_frente` | 105/118 | **127/127** |
| `eh_saida_box` | 118/118 | **127/127** |
| `eh_entrada_box` | 113/118 | 106/127 |

**O Dataflow não foi só mais rápido: foi mais correto.** Todas as colunas
batem com o lote, exceto `eh_entrada_box` — e essa é a divergência
fisicamente impossível de resolver ao vivo, explicada acima.

A razão liga as duas tabelas. A corretude da montagem depende da carência
ser maior que o atraso de reordenação. Quando o runner não acompanha o
fluxo, o atraso que importa deixa de ser o do Pub/Sub e passa a ser o da
própria fila interna do runner — 60 segundos, contra uma carência de 1 a 3.
As colunas que dependem de dado chegando a tempo (clima, gap, composto)
degradam.

Ou seja: **o runner lento não só atrasa o resultado, ele muda o
resultado.** É o argumento central deste caso.

## Por que o M4 perde para uma VM de 2 vCPU

Não é vazão de CPU, é arquitetura. Toda volta é emitida por um *timer*, e o
`DirectRunner` é um processo único que intercala processar elementos e
disparar timers: enquanto mastiga uma rajada de 1.325 `intervals`, o timer
que devia disparar em 3 segundos dispara 60 segundos depois.

O Dataflow com Streaming Engine mantém estado e timers **fora** do worker,
num serviço dedicado, e os dispara no prazo independentemente do que o
worker esteja fazendo. É para isso que o Streaming Engine existe.

A conclusão prática: o `DirectRunner` serve para achar erro de lógica — e
achou três, descritos na primeira parte deste documento. Ele não serve para
medir desempenho nem para validar o sistema, e número tirado dele não
descreve o pipeline.

# Parte 3: duas otimizações e um bug que elas revelaram

## Emitir quando a volta está completa

A carência existe para esperar dado atrasado. Quando nada está atrasado,
esperar só adiciona latência — e no caso comum nada está: o stint sai no
início da volta, o clima já vigia, e as medições de gap são feitas durante
ela.

A otimização: se a volta já tem `duracao_s`, já casou com um stint, já tem
clima vigente e já tem medição de gap cobrindo até o fim dela, ela sai
depois de 1 segundo (`GRACA_COMPLETA_S`) em vez de 3 (`GRACA_S`).

Não é heurística: cada condição é a presença efetiva do dado que alimenta
uma coluna. A mais exigente é a cobertura de gap, que é justamente a coluna
que mais sofria.

**E quase não funcionou.** Na primeira medição no Dataflow o piso foi de
2,68 s, não de ~1 s. A causa: a medição de gap que cobre o fim da volta é
publicada ~4 segundos de corrida *depois* do fim dela, ou seja ~0,13 s de
relógio depois da versão final no fator 30. Quando ela chegava, a volta já
tinha marcado os 3 segundos e nada reavaliava.

A correção foi reavaliar na chegada do stint e do interval, antecipando o
prazo pendente. Mesma raiz de sempre neste projeto: *decidir uma vez, no
momento errado*.

## O custo da otimização, medido

Baixar a carência efetiva de 3 s para 1 s estreita a tolerância a
reordenação na mesma proporção. A matriz do teste mostra onde está a
fronteira:

| deslocamento máximo da reordenação | execuções divergentes |
|---:|---:|
| 0,2 s | 0/15 |
| 0,5 s | 0/15 |
| 1,0 s | 0/15 |
| 2,0 s | 1/15 |

A margem sobre a reordenação real (~0,18 s a 110 mensagens/s) cai de ~16×
para ~5×. É uma troca consciente, e está escrita no código.

## A mediana envenenada

A `gold_degradacao_pneu` treina só com volta dentro de 1,15× a mediana da
sessão: safety car e bandeira ficam fora. Eu não tinha implementado isso, e
o efeito apareceu no erro do modelo:

| volta | real | previsto | erro |
|---|---:|---:|---:|
| 2 (corrida normal) | 88,5 s | 87,4 s | **1,32 s** |
| 1 (largada parada) | 94,1 s | 87,4 s | 6,73 s |
| 3 (bandeira vermelha) | 142,4 s | 87,4 s | 54,98 s |

O modelo é bom — 1,5% de erro em volta de corrida. A média de 19,68 s vinha
inteira das voltas neutralizadas. O contrato v2 já previa a solução: *"o
Dataflow só pode usar a mediana das voltas recebidas até o momento"*.

Implementei, e **não funcionou** — a volta 6 recebeu previsão com 58,62 s de
erro. O motivo é bonito: a corrida 11361 teve bandeira vermelha na volta 3,
e **4 das 6 primeiras voltas foram anormais**.

| volta | duração média | razão vs. mediana de todas |
|---|---:|---:|
| 1 | 94,1 s | 0,65 |
| 2 | 88,5 s | 0,61 |
| 3 | 144,7 s | 1,00 |
| 4 | 1.954,2 s | 13,44 |
| 5 | 197,5 s | 1,36 |
| 6 | 146,7 s | 1,01 |

Com as anômalas na amostra, a mediana corrente subiu para ~145 s. A volta 6,
de 146,7 s e completamente neutralizada, deu razão **1,01** e passou pelo
corte.

O lote não sofre disso porque divide pela mediana da corrida **inteira**, em
que as 53 voltas normais dominam as 4 anormais. Ao vivo, no início da
sessão, a proporção se inverte.

A correção: na amostra da mediana entra só a volta julgada normal. Julgar e
registrar ficam na mesma função, para que não exista volta julgada normal
que não entrou na amostra nem o contrário.

## O que fica desta parte

- **Medir no ambiente de verdade antes de otimizar.** Eu propus três
  otimizações contra um número que o runner local produziu; 56 dos 60
  segundos eram do runner.
- **Uma otimização pode não funcionar por um detalhe de 0,13 segundo.** A
  alavanca da volta completa estava correta e inerte, porque faltava
  reavaliar.
- **Amostra estatística ao vivo precisa se defender do próprio dado.** A
  mediana corrente parecia a tradução óbvia da mediana da sessão, e era —
  até a corrida ter quatro voltas anormais em seis.

## A simplificação que os números pediram

Depois de corrigir a mediana, rodei a consulta que o painel de BI do Davi
usa. Ela reportou `aguardando_predicao = 43` de 106 voltas aptas — 40% do
painel marcado como "predição pendente", com o pipeline funcionando
perfeitamente.

A causa não era um bug: era uma modelagem errada minha. Eu tinha duas noções
separadas — `apto_modelo` ("a volta serve ao modelo") e uma marca interna de
volta anômala ("a volta está dentro da faixa") — e passava a segunda à
predição por um campo auxiliar que viajava com a linha e era removido antes
da escrita.

Mas a `gold_degradacao_pneu` **não separa as duas**. O filtro dela é único:
composto seco, campos presentes, fora do box **e** dentro de 1,15× a
mediana. Uma volta de safety car não é "apta mas excluída" — ela simplesmente
não é apta, porque o modelo nunca viu uma.

Unificar consertou três coisas e removeu código:

| | antes | depois |
|---|---|---|
| volta sem previsão | "excluída de propósito" **ou** "API falhou", indistinguíveis | só "API falhou" |
| `volta_no_stint` | numerava um conjunto diferente do da Gold | numera o mesmo conjunto |
| campo auxiliar entre montagem e predição | existia | deixou de existir |

A lição é de modelagem, não de código: **duas condições que a fonte da
verdade trata como uma só não devem ser separadas a jusante.** O sintoma
apareceu como um número estranho no painel de outra pessoa, três camadas
depois de onde estava a causa.
