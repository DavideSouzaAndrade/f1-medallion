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
