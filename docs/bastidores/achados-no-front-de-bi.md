# Três achados na interface de BI

Encontrados em 01/11/2026 olhando a aba *Ao vivo* com o pipeline parado.
Dois são do front, um é consequência de uma correção do Dataflow. Nenhum
quebra dado — os três **mentem na tela**, que é pior de perceber.

## 1. `chegou há 6 s` reinicia a cada F5

**O que acontece:** o cartão *Última volta* mostra "chegou há 6 s" mesmo
quando o último evento é de uma hora atrás, e o contador reinicia a cada
recarga da página.

**Por quê:** o tempo é contado desde o carregamento da página, não desde o
`ts_evento` da volta.

**Por que importa:** é o pior dos três. Ele **afirma que o dado é fresco
quando não é**. Numa apresentação, ninguém percebe — até alguém perguntar
por que o gráfico não anda.

**Correção:** calcular a diferença contra `ts_evento` da volta mais recente,
não contra o relógio da página.

## 2. `n/a (volta de box)` em volta que não é de box

**O que acontece:** na tabela *Últimas voltas*, as 21 linhas da volta 7
aparecem com `Tempo —` e `Previsto: n/a (volta de box)`.

**Por quê:** o front infere "volta de box" de `duracao_s` nula. Mas
`eh_entrada_box = 0` em todas as 21 — medido. São voltas que **começaram e
nunca terminaram**, porque o replay foi cortado em 45 minutos de corrida.

A diferença é real e o pipeline a expressa: `eh_entrada_box` diz se houve
box, `duracao_s` nula diz que a volta não fechou. São duas informações
independentes.

**Correção:** usar `eh_entrada_box`/`eh_saida_box` para dizer "volta de
box", e `duracao_s IS NULL` para dizer "volta incompleta".

## 3. `Latência do pipeline: —`

**O que acontece:** o cartão mostra `—` em vez de um número, com a mediana
correta (5,0 s) logo abaixo.

**Por quê:** isto **não é bug**, é consequência de uma correção do Dataflow
de 01/11. Três voltas por corrida nunca recebem `lap_duration` (abandono,
corte do replay) e saem pelo prazo de abandono. Elas não têm `publish_time`
de versão final, então `latencia_ms` fica **nula** — antes ficava zero, o
que afirmava processamento instantâneo e contaminava o p95 que o próprio
painel de saúde cobra.

O cartão lê a latência da volta mais recente, e a mais recente é sempre uma
dessas — elas saem por último, por construção.

**Correção sugerida:** mostrar a mediana no cartão, ou a última latência
**não nula**, com "—" apenas quando não houver nenhuma.

## O que os três têm em comum

Nenhum deles perde dado. Os três **interpretam ausência como outra coisa**:
ausência de evento recente vira "acabou de chegar", ausência de tempo vira
"volta de box", ausência de latência vira "—" no lugar de destaque.

Em pipeline de dados, nulo quase sempre quer dizer algo específico, e vale
perguntar o quê antes de escolher o que mostrar.
