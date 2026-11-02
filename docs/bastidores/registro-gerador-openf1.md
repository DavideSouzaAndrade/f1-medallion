# Registro: gerador no formato da OpenF1 (início da parte 3)

O que mudou no gerador, no Pub/Sub e nos contratos em 31/10, e o que isso
significa para o novo Dataflow. Escrito para quem vai construir o Dataflow
começar com o contexto completo, sem precisar reconstruir a conversa.

Os detalhes ficam nos documentos de referência; este registro é o resumo e o
porquê:

- [contrato-openf1.md](../contrato-openf1.md): o que **entra** no Pub/Sub.
- [contrato-evento.md](../contrato-evento.md): o que o Dataflow **grava** em
  `f1_stream.voltas`.
- [producer/README.md](../../producer/README.md): como rodar o gerador.

---

## Por que mudou

O plano da equipe para a parte 3 pede que o gerador publique o dado **bruto**,
como se viesse direto da API da OpenF1, para que um dia seja possível trocar
o gerador pelo feed ao vivo sem mexer no resto. Também pede que o Dataflow
faça a passagem Bronze→Silver em streaming e chame o modelo.

Até a parte 2, o gerador lia a `silver_voltas` e publicava a volta já
montada e tratada (contrato v1). O Dataflow só validava e gravava: o
tratamento de verdade tinha sido feito antes, no batch. Com a fonte trocada
pela OpenF1, esse desenho não funcionaria, porque a OpenF1 não entrega voltas
montadas.

Agora:

```
Antes:  silver_voltas ──► gerador (monta a volta) ──► f1-voltas ──► Dataflow (valida) ──► f1_stream.voltas

Agora:  Bronze (JSON bruto) ──► gerador (replay) ──► f1-openf1 ──► Dataflow (monta, trata, prevê) ──► f1_stream.voltas
                                                                       │
                                                                       └──HTTP──► API do Davi
```

## O que mudou no GCP

Aplicado com `setup/setup_streaming.sh` e `producer/deploy.sh` (nada foi
feito pelo console).

| Recurso | Antes | Agora |
|---------|-------|-------|
| Tópico | `f1-voltas` | `f1-openf1` (o antigo foi **apagado**) |
| Assinatura do Dataflow | `f1-voltas-dataflow` | `f1-openf1-dataflow` |
| Assinatura de inspeção | `f1-voltas-debug` | `f1-openf1-debug` |
| Cloud Run Job `f1-gerador` | publicava a volta v1 em `f1-voltas` | publica as mensagens brutas em `f1-openf1` |
| Permissão da SA `f1-pipeline` | publicar em `f1-voltas` | publicar em `f1-openf1`; leitura da `f1_bronze` testada |

Não mudou: BigQuery (Bronze, Silver, Gold, tabelas e view de `f1_stream`),
Dataform, a API do Davi e os tópicos `aula-pdm-*`.

**Estado em 31/10:** a `f1-openf1-dataflow` tem ~1.046 mensagens de dois
testes (2 min e 1 min da corrida 11361, dois `replay_id` diferentes, 5 delas
injetadas). Servem de amostra para o primeiro teste do Dataflow; expiram
sozinhas em 7 dias. Para descartar:

```bash
gcloud pubsub subscriptions seek f1-openf1-dataflow --time=$(date -u +%Y-%m-%dT%H:%M:%SZ)
```

## O que mudou no código

| Arquivo | Mudança |
|---------|---------|
| `producer/fonte.py` | Lê da Bronze o `payload` de laps, stints, pit, weather, intervals, sessions e meetings, em vez da `silver_voltas` |
| `producer/cronograma.py` | **Novo.** Decide o que a OpenF1 publicaria e quando: versões de cada volta e de cada stint, na ordem da corrida |
| `producer/corrupcao.py` | **Novo.** Cópias quebradas para a demonstração da DLQ |
| `producer/publisher.py` | Publica cada registro bruto no ritmo da corrida, com `_key`, `_id` e atributos |
| `producer/test_cronograma.py` | **Novo.** 33 verificações offline |
| `producer/evento.py`, `producer/test_evento.py` | **Apagados.** Montar a volta passou a ser trabalho do Dataflow |
| `producer/config.py`, `producer/deploy.sh` | Tópico novo e parâmetros novos (`--minutos` no lugar de `--limite`) |
| `setup/setup_streaming.sh` | Cria o tópico e as assinaturas novos e apaga os do v1 |
| `docs/contrato-openf1.md` | **Novo.** Contrato da mensagem bruta (v2) |
| `docs/contrato-evento.md` | Passa a descrever a volta montada pelo Dataflow; ganhou a seção "Identificação" |
| `README.md`, `producer/README.md` | Atualizados; o parágrafo do Dataflow no README ficou como `[A ATUALIZAR — frente B]` |

## Como o gerador funciona agora

1. Lê da Bronze todos os registros brutos de uma corrida. O `payload` é o
   JSON que a API REST da OpenF1 devolveu na ingestão, sem alteração.
2. Monta a linha do tempo do que a OpenF1 publicaria ao vivo:
   - **laps**: cada volta sai em até 4 versões com o mesmo `_key`: ao
     cruzar a linha (sem tempos), ao fim do setor 1, ao fim do setor 2 e
     no fim da volta (com `lap_duration`). A última é idêntica à Bronze.
   - **stints**: o stint nasce com `lap_end = lap_start` e ganha uma versão
     a cada volta, com `lap_end` avançando. A Bronze só tem o `lap_end`
     final, que publicado no início entregaria o futuro.
   - **weather, intervals, pit**: uma mensagem por registro, no instante
     `date`.
   - **meetings, sessions**: uma mensagem cada, no início.
   - O replay começa 1 min antes da largada; do pré-corrida só entra o
     último registro de clima.
3. Publica no ritmo real dividido por `--fator` (padrão 30: a corrida
   inteira, ~27 mil mensagens, em ~4 min).

O gerador não inventa valores: tempos, gaps, pneus e clima são todos da
corrida real. Ele só cria as versões intermediárias (o registro real com
campos ainda nulos), o `_key`/`_id` e, com `--injetar-invalidos`, cópias
extras quebradas.

Conferido na corrida 11361 inteira: a última versão das 1.055 voltas e dos 52
stints é igual à Bronze; nenhuma versão de stint anuncia uma volta que ainda
não começou; nenhum `_key` duplicado em intervals, weather ou pit.

## A mensagem que o Dataflow recebe

Corpo (exemplo, versão final da volta 23 do piloto 44):

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

Atributos: `topico` (`v1/laps`, `v1/stints`...), `session_key`, `fonte`
(`replay` ou, no futuro, `openf1`), `replay_id` e, só nas cópias quebradas,
`injetado`.

## O que o novo Dataflow precisa fazer

Resumo do que os dois contratos pedem. A ordem é sugestão, não decisão.

1. **Ler** `f1-openf1-dataflow` e separar por atributo `topico`.
2. **Mandar para a DLQ** o que não dá para ler: JSON quebrado, sem
   `session_key`/`driver_number`, tipo errado, tópico desconhecido (os
   quatro tipos de `injetado`).
3. **Guardar o estado por `_key`**, ficando com a versão de maior `_id`: o
   Pub/Sub entrega pelo menos uma vez e fora de ordem, e laps e stints chegam
   em várias versões.
4. **Montar a volta quando chegar a versão com `lap_duration`**
   (ou a seguinte começar, para voltas sem tempo), juntando:
   - o stint ativo do piloto: `composto`, `stint_number`, `idade_pneu`;
   - o último clima: `temp_ar`, `temp_pista`, `umidade`, `chuva`;
   - a média do `interval` do piloto dentro da janela da volta:
     `gap_medio_a_frente`;
   - pit na mesma volta: `eh_entrada_box`;
   - sessions/meetings: `circuito`, `circuit_type`, `year`.
   Como a volta pode chegar antes do stint ou do pit dela, vale esperar um
   pouco antes de montar.
5. **Tratar o dado bruto como a Silver trata**: `"+1 LAP"`/`"+2 LAPS"` no
   gap viram nulo; gaps nulos ficam fora da média; composto em maiúsculas.
6. **Calcular `volta_no_stint`** como a Gold
   (`definitions/gold/gold_degradacao_pneu.sqlx`): posição entre as voltas
   limpas do stint. O filtro de volta limpa usa `razao_mediana_sessao <=
   1.15`; a Silver calcula essa razão com a mediana da corrida inteira, que
   ao vivo ainda não existe. **Em streaming só dá para usar a mediana das
   voltas recebidas até o momento.** Até a parte 2 quem calculava isso era o
   `producer/evento.py` (apagado; está no histórico, commit `78f9de8`).
7. **Gravar** em `f1_stream.voltas` no formato do
   [contrato-evento.md](../contrato-evento.md), com
   `evento_id = replay_id + ":" + _key da volta` e `ts_evento` = o
   `publish_time` da versão final. Com isso a `vw_voltas_unificada`
   continua deduplicando e juntando batch e streaming.
8. **Parte 3:** chamar a API do Davi por HTTP com a volta montada e
   preencher `tempo_previsto_s` e `modelo_versao`.

## O que ficou desatualizado em `streaming/`

Não mexi no código do Dataflow. Ele ainda é o da parte 2 e **não funciona
com o tópico novo**:

- `streaming/pipeline.py` lê `f1-voltas-dataflow` e espera a volta v1.
- `streaming/validacao.py` e `test_validacao.py` espelham o
  `producer/evento.py`, que não existe mais.
- `streaming/subir.sh` e `parar.sh` filtram jobs/assinaturas por `f1-voltas-`.
- `streaming/README.md` e `docs/plano-frente-b.md` descrevem o desenho
  antigo.

Partes que devem continuar valendo: a escrita em `f1_stream.voltas` e
`voltas_dlq`, as regras de validação da volta montada e o `apto_modelo`.

## Decisões em aberto

Propostas minhas, a confirmar por quem constrói o Dataflow:

- A regra de identificação da volta (`evento_id`, `ts_evento`) da seção
  "Identificação" do [contrato-evento.md](../contrato-evento.md).
- Quanto esperar antes de montar a volta (janela/gatilho).
- Como calcular a mediana parcial da corrida para o filtro de volta limpa.

## Como testar

```bash
cd producer
.venv/bin/python test_cronograma.py                                   # contrato, offline
PROJECT_ID=pdm-davi-2026 .venv/bin/python publisher.py --session-key 11361 --minutos 2 --dry-run

# publicar de verdade (Cloud Run)
gcloud run jobs execute f1-gerador --region=us-central1 --args="--session-key,11361,--minutos,5"

# inspecionar sem consumir do Dataflow
gcloud pubsub subscriptions pull f1-openf1-debug --auto-ack --limit=10 \
  --format="table(message.attributes.topico, message.data.decode(base64).decode(utf-8))"
```

`--minutos N` publica só os N primeiros minutos depois da largada, bom para
testar o Dataflow sem esperar a corrida inteira.
