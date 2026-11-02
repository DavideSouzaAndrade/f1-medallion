# Gerador de eventos

O gerador faz o replay de uma corrida real no Pub/Sub **no formato em que a
OpenF1 entrega ao vivo**: lê os registros brutos da corrida na Bronze e
publica cada um como mensagem, com o mesmo JSON, os campos `_key` e `_id`, e
os intervalos reais divididos por um fator de aceleração.

Trocar o gerador pela OpenF1 de verdade é trocar a fonte: uma ponte que
assina o MQTT da OpenF1 e repassa as mensagens para o mesmo tópico. Quem
consome não percebe a diferença.

O formato das mensagens está em
[docs/contrato-openf1.md](../docs/contrato-openf1.md).

## O que esta parte cria

| Recurso | Onde está o código |
|---------|--------------------|
| Tópico `f1-openf1`, assinaturas `f1-openf1-dataflow` e `f1-openf1-debug`, dataset `f1_stream` | [`setup/setup_streaming.sh`](../setup/setup_streaming.sh) |
| Tabelas `f1_stream.voltas` e `f1_stream.voltas_dlq` | [`definitions/stream/`](../definitions/stream/) |
| View `f1_silver.vw_voltas_unificada` (batch ∪ stream) | [`definitions/stream/vw_voltas_unificada.sqlx`](../definitions/stream/vw_voltas_unificada.sqlx) |
| Cloud Run Job `f1-gerador` | [`producer/deploy.sh`](deploy.sh) |

## Como funciona

| Arquivo | Papel |
|---------|-------|
| [`fonte.py`](fonte.py) | Lê da Bronze os registros da corrida, por endpoint, como a OpenF1 entregou |
| [`cronograma.py`](cronograma.py) | Decide o que a OpenF1 publicaria e quando: as versões de cada volta e de cada stint, na ordem da corrida |
| [`corrupcao.py`](corrupcao.py) | Copias quebradas de propósito, para a demonstração da DLQ |
| [`publisher.py`](publisher.py) | Publica no ritmo da corrida acelerada |
| [`injetar_dlq.py`](injetar_dlq.py) | Publica na hora uma mensagem quebrada de cada tipo, para mostrar a DLQ ao vivo |
| [`parar.sh`](parar.sh) | Interrompe o replay em andamento (cancela a execução do Cloud Run Job) |

Uma corrida (GP da Itália de 2026) vira ~27 mil mensagens: 22 mil de
`intervals`, 4,1 mil versões de `laps`, 1 mil de `stints` e o resto de
`weather`, `pit`, `sessions` e `meetings`. No fator 30 o replay leva ~4 min.

## Como executar

Provisionar, a partir da raiz do repositório:

```bash
export PROJECT_ID="pdm-davi-2026"
bash setup/setup_streaming.sh                  # tópico, assinaturas e dataset f1_stream
npx @dataform/cli@3.0.0 run --tags stream      # tabelas e view unificada
bash producer/deploy.sh                        # Cloud Run Job do gerador
```

O Dataform local precisa de um `.df-credentials.json` na raiz (já está no
`.gitignore`) com `{"projectId": "pdm-davi-2026", "location": "US"}` e das
credenciais padrão do gcloud (`gcloud auth application-default login`).

Testar localmente, sem publicar nada:

```bash
cd producer
pip install -r requirements.txt
python test_cronograma.py                              # contrato, offline
python publisher.py --listar                           # corridas disponíveis
python publisher.py --session-key 11361 --minutos 2 --dry-run
```

Publicar pela nuvem:

```bash
# corrida mais recente, 30x mais rápida que a real (~4 min)
gcloud run jobs execute f1-gerador --region=us-central1

# corrida, ritmo e 1% de cópias quebradas para exercitar a DLQ
gcloud run jobs execute f1-gerador --region=us-central1 \
  --args="--session-key,11361,--fator,60,--injetar-invalidos,0.01"
```

| Parâmetro | Efeito |
|-----------|--------|
| `--session-key` | corrida a reproduzir; padrão é a mais recente da Bronze |
| `--fator` | aceleração sobre os intervalos reais entre mensagens (padrão 30) |
| `--minutos N` | publica só os N primeiros minutos depois da largada |
| `--injetar-invalidos F` | fração de cópias quebradas extras, de 0 a 1 |
| `--semente` | repete o mesmo sorteio de inválidos numa nova execução |

### Corrida em tempo real, até mandar parar

Com `--fator 1` a corrida sai no ritmo real (~1h30, umas 14 voltas por
minuto): é o modo da apresentação, ligado antes e deixado rodando. Pela DAG
do Airflow, com `{"session_key": "11253", "fator": "1"}`. Para interromper:

```bash
bash producer/parar.sh
```

O script cancela a execução do gerador; a DAG não tenta de novo e drena o
Dataflow sozinha. Prefira uma corrida seca e sem bandeira vermelha: a 11361
(Monza) fica 32 min sem nenhuma volta no ritmo real.

### Mensagens para a DLQ, sob demanda

```bash
cd producer && python injetar_dlq.py          # uma de cada tipo
python injetar_dlq.py --tipo json_quebrado    # só uma
```

Com o Dataflow ligado, chegam a `f1_stream.voltas_dlq` em ~1 s, cada uma com
o seu motivo, e aparecem no gráfico "Rejeitados por motivo" da tela Saúde do
BI. O script imprime a consulta para mostrá-las no BigQuery.

## Como conferir

```bash
# ler mensagens sem roubá-las do Dataflow
gcloud pubsub subscriptions pull f1-openf1-debug --auto-ack --limit=10 \
  --format="table(message.attributes.topico, message.data.decode(base64).decode(utf-8))"
```

## Decisões

**Replay em vez da OpenF1 ao vivo.** Fora de um fim de semana de GP a OpenF1
não publica nada novo, e o acesso em tempo real durante as sessões é pago
(€9,90/mês). O replay usa dado real com ritmo controlado, sem depender do
calendário da F1.

**Fonte na Bronze, não na Silver.** A Bronze guarda o JSON de cada registro
exatamente como a OpenF1 entregou. Ler da Silver seria publicar dado já
tratado e esvaziar o trabalho do Dataflow.

**Laps e stints em várias versões.** Ao vivo, a OpenF1 publica a volta quando
o piloto cruza a linha e a completa setor a setor; o stint nasce na troca de
pneu e o `lap_end` avança a cada volta. A Bronze só tem a versão final, então
o gerador reconstrói as intermediárias seguindo o código de ingestão da
própria OpenF1. Publicar só a versão final daria ao Dataflow, no início de um
stint, a volta em que ele vai terminar.

**Começar 1 minuto antes da largada.** A Bronze tem ~1 h de pré-corrida (clima
e intervalos do grid) que, na demonstração, seria espera sem voltas. Só o
último registro de clima antes da largada entra, para as primeiras voltas
não chegarem sem temperatura.

**Datas originais no corpo.** `date` e `date_start` são o dado bruto e não são
reescritos. O instante em que o replay publicou fica no `publish_time` do
Pub/Sub.

**Inválidos são cópias extras.** O dado bruto já traz defeitos reais (texto
`"+1 LAP"` em campo numérico, campos nulos), que o Dataflow trata como dado
normal. As cópias quebradas são o que nenhuma mensagem da OpenF1 traria, e a
corrida original continua completa.

**Duas assinaturas.** Um `pull` de teste na assinatura do Dataflow consumiria
a mensagem; a `f1-openf1-debug` existe para inspecionar sem interferir.

**Job sem nova tentativa (`--max-retries=0`).** Uma nova tentativa
recomeçaria o replay e publicaria a corrida duplicada no tópico.
