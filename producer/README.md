# Gerador de eventos e camada de streaming (parte 2)

O gerador faz o replay de uma corrida real no Pub/Sub: lê as voltas na
`silver_voltas` e publica uma mensagem por volta, na ordem em que as voltas
terminaram, com os intervalos reais divididos por um fator de aceleração.

O formato de cada mensagem está em
[docs/contrato-evento.md](../docs/contrato-evento.md).

## O que esta parte cria

| Recurso | Onde está o código |
|---------|--------------------|
| Tópico `f1-voltas`, assinaturas `f1-voltas-dataflow` e `f1-voltas-debug`, dataset `f1_stream` | [`setup/setup_streaming.sh`](../setup/setup_streaming.sh) |
| Tabelas `f1_stream.voltas` e `f1_stream.voltas_dlq` | [`definitions/stream/`](../definitions/stream/) |
| View `f1_silver.vw_voltas_unificada` (batch ∪ stream) | [`definitions/stream/vw_voltas_unificada.sqlx`](../definitions/stream/vw_voltas_unificada.sqlx) |
| Cloud Run Job `f1-gerador` | [`producer/deploy.sh`](deploy.sh) |

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
python test_evento.py                                  # contrato, offline
python publisher.py --listar                           # corridas disponíveis
python publisher.py --session-key 11361 --limite 5 --dry-run
```

Publicar pela nuvem:

```bash
# corrida mais recente, 30x mais rápida que a real (~3 min)
gcloud run jobs execute f1-gerador --region=us-central1

# corrida, ritmo e 5% de eventos inválidos para exercitar a DLQ
gcloud run jobs execute f1-gerador --region=us-central1 \
  --args="--session-key,11361,--fator,60,--injetar-invalidos,0.05"
```

| Parâmetro | Efeito |
|-----------|--------|
| `--session-key` | corrida a reproduzir; padrão é a mais recente da Silver |
| `--fator` | aceleração sobre os intervalos reais entre voltas (padrão 30) |
| `--intervalo S` | ignora o ritmo real e publica um evento a cada S segundos |
| `--limite N` | publica só as N primeiras voltas |
| `--injetar-invalidos F` | fração de eventos quebrados de propósito, de 0 a 1 |
| `--semente` | repete o mesmo sorteio de inválidos numa nova execução |

## Como conferir

```bash
# ler mensagens sem roubá-las do Dataflow
gcloud pubsub subscriptions pull f1-voltas-debug --auto-ack --limit=10
```

```sql
-- o lado 'stream' cresce enquanto o gerador publica
SELECT origem, COUNT(*) AS voltas
FROM `pdm-davi-2026.f1_silver.vw_voltas_unificada`
GROUP BY origem;

-- eventos rejeitados, por motivo
SELECT motivo, COUNT(*) AS eventos
FROM `pdm-davi-2026.f1_stream.voltas_dlq`
GROUP BY motivo;
```

## Decisões

**Replay em vez da API ao vivo.** Fora de um fim de semana de GP a OpenF1 não
publica nada novo, e o acesso em tempo real durante as sessões é pago. O
replay usa dado real com ritmo controlado, sem depender do calendário da F1.

**Um evento é uma volta concluída.** É a unidade do modelo de degradação de
pneu. Publicar na ordem em que as voltas terminaram intercala os pilotos
como na corrida real.

**Dataset `f1_stream` separado.** A fronteira entre o que chegou em lote e o
que chegou em tempo real fica visível já pelo nome do dataset.

**Tabelas do stream como `operations` com `CREATE TABLE IF NOT EXISTS`.** Uma
tabela comum do Dataform é recriada a cada execução, o que apagaria tudo o
que o streaming já gravou.

**Partição por `ts_evento`, a hora da chegada.** Num replay ela difere da
hora original da volta em semanas, e para o stream o que importa é quando o
dado entrou. O cluster por sessão e piloto repete o da `silver_voltas`.

**DLQ como tabela, com partições de 30 dias.** Guardar o evento cru com o
motivo no BigQuery permite consultar os erros em SQL; a DLQ serve para
diagnóstico, não é histórico.

**Duas assinaturas.** Um `pull` de teste na assinatura do Dataflow consumiria
a mensagem; a `f1-voltas-debug` existe para inspecionar sem interferir.

**View unificada na Silver, deduplicando por `evento_id`.** O grão é o da
`silver_voltas`, sem agregação. A deduplicação cobre a entrega "pelo menos
uma vez" do Pub/Sub.

**Job sem nova tentativa (`--max-retries=0`).** Uma nova tentativa
recomeçaria o replay e publicaria voltas duplicadas no tópico.
