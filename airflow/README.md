# Orquestração com Airflow

O Airflow é o maestro do pipeline: ele aquece a API do modelo, sobe o job do
Dataflow, dispara o replay da corrida, espera o stream ficar saudável e
**drena o job no fim** — essa última é a que não pode ser esquecida, porque
o Dataflow cobra por hora de worker ligado.

| DAG | Quando roda | O que faz |
|---|---|---|
| [`f1_pipeline_fim_a_fim`](dags/f1_pipeline.py) | sob demanda | o pipeline de ponta a ponta, do aquecimento ao drain |
| [`f1_saude_pipeline`](dags/f1_saude.py) | a cada 5 min | vigia as duas APIs e as quatro checagens do pipeline |

A DAG de saúde **não exige stream ao vivo**. O painel de BI reprova por
padrão quando o último evento tem mais de 300 s, o que faz sentido durante
um replay — mas o replay só roda em demonstração, e uma DAG de 5 em 5
minutos cobrando isso falha o tempo todo (medido: três execuções vermelhas
seguidas com o pipeline perfeitamente são). Parede de vermelho ninguém olha,
e aí a falha de verdade passa batido. Aqui o frescor é medido em **dois
dias**, só para detectar abandono; cobrar frescor de minutos é tarefa da DAG
principal, que sabe que acabou de publicar.

## Por que não Cloud Composer

O Composer **é** este mesmo Airflow, rodando sobre um cluster GKE
gerenciado. Os US$ 300–400/mês dele não são licença: são o cluster, o Cloud
SQL do banco de metadados, o bucket e a taxa do control plane, tudo ligado
24 h por dia e sem escalar a zero.

| opção | custo/mês | persistente? |
|---|---|---|
| Cloud Composer | US$ 300–400 | sim |
| GKE Autopilot + Helm chart do Airflow | US$ 80–120 | sim |
| Cloud Run + Cloud SQL | ~US$ 45 | parcial |
| **`e2-medium`, ligada só quando precisa** | **~US$ 1–3** | **sim** |
| `e2-medium` 24/7 | ~US$ 25 | sim |

A carga de orquestração aqui é **uma DAG de seis tarefas em sequência**. O
paralelismo de dados está no Dataflow, que o faz por conta própria; o
Airflow só precisa de um scheduler e um worker. Kubernetes resolveria
escalar workers horizontalmente e tolerar falha de nó, que não é o problema
que temos.

Cloud Run não serve por outro motivo: o scheduler do Airflow é um laço
contínuo e o banco de metadados tem de sobreviver a reinício. O Cloud Run
reinicia o container quando quer e o disco é efêmero — o histórico de
execuções se perderia.

## Subir na VM

```bash
# 1. A VM. e2-medium = 2 vCPU, 4 GB: o LocalExecutor cabe, o CeleryExecutor não.
gcloud compute instances create f1-airflow \
  --project=pdm-davi-2026 --zone=us-central1-a \
  --machine-type=e2-medium \
  --image-family=debian-12 --image-project=debian-cloud \
  --boot-disk-size=30GB \
  --scopes=cloud-platform \
  --service-account=f1-dataflow@pdm-davi-2026.iam.gserviceaccount.com

# 2. Docker e o repositório
gcloud compute ssh f1-airflow --zone=us-central1-a
sudo apt-get update && sudo apt-get install -y docker.io docker-compose-v2 git
sudo usermod -aG docker "$USER" && exec newgrp docker
git clone <repo> f1-medallion && cd f1-medallion/airflow

# 3. Subir
mkdir -p logs && docker compose up -d --build

# 4. A interface, por túnel IAP (ver a seção abaixo)
gcloud compute start-iap-tunnel f1-airflow 8080 \
  --local-host-port=localhost:8080 --zone=us-central1-a
```

`--scopes=cloud-platform` com a service account é o que dispensa chave em
arquivo: o `gcloud` dentro do container pega as credenciais do servidor de
metadados da VM.

### A interface, por IAP TCP forwarding

```bash
gcloud compute start-iap-tunnel f1-airflow 8080 \
  --local-host-port=localhost:8080 --zone=us-central1-a
# interface em http://localhost:8080  (admin / admin)
```

O IAP encaminha TCP para a porta **sem SSH e sem OS Login**, que é o que
torna isso possível aqui: o SSH não funciona para conta externa à
organização do projeto (o OS Login exige
`roles/compute.osLoginExternalUser` no nível da org, e desligá-lo seria
enfraquecer a VM).

Precisa de uma regra de firewall, criada uma vez:

```bash
gcloud compute firewall-rules create f1-airflow-iap-8080 \
  --direction=INGRESS --action=ALLOW --rules=tcp:8080 \
  --source-ranges=35.235.240.0/20 \
  --target-tags=f1-airflow-ui

gcloud compute instances add-tags f1-airflow --zone=us-central1-a \
  --tags=f1-airflow-ui
```

A regra é estreita de propósito e **não expõe nada na internet**: a origem
é só a faixa documentada do IAP (`35.235.240.0/20`), uma porta, e só para a
tag `f1-airflow-ui`. Quem chega por ali já passou pela autenticação do
Google. Expor a 8080 ao mundo seria colocar um Airflow na internet.

Se o túnel falhar com `PERMISSION_DENIED`, falta
`roles/iap.tunnelResourceAccessor` — essa é **no projeto**, então o dono do
projeto concede.

Se falhar com `[Errno 8] nodename nor servname provided`, é **DNS, não o
IAP** — e a causa encontrada em 01/11 foi uma **VPN ativa**, que quebrava a
resolução de `tunnel.cloudproxy.app`, `github.com` e `dl.google.com`.
Desligar a VPN resolveu. Antes de investigar permissão, teste:

```bash
python3 -c "import socket; print(socket.gethostbyname('tunnel.cloudproxy.app'))"
```

### Provisionamento sem SSH

A VM se provisiona sozinha, por `startup-script` nos metadados — não por
SSH. Dois motivos: o SSH não atende conta externa à organização (acima), e
script de boot é reproduzível, comando digitado numa sessão não é.

O script é [`provisionar-vm.sh`](provisionar-vm.sh): instala o Docker, baixa
o repositório por **tarball HTTP** (o repositório é público, então não há
credencial envolvida) e sobe o compose. É idempotente — um `reset` da VM
refaz tudo sem reinstalar o que já existe, e o cache do Docker faz o
rebuild levar segundos.

Criar a VM e instalar o script:

```bash
gcloud compute instances create f1-airflow \
  --project=pdm-davi-2026 --zone=us-central1-a \
  --machine-type=e2-medium \
  --image-family=debian-12 --image-project=debian-cloud \
  --boot-disk-size=30GB --boot-disk-type=pd-balanced \
  --scopes=cloud-platform \
  --service-account=f1-dataflow@pdm-davi-2026.iam.gserviceaccount.com

gcloud compute instances add-metadata f1-airflow --zone=us-central1-a \
  --metadata-from-file=startup-script=airflow/provisionar-vm.sh

gcloud compute instances reset f1-airflow --zone=us-central1-a
```

Trocar o ramo que a VM baixa: a variável `RAMO` no topo do script.

Rodar o fim a fim sem SSH, pelo mesmo mecanismo:
[`rodar-fim-a-fim.sh`](rodar-fim-a-fim.sh), instalado como
`startup-script` e disparado por um `reset`.

Acompanhar sem SSH, pelo console serial — **filtrando por tempo**, porque o
console acumula o histórico de todos os boots e é fácil ler o anterior
pensando que é o atual:

```bash
gcloud compute instances get-serial-port-output f1-airflow \
  --zone=us-central1-a \
  | awk -F' ' '$1 >= "'"$(date -u -v-10M +%Y-%m-%dT%H:%M:%S 2>/dev/null \
      || date -u -d '10 min ago' +%Y-%m-%dT%H:%M:%S)"'"' \
  | grep 'command("/bin/bash")'
```

Duas armadilhas que esse caminho revelou e que estão resolvidas no script:

- **`docker-compose-v2` não existe no Debian 12** (é pacote do Ubuntu), e
  pedir vários pacotes numa chamada do `apt` faz um nome inexistente
  derrubar a transação inteira — nenhum dos outros é instalado. O script
  instala um por vez e busca o plugin do compose do binário oficial.
- **O container roda como uid 50000** e o repositório baixado é do root. O
  Beam escreve o `egg-info` no diretório do `streaming/setup.py` ao
  empacotar o job, então o script faz `chown -R 50000:0` no repositório.
- **Um volume nomeado herda dono do diretório correspondente na imagem.** O
  `~/.config/gcloud` não existia na imagem oficial, então o Docker criava o
  volume como root e o `gcloud` do container ficava sem identidade nenhuma —
  com o servidor de metadados respondendo — porque não conseguia escrever
  `credentials.db`. O `Dockerfile` cria o diretório com o dono certo.

**Desligar entre as demonstrações** — é isso que faz o custo ser de dólares
e não de dezenas:

```bash
gcloud compute instances stop  f1-airflow --zone=us-central1-a
gcloud compute instances start f1-airflow --zone=us-central1-a   # o compose volta sozinho
```

O `restart: unless-stopped` do compose religa os contêineres no boot, e o
volume do Postgres preserva o histórico de execuções entre as ligadas.

## Rodar na máquina, para desenvolver

```bash
cd airflow
export GCLOUD_CONFIG="$HOME/.config/gcloud"
mkdir -p logs && docker compose up -d --build
```

Monta o seu `~/.config/gcloud` no container, então o `gcloud` de dentro
autentica com a sua conta. Sem a variável, o compose usa um volume nomeado
vazio e só o servidor de metadados da VM funciona — que é o caso na VM.

O padrão é um volume nomeado, e não `/dev/null`: montar um dispositivo de
caractere onde o container espera um diretório quebra a subida.

## As duas ferramentas que a imagem acrescenta

A imagem oficial do Airflow não traz nenhuma das duas, e as DAGs precisam
das duas:

- **`gcloud`** — o `subir.sh` e o `parar.sh` falam com o Dataflow e o Cloud
  Run por linha de comando.
- **SDK do Beam** — lançar um job no Dataflow exige o Beam na máquina que
  lança: é ele que empacota o pipeline e o envia.

O Beam vai num venv **separado**, em `/opt/beam`. Os dois fixam versões de
`grpcio`, `protobuf` e `google-cloud-*`, e instalar um sobre o outro quebra
os dois. O `subir.sh` já aceita a variável `PYTHON`, e é por ela que o
compose aponta para esse venv.

Verificado dentro da imagem: `gcloud` 591, Beam 2.77 no venv, e o Python do
Airflow **não** vê o Beam (`ModuleNotFoundError`) — que é o ponto do
isolamento.

**Uma diferença a conhecer:** a imagem base do Airflow é Python 3.11, então
o venv do Beam é 3.11 e o Dataflow usa a imagem de worker
`beam_python3.11_sdk`. Lançando pelo venv do repositório, em 3.14, ele usa a
`beam_python3.14_sdk`. As duas são suportadas (Dataflow aceita Beam 2.73–2.77
com Python 3.10–3.14) e o código do pipeline não depende de versão, mas o
worker não é o mesmo binário — ao comparar medições, vale conferir de onde o
job foi lançado.

## Sessão da interface sobrevivendo a reinício

A chave que assina o cookie de sessão não está no repositório — segredo em
repositório público não é segredo. Por padrão o Airflow gera uma na subida,
e o custo é que **ela muda a cada reinício do webserver, derrubando quem
estava logado**. Com `restart: unless-stopped` isso acontece sozinho.

Para a sessão sobreviver, defina antes do `up`:

```bash
export AIRFLOW_SECRET_KEY="$(openssl rand -hex 32)"
docker compose up -d
```

Vale fazer isso na véspera da apresentação: cair da sessão no meio da demo é
um susto evitável.

## Consultar o estado sem abrir a interface

A API REST está habilitada com basic auth, então o estado de uma DAG é uma
requisição — útil para script e para quem não quer abrir navegador:

```bash
# com o túnel IAP de pé em localhost:8080
curl -s -u admin:"${AIRFLOW_SENHA:-admin}" \
  "http://localhost:8080/api/v1/dags/f1_pipeline_fim_a_fim/dagRuns?order_by=-execution_date&limit=3" \
  | python3 -m json.tool

# disparar a DAG principal com parâmetros
curl -s -u admin:"${AIRFLOW_SENHA:-admin}" -X POST \
  -H "Content-Type: application/json" \
  -d '{"conf": {"session_key": "11361", "minutos": "45"}}' \
  "http://localhost:8080/api/v1/dags/f1_pipeline_fim_a_fim/dagRuns"
```

Sem isso, a API responde 401 a tudo (o padrão do Airflow 2.10 é só sessão),
e consultar estado exigia abrir a interface — ou, sem SSH, reiniciar a VM só
para rodar um comando, que é insustentável.

## Senha da interface

O `init` cria o usuário `admin` com a senha de `AIRFLOW_SENHA`, cujo padrão
é `admin`. Para trocar:

```bash
AIRFLOW_SENHA='algo-que-nao-seja-admin' docker compose up -d
```

Por que o padrão fraco é aceitável **aqui**: a interface nunca está exposta
na internet. Na VM, chega-se a ela por IAP, que exige autenticação do Google
e autorização IAM antes de encaminhar qualquer pacote — a senha do Airflow é
a segunda barreira, não a primeira. Localmente, a porta só escuta em
`localhost`.

O que **não** se deve fazer é publicar a 8080 numa regra de firewall aberta.
Aí a senha passa a ser a única barreira, e `admin`/`admin` não é barreira.

## As DAGs sobem despausadas

O padrão do Airflow é criar DAG pausada. Aqui é `false`: este stack existe
para rodar exatamente estas duas. Com elas pausadas, subir o Airflow e
disparar **não faz nada** — a execução fica `queued` em silêncio, sem erro
nenhum e sem log que explique. Se acontecer, é isto:

```bash
docker compose exec -T scheduler airflow dags unpause f1_pipeline_fim_a_fim
docker compose exec -T scheduler airflow dags unpause f1_saude_pipeline
```

## Connections

As duas vêm por variável de ambiente, então não há nada a configurar na
interface:

| Connection | Para quê | Variável |
|---|---|---|
| `f1_api` | aquecer e vigiar a API do modelo | `AIRFLOW_CONN_F1_API` |
| `f1_bi_api` | as checagens de saúde do Davi | `AIRFLOW_CONN_F1_BI_API` |

Os padrões apontam para os serviços atuais. Para mudar, defina
`F1_API_URL` e `F1_BI_API_URL` antes do `docker compose up`.

As duas vão no **formato JSON**, e não como URI. O Airflow interpreta
`AIRFLOW_CONN_*` como URI quando o valor não começa com chave, e aí
`https://host` viraria `conn_type=https` — que o `HttpHook` não reconhece,
e o sensor falharia com um erro que não aponta para a causa.

## A ordem das tarefas, e por que ela é essa

```
aquecer_f1_api ─▶ subir_dataflow ─▶ esperar_dataflow_pronto ─▶ disparar_gerador
                                                                     │
                                            esperar_stream_saudavel ◀┘
                                                      │
                                              drenar_dataflow
```

Cada aresta vem de uma medição, não de preferência:

**`aquecer_f1_api` primeiro.** A API carrega um SavedModel do TensorFlow:
medido, 33 s de container, e o Cloud Run levou mais de um minuto só para
alocar a instância. O timeout de leitura do Dataflow é de 10 s. Publicar com
a API fria gravaria as primeiras voltas sem previsão — justamente o que se
quer demonstrar. O aquecimento é um `POST /predict` de verdade, não um
`GET /health`: o que demora é carregar o modelo, e isso só acontece na
primeira predição.

**`subir_dataflow` antes do gerador, não em paralelo.** A assinatura retém
as mensagens por 7 dias, então publicar antes não perde nada. O problema é
outro: o backlog inteiro seria entregue de uma vez, a reordenação passaria
da carência de 1 s e a média de gap da volta sairia degradada
([por quê](../docs/caso-carencia-reordenacao.md)).

**`esperar_dataflow_pronto` existe** porque `Running` no Dataflow é o estado
do *job*, não do worker: entre um e outro há 2 a 4 minutos de
provisionamento de VM e instalação de dependências.

**`drenar_dataflow` com `trigger_rule=ALL_DONE`.** Se qualquer tarefa acima
falhar, o job ficaria de pé cobrando. É a única tarefa da DAG que roda
mesmo quando o resto falha.

Ela usa `ESPERAR=1` e só conclui quando não há mais worker ligado. O drain
não é instantâneo: medido, o Dataflow reavalia o estado do job a cada 10
minutos, e a cobrança só termina quando as VMs saem. Com a espera, "a DAG
terminou" significa "paramos de pagar" — e a execução seguinte não esbarra
num job ainda ativo.

## Parâmetros da execução

A DAG principal aceita configuração no disparo:

```json
{"session_key": "11361", "minutos": "45", "injetar_invalidos": "0.01"}
```

| Chave | Padrão | Efeito |
|---|---|---|
| `session_key` | `11361` | corrida a reproduzir |
| `minutos` | `45` | só os N primeiros minutos depois da largada |
| `injetar_invalidos` | `0.01` | fração de mensagens corrompidas, para exercitar a DLQ |

## O que esta DAG não faz

A tela de tempo real do BI não depende do Airflow: ela lê o stream por SSE.
O Airflow dispara e vigia o pipeline; o que ele produz no BigQuery o front
mostra sozinho.
