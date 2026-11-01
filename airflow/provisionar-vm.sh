#!/bin/bash
# Provisiona o Airflow nesta VM. Roda como root no boot, pelo metadado
# startup-script. Acompanhar com:
#
#   gcloud compute instances get-serial-port-output f1-airflow \
#     --zone=us-central1-a | grep 'command("/bin/bash")'
#
# Idempotente: pode rodar a cada boot sem refazer o que ja esta feito.
#
# Duas decisoes que vieram de erro, nao de gosto:
#
# - um pacote por chamada do apt. Com uma lista, um nome inexistente faz o
#   apt recusar a transacao inteira e NENHUM pacote e instalado.
# - o repositorio vem por tarball HTTP, nao por git clone. O git nao
#   instalava nesta imagem ("held broken packages"), e para baixar um ramo
#   publico o git nao acrescenta nada: o GitHub serve tarball direto.
set -uo pipefail
marca() { echo "### F1-AIRFLOW [$(date -u +%H:%M:%S)] $*"; }
morrer() { marca "FALHA: $*"; exit 1; }

RAIZ=/opt/f1-medallion
RAMO=parte3/frente-b-dataflow
REPO=DavideSouzaAndrade/f1-medallion

marca "inicio do provisionamento"

# As tentativas anteriores podem ter deixado o dpkg a meio caminho, e ai
# qualquer instalacao nova falha com "held broken packages".
marca "conferindo o estado do dpkg"
dpkg --configure -a 2>&1 | tail -3
DEBIAN_FRONTEND=noninteractive apt-get -f install -y -qq 2>&1 | tail -3

if ! command -v docker >/dev/null; then
  marca "atualizando indices do apt"
  for t in 1 2 3; do
    DEBIAN_FRONTEND=noninteractive apt-get update -qq 2>&1 | tail -3 && break
    marca "  update tentativa ${t} falhou"; sleep 20
  done
  marca "instalando docker.io"
  for t in 1 2 3; do
    DEBIAN_FRONTEND=noninteractive apt-get install -y -qq docker.io 2>&1 | tail -4 && break
    marca "  docker.io tentativa ${t} falhou"; sleep 20
  done
fi
command -v docker >/dev/null || morrer "docker nao existe depois da instalacao"
command -v curl   >/dev/null || morrer "curl ausente na imagem"
systemctl enable --now docker || morrer "docker nao subiu"

# O Debian 12 nao empacota o plugin compose v2 (so o docker-compose v1, de
# outra linhagem). Pega o binario oficial, resolvendo a versao pela API em
# vez de chumbar uma que pode nao existir.
if ! docker compose version >/dev/null 2>&1; then
  marca "instalando o plugin compose v2"
  TAG=$(curl -fsSL https://api.github.com/repos/docker/compose/releases/latest \
        | grep -oP '"tag_name":\s*"\K[^"]+' | head -1)
  [[ -n "${TAG:-}" ]] || morrer "nao descobri a versao do compose"
  marca "  versao ${TAG}, arquitetura $(uname -m)"
  mkdir -p /usr/libexec/docker/cli-plugins
  curl -fsSL -o /usr/libexec/docker/cli-plugins/docker-compose \
    "https://github.com/docker/compose/releases/download/${TAG}/docker-compose-linux-$(uname -m)" \
    || morrer "download do compose falhou"
  chmod +x /usr/libexec/docker/cli-plugins/docker-compose
fi
docker compose version >/dev/null 2>&1 || morrer "plugin compose ausente"
marca "docker $(docker --version | awk '{print $3}' | tr -d ,) | compose $(docker compose version --short)"

marca "baixando o repositorio (publico, por tarball)"
rm -rf /tmp/f1 && mkdir -p /tmp/f1
curl -fsSL "https://api.github.com/repos/${REPO}/tarball/${RAMO}" \
  | tar xz -C /tmp/f1 --strip-components=1 || morrer "download do repositorio falhou"
[[ -f /tmp/f1/airflow/docker-compose.yml ]] || morrer "o compose nao veio no tarball"
rm -rf "$RAIZ" && mv /tmp/f1 "$RAIZ"
marca "repositorio em $RAIZ ($(find "$RAIZ" -type f | wc -l) arquivos)"

mkdir -p "$RAIZ/airflow/logs"
# O container roda como uid 50000 (usuario airflow da imagem oficial), e o
# repositorio baixado pertence ao root. O Beam escreve o egg-info e o sdist
# no diretorio do streaming/setup.py ao empacotar o job, entao o repositorio
# tem que ser escrivel por esse uid -- sem isto o subir_dataflow falha com
# permissao negada no empacotamento.
chown -R 50000:0 "$RAIZ"
chmod -R u+rwX "$RAIZ"

cd "$RAIZ/airflow" || morrer "nao achei $RAIZ/airflow"
marca "construindo a imagem (gcloud + venv do Beam); leva de 5 a 12 min"
if ! docker compose build >/tmp/build.log 2>&1; then
  marca "build falhou; ultimas 25 linhas:"; tail -25 /tmp/build.log; exit 1
fi
marca "imagem construida"

marca "subindo o Airflow"
if ! docker compose up -d >/tmp/up.log 2>&1; then
  marca "up falhou; ultimas 25 linhas:"; tail -25 /tmp/up.log; exit 1
fi

marca "esperando o scheduler carregar as DAGs (ate 10 min)"
pronto=0
for _ in $(seq 1 40); do
  if docker compose exec -T scheduler airflow dags list 2>/dev/null | grep -q f1_pipeline_fim_a_fim; then
    pronto=1; break
  fi
  sleep 15
done

marca "containers:"
docker compose ps --format '  {{.Service}}: {{.Status}}' 2>&1
if [[ "$pronto" != "1" ]]; then
  marca "o scheduler nao listou as DAGs; log dele:"
  docker compose logs --tail=30 scheduler 2>&1 | sed 's/^/  /'
  exit 1
fi
marca "DAGs vistas pelo scheduler:"
docker compose exec -T scheduler airflow dags list 2>/dev/null | grep f1_ | sed 's/^/  /'
marca "erros de import:"
docker compose exec -T scheduler airflow dags list-import-errors 2>/dev/null | sed 's/^/  /'
marca "identidade do gcloud no container (vem do servidor de metadados):"
conta=$(docker compose exec -T scheduler gcloud auth list --format='value(account)' 2>&1 | head -2)
echo "  ${conta:-VAZIO}"
if [[ -z "${conta// /}" ]] || echo "$conta" | grep -qi "error\|credential"; then
  marca "FALHA: o gcloud do container nao tem identidade."
  marca "  Sem isto o subir.sh e o parar.sh nao falam com o Dataflow."
  docker compose exec -T scheduler gcloud auth list 2>&1 | head -5 | sed 's/^/    /'
  exit 1
fi
marca "projeto do gcloud: $(docker compose exec -T scheduler gcloud config get-value project 2>/dev/null | tr -d '\r')"
marca "o venv do Beam responde: $(docker compose exec -T scheduler /opt/beam/bin/python -c 'import apache_beam; print(apache_beam.__version__)' 2>&1 | tr -d '\r')"
marca "PROVISIONAMENTO CONCLUIDO"
