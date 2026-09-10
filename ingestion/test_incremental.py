"""Validacao do modo incremental com cliente falso.

Nao toca na API nem no Cloud Storage: usa um cliente falso e disco
temporario, entao roda offline e em segundos.

    python test_incremental.py

Sai com codigo 1 se qualquer verificacao falhar.
"""
import json, sys, tempfile
from datetime import datetime, timedelta, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
import config
from main import Ingestor
from openf1_client import OpenF1Error
from storage import LocalStorage, state_path

AGORA = datetime.now(timezone.utc)
def iso(dias_atras, horas=0):
    return (AGORA - timedelta(days=dias_atras, hours=horas)).isoformat()

# 4 sessoes: 3 antigas e consolidadas, 1 que acabou ha 2 horas (dentro da carencia)
SESSOES = [
    {"session_key": 1, "session_name": "Race", "location": "A", "date_start": iso(30, 2), "date_end": iso(30)},
    {"session_key": 2, "session_name": "Race", "location": "B", "date_start": iso(20, 2), "date_end": iso(20)},
    {"session_key": 3, "session_name": "Race", "location": "C", "date_start": iso(10, 2), "date_end": iso(10)},
    {"session_key": 4, "session_name": "Race", "location": "D", "date_start": iso(0, 4),  "date_end": iso(0, 2)},
]

ONDA = {"nome": "teste", "endpoints": [
    {"name": "sessions", "scope": "year"},
    {"name": "laps", "scope": "session"},
]}


class ClienteFalso:
    def __init__(self, sessoes=None, falhar_em=None):
        self.sessoes = sessoes if sessoes is not None else SESSOES
        self.falhar_em = falhar_em or set()
        self.chamadas = []
        self.descobertas = 0   # chamadas a /sessions para descoberta

    def sessions(self, year):
        self.descobertas += 1
        return [dict(s) for s in self.sessoes]

    def drivers(self, session_key):
        return [{"driver_number": 1}]

    def fetch(self, endpoint, filters=None, date_start=None, date_end=None):
        sk = (filters or {}).get("session_key")
        self.chamadas.append((endpoint, sk))
        if sk in self.falhar_em:
            raise OpenF1Error(f"falha simulada em {endpoint} sk={sk}")
        return [{"session_key": sk, "x": 1}]


def montar(raiz, cliente):
    st = LocalStorage(str(raiz))
    ing = Ingestor(cliente, st, "openf1", "runtest")
    config.WAVES[99] = ONDA
    return ing, st


falhas_teste = []
def checar(nome, condicao, detalhe=""):
    marca = "PASSOU" if condicao else "FALHOU"
    if not condicao:
        falhas_teste.append(nome)
    print(f"  [{marca}] {nome}" + (f"  -- {detalhe}" if detalhe and not condicao else ""))


def caso(titulo):
    print(f"\n{'=' * 74}\n{titulo}\n{'=' * 74}")
    d = Path(tempfile.mkdtemp())
    return d


# ---------------------------------------------------------------- 1
d = caso("1. Bootstrap: sem marca, varre tudo (menos a carencia) e grava a marca")
c = ClienteFalso()
ing, st = montar(d, c)
ing.run_wave(99, [2026], None, incremental=True)
sks = [sk for ep, sk in c.chamadas if ep == "laps"]
checar("considerou as 3 sessoes consolidadas", sks == [1, 2, 3], f"obteve {sks}")
checar("excluiu a sessao dentro da carencia de 24h", 4 not in sks)
estado = json.loads((d / state_path("openf1")).read_text())
checar("gravou a marca d'agua", "99" in estado.get("ondas", {}))
checar("marca = date_end da ultima consolidada",
       estado["ondas"]["99"]["ate"] == SESSOES[2]["date_end"],
       f"obteve {estado['ondas']['99']['ate']}")
checar("contabilizou fora_da_marca=1", ing.stats["fora_da_marca"] == 1, str(ing.stats))

# ---------------------------------------------------------------- 2
print("\n2. Segunda execucao no mesmo estado: nada novo")
c2 = ClienteFalso()
ing2, _ = montar(d, c2)
ing2.run_wave(99, [2026], None, incremental=True)
checar("nenhuma sessao considerada", [sk for ep, sk in c2.chamadas if ep == "laps"] == [])
checar("uma unica requisicao de descoberta", c2.descobertas == 1, f"obteve {c2.descobertas}")
# A unica escrita e o refresh da lista de nivel de ano (sessions/year=2026),
# que precisa acompanhar as sessoes novas. Nenhum dado de sessao e reescrito.
checar("unica escrita e o refresh da lista do ano",
       ing2.stats["gravados"] == 1 and ing2.stats["pulados"] == 0, str(ing2.stats))

# ---------------------------------------------------------------- 3
print("\n3. Chega uma sessao nova: so ela e considerada")
nova = dict(SESSOES[0], session_key=5, date_start=iso(2, 2), date_end=iso(2))
c3 = ClienteFalso(SESSOES + [nova])
ing3, _ = montar(d, c3)
ing3.run_wave(99, [2026], None, incremental=True)
sks = [sk for ep, sk in c3.chamadas if ep == "laps"]
checar("considerou apenas a sessao nova", sks == [5], f"obteve {sks}")
estado = json.loads((d / state_path("openf1")).read_text())
checar("marca avancou para a nova", estado["ondas"]["99"]["ate"] == nova["date_end"])

# ---------------------------------------------------------------- 4
d = caso("4. Falha na execucao: a marca NAO avanca")
c = ClienteFalso(falhar_em={2})
ing, _ = montar(d, c)
ing.run_wave(99, [2026], None, incremental=True)
checar("registrou a falha", ing.stats["falhas"] == 1, str(ing.stats))
checar("NAO criou arquivo de estado", not (d / state_path("openf1")).exists())
print("   -> reexecucao apos a falha (sem marca, reconsidera as 3):")
c = ClienteFalso()
ing, _ = montar(d, c)
ing.run_wave(99, [2026], None, incremental=True)
sks = [sk for ep, sk in c.chamadas if ep == "laps"]
# As sessoes 1 e 3 ja tinham sido gravadas antes da falha, entao sao puladas
# pela checagem de existencia. So a 2 volta a API. E o ponto do desenho:
# reconsiderar apos falha e barato.
checar("so a sessao que falhou voltou a API", sks == [2], f"obteve {sks}")
checar("as ja gravadas foram puladas", ing.stats["pulados"] == 2, str(ing.stats))
checar("agora sim gravou a marca", (d / state_path("openf1")).exists())

# ---------------------------------------------------------------- 5
d = caso("5. Carencia: com GRACE_HOURS alto, tudo fica de fora")
original = config.GRACE_HOURS
config.GRACE_HOURS = 24 * 365 * 50
c = ClienteFalso()
ing, _ = montar(d, c)
ing.run_wave(99, [2026], None, incremental=True)
checar("nenhuma sessao passou pela carencia", [sk for ep, sk in c.chamadas if ep == "laps"] == [])
checar("marca nao foi criada", not (d / state_path("openf1")).exists())
config.GRACE_HOURS = original

# ---------------------------------------------------------------- 6
d = caso("6. Listas de nivel de ano sao reescritas em modo incremental")
c = ClienteFalso()
ing, _ = montar(d, c)
ing.run_wave(99, [2026], None, incremental=True)
alvo = d / "openf1/sessions/year=2026/data.json.gz"
checar("gravou sessions/year=2026", alvo.exists())
antes = alvo.stat().st_mtime_ns
import time as _t; _t.sleep(0.01)
c = ClienteFalso()
ing, _ = montar(d, c)
ing.run_wave(99, [2026], None, incremental=True)
checar("reescreveu mesmo ja existindo", alvo.stat().st_mtime_ns != antes)
checar("chamou o endpoint sessions de novo",
       ("sessions", None) in c.chamadas or any(ep == "sessions" for ep, _ in c.chamadas))

# ---------------------------------------------------------------- 7
d = caso("7. Modo normal nao cria nem le marca d'agua")
c = ClienteFalso()
ing, _ = montar(d, c)
ing.run_wave(99, [2026], None)
sks = [sk for ep, sk in c.chamadas if ep == "laps"]
checar("considerou TODAS as sessoes, inclusive a recente", sks == [1, 2, 3, 4], f"obteve {sks}")
checar("nao criou arquivo de estado", not (d / state_path("openf1")).exists())
antes = (d / "openf1/sessions/year=2026/data.json.gz").stat().st_mtime_ns
_t.sleep(0.01)
c = ClienteFalso()
ing, _ = montar(d, c)
ing.run_wave(99, [2026], None)
checar("segunda passada pula tudo", ing.stats["pulados"] > 0 and ing.stats["gravados"] == 0, str(ing.stats))
checar("NAO reescreveu sessions em modo normal",
       (d / "openf1/sessions/year=2026/data.json.gz").stat().st_mtime_ns == antes)

# ---------------------------------------------------------------- 8
d = caso("8. --refetch sobrescreve; --session-keys restringe")
c = ClienteFalso()
ing, _ = montar(d, c)
ing.run_wave(99, [2026], None)
c = ClienteFalso()
ing, _ = montar(d, c)
ing.run_wave(99, [2026], None, refetch=True)
# 4 sessoes + 1 objeto de nivel de ano (sessions/year=2026) = 5 escritas
checar("refetch regravou tudo", ing.stats["gravados"] == 5 and ing.stats["pulados"] == 0, str(ing.stats))
c = ClienteFalso()
ing, _ = montar(d, c)
ing.run_wave(99, [2026], None, refetch=True, session_keys={2})
sks = [sk for ep, sk in c.chamadas if ep == "laps"]
checar("session_keys restringiu a uma sessao", sks == [2], f"obteve {sks}")

# ---------------------------------------------------------------- 9
d = caso("9. Sessao sem date_end e descartada no modo incremental")
sem_data = [dict(SESSOES[0]), {"session_key": 9, "session_name": "Race", "location": "X",
                               "date_start": iso(5, 2), "date_end": None}]
c = ClienteFalso(sem_data)
ing, _ = montar(d, c)
ing.run_wave(99, [2026], None, incremental=True)
sks = [sk for ep, sk in c.chamadas if ep == "laps"]
checar("ignorou a sessao sem date_end", sks == [1], f"obteve {sks}")

# --------------------------------------------------------------- 10
d = caso("10. Estado corrompido nao derruba a execucao")
alvo = d / state_path("openf1")
alvo.parent.mkdir(parents=True, exist_ok=True)
alvo.write_text("{isto nao e json")
c = ClienteFalso()
ing, _ = montar(d, c)
ing.run_wave(99, [2026], None, incremental=True)
sks = [sk for ep, sk in c.chamadas if ep == "laps"]
checar("tratou como se nao houvesse marca", sks == [1, 2, 3], f"obteve {sks}")
checar("regravou um estado valido", "ondas" in json.loads(alvo.read_text()))

# --------------------------------------------------------------- 11
def semear_marca(raiz, ondas: dict):
    """Grava um estado inicial sem que nenhum dado tenha sido ingerido."""
    alvo = raiz / state_path("openf1")
    alvo.parent.mkdir(parents=True, exist_ok=True)
    alvo.write_text(json.dumps({"ondas": {k: {"ate": v} for k, v in ondas.items()}}))


d = caso("11. Marcas de ondas diferentes sao independentes")
config.WAVES[98] = ONDA
# Diretorio limpo com a marca da onda 99 ja adiantada, mas sem nenhum dado
# gravado -- assim as chamadas a API refletem so o filtro, nao o checkpoint.
semear_marca(d, {"99": SESSOES[2]["date_end"]})
c = ClienteFalso()
ing, _ = montar(d, c)
ing.run_wave(98, [2026], None, incremental=True)
sks = [sk for ep, sk in c.chamadas if ep == "laps"]
checar("onda 98 nao herdou a marca da 99", sks == [1, 2, 3], f"obteve {sks}")
estado = json.loads((d / state_path("openf1")).read_text())
checar("as duas marcas convivem no mesmo arquivo",
       set(estado["ondas"]) == {"98", "99"}, str(estado.get("ondas")))
checar("a marca da onda 99 nao foi alterada",
       estado["ondas"]["99"]["ate"] == SESSOES[2]["date_end"])

d = caso("11b. A marca da propria onda de fato filtra")
semear_marca(d, {"99": SESSOES[1]["date_end"]})   # ja cuidou ate a sessao 2
c = ClienteFalso()
ing, _ = montar(d, c)
ing.run_wave(99, [2026], None, incremental=True)
sks = [sk for ep, sk in c.chamadas if ep == "laps"]
checar("considerou so o que veio depois da marca", sks == [3], f"obteve {sks}")

# --------------------------------------------------------------- 12
d = caso("12. --since sobrescreve a marca gravada")
semear_marca(d, {"99": SESSOES[2]["date_end"]})   # marca adiantada: sozinha, filtraria tudo
c = ClienteFalso()
ing, _ = montar(d, c)
ing.run_wave(99, [2026], None, incremental=True, since=iso(25))   # volta no tempo
sks = [sk for ep, sk in c.chamadas if ep == "laps"]
checar("reconsiderou a partir da data informada", sks == [2, 3], f"obteve {sks}")

print(f"\n{'=' * 74}")
if falhas_teste:
    print(f"{len(falhas_teste)} VERIFICACAO(OES) FALHOU/FALHARAM: {falhas_teste}")
    sys.exit(1)
print("TODAS AS VERIFICACOES PASSARAM")
