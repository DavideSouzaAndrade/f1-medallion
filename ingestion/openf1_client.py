"""Cliente HTTP para a API da OpenF1.

Responsabilidades:
  - respeitar os limites do tier gratuito (3 req/s e 30 req/min);
  - repetir requisicoes que falham, com backoff exponencial;
  - distinguir os significados que a API da a cada codigo de erro;
  - quebrar respostas grandes demais em janelas de tempo menores.

A API nao possui paginacao: o controle de volume e feito filtrando
por piloto e, quando necessario, por intervalo de tempo.

Semantica dos codigos de resposta, verificada contra a API:

  200  lista de registros.
  404  {"detail": "No results found."} -- combinacao valida sem dado.
       Nao e falha: e resultado vazio e precisa ser gravado como tal.
       Tratar como erro faria a combinacao nunca entrar no checkpoint,
       e ela seria reconsultada em toda execucao. Atencao: um endpoint
       inexistente responde exatamente igual, entao os nomes em
       config.py precisam estar corretos -- um erro de digitacao vira
       arquivo vazio, nao excecao.
  422  {"detail": "You're likely asking for too much data at once."}
       Unico sinal de resposta grande demais; e o que aciona a quebra
       por janela de tempo. O teto medido fica perto de 250 mil
       registros (~40 MB); um piloto numa corrida da ~35 mil, entao na
       pratica a quebra quase nunca e necessaria.
  401  acesso global bloqueado durante sessao de F1 ao vivo. Definitivo.
  429  rate limit estourado. Repetir com backoff.
  5xx  falha do servidor. Repetir com backoff.
"""

from __future__ import annotations

import logging
import random
import threading
import time
from collections import deque
from datetime import datetime, timedelta
from typing import Any
from urllib.parse import quote

import requests

BASE_URL = "https://api.openf1.org/v1"

# Margem de seguranca sobre o limite publicado (3 req/s, 30 req/min).
MAX_REQUESTS_PER_MINUTE = 25
MIN_INTERVAL_SECONDS = 0.5

MAX_ATTEMPTS = 5
REQUEST_TIMEOUT = 180
MAX_SPLIT_DEPTH = 4

# A janela vinda de /sessions e o horario *agendado*. A telemetria comeca
# bem antes: instalacao, aquecimento e volta de apresentacao ficam fora do
# horario nominal -- medido em Bahrain 2024, 36% das amostras de car_data
# estao antes do date_start. Sem esta folga, toda quebra por janela
# descartaria esse terco silenciosamente.
WINDOW_PAD_BEFORE = timedelta(hours=2)
WINDOW_PAD_AFTER = timedelta(hours=1)

log = logging.getLogger(__name__)


class RateLimiter:
    """Limitador duplo: intervalo minimo entre chamadas + teto por minuto."""

    def __init__(
        self,
        max_per_minute: int = MAX_REQUESTS_PER_MINUTE,
        min_interval: float = MIN_INTERVAL_SECONDS,
    ) -> None:
        self.max_per_minute = max_per_minute
        self.min_interval = min_interval
        self._calls: deque[float] = deque()
        self._last_call = 0.0
        self._lock = threading.Lock()

    def acquire(self) -> None:
        with self._lock:
            while True:
                now = time.monotonic()

                while self._calls and now - self._calls[0] >= 60:
                    self._calls.popleft()

                wait = 0.0
                if len(self._calls) >= self.max_per_minute:
                    wait = 60 - (now - self._calls[0]) + 0.05

                since_last = now - self._last_call
                if since_last < self.min_interval:
                    wait = max(wait, self.min_interval - since_last)

                if wait <= 0:
                    self._calls.append(now)
                    self._last_call = now
                    return

                time.sleep(wait)


class OpenF1Error(RuntimeError):
    """Falha definitiva ao consultar a API."""


class ResponseTooLarge(OpenF1Error):
    """HTTP 422: a resposta excede o que a API entrega de uma vez."""


class AccessBlocked(OpenF1Error):
    """HTTP 401: sessao ao vivo bloqueia o acesso global sem autenticacao."""


class OpenF1Client:
    def __init__(self, rate_limiter: RateLimiter | None = None) -> None:
        self.rate_limiter = rate_limiter or RateLimiter()
        self.session = requests.Session()
        self.session.headers.update({"User-Agent": "f1-medallion/1.0 (projeto academico)"})

    # -- construcao de URL ------------------------------------------------

    @staticmethod
    def build_url(endpoint: str, filters: dict[str, Any] | None = None) -> str:
        """Monta a URL. Chaves podem trazer operador embutido: "date>=".

        O valor e codificado manualmente porque datas da OpenF1 contem "+",
        que em query string seria interpretado como espaco.
        """
        parts: list[str] = []
        for key, value in (filters or {}).items():
            if value is None:
                continue
            if any(key.endswith(op) for op in (">=", "<=", ">", "<")):
                field = key
            else:
                field = f"{key}="
            parts.append(f"{field}{quote(str(value), safe='')}")

        query = "&".join(parts)
        return f"{BASE_URL}/{endpoint}" + (f"?{query}" if query else "")

    # -- requisicao unitaria ----------------------------------------------

    def _get(self, url: str) -> list[dict[str, Any]]:
        """Devolve os registros da URL. Lista vazia quando nao ha dado."""
        last_error: Exception | None = None

        for attempt in range(1, MAX_ATTEMPTS + 1):
            self.rate_limiter.acquire()
            try:
                response = self.session.get(url, timeout=REQUEST_TIMEOUT)
            except requests.RequestException as exc:
                last_error = exc
                log.warning("tentativa %d/%d falhou (%s): %s", attempt, MAX_ATTEMPTS, type(exc).__name__, url)
            else:
                status = response.status_code

                if status == 200:
                    return response.json()

                # Resultado vazio legitimo, nao falha. Ver docstring do modulo.
                if status == 404:
                    log.debug("sem registros: %s", url)
                    return []

                if status == 422:
                    raise ResponseTooLarge(f"resposta grande demais: {url}")

                if status == 401:
                    raise AccessBlocked(
                        "HTTP 401: a OpenF1 bloqueia o acesso global durante sessao ao vivo. "
                        "Aguarde o fim da sessao antes de ingerir."
                    )

                last_error = OpenF1Error(f"HTTP {status} em {url}")

                # Demais 4xx (exceto 429) sao definitivos: repetir nao ajuda.
                if 400 <= status < 500 and status != 429:
                    raise last_error

                log.warning(
                    "tentativa %d/%d recebeu HTTP %d: %s",
                    attempt, MAX_ATTEMPTS, status, url,
                )

            if attempt < MAX_ATTEMPTS:
                backoff = min(2 ** attempt, 60) + random.uniform(0, 1.5)
                time.sleep(backoff)

        raise OpenF1Error(f"esgotadas {MAX_ATTEMPTS} tentativas: {url}") from last_error

    # -- requisicao com quebra automatica ---------------------------------

    def fetch(
        self,
        endpoint: str,
        filters: dict[str, Any] | None = None,
        date_start: str | None = None,
        date_end: str | None = None,
        _depth: int = 0,
    ) -> list[dict[str, Any]]:
        """Busca registros, quebrando por janela de tempo se a resposta for grande demais.

        `date_start` e `date_end` vem do endpoint /sessions e so entram na URL
        quando a chamada cheia devolve 422. A janela recebe folga antes de ser
        usada -- ver WINDOW_PAD_BEFORE.
        """
        filters = dict(filters or {})
        window = {}
        if date_start and date_end and _depth > 0:
            window = {"date>=": date_start, "date<": date_end}

        url = self.build_url(endpoint, {**filters, **window})

        try:
            return self._get(url)
        except ResponseTooLarge:
            if _depth == 0:
                date_start, date_end = _pad_window(date_start, date_end)

            if _depth >= MAX_SPLIT_DEPTH or not (date_start and date_end):
                raise

            middle = _midpoint(date_start, date_end)
            if middle is None:
                raise

            log.info("quebrando janela em duas: %s .. %s .. %s", date_start, middle, date_end)
            left = self.fetch(endpoint, filters, date_start, middle, _depth + 1)
            right = self.fetch(endpoint, filters, middle, date_end, _depth + 1)
            return left + right

    # -- atalhos ----------------------------------------------------------

    def sessions(self, year: int) -> list[dict[str, Any]]:
        return self.fetch("sessions", {"year": year})

    def drivers(self, session_key: int) -> list[dict[str, Any]]:
        return self.fetch("drivers", {"session_key": session_key})


def _pad_window(start: str | None, end: str | None) -> tuple[str | None, str | None]:
    """Alarga a janela agendada para cobrir o que roda fora dela."""
    if not (start and end):
        return start, end
    try:
        a = datetime.fromisoformat(start)
        b = datetime.fromisoformat(end)
    except ValueError:
        return start, end
    return (a - WINDOW_PAD_BEFORE).isoformat(), (b + WINDOW_PAD_AFTER).isoformat()


def _midpoint(start: str, end: str) -> str | None:
    """Ponto medio entre dois timestamps ISO da OpenF1."""
    try:
        a = datetime.fromisoformat(start)
        b = datetime.fromisoformat(end)
    except ValueError:
        return None
    if b <= a + timedelta(seconds=30):
        return None
    return (a + (b - a) / 2).isoformat()
