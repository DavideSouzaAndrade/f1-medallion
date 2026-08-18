"""Cliente HTTP para a API da OpenF1.

Responsabilidades:
  - respeitar os limites do tier gratuito (3 req/s e 30 req/min);
  - repetir requisicoes que falham, com backoff exponencial;
  - quebrar respostas grandes demais em janelas de tempo menores.

A API nao possui paginacao: o controle de volume e feito filtrando
por piloto e, quando necessario, por intervalo de tempo.
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
        last_error: Exception | None = None

        for attempt in range(1, MAX_ATTEMPTS + 1):
            self.rate_limiter.acquire()
            try:
                response = self.session.get(url, timeout=REQUEST_TIMEOUT)
            except requests.RequestException as exc:
                last_error = exc
                log.warning("tentativa %d/%d falhou (%s): %s", attempt, MAX_ATTEMPTS, type(exc).__name__, url)
            else:
                if response.status_code == 200:
                    return response.json()

                last_error = OpenF1Error(f"HTTP {response.status_code} em {url}")

                # 4xx que nao seja 429 nao adianta repetir.
                if 400 <= response.status_code < 500 and response.status_code != 429:
                    raise last_error

                log.warning(
                    "tentativa %d/%d recebeu HTTP %d: %s",
                    attempt, MAX_ATTEMPTS, response.status_code, url,
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
        """Busca registros, quebrando por janela de tempo se a resposta falhar.

        `date_start` e `date_end` vem do endpoint /sessions e so sao usados
        quando a chamada cheia nao passa (resposta grande demais).
        """
        filters = dict(filters or {})
        window = {}
        if date_start and date_end and _depth > 0:
            window = {"date>=": date_start, "date<": date_end}

        url = self.build_url(endpoint, {**filters, **window})

        try:
            return self._get(url)
        except OpenF1Error:
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
