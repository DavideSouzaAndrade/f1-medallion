"""Distribuição dos eventos ao vivo por SSE.

Uma única tarefa consulta o repositório a cada STREAM_POLL_S e entrega o resultado a todos os
clientes conectados. Cem abas abertas custam o mesmo que uma, e o navegador continua sem tocar
no BigQuery. A tarefa só roda enquanto há alguém conectado.

Mensagens que o cliente recebe (campo `event` do SSE):
  volta  um EventoStream em JSON
  ok     batimento a cada consulta: {"ts": ..., "novos": N}. Mostra que a API e o BigQuery estão vivos
  erro   a consulta falhou: {"mensagem": ...}. A tarefa tenta de novo no próximo ciclo
"""
import asyncio
import json
import logging
from collections import deque
from datetime import datetime, timezone
from typing import AsyncIterator, Optional

import config as cfg
from schemas import EventoStream

log = logging.getLogger("bi.stream")

Mensagem = tuple[str, str]   # (event, data já em JSON)


class Difusor:
    def __init__(self, repo):
        self.repo = repo
        self._filas: set[asyncio.Queue] = set()
        self._recentes: deque[Mensagem] = deque(maxlen=cfg.STREAM_BUFFER)   # entregues a quem conecta
        self._vistos: deque[str] = deque(maxlen=5000)
        self._vistos_set: set[str] = set()
        self._cursor: Optional[datetime] = None
        self._tarefa: Optional[asyncio.Task] = None

    async def assinar(self) -> AsyncIterator[Optional[Mensagem]]:
        """Primeiro o que já passou (para a tela não abrir vazia), depois o que chegar.

        Termina (devolve None) se o cliente ficar tão atrás que a fila encheu: ele reconecta.
        """
        fila: asyncio.Queue = asyncio.Queue(maxsize=1000)
        passado = list(self._recentes)      # sem await até aqui: nada se perde entre cópia e fila
        self._filas.add(fila)
        self._garantir_tarefa()
        try:
            for m in passado:
                yield m
            while True:
                m = await fila.get()
                yield m
                if m is None:
                    return
        finally:
            self._filas.discard(fila)

    def _garantir_tarefa(self) -> None:
        if self._tarefa is None or self._tarefa.done():
            self._tarefa = asyncio.create_task(self._laco())

    async def _laco(self) -> None:
        while self._filas:
            try:
                novos = await asyncio.to_thread(self._buscar)
                for ev in novos:
                    self._publicar(("volta", ev.model_dump_json()), guardar=True)
                self._publicar(("ok", json.dumps({"ts": datetime.now(timezone.utc).isoformat(),
                                                  "novos": len(novos)})))
            except Exception:
                log.exception("falha ao consultar eventos do stream")
                self._publicar(("erro", json.dumps({"mensagem": "falha ao consultar o BigQuery"})))
            await asyncio.sleep(cfg.STREAM_POLL_S)

    def _buscar(self) -> list[EventoStream]:
        # Primeira consulta: os mais recentes. Depois: tudo a partir do cursor, inclusive, porque
        # duas linhas podem ter o mesmo _ts_processamento; os já vistos são descartados por id.
        linhas = self.repo.eventos_stream(self._cursor, 500 if self._cursor else cfg.STREAM_BUFFER)
        novos = []
        for linha in linhas:
            if linha["evento_id"] in self._vistos_set:
                continue
            if len(self._vistos) == self._vistos.maxlen:
                self._vistos_set.discard(self._vistos[0])
            self._vistos.append(linha["evento_id"])
            self._vistos_set.add(linha["evento_id"])
            novos.append(EventoStream.model_validate(linha))
        if linhas:
            self._cursor = max(linha["ts_processamento"] for linha in linhas)
        return novos

    def _publicar(self, msg: Mensagem, guardar: bool = False) -> None:
        if guardar:
            self._recentes.append(msg)
        for fila in list(self._filas):
            try:
                fila.put_nowait(msg)
            except asyncio.QueueFull:
                # Cliente travado: abre espaço para o sinal de fim e solta a fila.
                self._filas.discard(fila)
                fila.get_nowait()
                fila.put_nowait(None)
