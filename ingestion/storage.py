"""Escrita dos dados brutos em NDJSON comprimido.

Duas implementacoes com a mesma interface: GCS (producao) e disco local
(testes). O checkpoint da ingestao e a propria existencia do objeto:
antes de chamar a API, verificamos se o arquivo ja esta la.

Resultado vazio tambem vira arquivo -- um .json.gz de zero linhas. Sem
isso a combinacao nunca entra no checkpoint e volta a ser consultada em
toda execucao.
"""

from __future__ import annotations

import gzip
import io
import json
import logging
from abc import ABC, abstractmethod
from pathlib import Path
from typing import Any

log = logging.getLogger(__name__)


def to_ndjson_gz(records: list[dict[str, Any]]) -> bytes:
    """Converte a lista devolvida pela API em NDJSON comprimido.

    Um objeto por linha e o formato que o BigQuery carrega nativamente.
    Nenhum campo e renomeado, convertido ou filtrado: a camada bronze
    deve ser copia fiel da origem.
    """
    buffer = io.BytesIO()
    with gzip.GzipFile(fileobj=buffer, mode="wb", mtime=0) as gz:
        for record in records:
            gz.write(json.dumps(record, ensure_ascii=False, separators=(",", ":")).encode())
            gz.write(b"\n")
    return buffer.getvalue()


class Storage(ABC):
    @abstractmethod
    def exists(self, path: str) -> bool: ...

    @abstractmethod
    def write_bytes(self, path: str, payload: bytes) -> None: ...

    @abstractmethod
    def write_text(self, path: str, text: str) -> None: ...

    def write_records(self, path: str, records: list[dict[str, Any]]) -> int:
        payload = to_ndjson_gz(records)
        self.write_bytes(path, payload)
        return len(payload)


class GCSStorage(Storage):
    def __init__(self, bucket_name: str) -> None:
        from google.cloud import storage as gcs  # import tardio: opcional em testes locais

        self.client = gcs.Client()
        self.bucket = self.client.bucket(bucket_name)
        self.bucket_name = bucket_name

    def exists(self, path: str) -> bool:
        return self.bucket.blob(path).exists(self.client)

    def write_bytes(self, path: str, payload: bytes) -> None:
        blob = self.bucket.blob(path)
        blob.content_encoding = None
        blob.upload_from_string(payload, content_type="application/gzip")

    def write_text(self, path: str, text: str) -> None:
        self.bucket.blob(path).upload_from_string(text, content_type="application/x-ndjson")


class LocalStorage(Storage):
    def __init__(self, root: str) -> None:
        self.root = Path(root)

    def _full(self, path: str) -> Path:
        return self.root / path

    def exists(self, path: str) -> bool:
        return self._full(path).exists()

    def write_bytes(self, path: str, payload: bytes) -> None:
        full = self._full(path)
        full.parent.mkdir(parents=True, exist_ok=True)
        full.write_bytes(payload)

    def write_text(self, path: str, text: str) -> None:
        full = self._full(path)
        full.parent.mkdir(parents=True, exist_ok=True)
        full.write_text(text, encoding="utf-8")


class ManifestWriter:
    """Acumula as entradas do manifesto e grava em partes.

    O GCS nao tem append. Reescrever o manifesto inteiro a cada requisicao
    custa uma leitura e uma escrita do arquivo acumulado -- na onda 4, com
    milhares de objetos, isso e trafego quadratico e ainda abre corrida
    entre duas execucoes simultaneas.

    Aqui cada flush cria um objeto novo, `part-NNNN.jsonl`, sob o prefixo
    do run_id. Nada e lido de volta, nada e sobrescrito, e execucoes
    distintas tem run_id distinto, entao nao colidem. A tabela externa do
    manifesto le todas as partes como um arquivo so.
    """

    def __init__(self, storage: Storage, prefix: str, run_id: str, flush_every: int = 200) -> None:
        self.storage = storage
        self.base = manifest_prefix(prefix, run_id)
        self.flush_every = flush_every
        self._buffer: list[str] = []
        self._parts = 0

    def add(self, entry: dict[str, Any]) -> None:
        self._buffer.append(json.dumps(entry, ensure_ascii=False))
        if len(self._buffer) >= self.flush_every:
            self.flush()

    def flush(self) -> None:
        if not self._buffer:
            return
        self._parts += 1
        path = f"{self.base}/part-{self._parts:04d}.jsonl"
        try:
            self.storage.write_text(path, "\n".join(self._buffer) + "\n")
        except Exception as exc:  # manifesto nao pode derrubar a ingestao
            log.warning("nao foi possivel gravar a parte %s do manifesto: %s", path, exc)
        else:
            self._buffer.clear()


# --- convencao de caminhos -------------------------------------------------
#
# Particionamento no estilo Hive: o BigQuery reconhece year= e session_key=
# automaticamente ao criar a tabela externa, e uma consulta filtrada por
# sessao nem chega a ler os demais arquivos.

def object_path(
    prefix: str,
    endpoint: str,
    year: int,
    session_key: int | None = None,
    driver_number: int | None = None,
) -> str:
    parts = [prefix, endpoint, f"year={year}"]
    if session_key is not None:
        parts.append(f"session_key={session_key}")
    if driver_number is not None:
        parts.append(f"driver_{driver_number}.json.gz")
    else:
        parts.append("data.json.gz")
    return "/".join(parts)


def manifest_prefix(prefix: str, run_id: str) -> str:
    return f"{prefix}/_manifest/run_id={run_id}"
