"""Escrita dos dados brutos em NDJSON comprimido.

Duas implementacoes com a mesma interface: GCS (producao) e disco local
(testes). O checkpoint da ingestao e a propria existencia do objeto:
antes de chamar a API, verificamos se o arquivo ja esta la.
"""

from __future__ import annotations

import gzip
import io
import json
import logging
import os
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
    def append_line(self, path: str, line: str) -> None: ...

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

    def append_line(self, path: str, line: str) -> None:
        # GCS nao tem append; cada execucao grava seu proprio arquivo de manifesto.
        blob = self.bucket.blob(path)
        existing = blob.download_as_text() if blob.exists(self.client) else ""
        blob.upload_from_string(existing + line + "\n", content_type="application/x-ndjson")


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

    def append_line(self, path: str, line: str) -> None:
        full = self._full(path)
        full.parent.mkdir(parents=True, exist_ok=True)
        with full.open("a", encoding="utf-8") as handle:
            handle.write(line + "\n")


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


def manifest_path(prefix: str, run_id: str) -> str:
    return f"{prefix}/_manifest/run_id={run_id}/log.jsonl"
