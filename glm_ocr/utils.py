"""Utility helpers."""

from __future__ import annotations

import hashlib
import json
import mimetypes
import socket
import uuid
from pathlib import Path
from typing import Any

from .models import OcrSettings, serialize


def sha256_bytes(content: bytes) -> str:
    return hashlib.sha256(content).hexdigest()


def stable_document_id(sha256: str) -> str:
    return str(uuid.uuid5(uuid.NAMESPACE_URL, f"glm-ocr-document:{sha256}"))


def stable_result_id(run_id: str, logical_source_id: str) -> str:
    return str(uuid.uuid5(uuid.NAMESPACE_URL, f"glm-ocr-result:{run_id}:{logical_source_id}"))


def stable_page_id(result_id: str, page_index: int) -> str:
    return str(uuid.uuid5(uuid.NAMESPACE_URL, f"glm-ocr-page:{result_id}:{page_index}"))


def stable_region_id(result_id: str, page_index: int, region_index: int) -> str:
    return str(
        uuid.uuid5(uuid.NAMESPACE_URL, f"glm-ocr-region:{result_id}:{page_index}:{region_index}")
    )


def settings_hash(settings: OcrSettings) -> str:
    payload = json.dumps(serialize(settings), sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def json_dumps(data: Any) -> str:
    return json.dumps(serialize(data), indent=2, ensure_ascii=True) + "\n"


def detect_mime_type(name: str) -> str | None:
    mime_type, _ = mimetypes.guess_type(name)
    return mime_type


def local_hostname() -> str:
    return socket.gethostname()


def safe_relative_path(source_root: Path, path: Path) -> Path:
    if source_root.is_file():
        return Path(path.name)
    return path.relative_to(source_root)
