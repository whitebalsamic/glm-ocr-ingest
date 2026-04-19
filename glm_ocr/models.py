"""Typed models used across the wrapper."""

from __future__ import annotations

from dataclasses import asdict, dataclass, field, is_dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

JsonValue = None | bool | int | float | str | list["JsonValue"] | dict[str, "JsonValue"]


def utc_now() -> datetime:
    return datetime.now(UTC)


def serialize(value: Any) -> Any:
    if is_dataclass(value):
        return {key: serialize(item) for key, item in asdict(value).items()}
    if isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, Path):
        return str(value)
    if isinstance(value, list):
        return [serialize(item) for item in value]
    if isinstance(value, dict):
        return {key: serialize(item) for key, item in value.items()}
    return value


@dataclass(slots=True)
class ExecutionContext:
    command: str
    cwd: str
    hostname: str
    username: str
    started_at: datetime
    database_url_present: bool
    environment: dict[str, str] = field(default_factory=dict)


@dataclass(slots=True)
class ExecutionBudget:
    jobs: int = 1
    provider_max_workers: int = 1
    max_documents: int | None = None


@dataclass(slots=True)
class OcrSettings:
    provider: str
    model: str
    api_url: str
    layout_device: str
    page_loader_max_tokens: int
    seed: int
    temperature: float
    top_p: float
    top_k: int
    repeat_penalty: float
    allow_untested_provider: bool = False
    best_effort_determinism: bool = False


@dataclass(slots=True)
class DocumentInput:
    raw_bytes: bytes
    display_name: str
    logical_source_id: str
    mime_type: str | None
    source_metadata: dict[str, Any] = field(default_factory=dict)


@dataclass(slots=True)
class DocumentIdentity:
    document_id: str
    sha256: str
    byte_size: int
    mime_type: str | None
    file_extension: str | None
    display_name: str
    logical_source_id: str
    source_metadata: dict[str, Any] = field(default_factory=dict)


@dataclass(slots=True)
class ArtifactRef:
    kind: str
    path: str


@dataclass(slots=True)
class CanonicalRegion:
    region_index: int
    label: str
    native_label: str | None
    content: str
    bbox_2d: list[float] | None
    polygon: list[list[float]] | None
    extra_fields: dict[str, Any] = field(default_factory=dict)


@dataclass(slots=True)
class CanonicalPage:
    page_index: int
    regions: list[CanonicalRegion]
    page_extra: dict[str, Any] = field(default_factory=dict)


@dataclass(slots=True)
class CanonicalDocumentResult:
    pages: list[CanonicalPage]
    summary: dict[str, Any]
    provider_extra: dict[str, Any] = field(default_factory=dict)


@dataclass(slots=True)
class ProviderParseResult:
    provider_name: str
    provider_version: str
    provider_metadata: dict[str, Any]
    raw_payload: dict[str, Any]
    canonical_result: CanonicalDocumentResult
    warnings: list[str] = field(default_factory=list)
    started_at: datetime | None = None
    finished_at: datetime | None = None


@dataclass(slots=True)
class RecordManifest:
    artifact_manifest_version: int
    run_id: str
    result_id: str
    provider_contract_version: int
    canonical_schema_version: int
    run_metadata: dict[str, Any]
    document_identity: DocumentIdentity
    ocr_settings: OcrSettings
    provider_metadata: dict[str, Any]
    canonical_result: CanonicalDocumentResult
    raw_provider_payload: dict[str, Any]
    warnings: list[str]
    artifact_refs: list[ArtifactRef]
    provenance: dict[str, Any]
    timings: dict[str, Any] = field(default_factory=dict)


@dataclass(slots=True)
class RunSummary:
    run_id: str
    status: str
    provider_name: str
    provider_version: str
    canonical_schema_version: int
    artifact_manifest_version: int
    provider_contract_version: int
    started_at: datetime
    finished_at: datetime
    settings: OcrSettings
    execution_context: ExecutionContext
    warnings: list[str] = field(default_factory=list)
    counts: dict[str, int] = field(default_factory=dict)
    documents: list[dict[str, Any]] = field(default_factory=list)
