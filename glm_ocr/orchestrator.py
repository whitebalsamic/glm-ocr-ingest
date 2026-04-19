"""Parse, replay, and doctor orchestration."""

from __future__ import annotations

import getpass
import socket
import uuid
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from .artifacts import LocalArtifactSink
from .constants import (
    ARTIFACT_MANIFEST_VERSION,
    CANONICAL_SCHEMA_VERSION,
    PROVIDER_CONTRACT_VERSION,
)
from .models import (
    ArtifactRef,
    DocumentIdentity,
    ExecutionBudget,
    ExecutionContext,
    OcrSettings,
    RecordManifest,
    RunSummary,
    serialize,
)
from .providers.base import OcrProvider
from .store import PostgresResultStore, load_json_file
from .utils import settings_hash, sha256_bytes, stable_document_id, stable_result_id


@dataclass(slots=True)
class ParseOutcome:
    failures: int
    skipped: int
    processed: int
    run_summary: RunSummary


def build_execution_context(command: str, cwd: str, database_url: str | None) -> ExecutionContext:
    return ExecutionContext(
        command=command,
        cwd=cwd,
        hostname=socket.gethostname(),
        username=getpass.getuser(),
        started_at=datetime.now(UTC),
        database_url_present=bool(database_url),
        environment={},
    )


def parse_documents(
    *,
    source: Any,
    sink: LocalArtifactSink,
    provider: OcrProvider,
    settings: OcrSettings,
    budget: ExecutionBudget,
    execution_context: ExecutionContext,
    overwrite: bool,
    database_url: str | None,
    db_schema: str,
) -> ParseOutcome:
    run_id = str(uuid.uuid4())
    provider_warnings = provider.validate_environment(settings)
    started_at = datetime.now(UTC)
    sink.ensure_root()

    discovered_paths = source.discovered_paths(max_documents=budget.max_documents)
    if not discovered_paths:
        raise RuntimeError("No supported files found.")

    provider_info = provider.provider_metadata()
    summary = RunSummary(
        run_id=run_id,
        status="running",
        provider_name=str(provider_info["provider_name"]),
        provider_version=str(provider_info["provider_version"]),
        canonical_schema_version=CANONICAL_SCHEMA_VERSION,
        artifact_manifest_version=ARTIFACT_MANIFEST_VERSION,
        provider_contract_version=PROVIDER_CONTRACT_VERSION,
        started_at=started_at,
        finished_at=started_at,
        settings=settings,
        execution_context=execution_context,
        warnings=list(provider_warnings),
        counts={"discovered": len(discovered_paths), "processed": 0, "failed": 0, "skipped": 0},
    )
    store = PostgresResultStore(database_url, db_schema) if database_url else None
    if store:
        store.bootstrap()

    failures = 0
    skipped = 0
    processed = 0

    for document in source.iter_documents():
        completed_count = processed + failures + skipped
        if budget.max_documents is not None and completed_count >= budget.max_documents:
            break
        compatibility_ref = None
        try:
            result_id = stable_result_id(run_id, document.logical_source_id)
            compatibility_path = sink.compatibility_json_path(document.logical_source_id)
            if compatibility_path.exists() and not overwrite:
                skipped += 1
                summary.counts["skipped"] = skipped
                continue

            sha256 = sha256_bytes(document.raw_bytes)
            identity = DocumentIdentity(
                document_id=stable_document_id(sha256),
                sha256=sha256,
                byte_size=len(document.raw_bytes),
                mime_type=document.mime_type,
                file_extension=document.source_metadata.get("extension"),
                display_name=document.display_name,
                logical_source_id=document.logical_source_id,
                source_metadata=document.source_metadata,
            )
            provider_result = provider.parse_document(document, settings, budget)
            raw_sdk_json = provider_result.raw_payload.get(
                "json_result", provider_result.raw_payload
            )
            compatibility_ref = sink.write_compatibility_json(
                document.logical_source_id,
                raw_sdk_json,
                overwrite=overwrite,
            )
            record_ref = sink.record_ref(run_id, document.logical_source_id)
            manifest = RecordManifest(
                artifact_manifest_version=ARTIFACT_MANIFEST_VERSION,
                run_id=run_id,
                result_id=result_id,
                provider_contract_version=PROVIDER_CONTRACT_VERSION,
                canonical_schema_version=CANONICAL_SCHEMA_VERSION,
                run_metadata={
                    "status": "completed",
                    "provider_name": provider_result.provider_name,
                    "provider_version": provider_result.provider_version,
                    "settings_hash": settings_hash(settings),
                },
                document_identity=identity,
                ocr_settings=settings,
                provider_metadata=provider_result.provider_metadata,
                canonical_result=provider_result.canonical_result,
                raw_provider_payload=provider_result.raw_payload,
                warnings=provider_result.warnings,
                artifact_refs=[compatibility_ref, record_ref],
                provenance={
                    "execution_context": serialize(execution_context),
                    "logical_source_id": document.logical_source_id,
                },
                timings={
                    "started_at": provider_result.started_at,
                    "finished_at": provider_result.finished_at,
                },
            )
            sink.write_record(manifest)
            if store:
                store.persist_result(summary, manifest)
            processed += 1
            summary.counts["processed"] = processed
            summary.documents.append(
                {
                    "logical_source_id": document.logical_source_id,
                    "result_id": result_id,
                    "status": "ok",
                    "artifact_refs": [serialize(ref) for ref in manifest.artifact_refs],
                }
            )
        except Exception as exc:  # noqa: BLE001
            failures += 1
            summary.counts["failed"] = failures
            summary.warnings.append(f"{document.logical_source_id}: {exc}")
            summary.documents.append(
                {
                    "logical_source_id": document.logical_source_id,
                    "status": "error",
                    "error": str(exc),
                    "compatibility_path": None
                    if compatibility_ref is None
                    else compatibility_ref.path,
                }
            )

    summary.status = "completed" if failures == 0 else "completed_with_errors"
    summary.finished_at = datetime.now(UTC)
    sink.write_run_summary(summary)
    if store:
        store.upsert_run_summary(summary)
    return ParseOutcome(failures, skipped, processed, summary)


def replay_artifacts(
    *,
    path: Path,
    database_url: str,
    db_schema: str,
) -> dict[str, Any]:
    store = PostgresResultStore(database_url, db_schema)
    store.bootstrap()
    summaries: list[RunSummary] = []
    manifests: list[RecordManifest] = []

    if path.is_file():
        if not path.name.endswith(".record.json"):
            raise ValueError("Single-file replay requires a *.record.json manifest.")
        manifests.append(_record_from_dict(load_json_file(path)))
    else:
        run_files = sorted((path / "_runs").glob("*.json"))
        for run_file in run_files:
            summaries.append(_summary_from_dict(load_json_file(run_file)))
        record_files = sorted((path / "_records").glob("**/*.record.json"))
        for record_file in record_files:
            manifests.append(_record_from_dict(load_json_file(record_file)))

    by_run_id = {summary.run_id: summary for summary in summaries}
    replayed = 0
    for manifest in manifests:
        summary = by_run_id.get(manifest.run_id)
        if summary is None:
            now = datetime.now(UTC)
            summary = RunSummary(
                run_id=manifest.run_id,
                status="replayed",
                provider_name=str(manifest.provider_metadata.get("provider_name", "glm")),
                provider_version=str(manifest.provider_metadata.get("provider_version", "unknown")),
                canonical_schema_version=manifest.canonical_schema_version,
                artifact_manifest_version=manifest.artifact_manifest_version,
                provider_contract_version=manifest.provider_contract_version,
                started_at=now,
                finished_at=now,
                settings=manifest.ocr_settings,
                execution_context=ExecutionContext(
                    command="replay",
                    cwd=str(path),
                    hostname=socket.gethostname(),
                    username=getpass.getuser(),
                    started_at=now,
                    database_url_present=True,
                    environment={},
                ),
                counts={"processed": 0, "failed": 0, "skipped": 0, "discovered": 0},
            )
            by_run_id[summary.run_id] = summary
        store.persist_result(summary, manifest)
        replayed += 1

    for summary in by_run_id.values():
        store.upsert_run_summary(summary)
    return {"replayed_records": replayed, "runs": len(by_run_id)}


def doctor_checks(
    *,
    provider: OcrProvider,
    settings: OcrSettings,
    output_dir: Path,
    database_url: str | None,
    db_schema: str,
) -> dict[str, Any]:
    results: dict[str, Any] = {"ok": True, "checks": []}

    try:
        metadata = provider.provider_metadata()
        results["checks"].append({"name": "provider_import", "ok": True, "details": metadata})
    except Exception as exc:  # noqa: BLE001
        results["ok"] = False
        results["checks"].append({"name": "provider_import", "ok": False, "error": str(exc)})
        return results

    try:
        provider.validate_environment(settings)
        results["checks"].append({"name": "provider_environment", "ok": True})
    except Exception as exc:  # noqa: BLE001
        results["ok"] = False
        results["checks"].append({"name": "provider_environment", "ok": False, "error": str(exc)})

    try:
        output_dir.mkdir(parents=True, exist_ok=True)
        probe = output_dir / ".doctor-write-test"
        probe.write_text("ok\n", encoding="utf-8")
        probe.unlink()
        results["checks"].append({"name": "artifact_root", "ok": True})
    except OSError as exc:
        results["ok"] = False
        results["checks"].append({"name": "artifact_root", "ok": False, "error": str(exc)})

    if database_url:
        try:
            store = PostgresResultStore(database_url, db_schema)
            store.bootstrap()
            results["checks"].append({"name": "postgres", "ok": True})
        except Exception as exc:  # noqa: BLE001
            results["ok"] = False
            results["checks"].append({"name": "postgres", "ok": False, "error": str(exc)})

    return results


def _record_from_dict(data: dict[str, Any]) -> RecordManifest:
    return RecordManifest(
        artifact_manifest_version=int(data["artifact_manifest_version"]),
        run_id=data["run_id"],
        result_id=data["result_id"],
        provider_contract_version=int(data["provider_contract_version"]),
        canonical_schema_version=int(data["canonical_schema_version"]),
        run_metadata=data["run_metadata"],
        document_identity=DocumentIdentity(**data["document_identity"]),
        ocr_settings=OcrSettings(**data["ocr_settings"]),
        provider_metadata=data["provider_metadata"],
        canonical_result=_canonical_from_dict(data["canonical_result"]),
        raw_provider_payload=data["raw_provider_payload"],
        warnings=data.get("warnings", []),
        artifact_refs=[ArtifactRef(**item) for item in data.get("artifact_refs", [])],
        provenance=data.get("provenance", {}),
        timings=data.get("timings", {}),
    )


def _summary_from_dict(data: dict[str, Any]) -> RunSummary:
    return RunSummary(
        run_id=data["run_id"],
        status=data["status"],
        provider_name=data["provider_name"],
        provider_version=data["provider_version"],
        canonical_schema_version=int(data["canonical_schema_version"]),
        artifact_manifest_version=int(data["artifact_manifest_version"]),
        provider_contract_version=int(data["provider_contract_version"]),
        started_at=datetime.fromisoformat(data["started_at"]),
        finished_at=datetime.fromisoformat(data["finished_at"]),
        settings=OcrSettings(**data["settings"]),
        execution_context=ExecutionContext(
            command=data["execution_context"]["command"],
            cwd=data["execution_context"]["cwd"],
            hostname=data["execution_context"]["hostname"],
            username=data["execution_context"]["username"],
            started_at=datetime.fromisoformat(data["execution_context"]["started_at"]),
            database_url_present=bool(data["execution_context"]["database_url_present"]),
            environment=data["execution_context"].get("environment", {}),
        ),
        warnings=data.get("warnings", []),
        counts=data.get("counts", {}),
        documents=data.get("documents", []),
    )


def _canonical_from_dict(data: dict[str, Any]) -> Any:
    from .models import CanonicalDocumentResult, CanonicalPage, CanonicalRegion

    return CanonicalDocumentResult(
        pages=[
            CanonicalPage(
                page_index=item["page_index"],
                regions=[CanonicalRegion(**region) for region in item["regions"]],
                page_extra=item.get("page_extra", {}),
            )
            for item in data["pages"]
        ],
        summary=data["summary"],
        provider_extra=data.get("provider_extra", {}),
    )
