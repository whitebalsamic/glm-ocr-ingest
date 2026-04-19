"""Postgres storage and replay helpers."""

from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from .constants import SCHEMA_DIR
from .models import RecordManifest, RunSummary, serialize
from .utils import settings_hash, stable_page_id, stable_region_id


@dataclass(slots=True)
class PostgresResultStore:
    database_url: str
    schema: str = "public"

    def bootstrap(self) -> None:
        psycopg = _load_psycopg()
        sql_path = SCHEMA_DIR / "001_init.sql"
        sql_text = sql_path.read_text(encoding="utf-8").replace("__DB_SCHEMA__", self.schema)
        with psycopg.connect(self.database_url) as conn:
            with conn.cursor() as cur:
                cur.execute(sql_text)
            conn.commit()

    def persist_result(self, summary: RunSummary, manifest: RecordManifest) -> None:
        psycopg = _load_psycopg()
        payload = serialize(manifest)
        document = payload["document_identity"]
        provider_name = manifest.provider_metadata.get(
            "provider_name"
        ) or manifest.provider_metadata.get("name", "glm")
        provider_version = manifest.provider_metadata.get("provider_version")
        set_hash = settings_hash(manifest.ocr_settings)
        schema = self.schema
        with psycopg.connect(self.database_url) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    f"""
                    insert into {schema}.ocr_runs (
                        run_id,
                        status,
                        provider_name,
                        provider_version,
                        canonical_schema_version,
                        artifact_manifest_version,
                        provider_contract_version,
                        started_at,
                        finished_at,
                        settings_json,
                        execution_context_json,
                        warnings_json,
                        aggregate_counts_json
                    )
                    values (
                        %s, %s, %s, %s, %s, %s, %s, %s, %s,
                        %s::jsonb, %s::jsonb, %s::jsonb, %s::jsonb
                    )
                    on conflict (run_id) do nothing
                    """,
                    (
                        summary.run_id,
                        summary.status,
                        summary.provider_name,
                        summary.provider_version,
                        summary.canonical_schema_version,
                        summary.artifact_manifest_version,
                        summary.provider_contract_version,
                        summary.started_at,
                        summary.finished_at,
                        json.dumps(serialize(summary.settings)),
                        json.dumps(serialize(summary.execution_context)),
                        json.dumps(summary.warnings),
                        json.dumps(summary.counts),
                    ),
                )
                cur.execute(
                    f"""
                    insert into {schema}.ocr_documents (
                        document_id, sha256, display_name, logical_source_id, byte_size, mime_type,
                        file_extension, source_metadata, first_seen_at, last_seen_at
                    )
                    values (%s, %s, %s, %s, %s, %s, %s, %s::jsonb, now(), now())
                    on conflict (sha256) do update set
                        display_name = excluded.display_name,
                        logical_source_id = excluded.logical_source_id,
                        byte_size = excluded.byte_size,
                        mime_type = excluded.mime_type,
                        file_extension = excluded.file_extension,
                        source_metadata = excluded.source_metadata,
                        last_seen_at = now()
                    """,
                    (
                        document["document_id"],
                        document["sha256"],
                        document["display_name"],
                        document["logical_source_id"],
                        document["byte_size"],
                        document["mime_type"],
                        document["file_extension"],
                        json.dumps(document["source_metadata"]),
                    ),
                )
                cur.execute(
                    f"""
                    insert into {schema}.ocr_results (
                        result_id,
                        run_id,
                        document_id,
                        provider_name,
                        provider_version,
                        provider_metadata,
                        settings_hash,
                        started_at,
                        finished_at,
                        canonical_result,
                        raw_provider_payload,
                        artifact_refs,
                        provenance,
                        warnings_json
                    )
                    values (
                        %s, %s, %s, %s, %s, %s::jsonb, %s, %s, %s,
                        %s::jsonb, %s::jsonb, %s::jsonb, %s::jsonb, %s::jsonb
                    )
                    on conflict (result_id) do nothing
                    """,
                    (
                        manifest.result_id,
                        manifest.run_id,
                        document["document_id"],
                        provider_name,
                        provider_version,
                        json.dumps(serialize(manifest.provider_metadata)),
                        set_hash,
                        manifest.timings.get("started_at"),
                        manifest.timings.get("finished_at"),
                        json.dumps(serialize(manifest.canonical_result)),
                        json.dumps(serialize(manifest.raw_provider_payload)),
                        json.dumps(serialize(manifest.artifact_refs)),
                        json.dumps(serialize(manifest.provenance)),
                        json.dumps(manifest.warnings),
                    ),
                )
                for page in manifest.canonical_result.pages:
                    page_id = stable_page_id(manifest.result_id, page.page_index)
                    cur.execute(
                        f"""
                        insert into {schema}.ocr_pages (
                            page_id, result_id, page_index, region_count, page_json
                        )
                        values (%s, %s, %s, %s, %s::jsonb)
                        on conflict (result_id, page_index) do nothing
                        """,
                        (
                            page_id,
                            manifest.result_id,
                            page.page_index,
                            len(page.regions),
                            json.dumps(serialize(page)),
                        ),
                    )
                    for region in page.regions:
                        cur.execute(
                            f"""
                            insert into {schema}.ocr_regions (
                                region_id, result_id, page_id, page_index, region_index, label,
                                native_label, content, bbox_2d, polygon, region_json
                            )
                            values (%s, %s, %s, %s, %s, %s, %s, %s, %s::jsonb, %s::jsonb, %s::jsonb)
                            on conflict (result_id, page_index, region_index) do nothing
                            """,
                            (
                                stable_region_id(
                                    manifest.result_id, page.page_index, region.region_index
                                ),
                                manifest.result_id,
                                page_id,
                                page.page_index,
                                region.region_index,
                                region.label,
                                region.native_label,
                                region.content,
                                json.dumps(region.bbox_2d),
                                json.dumps(region.polygon),
                                json.dumps(serialize(region)),
                            ),
                        )
            conn.commit()

    def upsert_run_summary(self, summary: RunSummary) -> None:
        psycopg = _load_psycopg()
        schema = self.schema
        with psycopg.connect(self.database_url) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    f"""
                    insert into {schema}.ocr_runs (
                        run_id,
                        status,
                        provider_name,
                        provider_version,
                        canonical_schema_version,
                        artifact_manifest_version,
                        provider_contract_version,
                        started_at,
                        finished_at,
                        settings_json,
                        execution_context_json,
                        warnings_json,
                        aggregate_counts_json
                    )
                    values (
                        %s, %s, %s, %s, %s, %s, %s, %s, %s,
                        %s::jsonb, %s::jsonb, %s::jsonb, %s::jsonb
                    )
                    on conflict (run_id) do update set
                        status = excluded.status,
                        finished_at = excluded.finished_at,
                        warnings_json = excluded.warnings_json,
                        aggregate_counts_json = excluded.aggregate_counts_json
                    """,
                    (
                        summary.run_id,
                        summary.status,
                        summary.provider_name,
                        summary.provider_version,
                        summary.canonical_schema_version,
                        summary.artifact_manifest_version,
                        summary.provider_contract_version,
                        summary.started_at,
                        summary.finished_at,
                        json.dumps(serialize(summary.settings)),
                        json.dumps(serialize(summary.execution_context)),
                        json.dumps(summary.warnings),
                        json.dumps(summary.counts),
                    ),
                )
            conn.commit()


def load_json_file(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def _load_psycopg() -> Any:
    try:
        import psycopg
    except ImportError as exc:  # pragma: no cover - env-dependent
        raise RuntimeError(
            "psycopg is not importable. Install project dependencies first."
        ) from exc
    return psycopg
