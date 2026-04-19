from __future__ import annotations

import json
from datetime import UTC, datetime
from pathlib import Path

from glm_ocr.artifacts import LocalArtifactSink
from glm_ocr.models import (
    ArtifactRef,
    CanonicalDocumentResult,
    CanonicalPage,
    CanonicalRegion,
    DocumentIdentity,
    OcrSettings,
    RecordManifest,
)


def test_artifact_paths_and_manifest_write(tmp_path: Path) -> None:
    sink = LocalArtifactSink(tmp_path)
    settings = OcrSettings(
        provider="glm",
        model="m",
        api_url="http://example.com",
        layout_device="cpu",
        page_loader_max_tokens=1,
        seed=42,
        temperature=0.0,
        top_p=0.0,
        top_k=1,
        repeat_penalty=1.0,
    )
    manifest = RecordManifest(
        artifact_manifest_version=1,
        run_id="run-1",
        result_id="result-1",
        provider_contract_version=1,
        canonical_schema_version=1,
        run_metadata={},
        document_identity=DocumentIdentity(
            document_id="doc-1",
            sha256="abc",
            byte_size=3,
            mime_type="application/pdf",
            file_extension=".pdf",
            display_name="invoice.pdf",
            logical_source_id="nested/invoice.pdf",
            source_metadata={},
        ),
        ocr_settings=settings,
        provider_metadata={"provider_name": "glm", "provider_version": "1"},
        canonical_result=CanonicalDocumentResult(
            pages=[
                CanonicalPage(
                    page_index=0,
                    regions=[
                        CanonicalRegion(
                            region_index=0,
                            label="text",
                            native_label="text",
                            content="hello",
                            bbox_2d=None,
                            polygon=None,
                            extra_fields={},
                        )
                    ],
                )
            ],
            summary={"page_count": 1, "region_count": 1},
        ),
        raw_provider_payload={"json_result": []},
        warnings=[],
        artifact_refs=[
            ArtifactRef(kind="compatibility_json", path=str(tmp_path / "nested" / "invoice.json")),
            sink.record_ref("run-1", "nested/invoice.pdf"),
        ],
        provenance={},
        timings={"started_at": datetime.now(UTC), "finished_at": datetime.now(UTC)},
    )

    sink.write_record(manifest)
    data = json.loads(
        (tmp_path / "_records" / "run-1" / "nested" / "invoice.record.json").read_text()
    )

    assert data["artifact_refs"][1]["path"].endswith("invoice.record.json")
