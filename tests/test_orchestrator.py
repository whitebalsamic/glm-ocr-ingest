from __future__ import annotations

import json
from pathlib import Path

from glm_ocr.artifacts import LocalArtifactSink
from glm_ocr.models import ExecutionBudget, OcrSettings
from glm_ocr.orchestrator import build_execution_context, parse_documents
from glm_ocr.sources import LocalPathDocumentSource


def test_parse_documents_writes_compatibility_and_record_artifacts(
    tmp_path: Path,
    fake_provider,
) -> None:
    input_dir = tmp_path / "in"
    input_dir.mkdir()
    (input_dir / "invoice.pdf").write_bytes(b"pdf")

    output_dir = tmp_path / "out"
    outcome = parse_documents(
        source=LocalPathDocumentSource(input_dir),
        sink=LocalArtifactSink(output_dir),
        provider=fake_provider,
        settings=OcrSettings(
            provider="fake",
            model="m",
            api_url="http://example.com",
            layout_device="cpu",
            page_loader_max_tokens=1,
            seed=42,
            temperature=0.0,
            top_p=0.0,
            top_k=1,
            repeat_penalty=1.0,
        ),
        budget=ExecutionBudget(),
        execution_context=build_execution_context("parse", str(tmp_path), None),
        overwrite=False,
        database_url=None,
        db_schema="public",
    )

    assert outcome.failures == 0
    assert json.loads((output_dir / "invoice.json").read_text())[0][0]["content"] == "invoice.pdf"
    run_summary_path = next((output_dir / "_runs").glob("*.json"))
    record_path = next((output_dir / "_records").glob("**/*.record.json"))
    assert run_summary_path.exists()
    assert record_path.exists()


def test_parse_documents_skips_existing_compatibility_artifact(
    tmp_path: Path,
    fake_provider,
) -> None:
    input_dir = tmp_path / "in"
    input_dir.mkdir()
    (input_dir / "invoice.pdf").write_bytes(b"pdf")
    output_dir = tmp_path / "out"
    output_dir.mkdir()
    (output_dir / "invoice.json").write_text("[]\n", encoding="utf-8")

    outcome = parse_documents(
        source=LocalPathDocumentSource(input_dir),
        sink=LocalArtifactSink(output_dir),
        provider=fake_provider,
        settings=OcrSettings(
            provider="fake",
            model="m",
            api_url="http://example.com",
            layout_device="cpu",
            page_loader_max_tokens=1,
            seed=42,
            temperature=0.0,
            top_p=0.0,
            top_k=1,
            repeat_penalty=1.0,
        ),
        budget=ExecutionBudget(),
        execution_context=build_execution_context("parse", str(tmp_path), None),
        overwrite=False,
        database_url=None,
        db_schema="public",
    )

    assert outcome.skipped == 1
