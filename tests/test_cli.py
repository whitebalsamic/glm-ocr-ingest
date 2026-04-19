from __future__ import annotations

import json
from pathlib import Path

from glm_ocr import cli


def test_legacy_cli_invocation_still_parses(tmp_path: Path, monkeypatch) -> None:
    input_file = tmp_path / "invoice.pdf"
    input_file.write_bytes(b"pdf")
    output_dir = tmp_path / "out"

    class Provider:
        def validate_environment(self, settings):  # noqa: ANN001, ARG002
            return []

        def provider_metadata(self):
            return {"provider_name": "fake", "provider_version": "1.0.0"}

        def parse_document(self, document, settings, budget):  # noqa: ANN001, ARG002
            from datetime import UTC, datetime

            from glm_ocr.models import (
                CanonicalDocumentResult,
                CanonicalPage,
                CanonicalRegion,
                ProviderParseResult,
            )

            now = datetime.now(UTC)
            return ProviderParseResult(
                provider_name="fake",
                provider_version="1.0.0",
                provider_metadata={"provider_name": "fake", "provider_version": "1.0.0"},
                raw_payload={"json_result": [[{"index": 0, "label": "text", "content": "legacy"}]]},
                canonical_result=CanonicalDocumentResult(
                    pages=[
                        CanonicalPage(
                            page_index=0,
                            regions=[
                                CanonicalRegion(
                                    region_index=0,
                                    label="text",
                                    native_label="text",
                                    content="legacy",
                                    bbox_2d=None,
                                    polygon=None,
                                    extra_fields={},
                                )
                            ],
                        )
                    ],
                    summary={"page_count": 1, "region_count": 1},
                ),
                warnings=[],
                started_at=now,
                finished_at=now,
            )

    monkeypatch.setattr(cli, "_provider_from_args", lambda _args: Provider())

    exit_code = cli.main([str(input_file), "-o", str(output_dir), "--provider", "glm"])

    assert exit_code == 0
    assert json.loads((output_dir / "invoice.json").read_text())[0][0]["content"] == "legacy"


def test_compare_cli_writes_default_report(tmp_path: Path) -> None:
    dataset_dir = tmp_path / "dataset"
    dataset_dir.mkdir()
    artifact_dir = tmp_path / "artifacts"
    records_dir = artifact_dir / "_records" / "run-1"
    records_dir.mkdir(parents=True)

    (dataset_dir / "invoice.json").write_text(
        json.dumps(
            {
                "schemaVersion": "1",
                "document": {
                    "invoiceNumber": {"status": "present", "value": "INV-001", "raw": "INV-001"},
                    "invoiceDate": {"status": "present", "value": "1999-03-25", "raw": "03/25/99"},
                    "sellerName": {
                        "status": "present",
                        "value": "Seller Example Ltd.",
                        "raw": "Seller Example Ltd.",
                    },
                    "customerName": {
                        "status": "present",
                        "value": "Customer Example LLC",
                        "raw": "Customer Example LLC",
                    },
                    "currency": {"status": "present", "value": "$", "raw": "$"},
                    "country": {"status": "absent"},
                },
                "summary": {
                    "subtotal": {"status": "absent"},
                    "tax": {"status": "absent"},
                    "discount": {"status": "absent"},
                    "shipping": {"status": "absent"},
                    "totalAmount": {"status": "present", "value": 100.0, "raw": "$100.00"},
                },
                "lineItems": [],
                "notes": [],
                "sourceImage": "invoice.pdf",
            }
        ),
        encoding="utf-8",
    )
    (records_dir / "invoice.record.json").write_text(
        json.dumps(
            {
                "run_id": "run-1",
                "result_id": "result-1",
                "raw_provider_payload": {
                    "markdown_result": "\n".join(
                        [
                            "Seller Example Ltd.",
                            "Customer Example LLC",
                            "INVOICE NUMBER: INV-001",
                            "INVOICE DATE: 03/25/99",
                            "Total Amount: $100.00",
                        ]
                    )
                },
                "canonical_result": {"pages": []},
            }
        ),
        encoding="utf-8",
    )

    exit_code = cli.main(["compare", str(dataset_dir), str(artifact_dir)])

    assert exit_code == 0
    assert (artifact_dir / "_evaluation" / "comparison_report.json").exists()
