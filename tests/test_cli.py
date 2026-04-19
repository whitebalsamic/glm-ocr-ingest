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
