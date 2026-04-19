from __future__ import annotations

from datetime import UTC, datetime

import pytest

from glm_ocr.models import (
    CanonicalDocumentResult,
    CanonicalPage,
    CanonicalRegion,
    ProviderParseResult,
)


class FakeProvider:
    def __init__(self, *, warning: str | None = None) -> None:
        self.warning = warning
        self.begin_calls = 0
        self.end_calls = 0
        self.batch_calls = 0

    def validate_environment(self, settings):  # noqa: ANN001, ARG002
        return []

    def provider_metadata(self):
        return {"provider_name": "fake", "provider_version": "1.0.0"}

    def begin_run(self, settings, budget):  # noqa: ANN001, ARG002
        self.begin_calls += 1

    def end_run(self) -> None:
        self.end_calls += 1

    def run_telemetry(self):
        return {"parser_initializations": self.begin_calls}

    def parse_document(self, document, settings, budget):  # noqa: ANN001, ARG002
        return self.parse_documents_batch([document], settings, budget)[0]

    def parse_documents_batch(self, documents, settings, budget):  # noqa: ANN001, ARG002
        self.batch_calls += 1
        results = []
        for document in documents:
            now = datetime.now(UTC)
            results.append(
                ProviderParseResult(
                    provider_name="fake",
                    provider_version="1.0.0",
                    provider_metadata={"provider_name": "fake", "provider_version": "1.0.0"},
                    raw_payload={
                        "json_result": [
                            [{"index": 0, "label": "text", "content": document.display_name}]
                        ],
                        "markdown_result": document.display_name,
                        "raw_field": True,
                    },
                    canonical_result=CanonicalDocumentResult(
                        pages=[
                            CanonicalPage(
                                page_index=0,
                                regions=[
                                    CanonicalRegion(
                                        region_index=0,
                                        label="text",
                                        native_label="text",
                                        content=document.display_name,
                                        bbox_2d=None,
                                        polygon=None,
                                        extra_fields={},
                                    )
                                ],
                            )
                        ],
                        summary={"page_count": 1, "region_count": 1},
                        provider_extra={"raw_field": True},
                    ),
                    warnings=[] if self.warning is None else [self.warning],
                    started_at=now,
                    finished_at=now,
                )
            )
        return results


@pytest.fixture
def fake_provider():
    return FakeProvider()
