from __future__ import annotations

from glm_ocr.models import DocumentInput, ExecutionBudget, OcrSettings


def test_fake_provider_contract(fake_provider) -> None:
    result = fake_provider.parse_document(
        DocumentInput(
            raw_bytes=b"abc",
            display_name="doc.pdf",
            logical_source_id="doc.pdf",
            mime_type="application/pdf",
        ),
        OcrSettings(
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
        ExecutionBudget(),
    )

    assert result.provider_name == "fake"
    assert result.canonical_result.summary["page_count"] == 1
