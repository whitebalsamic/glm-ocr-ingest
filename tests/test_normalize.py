from __future__ import annotations

from glm_ocr.normalize import normalize_glm_payload


def test_normalize_glm_payload_applies_defaults() -> None:
    payload = {
        "json_result": [
            [
                {
                    "content": None,
                    "native_label": "foo",
                    "bbox_2d": [1, 2, 3, 4],
                    "polygon": [[1, 2], [3, 4]],
                    "extra": "x",
                }
            ]
        ],
        "raw": "payload",
    }

    result = normalize_glm_payload(payload)

    assert result.summary == {"page_count": 1, "region_count": 1}
    assert result.provider_extra == {"raw": "payload"}
    region = result.pages[0].regions[0]
    assert region.region_index == 0
    assert region.label == "unknown"
    assert region.content == ""
    assert region.extra_fields == {"extra": "x"}
