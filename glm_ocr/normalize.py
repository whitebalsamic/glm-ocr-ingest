"""Canonical normalization helpers."""

from __future__ import annotations

from typing import Any

from .models import CanonicalDocumentResult, CanonicalPage, CanonicalRegion


def _normalize_polygon(value: Any) -> list[list[float]] | None:
    if not isinstance(value, list):
        return None
    polygon: list[list[float]] = []
    for point in value:
        if isinstance(point, list):
            polygon.append([float(coord) for coord in point])
    return polygon or None


def _normalize_bbox(value: Any) -> list[float] | None:
    if not isinstance(value, list):
        return None
    return [float(coord) for coord in value]


def normalize_glm_payload(raw_payload: dict[str, Any]) -> CanonicalDocumentResult:
    pages_input = raw_payload.get("json_result") or raw_payload.get("pages") or []
    provider_extra = {key: value for key, value in raw_payload.items() if key != "json_result"}
    pages: list[CanonicalPage] = []
    region_count = 0

    for page_index, page in enumerate(pages_input):
        if not isinstance(page, list):
            page_regions = []
            page_extra = {"raw_page": page}
        else:
            page_regions = page
            page_extra = {}

        regions: list[CanonicalRegion] = []
        for ordinal, region in enumerate(page_regions):
            region_dict = region if isinstance(region, dict) else {}
            index = region_dict.get("index")
            normalized_index = ordinal if index is None else int(index)
            label = region_dict.get("label") or "unknown"
            native_label = region_dict.get("native_label")
            content = region_dict.get("content") or ""
            extra_fields = {
                key: value
                for key, value in region_dict.items()
                if key not in {"index", "label", "native_label", "content", "bbox_2d", "polygon"}
            }
            regions.append(
                CanonicalRegion(
                    region_index=normalized_index,
                    label=str(label),
                    native_label=None if native_label is None else str(native_label),
                    content=str(content),
                    bbox_2d=_normalize_bbox(region_dict.get("bbox_2d")),
                    polygon=_normalize_polygon(region_dict.get("polygon")),
                    extra_fields=extra_fields,
                )
            )
        pages.append(CanonicalPage(page_index=page_index, regions=regions, page_extra=page_extra))
        region_count += len(regions)

    summary = {
        "page_count": len(pages),
        "region_count": region_count,
    }
    return CanonicalDocumentResult(pages=pages, summary=summary, provider_extra=provider_extra)
