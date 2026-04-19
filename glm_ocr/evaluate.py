"""Artifact comparison helpers for dataset-backed invoice evaluation."""

from __future__ import annotations

import json
import re
from collections import Counter
from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal, InvalidOperation
from html.parser import HTMLParser
from pathlib import Path
from typing import Any

HEADER_FIELDS = (
    "invoiceNumber",
    "invoiceDate",
    "sellerName",
    "customerName",
    "currency",
    "country",
)
SUMMARY_FIELDS = ("subtotal", "tax", "discount", "shipping", "totalAmount")
LINE_ITEM_FIELDS = (
    "description",
    "quantity",
    "unitPrice",
    "amount",
    "tax",
    "taxRate",
    "sku",
    "itemCode",
)
NUMERIC_FIELDS = {
    "subtotal",
    "tax",
    "discount",
    "shipping",
    "totalAmount",
    "quantity",
    "unitPrice",
    "amount",
}
DATE_FIELDS = {"invoiceDate"}
CURRENCY_FIELDS = {"currency"}
TEXT_FIELDS = {
    "invoiceNumber",
    "sellerName",
    "customerName",
    "country",
    "description",
    "sku",
    "itemCode",
}
COUNTRY_CODES = {
    "usa",
    "united states",
    "uk",
    "united kingdom",
    "germany",
    "france",
    "italy",
    "spain",
    "canada",
    "australia",
}
SUMMARY_LABELS = {
    "subtotal": ("subtotal", "sub total"),
    "tax": ("tax", "vat"),
    "discount": ("discount",),
    "shipping": ("shipping", "freight", "delivery"),
    "totalAmount": ("total amount", "amount due", "total due", "invoice total", "total"),
}
LINE_ITEM_HEADERS = {
    "description": ("description", "product/description", "product description", "details"),
    "quantity": ("quantity", "qty"),
    "unitPrice": ("unit price", "price", "unit cost", "rate"),
    "amount": ("ext. price", "extended price", "amount", "line total", "total"),
    "tax": ("tax",),
    "taxRate": ("tax rate", "vat %", "tax %", "rate %"),
    "sku": ("sku",),
    "itemCode": ("item code", "code", "item no", "item #", "product code"),
}
LABEL_SYNONYMS = {
    "invoiceNumber": ("invoice number", "invoice no", "invoice #", "inv no", "inv #"),
    "invoiceDate": ("invoice date", "date"),
    "customerName": ("bill to", "sold to", "customer", "customer name"),
}
TABLE_FIELD_LABELS = {
    "invoiceNumber": ("invoice number", "invoice #", "invoice no", "document number", "number"),
    "invoiceDate": ("invoice date", "document date", "date"),
    "sellerName": ("seller", "vendor", "property", "send payment to", "remit to", "from"),
    "customerName": ("bill to", "sold to", "customer", "account", "advertiser", "to"),
    "subtotal": ("subtotal", "sub total"),
    "tax": ("tax", "vat"),
    "discount": ("discount",),
    "shipping": ("shipping", "freight", "delivery"),
    "totalAmount": ("invoice total", "total amount", "amount due", "total due", "total"),
}


@dataclass(slots=True)
class ComparisonPaths:
    report_path: Path
    summary_path: Path


@dataclass(slots=True)
class TableData:
    headers: list[str]
    rows: list[list[str]]


class _TableHtmlParser(HTMLParser):
    def __init__(self) -> None:
        super().__init__()
        self.tables: list[TableData] = []
        self._inside_table = False
        self._current_headers: list[str] = []
        self._current_rows: list[list[str]] = []
        self._current_row: list[str] = []
        self._cell_chunks: list[str] = []
        self._cell_tag: str | None = None

    def handle_starttag(self, tag: str, _attrs: list[tuple[str, str | None]]) -> None:
        if tag == "table":
            self._inside_table = True
            self._current_headers = []
            self._current_rows = []
            return
        if not self._inside_table:
            return
        if tag == "tr":
            self._current_row = []
        elif tag in {"th", "td"}:
            self._cell_tag = tag
            self._cell_chunks = []
        elif tag == "br" and self._cell_tag is not None:
            self._cell_chunks.append("\n")

    def handle_endtag(self, tag: str) -> None:
        if tag == "table" and self._inside_table:
            self.tables.append(TableData(headers=self._current_headers, rows=self._current_rows))
            self._inside_table = False
            return
        if not self._inside_table:
            return
        if tag in {"th", "td"} and self._cell_tag == tag:
            cell = _collapse_ws("".join(self._cell_chunks))
            self._current_row.append(cell)
            self._cell_tag = None
            self._cell_chunks = []
        elif tag == "tr" and self._current_row:
            if self._current_headers:
                self._current_rows.append(self._current_row)
            elif self._current_row:
                self._current_headers = self._current_row
            self._current_row = []

    def handle_data(self, data: str) -> None:
        if self._cell_tag is not None:
            self._cell_chunks.append(data)


def compare_artifacts_to_ground_truth(
    *,
    dataset_dir: Path,
    artifact_dir: Path,
    report_path: Path | None = None,
    stems: set[str] | None = None,
) -> dict[str, Any]:
    ground_truth = _load_ground_truth(dataset_dir)
    if stems is not None:
        ground_truth = {stem: payload for stem, payload in ground_truth.items() if stem in stems}
    records = _load_record_manifests(artifact_dir)
    run_summary = _load_latest_run_summary(artifact_dir)
    output_paths = _resolve_output_paths(artifact_dir, report_path)

    document_reports: dict[str, Any] = {}
    mismatch_counter: Counter[str] = Counter()
    exact_match_documents = 0
    missing_artifacts = 0

    for stem, expected in sorted(ground_truth.items()):
        manifest = records.get(stem)
        if manifest is None:
            missing_artifacts += 1
            document_reports[stem] = {
                "stem": stem,
                "status": "missing_artifact",
                "matched": False,
                "mismatch_count": 1,
                "mismatches": [
                    {
                        "path": "artifact",
                        "reason": "missing_record_manifest",
                    }
                ],
            }
            mismatch_counter["artifact.missing"] += 1
            continue

        actual = extract_invoice_view(manifest)
        document_report = compare_invoice_views(expected=expected, actual=actual, stem=stem)
        document_reports[stem] = document_report
        if document_report["matched"]:
            exact_match_documents += 1
        for mismatch in document_report["mismatches"]:
            mismatch_counter[mismatch["path"]] += 1

    summary = {
        "dataset_dir": str(dataset_dir),
        "artifact_dir": str(artifact_dir),
        "total_ground_truth_documents": len(ground_truth),
        "artifact_record_documents": len(records),
        "documents_compared": len(ground_truth),
        "documents_with_exact_match": exact_match_documents,
        "documents_with_mismatches": len(ground_truth) - exact_match_documents,
        "missing_artifact_documents": missing_artifacts,
        "top_mismatch_categories": [
            {"path": path, "count": count} for path, count in mismatch_counter.most_common(15)
        ],
        "run_summary": run_summary,
        "report_path": str(output_paths.report_path),
        "summary_path": str(output_paths.summary_path),
    }
    report = {
        "summary": summary,
        "documents": document_reports,
    }

    output_paths.report_path.parent.mkdir(parents=True, exist_ok=True)
    output_paths.report_path.write_text(
        json.dumps(report, indent=2, sort_keys=True, ensure_ascii=True) + "\n",
        encoding="utf-8",
    )
    output_paths.summary_path.parent.mkdir(parents=True, exist_ok=True)
    output_paths.summary_path.write_text(render_summary_markdown(summary), encoding="utf-8")
    return report


def compare_invoice_views(
    *,
    expected: dict[str, Any],
    actual: dict[str, Any],
    stem: str,
) -> dict[str, Any]:
    mismatches: list[dict[str, Any]] = []
    for field in HEADER_FIELDS:
        _compare_field(
            mismatches=mismatches,
            path=f"document.{field}",
            field_name=field,
            expected_field=expected["document"].get(field, {"status": "absent"}),
            actual_field=actual["document"].get(field, {"status": "absent"}),
            searchable_text=actual["_search_text"],
        )
    for field in SUMMARY_FIELDS:
        _compare_field(
            mismatches=mismatches,
            path=f"summary.{field}",
            field_name=field,
            expected_field=expected["summary"].get(field, {"status": "absent"}),
            actual_field=actual["summary"].get(field, {"status": "absent"}),
            searchable_text=actual["_search_text"],
        )

    expected_items = expected.get("lineItems", [])
    actual_items = actual.get("lineItems", [])
    if len(expected_items) != len(actual_items):
        mismatches.append(
            {
                "path": "lineItems.count",
                "reason": "count_mismatch",
                "expected_count": len(expected_items),
                "actual_count": len(actual_items),
            }
        )
    overlap_count = min(len(expected_items), len(actual_items))
    for index, expected_item in enumerate(expected_items[:overlap_count], start=1):
        if index - 1 < len(actual_items):
            actual_item = actual_items[index - 1]
        else:
            actual_item = _absent_line_item(index)
        row_text = actual_item.get("_row_text", "")
        for field in LINE_ITEM_FIELDS:
            _compare_field(
                mismatches=mismatches,
                path=f"lineItems[{index}].{field}",
                field_name=field,
                expected_field=expected_item.get(field, {"status": "absent"}),
                actual_field=actual_item.get(field, {"status": "absent"}),
                searchable_text=row_text,
            )

    return {
        "stem": stem,
        "status": "matched" if not mismatches else "mismatched",
        "matched": not mismatches,
        "mismatch_count": len(mismatches),
        "mismatches": mismatches,
    }


def extract_invoice_view(manifest: dict[str, Any]) -> dict[str, Any]:
    raw_payload = manifest.get("raw_provider_payload", {})
    markdown = raw_payload.get("markdown_result")
    if isinstance(markdown, list):
        markdown_text = "\n\n".join(str(item) for item in markdown)
    elif isinstance(markdown, str):
        markdown_text = markdown
    else:
        markdown_text = _fallback_text_from_manifest(manifest)
    lines = _extract_text_lines(markdown_text)
    region_lines = _extract_region_lines(manifest)
    merged_lines = _unique_lines(lines + region_lines)
    tables = _extract_tables(markdown_text)
    table_pairs = _extract_table_pairs(tables)
    line_pairs = _extract_line_pairs(merged_lines)
    searchable_text = _searchable_text("\n".join(merged_lines))

    line_items = _extract_line_items(tables)
    summary = _extract_summary_fields(
        merged_lines,
        searchable_text,
        line_items,
        table_pairs=table_pairs,
        line_pairs=line_pairs,
    )
    document = {
        "invoiceNumber": _extract_labeled_field(
            merged_lines,
            "invoiceNumber",
            table_pairs=table_pairs,
            line_pairs=line_pairs,
        ),
        "invoiceDate": _extract_labeled_field(
            merged_lines,
            "invoiceDate",
            table_pairs=table_pairs,
            line_pairs=line_pairs,
        ),
        "sellerName": _extract_seller_name(merged_lines, table_pairs=table_pairs),
        "customerName": _extract_customer_name(
            merged_lines,
            table_pairs=table_pairs,
            line_pairs=line_pairs,
        ),
        "currency": _extract_currency(
            searchable_text,
            summary=summary,
            line_items=line_items,
        ),
        "country": _extract_country(merged_lines),
    }
    return {
        "document": document,
        "summary": summary,
        "lineItems": line_items,
        "_search_text": searchable_text,
        "_provenance": {"table_pairs": table_pairs, "line_pairs": line_pairs},
    }


def render_summary_markdown(summary: dict[str, Any]) -> str:
    top_categories = summary.get("top_mismatch_categories", [])
    lines = [
        "# Comparison Summary",
        "",
        f"- Dataset documents: {summary['total_ground_truth_documents']}",
        f"- Artifact record documents: {summary['artifact_record_documents']}",
        f"- Documents compared: {summary['documents_compared']}",
        f"- Exact matches: {summary['documents_with_exact_match']}",
        f"- Documents with mismatches: {summary['documents_with_mismatches']}",
        f"- Missing artifacts: {summary['missing_artifact_documents']}",
    ]
    run_summary = summary.get("run_summary")
    if run_summary:
        lines.extend(
            [
                "",
                "## Parse Run",
                "",
                f"- Run ID: {run_summary['run_id']}",
                f"- Status: {run_summary['status']}",
                f"- Processed: {run_summary['counts'].get('processed', 0)}",
                f"- Failed: {run_summary['counts'].get('failed', 0)}",
                f"- Skipped: {run_summary['counts'].get('skipped', 0)}",
            ]
        )
    if top_categories:
        lines.extend(["", "## Top Mismatches", ""])
        for item in top_categories:
            lines.append(f"- `{item['path']}`: {item['count']}")
    lines.extend(
        [
            "",
            f"Report: `{summary['report_path']}`",
            f"Summary: `{summary['summary_path']}`",
            "",
        ]
    )
    return "\n".join(lines)


def _resolve_output_paths(artifact_dir: Path, report_path: Path | None) -> ComparisonPaths:
    evaluation_dir = artifact_dir / "_evaluation"
    final_report_path = report_path or evaluation_dir / "comparison_report.json"
    summary_path = evaluation_dir / "comparison_summary.md"
    return ComparisonPaths(report_path=final_report_path, summary_path=summary_path)


def _load_ground_truth(dataset_dir: Path) -> dict[str, dict[str, Any]]:
    result: dict[str, dict[str, Any]] = {}
    for path in sorted(dataset_dir.glob("*.json")):
        payload = json.loads(path.read_text(encoding="utf-8"))
        result[path.stem] = payload
    return result


def _load_record_manifests(artifact_dir: Path) -> dict[str, dict[str, Any]]:
    result: dict[str, dict[str, Any]] = {}
    for path in sorted((artifact_dir / "_records").glob("**/*.record.json")):
        payload = json.loads(path.read_text(encoding="utf-8"))
        stem = Path(path.name).stem.removesuffix(".record")
        result[stem] = payload
    return result


def _load_latest_run_summary(artifact_dir: Path) -> dict[str, Any] | None:
    run_files = sorted((artifact_dir / "_runs").glob("*.json"))
    if not run_files:
        return None
    payload = json.loads(run_files[-1].read_text(encoding="utf-8"))
    return {
        "run_id": payload["run_id"],
        "status": payload["status"],
        "counts": payload.get("counts", {}),
    }


def _fallback_text_from_manifest(manifest: dict[str, Any]) -> str:
    pages = manifest.get("canonical_result", {}).get("pages", [])
    chunks: list[str] = []
    for page in pages:
        for region in page.get("regions", []):
            content = region.get("content")
            if isinstance(content, str) and content.strip():
                chunks.append(content)
    return "\n\n".join(chunks)


def _extract_text_lines(markdown_text: str) -> list[str]:
    text = re.sub(r"!\[[^\]]*]\([^)]+\)", " ", markdown_text)
    text = re.sub(r"</?(?:p|div|thead|tbody|tr|table|th|td|h\d)[^>]*>", "\n", text, flags=re.I)
    text = re.sub(r"<br\s*/?>", "\n", text, flags=re.I)
    text = re.sub(r"<[^>]+>", " ", text)
    lines = [_collapse_ws(line) for line in text.splitlines()]
    return [line for line in lines if line]


def _extract_tables(markdown_text: str) -> list[TableData]:
    parser = _TableHtmlParser()
    parser.feed(markdown_text)
    return parser.tables


def _extract_region_lines(manifest: dict[str, Any]) -> list[str]:
    pages = manifest.get("canonical_result", {}).get("pages", [])
    lines: list[str] = []
    for page in pages:
        for region in page.get("regions", []):
            content = region.get("content")
            if isinstance(content, str) and content.strip():
                lines.extend(_extract_text_lines(content))
    return lines


def _extract_table_pairs(tables: list[TableData]) -> dict[str, list[str]]:
    pairs: dict[str, list[str]] = {}
    for table in tables:
        for row in [table.headers, *table.rows]:
            cells = [cell for cell in row if cell]
            if len(cells) < 2:
                continue
            if len(cells) == 2:
                _append_pair(pairs, cells[0], cells[1])
                continue
            if len(cells) % 2 == 0:
                for index in range(0, len(cells), 2):
                    _append_pair(pairs, cells[index], cells[index + 1])
    return pairs


def _extract_line_pairs(lines: list[str]) -> dict[str, list[str]]:
    pairs: dict[str, list[str]] = {}
    for index, line in enumerate(lines):
        lowered = line.casefold()
        for labels in TABLE_FIELD_LABELS.values():
            matched_label = next((label for label in labels if label in lowered), None)
            if matched_label is None:
                continue
            value = _value_after_label(line, labels)
            if value:
                _append_pair(pairs, matched_label, value)
                continue
            block = _block_after_label(lines, index=index, labels=labels, max_lines=1)
            if block:
                _append_pair(pairs, matched_label, block)
    return pairs


def _extract_labeled_field(
    lines: list[str],
    field_name: str,
    *,
    table_pairs: dict[str, list[str]] | None = None,
    line_pairs: dict[str, list[str]] | None = None,
) -> dict[str, Any]:
    for pairs in (table_pairs, line_pairs):
        candidate = _field_from_pairs(field_name, pairs)
        if candidate["status"] == "present":
            return candidate
    labels = LABEL_SYNONYMS[field_name]
    for index, line in enumerate(lines):
        lowered = line.casefold()
        if any(label in lowered for label in labels):
            max_lines = 1 if field_name in {"invoiceNumber", "invoiceDate"} else 2
            block = _block_after_label(lines, index=index, labels=labels, max_lines=max_lines)
            if block:
                return _present_field(field_name, block)
    return _absent_field()


def _extract_seller_name(
    lines: list[str],
    *,
    table_pairs: dict[str, list[str]] | None = None,
) -> dict[str, Any]:
    candidate = _field_from_pairs("sellerName", table_pairs)
    if candidate["status"] == "present":
        return candidate
    for line in lines:
        lowered = line.casefold()
        if any(
            token in lowered
            for token in (
                "invoice",
                "bill to",
                "sold to",
                "ship to",
                "customer",
                "advertiser",
                "account",
                "send payment to",
                "remit to",
            )
        ):
            break
        if any(char.isdigit() for char in line):
            continue
        if len(line.split()) < 2:
            continue
        return _present_field("sellerName", line)
    return _absent_field()


def _extract_customer_name(
    lines: list[str],
    *,
    table_pairs: dict[str, list[str]] | None = None,
    line_pairs: dict[str, list[str]] | None = None,
) -> dict[str, Any]:
    labeled = _extract_labeled_field(
        lines,
        "customerName",
        table_pairs=table_pairs,
        line_pairs=line_pairs,
    )
    if labeled["status"] == "present":
        return labeled
    invoice_index = next(
        (idx for idx, line in enumerate(lines) if "invoice number" in line.casefold()),
        len(lines),
    )
    seller = _extract_seller_name(lines)
    for line in lines[:invoice_index]:
        lowered = line.casefold()
        if seller.get("raw") == line:
            continue
        if any(
            token in lowered
            for token in ("wire instructions", "remit to", "account number", "phone", "fax")
        ):
            continue
        if any(char.isdigit() for char in line):
            continue
        if len(line.split()) < 2:
            continue
        if line.isupper() or sum(ch.isupper() for ch in line) >= max(4, len(line) // 3):
            return _present_field("customerName", line.split("ATTN:")[0].strip())
    return _absent_field()


def _extract_currency(
    searchable_text: str,
    *,
    summary: dict[str, dict[str, Any]],
    line_items: list[dict[str, Any]],
) -> dict[str, Any]:
    for symbol in ("$", "EUR", "€", "GBP", "£"):
        if symbol.casefold() in searchable_text:
            return _present_field("currency", symbol)
    for field_name in ("subtotal", "tax", "shipping", "totalAmount"):
        raw = summary.get(field_name, {}).get("raw")
        if isinstance(raw, str):
            for symbol in ("$", "€", "£"):
                if symbol in raw:
                    return _present_field("currency", symbol)
    for item in line_items:
        for field_name in ("unitPrice", "amount", "tax"):
            raw = item.get(field_name, {}).get("raw")
            if isinstance(raw, str):
                for symbol in ("$", "€", "£"):
                    if symbol in raw:
                        return _present_field("currency", symbol)
    return _absent_field()


def _extract_country(lines: list[str]) -> dict[str, Any]:
    for line in lines:
        lowered = line.casefold()
        if re.search(r"\b(country|nation)\b", lowered):
            return _present_field("country", line)
    return _absent_field()


def _extract_summary_fields(
    lines: list[str],
    searchable_text: str,
    line_items: list[dict[str, Any]],
    *,
    table_pairs: dict[str, list[str]] | None = None,
    line_pairs: dict[str, list[str]] | None = None,
) -> dict[str, dict[str, Any]]:
    result: dict[str, dict[str, Any]] = {}
    for field_name, labels in SUMMARY_LABELS.items():
        result[field_name] = _extract_summary_field(
            lines,
            field_name,
            labels,
            table_pairs=table_pairs,
            line_pairs=line_pairs,
        )
    if result["totalAmount"]["status"] == "absent":
        amounts: list[Decimal] = []
        for item in line_items:
            amount_field = item.get("amount", {})
            if amount_field.get("status") != "present":
                continue
            try:
                amounts.append(Decimal(str(amount_field["value"])))
            except (InvalidOperation, KeyError):
                continue
        if amounts:
            result["totalAmount"] = _present_field("totalAmount", str(sum(amounts)))
    if result["tax"]["status"] == "absent" and " tax " not in searchable_text:
        result["tax"] = _absent_field()
    return result


def _extract_summary_field(
    lines: list[str],
    field_name: str,
    labels: tuple[str, ...],
    *,
    table_pairs: dict[str, list[str]] | None = None,
    line_pairs: dict[str, list[str]] | None = None,
) -> dict[str, Any]:
    for pairs in (table_pairs, line_pairs):
        candidate = _field_from_pairs(field_name, pairs)
        if candidate["status"] == "present":
            return candidate
    for index, line in enumerate(lines):
        lowered = line.casefold()
        if not any(label in lowered for label in labels):
            continue
        candidate = _value_after_label(line, labels)
        if candidate:
            return _present_field(field_name, candidate)
        if index + 1 < len(lines) and not _looks_like_label_only(lines[index + 1]):
            return _present_field(field_name, lines[index + 1])
    return _absent_field()


def _extract_line_items(tables: list[TableData]) -> list[dict[str, Any]]:
    if not tables:
        return []
    candidate_rows: list[dict[str, Any]] = []
    seen_row_texts: set[str] = set()
    for table in tables:
        headers, rows = _normalize_table_headers(table)
        if not headers:
            continue
        mapping = _column_mapping(headers)
        score = len(mapping)
        if score < 3 or "amount" not in mapping:
            continue
        for item in _rows_to_line_items(rows, mapping):
            row_text = item.get("_row_text", "")
            if not row_text or row_text in seen_row_texts:
                continue
            seen_row_texts.add(row_text)
            candidate_rows.append(item)
    return candidate_rows


def _normalize_table_headers(table: TableData) -> tuple[list[str], list[list[str]]]:
    if _recognized_header_count(table.headers) >= 2:
        return table.headers, table.rows
    for index, row in enumerate(table.rows[:4]):
        if _recognized_header_count(row) >= 2:
            return row, table.rows[index + 1 :]
    return table.headers, table.rows


def _recognized_header_count(cells: list[str]) -> int:
    lowered = [_collapse_ws(cell).casefold() for cell in cells]
    return sum(
        any(alias in cell for alias in aliases)
        for cell in lowered
        for aliases in LINE_ITEM_HEADERS.values()
    )


def _column_mapping(headers: list[str]) -> dict[str, int]:
    mapping: dict[str, int] = {}
    for index, header in enumerate(headers):
        lowered = _collapse_ws(header).casefold()
        for field_name, aliases in LINE_ITEM_HEADERS.items():
            if field_name in mapping:
                continue
            if any(alias in lowered for alias in aliases):
                mapping[field_name] = index
    return mapping


def _rows_to_line_items(rows: list[list[str]], mapping: dict[str, int]) -> list[dict[str, Any]]:
    items: list[dict[str, Any]] = []
    for row in rows:
        row_text = " ".join(cell for cell in row if cell).strip()
        if not row_text:
            continue
        if _recognized_header_count(row) >= 2:
            continue
        item = _absent_line_item(len(items) + 1)
        item["_row_text"] = _searchable_text(row_text)
        meaningful = False
        for field_name, column_index in mapping.items():
            if column_index >= len(row):
                continue
            cell = row[column_index]
            if not cell:
                continue
            item[field_name] = _present_field(field_name, cell)
            meaningful = True
        description_field = item.get("description", {"status": "absent"})
        if description_field["status"] == "present":
            inferred_code = re.match(r"([A-Z0-9]+(?:-[A-Z0-9]+)+)\b", description_field["raw"])
            if inferred_code:
                code = inferred_code.group(1)
                if item["sku"]["status"] == "absent":
                    item["sku"] = _present_field("sku", code)
                if item["itemCode"]["status"] == "absent":
                    item["itemCode"] = _present_field("itemCode", code)
        if meaningful:
            items.append(item)
    return items


def _field_from_pairs(
    field_name: str,
    pairs: dict[str, list[str]] | None,
) -> dict[str, Any]:
    if not pairs:
        return _absent_field()
    labels = TABLE_FIELD_LABELS.get(field_name, ())
    for label in sorted(labels, key=len, reverse=True):
        normalized_label = _normalize_label(label)
        values = pairs.get(normalized_label, [])
        for value in values:
            if value:
                return _present_field(field_name, value)
    return _absent_field()


def _append_pair(pairs: dict[str, list[str]], key: str, value: str) -> None:
    normalized = _normalize_label(key)
    cleaned_value = _collapse_ws(value)
    if not normalized or not cleaned_value:
        return
    pairs.setdefault(normalized, []).append(cleaned_value)


def _normalize_label(value: str) -> str:
    lowered = _collapse_ws(value).casefold()
    lowered = re.sub(r"[^a-z0-9]+", " ", lowered)
    return _collapse_ws(lowered)


def _block_after_label(
    lines: list[str],
    *,
    index: int,
    labels: tuple[str, ...],
    max_lines: int,
) -> str | None:
    line = lines[index]
    candidate = _value_after_label(line, labels)
    if candidate:
        return candidate
    block_lines: list[str] = []
    for next_line in lines[index + 1 : index + 4]:
        if _looks_like_label_only(next_line):
            continue
        if _is_address_like(next_line):
            if block_lines:
                break
            continue
        block_lines.append(next_line)
        if len(block_lines) >= max_lines:
            break
    if not block_lines:
        return None
    return _collapse_ws(" ".join(block_lines))


def _is_address_like(value: str) -> bool:
    return any(char.isdigit() for char in value) and any(
        token in value.casefold()
        for token in ("st", "road", "ave", "suite", "po box", "floor", "ct")
    )


def _unique_lines(lines: list[str]) -> list[str]:
    seen: set[str] = set()
    result: list[str] = []
    for line in lines:
        cleaned = _collapse_ws(line)
        if not cleaned or cleaned in seen:
            continue
        seen.add(cleaned)
        result.append(cleaned)
    return result


def _compare_field(
    *,
    mismatches: list[dict[str, Any]],
    path: str,
    field_name: str,
    expected_field: dict[str, Any],
    actual_field: dict[str, Any],
    searchable_text: str,
) -> None:
    expected_status = expected_field.get("status", "absent")
    actual_status = actual_field.get("status", "absent")
    if expected_status == "absent":
        if actual_status == "present":
            mismatches.append(
                {
                    "path": path,
                    "reason": "unexpected_value",
                    "expected_status": "absent",
                    "actual_status": "present",
                    "actual_value": actual_field.get("value"),
                    "actual_raw": actual_field.get("raw"),
                }
            )
        return

    expected_value = _normalize_value(field_name, expected_field.get("value"))
    actual_value = _normalize_value(field_name, actual_field.get("value"))
    if actual_status == "present" and actual_value == expected_value:
        return
    if _expected_value_present_in_context(field_name, expected_field, searchable_text):
        return
    mismatches.append(
        {
            "path": path,
            "reason": "value_mismatch" if actual_status == "present" else "missing_value",
            "expected_status": expected_status,
            "expected_value": expected_value,
            "expected_raw": expected_field.get("raw"),
            "actual_status": actual_status,
            "actual_value": actual_value,
            "actual_raw": actual_field.get("raw"),
        }
    )


def _expected_value_present_in_context(
    field_name: str,
    expected_field: dict[str, Any],
    searchable_text: str,
) -> bool:
    raw_candidates = [expected_field.get("raw"), expected_field.get("value")]
    for candidate in raw_candidates:
        if candidate in (None, ""):
            continue
        if field_name in DATE_FIELDS:
            variants = {str(candidate), _normalize_value(field_name, candidate)}
        elif field_name in NUMERIC_FIELDS:
            variants = {str(candidate), _normalize_numeric_string(str(candidate))}
        else:
            variants = {str(candidate)}
        for variant in variants:
            if not variant:
                continue
            if _searchable_text(str(variant)) in searchable_text:
                return True
    return False


def _value_after_label(line: str, labels: tuple[str, ...]) -> str | None:
    lowered = line.casefold()
    for label in sorted(labels, key=len, reverse=True):
        if label not in lowered:
            continue
        escaped = re.escape(label)
        empty_pattern = re.compile(rf"^\s*(?<!\w){escaped}(?!\w)\s*[:#-]?\s*$", flags=re.I)
        if empty_pattern.search(line):
            continue
        pattern = re.compile(
            rf"(?<!\w){escaped}(?!\w)(?:\s*[:#-]\s*|\s+)(.+)$",
            flags=re.I,
        )
        match = pattern.search(line)
        if not match:
            continue
        candidate = _collapse_ws(match.group(1))
        if candidate and not _looks_like_label_only(candidate):
            return candidate
    return None


def _looks_like_label_only(value: str) -> bool:
    lowered = value.casefold()
    return any(
        label in lowered for label_group in LABEL_SYNONYMS.values() for label in label_group
    ) or any(alias in lowered for aliases in SUMMARY_LABELS.values() for alias in aliases)


def _present_field(field_name: str, raw_value: Any) -> dict[str, Any]:
    raw = _collapse_ws(str(raw_value))
    normalized = _normalize_value(field_name, raw)
    if normalized in (None, ""):
        return _absent_field()
    return {"status": "present", "value": normalized, "raw": raw}


def _absent_field() -> dict[str, Any]:
    return {"status": "absent"}


def _absent_line_item(index: int) -> dict[str, Any]:
    item = {"index": index, "_row_text": ""}
    for field_name in LINE_ITEM_FIELDS:
        item[field_name] = _absent_field()
    return item


def _normalize_value(field_name: str, value: Any) -> Any:
    if value is None:
        return None
    if field_name in DATE_FIELDS:
        return _normalize_date_value(value)
    if field_name in NUMERIC_FIELDS:
        return _normalize_numeric_value(value)
    if field_name in CURRENCY_FIELDS:
        return _normalize_currency_value(value)
    if field_name in TEXT_FIELDS:
        return _collapse_ws(str(value))
    return value


def _normalize_date_value(value: Any) -> str:
    raw = _collapse_ws(str(value))
    for fmt in ("%Y-%m-%d", "%m/%d/%Y", "%m/%d/%y", "%d/%m/%Y", "%d.%m.%Y", "%Y/%m/%d"):
        try:
            return datetime.strptime(raw, fmt).date().isoformat()
        except ValueError:
            continue
    return raw


def _normalize_numeric_value(value: Any) -> str:
    if isinstance(value, int | float):
        decimal = Decimal(str(value))
        return _decimal_to_string(decimal)
    raw = _normalize_numeric_string(str(value))
    if not raw:
        return ""
    try:
        return _decimal_to_string(Decimal(raw))
    except InvalidOperation:
        return raw


def _normalize_numeric_string(value: str) -> str:
    raw = _collapse_ws(value)
    raw = raw.replace(",", "")
    raw = re.sub(r"[^0-9.\-]", "", raw)
    if raw.count(".") > 1:
        first, *rest = raw.split(".")
        raw = first + "." + "".join(rest)
    return raw


def _normalize_currency_value(value: Any) -> str:
    raw = _collapse_ws(str(value))
    if "$" in raw:
        return "$"
    if "EUR" in raw.upper() or "€" in raw:
        return "EUR" if "EUR" in raw.upper() else "€"
    if "GBP" in raw.upper() or "£" in raw:
        return "GBP" if "GBP" in raw.upper() else "£"
    return raw


def _decimal_to_string(value: Decimal) -> str:
    normalized = value.normalize()
    if normalized == normalized.to_integral():
        return str(normalized.quantize(Decimal("1")))
    return format(normalized, "f").rstrip("0").rstrip(".")


def _searchable_text(text: str) -> str:
    return _collapse_ws(text).casefold()


def _collapse_ws(value: str) -> str:
    return re.sub(r"\s+", " ", value).strip()
