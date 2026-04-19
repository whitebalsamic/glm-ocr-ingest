from __future__ import annotations

import json
from pathlib import Path

from glm_ocr.evaluate import compare_artifacts_to_ground_truth, extract_invoice_view


def test_extract_invoice_view_from_markdown_tables() -> None:
    manifest = _sample_manifest(
        """
Seller Example Ltd.
Customer Example LLC
INVOICE NUMBER:
INV-001
INVOICE DATE:
03/25/99
Total Amount: $21,000.00
<table>
  <tbody>
    <tr><td>QUANTITY</td><td>PRODUCT/DESCRIPTION</td><td>UNIT PRICE</td><td>EXT. PRICE</td></tr>
    <tr><td>1</td><td>18-999 ADVANCE BILLING</td><td>21,000.00</td><td>21,000.00</td></tr>
  </tbody>
</table>
"""
    )

    invoice_view = extract_invoice_view(manifest)

    assert invoice_view["document"]["invoiceNumber"]["value"] == "INV-001"
    assert invoice_view["document"]["invoiceDate"]["value"] == "1999-03-25"
    assert invoice_view["summary"]["totalAmount"]["value"] == "21000"
    assert len(invoice_view["lineItems"]) == 1
    assert invoice_view["lineItems"][0]["quantity"]["value"] == "1"
    assert invoice_view["lineItems"][0]["sku"]["value"] == "18-999"


def test_compare_artifacts_to_ground_truth_writes_report_and_summary(tmp_path: Path) -> None:
    dataset_dir = tmp_path / "dataset"
    dataset_dir.mkdir()
    artifact_dir = tmp_path / "artifacts"
    records_dir = artifact_dir / "_records" / "run-1"
    records_dir.mkdir(parents=True)
    runs_dir = artifact_dir / "_runs"
    runs_dir.mkdir(parents=True)

    ground_truth = {
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
            "totalAmount": {"status": "present", "value": 21000.0, "raw": "$21,000.00"},
        },
        "lineItems": [
            {
                "index": 1,
                "description": {
                    "status": "present",
                    "value": "18-999 ADVANCE BILLING",
                    "raw": "18-999 ADVANCE BILLING",
                },
                "quantity": {"status": "present", "value": 1.0, "raw": "1"},
                "unitPrice": {"status": "present", "value": 21000.0, "raw": "21,000.00"},
                "amount": {"status": "present", "value": 21000.0, "raw": "21,000.00"},
                "tax": {"status": "absent"},
                "taxRate": {"status": "absent"},
                "sku": {"status": "present", "value": "18-999", "raw": "18-999"},
                "itemCode": {"status": "present", "value": "18-999", "raw": "18-999"},
                "notes": [],
            }
        ],
        "notes": [],
        "sourceImage": "invoice.pdf",
    }
    (dataset_dir / "invoice.json").write_text(json.dumps(ground_truth), encoding="utf-8")
    (records_dir / "invoice.record.json").write_text(
        json.dumps(
            _sample_manifest(
                """
Seller Example Ltd.
Customer Example LLC
INVOICE NUMBER:
INV-001
INVOICE DATE:
03/25/99
Total Amount: $21,000.00
<table>
  <tbody>
    <tr><td>QUANTITY</td><td>PRODUCT/DESCRIPTION</td><td>UNIT PRICE</td><td>EXT. PRICE</td></tr>
    <tr><td>1</td><td>18-999 ADVANCE BILLING</td><td>21,000.00</td><td>21,000.00</td></tr>
  </tbody>
</table>
"""
            )
        ),
        encoding="utf-8",
    )
    (runs_dir / "run-1.json").write_text(
        json.dumps(
            {
                "run_id": "run-1",
                "status": "completed",
                "counts": {"processed": 1, "failed": 0, "skipped": 0},
            }
        ),
        encoding="utf-8",
    )

    report = compare_artifacts_to_ground_truth(dataset_dir=dataset_dir, artifact_dir=artifact_dir)

    assert report["summary"]["documents_with_exact_match"] == 1
    assert report["documents"]["invoice"]["matched"] is True
    assert (artifact_dir / "_evaluation" / "comparison_report.json").exists()
    assert (artifact_dir / "_evaluation" / "comparison_summary.md").exists()


def test_compare_artifacts_to_ground_truth_flags_absent_field_mismatch(tmp_path: Path) -> None:
    dataset_dir = tmp_path / "dataset"
    dataset_dir.mkdir()
    artifact_dir = tmp_path / "artifacts"
    records_dir = artifact_dir / "_records" / "run-1"
    records_dir.mkdir(parents=True)

    ground_truth = {
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
            "totalAmount": {"status": "present", "value": 21000.0, "raw": "$21,000.00"},
        },
        "lineItems": [],
        "notes": [],
        "sourceImage": "invoice.pdf",
    }
    (dataset_dir / "invoice.json").write_text(json.dumps(ground_truth), encoding="utf-8")
    (records_dir / "invoice.record.json").write_text(
        json.dumps(
            _sample_manifest(
                """
Seller Example Ltd.
Customer Example LLC
INVOICE NUMBER: INV-001
INVOICE DATE: 03/25/99
Tax: 15.00
Total Amount: $21,000.00
"""
            )
        ),
        encoding="utf-8",
    )

    report = compare_artifacts_to_ground_truth(dataset_dir=dataset_dir, artifact_dir=artifact_dir)

    mismatches = report["documents"]["invoice"]["mismatches"]
    assert any(item["path"] == "summary.tax" for item in mismatches)


def _sample_manifest(markdown_result: str) -> dict[str, object]:
    return {
        "run_id": "run-1",
        "result_id": "result-1",
        "raw_provider_payload": {"markdown_result": markdown_result},
        "canonical_result": {"pages": []},
    }
