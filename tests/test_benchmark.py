from __future__ import annotations

import json
from pathlib import Path

from glm_ocr.benchmark import (
    build_cluster_manifests,
    evaluate_extraction_gate,
    evaluate_runtime_gate,
)


def test_build_cluster_manifests_uses_fixed_exemplars_and_controls(tmp_path: Path) -> None:
    report_path = tmp_path / "report.json"
    documents = {
        "00136a27c7774c1e8dc6b2f2": {"matched": False, "mismatches": [{"path": "lineItems.count"}]},
        "002e3cf97973428f905671b3": {
            "matched": False,
            "mismatches": [{"path": "document.sellerName"}],
        },
        "0178861dd64f4c58bbd4367a": {
            "matched": False,
            "mismatches": [{"path": "document.invoiceNumber"}],
        },
        "00aa98164d264f4e924f55a9": {
            "matched": False,
            "mismatches": [{"path": "summary.totalAmount"}],
        },
        "1000": {"matched": True, "mismatches": []},
        "1001": {"matched": True, "mismatches": []},
        "1002": {"matched": True, "mismatches": []},
        "1003": {"matched": True, "mismatches": []},
        "1004": {"matched": True, "mismatches": []},
        "1005": {"matched": True, "mismatches": []},
        "2000": {"matched": False, "mismatches": [{"path": "document.customerName"}]},
        "2001": {"matched": False, "mismatches": [{"path": "document.invoiceDate"}]},
    }
    report_path.write_text(json.dumps({"documents": documents}), encoding="utf-8")

    manifests = build_cluster_manifests(report_path)

    assert manifests["cluster10"][:4] == [
        "00136a27c7774c1e8dc6b2f2",
        "002e3cf97973428f905671b3",
        "0178861dd64f4c58bbd4367a",
        "00aa98164d264f4e924f55a9",
    ]
    assert manifests["cluster10"][4:] == ["1000", "1001", "1002", "1003", "1004", "1005"]
    assert len(manifests["cluster25"]) >= len(manifests["cluster10"])


def test_runtime_gate_requires_speedup_and_no_exact_match_regression() -> None:
    baseline = {"average_parse_seconds": 10.0, "exact_matches": 4}
    faster = {"average_parse_seconds": 7.5, "exact_matches": 4}
    slower = {"average_parse_seconds": 8.5, "exact_matches": 3}

    assert evaluate_runtime_gate(baseline_metrics=baseline, candidate_metrics=faster) is True
    assert evaluate_runtime_gate(baseline_metrics=baseline, candidate_metrics=slower) is False


def test_extraction_gate_requires_meaningful_improvement() -> None:
    baseline = {"exact_matches": 4, "documents_with_mismatches": 20}
    better_exact = {"exact_matches": 7, "documents_with_mismatches": 20}
    fewer_mismatches = {"exact_matches": 4, "documents_with_mismatches": 18}
    regression = {"exact_matches": 3, "documents_with_mismatches": 15}

    assert evaluate_extraction_gate(baseline_metrics=baseline, candidate_metrics=better_exact)
    assert evaluate_extraction_gate(baseline_metrics=baseline, candidate_metrics=fewer_mismatches)
    assert not evaluate_extraction_gate(baseline_metrics=baseline, candidate_metrics=regression)
