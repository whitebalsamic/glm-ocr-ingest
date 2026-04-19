"""Benchmark helpers for cluster-based OCR evaluation."""

from __future__ import annotations

import json
import shutil
import uuid
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from .artifacts import LocalArtifactSink
from .evaluate import compare_artifacts_to_ground_truth
from .models import ExecutionBudget, OcrSettings
from .orchestrator import build_execution_context, parse_documents
from .providers.base import OcrProvider
from .sources import ExplicitPathDocumentSource

FIXED_EXEMPLARS = [
    "00136a27c7774c1e8dc6b2f2",
    "002e3cf97973428f905671b3",
    "0178861dd64f4c58bbd4367a",
    "00aa98164d264f4e924f55a9",
]
MISMATCH_FILL_ORDER = [
    "lineItems.count",
    "document.sellerName",
    "document.invoiceDate",
    "document.customerName",
    "summary.totalAmount",
    "document.invoiceNumber",
    "document.currency",
]


@dataclass(slots=True)
class BenchmarkResult:
    parse_summary: dict[str, Any]
    compare_summary: dict[str, Any]
    metrics: dict[str, Any]


def load_cluster_manifest(path: Path) -> dict[str, Any]:
    data = json.loads(path.read_text(encoding="utf-8"))
    if isinstance(data, list):
        return {"name": path.stem, "stems": data}
    return data


def write_cluster_manifest(path: Path, name: str, stems: list[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    payload = {"name": name, "stems": stems}
    path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def build_cluster_manifests(report_path: Path) -> dict[str, list[str]]:
    report = json.loads(report_path.read_text(encoding="utf-8"))
    documents = report["documents"]
    exact_match_controls = sorted(
        stem for stem, details in documents.items() if details.get("matched")
    )[:6]
    category_map: dict[str, list[str]] = {}
    for stem, details in documents.items():
        for mismatch in details.get("mismatches", []):
            category_map.setdefault(mismatch["path"], []).append(stem)
    for stems in category_map.values():
        stems.sort()

    cluster10 = _unique_in_order(FIXED_EXEMPLARS + exact_match_controls)
    cluster25 = _fill_cluster(cluster10, category_map, 25)
    cluster50 = _fill_cluster(cluster25, category_map, 50)
    return {
        "cluster10": cluster10,
        "cluster25": cluster25,
        "cluster50": cluster50,
    }


def run_benchmark(
    *,
    dataset_dir: Path,
    artifact_dir: Path,
    cluster_manifest: Path,
    metrics_path: Path,
    database_url: str,
    db_schema: str,
    provider: OcrProvider,
    settings: OcrSettings,
    budget: ExecutionBudget,
) -> BenchmarkResult:
    manifest = load_cluster_manifest(cluster_manifest)
    cluster_stems = [str(stem) for stem in manifest["stems"]]
    cluster_paths = [dataset_dir / f"{stem}.pdf" for stem in cluster_stems]
    source = ExplicitPathDocumentSource(source_root=dataset_dir, paths=cluster_paths)

    if artifact_dir.exists():
        shutil.rmtree(artifact_dir)
    artifact_dir.mkdir(parents=True, exist_ok=True)

    parse_started = datetime.now(UTC)
    outcome = parse_documents(
        source=source,
        sink=LocalArtifactSink(artifact_dir),
        provider=provider,
        settings=settings,
        budget=budget,
        execution_context=build_execution_context("benchmark", str(dataset_dir), database_url),
        overwrite=True,
        database_url=database_url,
        db_schema=db_schema,
    )
    parse_finished = datetime.now(UTC)

    compare_started = datetime.now(UTC)
    report = compare_artifacts_to_ground_truth(
        dataset_dir=dataset_dir,
        artifact_dir=artifact_dir,
        stems=set(cluster_stems),
    )
    compare_finished = datetime.now(UTC)

    metrics = {
        "cluster_name": manifest.get("name", cluster_manifest.stem),
        "cluster_stems": cluster_stems,
        "db_schema": db_schema,
        "parse_wall_seconds": (parse_finished - parse_started).total_seconds(),
        "compare_wall_seconds": (compare_finished - compare_started).total_seconds(),
        "average_parse_seconds": outcome.run_summary.telemetry.get("average_parse_seconds", 0.0),
        "documents_processed": outcome.processed,
        "documents_failed": outcome.failures,
        "documents_skipped": outcome.skipped,
        "exact_matches": report["summary"]["documents_with_exact_match"],
        "documents_with_mismatches": report["summary"]["documents_with_mismatches"],
        "top_mismatch_categories": report["summary"]["top_mismatch_categories"],
        "parser_initializations": outcome.run_summary.telemetry.get("parser_initializations", 0),
        "selected_config": {
            "batch_documents": budget.batch_documents,
            "provider_max_workers": budget.provider_max_workers,
            "api_mode": settings.api_mode,
            "api_path": settings.api_path,
            "layout_use_polygon": settings.layout_use_polygon,
            "pdf_dpi": settings.pdf_dpi,
            "save_layout_visualization": settings.save_layout_visualization,
        },
        "report_path": report["summary"]["report_path"],
        "summary_path": report["summary"]["summary_path"],
    }
    metrics_path.parent.mkdir(parents=True, exist_ok=True)
    metrics_path.write_text(json.dumps(metrics, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    return BenchmarkResult(
        parse_summary=outcome.run_summary.telemetry,
        compare_summary=report["summary"],
        metrics=metrics,
    )


def evaluate_runtime_gate(
    *,
    baseline_metrics: dict[str, Any],
    candidate_metrics: dict[str, Any],
) -> bool:
    baseline_avg = float(baseline_metrics["average_parse_seconds"])
    candidate_avg = float(candidate_metrics["average_parse_seconds"])
    if candidate_metrics["exact_matches"] < baseline_metrics["exact_matches"]:
        return False
    return candidate_avg <= baseline_avg * 0.8


def evaluate_extraction_gate(
    *,
    baseline_metrics: dict[str, Any],
    candidate_metrics: dict[str, Any],
) -> bool:
    if candidate_metrics["exact_matches"] < baseline_metrics["exact_matches"]:
        return False
    if candidate_metrics["exact_matches"] >= baseline_metrics["exact_matches"] + 3:
        return True
    baseline_mismatches = int(baseline_metrics["documents_with_mismatches"])
    candidate_mismatches = int(candidate_metrics["documents_with_mismatches"])
    return candidate_mismatches <= baseline_mismatches * 0.9


def evaluate_knob_gate(
    *,
    baseline_metrics: dict[str, Any],
    candidate_metrics: dict[str, Any],
) -> bool:
    if candidate_metrics["exact_matches"] < baseline_metrics["exact_matches"]:
        return False
    if (
        candidate_metrics["average_parse_seconds"]
        > baseline_metrics["average_parse_seconds"] * 1.15
    ):
        return False
    return candidate_metrics["exact_matches"] > baseline_metrics["exact_matches"]


def allocate_benchmark_schema(prefix: str) -> str:
    suffix = uuid.uuid4().hex[:8]
    return f"{prefix}_{suffix}"


def _fill_cluster(
    initial: list[str],
    category_map: dict[str, list[str]],
    target_size: int,
) -> list[str]:
    cluster = list(initial)
    seen = set(cluster)
    while len(cluster) < target_size:
        advanced = False
        for category in MISMATCH_FILL_ORDER:
            for stem in category_map.get(category, []):
                if stem in seen:
                    continue
                cluster.append(stem)
                seen.add(stem)
                advanced = True
                break
            if len(cluster) >= target_size:
                break
        if not advanced:
            break
    return cluster


def _unique_in_order(items: list[str]) -> list[str]:
    seen: set[str] = set()
    result: list[str] = []
    for item in items:
        if item in seen:
            continue
        seen.add(item)
        result.append(item)
    return result
