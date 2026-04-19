"""CLI entrypoints."""

from __future__ import annotations

import argparse
import json
import os
from pathlib import Path

from .artifacts import LocalArtifactSink
from .benchmark import run_benchmark
from .config_defaults import default_layout_device
from .constants import (
    DEFAULT_MODEL,
    DEFAULT_OLLAMA_API_URL,
    DEFAULT_PAGE_LOADER_MAX_TOKENS,
    DEFAULT_PROVIDER_NAME,
    DEFAULT_REPEAT_PENALTY,
    DEFAULT_SEED,
    DEFAULT_TEMPERATURE,
    DEFAULT_TOP_K,
    DEFAULT_TOP_P,
)
from .evaluate import compare_artifacts_to_ground_truth, render_summary_markdown
from .models import ExecutionBudget, OcrSettings
from .orchestrator import build_execution_context, doctor_checks, parse_documents, replay_artifacts
from .providers.glm_provider import GlmOcrProvider
from .sources import LocalPathDocumentSource


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Run GLM-OCR through a sandbox-ready ingestion wrapper."
    )
    subparsers = parser.add_subparsers(dest="command")

    parse_parser = subparsers.add_parser("parse", help="Parse documents into artifacts.")
    _add_parse_arguments(parse_parser)

    replay_parser = subparsers.add_parser("replay", help="Replay record artifacts into Postgres.")
    replay_parser.add_argument(
        "path", type=Path, help="A single *.record.json file or an artifact output directory."
    )
    replay_parser.add_argument("--database-url", default=os.environ.get("DATABASE_URL"))
    replay_parser.add_argument("--db-schema", default="public")

    compare_parser = subparsers.add_parser(
        "compare",
        help="Compare artifact output against dataset ground truth JSON files.",
    )
    compare_parser.add_argument("dataset_dir", type=Path)
    compare_parser.add_argument("artifact_dir", type=Path)
    compare_parser.add_argument("--report-path", type=Path)

    benchmark_parser = subparsers.add_parser(
        "benchmark",
        help="Run parse plus compare for a deterministic document cluster.",
    )
    benchmark_parser.add_argument("dataset_dir", type=Path)
    benchmark_parser.add_argument("artifact_dir", type=Path)
    benchmark_parser.add_argument("--database-url", default=os.environ.get("DATABASE_URL"))
    benchmark_parser.add_argument("--db-schema", required=True)
    benchmark_parser.add_argument("--cluster-manifest", type=Path, required=True)
    benchmark_parser.add_argument("--metrics-path", type=Path, required=True)
    _add_provider_arguments(benchmark_parser)
    benchmark_parser.add_argument("--batch-documents", type=int, default=1)

    doctor_parser = subparsers.add_parser("doctor", help="Validate local runtime dependencies.")
    _add_provider_arguments(doctor_parser)
    doctor_parser.add_argument("-o", "--output-dir", type=Path, required=True)
    doctor_parser.add_argument("--database-url", default=os.environ.get("DATABASE_URL"))
    doctor_parser.add_argument("--db-schema", default="public")
    doctor_parser.add_argument("--json", action="store_true", dest="json_output")
    return parser


def build_legacy_parse_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Run GLM-OCR through a sandbox-ready ingestion wrapper."
    )
    _add_parse_arguments(parser)
    return parser


def _add_provider_arguments(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--provider", default=DEFAULT_PROVIDER_NAME)
    parser.add_argument("--model", default=DEFAULT_MODEL)
    parser.add_argument("--api-url", default=DEFAULT_OLLAMA_API_URL)
    parser.add_argument("--layout-device", default=default_layout_device())
    parser.add_argument("--seed", type=int, default=DEFAULT_SEED)
    parser.add_argument("--temperature", type=float, default=DEFAULT_TEMPERATURE)
    parser.add_argument("--top-p", type=float, default=DEFAULT_TOP_P)
    parser.add_argument("--top-k", type=int, default=DEFAULT_TOP_K)
    parser.add_argument("--repeat-penalty", type=float, default=DEFAULT_REPEAT_PENALTY)
    parser.add_argument(
        "--page-loader-max-tokens", type=int, default=DEFAULT_PAGE_LOADER_MAX_TOKENS
    )
    parser.add_argument("--allow-untested-provider", action="store_true")
    parser.add_argument("--best-effort-determinism", action="store_true")
    parser.add_argument("--provider-max-workers", type=int, default=1)
    parser.add_argument("--api-mode", default="ollama_generate")
    parser.add_argument("--api-path", default="/api/generate")
    parser.add_argument("--layout-use-polygon", action="store_true")
    parser.add_argument("--pdf-dpi", type=int, default=200)
    parser.add_argument("--save-layout-visualization", action="store_true")


def _add_parse_arguments(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("input_path", type=Path, nargs="?")
    parser.add_argument("-o", "--output-dir", type=Path, required=True)
    parser.add_argument("--recursive", action="store_true")
    parser.add_argument("--overwrite", action="store_true")
    parser.add_argument("--jobs", type=int, default=1)
    parser.add_argument("--batch-documents", type=int, default=1)
    parser.add_argument("--max-documents", type=int, default=None)
    parser.add_argument("--database-url", default=os.environ.get("DATABASE_URL"))
    parser.add_argument("--db-schema", default="public")
    _add_provider_arguments(parser)


def _coerce_legacy_parse(argv: list[str]) -> list[str]:
    if not argv:
        return argv
    if argv[0] in {"parse", "replay", "compare", "benchmark", "doctor", "-h", "--help"}:
        return argv
    return ["parse", *argv]


def _settings_from_args(args: argparse.Namespace) -> OcrSettings:
    return OcrSettings(
        provider=args.provider,
        model=args.model,
        api_url=args.api_url,
        layout_device=args.layout_device,
        page_loader_max_tokens=args.page_loader_max_tokens,
        seed=args.seed,
        temperature=args.temperature,
        top_p=args.top_p,
        top_k=args.top_k,
        repeat_penalty=args.repeat_penalty,
        api_mode=args.api_mode,
        api_path=args.api_path,
        layout_use_polygon=args.layout_use_polygon,
        pdf_dpi=args.pdf_dpi,
        save_layout_visualization=args.save_layout_visualization,
        allow_untested_provider=args.allow_untested_provider,
        best_effort_determinism=args.best_effort_determinism,
    )


def _provider_from_args(args: argparse.Namespace) -> GlmOcrProvider:
    if args.provider != "glm":
        raise ValueError(f"Unsupported provider: {args.provider}")
    return GlmOcrProvider()


def main(argv: list[str] | None = None) -> int:
    import sys

    raw_argv = sys.argv[1:] if argv is None else argv
    legacy_mode = bool(raw_argv) and raw_argv[0] not in {
        "parse",
        "replay",
        "compare",
        "benchmark",
        "doctor",
        "-h",
        "--help",
    }
    parser = build_legacy_parse_parser() if legacy_mode else build_parser()
    args = parser.parse_args(raw_argv if legacy_mode else _coerce_legacy_parse(raw_argv))
    if legacy_mode:
        args.command = "parse"

    if args.command == "replay":
        if not args.database_url:
            print("--database-url or DATABASE_URL is required for replay.")
            return 1
        result = replay_artifacts(
            path=args.path, database_url=args.database_url, db_schema=args.db_schema
        )
        print(json.dumps(result, indent=2))
        return 0

    if args.command == "compare":
        report = compare_artifacts_to_ground_truth(
            dataset_dir=args.dataset_dir,
            artifact_dir=args.artifact_dir,
            report_path=args.report_path,
        )
        print(render_summary_markdown(report["summary"]))
        return 0

    settings = _settings_from_args(args)
    provider = _provider_from_args(args)

    if args.command == "benchmark":
        if not args.database_url:
            print("--database-url or DATABASE_URL is required for benchmark.")
            return 1
        if args.batch_documents < 1 or args.provider_max_workers < 1:
            print("--batch-documents and --provider-max-workers must be at least 1.")
            return 1
        result = run_benchmark(
            dataset_dir=args.dataset_dir,
            artifact_dir=args.artifact_dir,
            cluster_manifest=args.cluster_manifest,
            metrics_path=args.metrics_path,
            database_url=args.database_url,
            db_schema=args.db_schema,
            provider=provider,
            settings=settings,
            budget=ExecutionBudget(
                jobs=1,
                provider_max_workers=args.provider_max_workers,
                batch_documents=args.batch_documents,
            ),
        )
        print(json.dumps(result.metrics, indent=2))
        return 0

    if args.command == "doctor":
        result = doctor_checks(
            provider=provider,
            settings=settings,
            output_dir=args.output_dir,
            database_url=args.database_url,
            db_schema=args.db_schema,
        )
        if args.json_output:
            print(json.dumps(result, indent=2))
        else:
            for check in result["checks"]:
                status = "ok" if check["ok"] else "fail"
                detail = check.get("error") or check.get("details", "")
                print(f"{check['name']}: {status} {detail}")
        return 0 if result["ok"] else 1

    if not args.input_path:
        parser.error("parse requires an input_path")
    if args.max_documents is not None and args.max_documents < 1:
        print("--max-documents must be at least 1.")
        return 1
    if args.jobs < 1 or args.provider_max_workers < 1 or args.batch_documents < 1:
        print("--jobs, --provider-max-workers, and --batch-documents must be at least 1.")
        return 1

    source = LocalPathDocumentSource(input_path=args.input_path, recursive=args.recursive)
    sink = LocalArtifactSink(output_dir=args.output_dir)
    budget = ExecutionBudget(
        jobs=args.jobs,
        provider_max_workers=args.provider_max_workers,
        batch_documents=args.batch_documents,
        max_documents=args.max_documents,
    )
    execution_context = build_execution_context(
        command="parse",
        cwd=os.getcwd(),
        database_url=args.database_url,
    )
    outcome = parse_documents(
        source=source,
        sink=sink,
        provider=provider,
        settings=settings,
        budget=budget,
        execution_context=execution_context,
        overwrite=args.overwrite,
        database_url=args.database_url,
        db_schema=args.db_schema,
    )
    print(
        f"[summary] processed {outcome.processed} document(s), failed {outcome.failures}, "
        f"skipped {outcome.skipped}"
    )
    return 1 if outcome.failures else 0
