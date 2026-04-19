# glm-ocr sandbox-ready wrapper

[![CI](https://github.com/whitebalsamic/glm-ocr-ingest/actions/workflows/ci.yml/badge.svg)](https://github.com/whitebalsamic/glm-ocr-ingest/actions/workflows/ci.yml)
[![Python 3.14](https://img.shields.io/badge/python-3.14-blue.svg)](https://www.python.org/downloads/release/python-3144/)
[![License: MIT](https://img.shields.io/badge/license-MIT-green.svg)](LICENSE)

Small package wrapper around the official `glmocr` SDK with a bytes-first core, immutable replay artifacts, and optional Postgres persistence.

## What changed

- The repo now ships a real package under `glm_ocr/`.
- `glm_ocr_cli.py` remains as a thin compatibility shim.
- Legacy parse usage still works:

```bash
python3 ./glm_ocr_cli.py /path/to/input -o /path/to/output
```

- Subcommands are now supported:

```bash
python3 ./glm_ocr_cli.py parse /path/to/input -o /path/to/output
python3 ./glm_ocr_cli.py replay /path/to/output --database-url postgresql://...
python3 ./glm_ocr_cli.py doctor -o /path/to/output
```

## Package layout

- `glm_ocr.cli`: CLI entrypoints and legacy argument compatibility
- `glm_ocr.orchestrator`: parse, replay, and doctor flows
- `glm_ocr.providers`: provider contract and the v1 `GlmOcrProvider`
- `glm_ocr.sources`: lazy local document discovery and bytes loading
- `glm_ocr.artifacts`: compatibility JSONs, immutable record manifests, run summaries
- `glm_ocr.store`: optional Postgres persistence and replay bootstrap
- `glm_ocr.normalize`: canonical result v1 normalization
- `glm_ocr.models`: typed provenance, artifact, and canonical data models

## Supported provider policy

- v1 ships one provider: `glm`
- Supported SDK range: `glmocr>=0.1.5,<0.2.0`
- Strict determinism is the default
- If the private determinism hook disappears, parsing fails unless `--best-effort-determinism` is set
- Untested SDK versions are rejected unless `--allow-untested-provider` is set

## Artifacts

For an input document `nested/invoice.pdf` and run id `<run_id>`, the wrapper writes:

- Compatibility JSON: `<output_dir>/nested/invoice.json`
- Immutable record manifest: `<output_dir>/_records/<run_id>/nested/invoice.record.json`
- Run summary: `<output_dir>/_runs/<run_id>.json`

Compatibility JSON preserves the raw SDK `json_result` path and shape. Record manifests add provenance, settings, canonical output, provider metadata, warnings, and artifact references for replay.

## Canonical schema v1

- `document`: `pages`, `summary`, `provider_extra`
- `page`: `page_index`, `regions`, `page_extra`
- `region`: `region_index`, `label`, `native_label`, `content`, `bbox_2d`, `polygon`, `extra_fields`

Normalization rules:

- Missing `index` becomes the ordinal position within the page
- Missing `content` becomes `""`
- Missing `label` becomes `"unknown"`
- Unknown provider fields are copied into `extra_fields`
- Canonical summaries always include `page_count` and `region_count`

## Database schema

SQL bootstrap lives in [glm_ocr/sql/001_init.sql](glm_ocr/sql/001_init.sql). The wrapper creates:

- `ocr_runs`
- `ocr_documents`
- `ocr_results`
- `ocr_pages`
- `ocr_regions`

The database does not store original document bytes in v1.

## Installation

```bash
python3 -m pip install -e ".[dev]"
```

`config.py` remains optional and is only used as a local default source for `layout_device`.

## CLI options

Parse supports the existing flags plus:

- `--provider`
- `--database-url`
- `--db-schema`
- `--allow-untested-provider`
- `--best-effort-determinism`
- `--provider-max-workers`

`--jobs` is still accepted for backward compatibility and is routed through the execution budget.

## Testing

Run:

```bash
pytest
ruff check .
ruff format --check .
```

If `ruff format --check .` fails, run `ruff format` and rerun both Ruff commands.
