"""Artifact writing helpers."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

from .models import ArtifactRef, RecordManifest, RunSummary
from .utils import json_dumps


@dataclass(slots=True)
class LocalArtifactSink:
    output_dir: Path

    def compatibility_json_path(self, logical_source_id: str) -> Path:
        return self.output_dir / Path(logical_source_id).with_suffix(".json")

    def record_path(self, run_id: str, logical_source_id: str) -> Path:
        return (
            self.output_dir
            / "_records"
            / run_id
            / Path(logical_source_id).with_suffix(".record.json")
        )

    def run_summary_path(self, run_id: str) -> Path:
        return self.output_dir / "_runs" / f"{run_id}.json"

    def ensure_root(self) -> None:
        self.output_dir.mkdir(parents=True, exist_ok=True)

    def write_compatibility_json(
        self, logical_source_id: str, sdk_json_result: object, overwrite: bool
    ) -> ArtifactRef:
        destination = self.compatibility_json_path(logical_source_id)
        destination.parent.mkdir(parents=True, exist_ok=True)
        if destination.exists() and not overwrite:
            raise FileExistsError(f"Output already exists: {destination}")
        destination.write_text(json_dumps(sdk_json_result), encoding="utf-8")
        return ArtifactRef(kind="compatibility_json", path=str(destination))

    def write_record(self, manifest: RecordManifest) -> ArtifactRef:
        destination = self.record_path(
            manifest.run_id, manifest.document_identity.logical_source_id
        )
        destination.parent.mkdir(parents=True, exist_ok=True)
        if destination.exists():
            raise FileExistsError(f"Record manifest already exists: {destination}")
        destination.write_text(json_dumps(manifest), encoding="utf-8")
        return ArtifactRef(kind="record_manifest", path=str(destination))

    def record_ref(self, run_id: str, logical_source_id: str) -> ArtifactRef:
        return ArtifactRef(
            kind="record_manifest", path=str(self.record_path(run_id, logical_source_id))
        )

    def write_run_summary(self, summary: RunSummary) -> ArtifactRef:
        destination = self.run_summary_path(summary.run_id)
        destination.parent.mkdir(parents=True, exist_ok=True)
        destination.write_text(json_dumps(summary), encoding="utf-8")
        return ArtifactRef(kind="run_summary", path=str(destination))
