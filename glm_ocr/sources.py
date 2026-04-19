"""Document source adapters."""

from __future__ import annotations

from collections.abc import Iterator
from dataclasses import dataclass
from pathlib import Path

from .constants import SUPPORTED_DOC_SUFFIXES
from .models import DocumentInput
from .utils import detect_mime_type, safe_relative_path


def validate_supported(path: Path) -> None:
    if path.suffix.lower() not in SUPPORTED_DOC_SUFFIXES:
        supported = ", ".join(sorted(SUPPORTED_DOC_SUFFIXES))
        raise ValueError(f"Unsupported file type for {path}. Supported: {supported}")


@dataclass(slots=True)
class LocalPathDocumentSource:
    input_path: Path
    recursive: bool = False

    def iter_paths(self) -> Iterator[Path]:
        if self.input_path.is_file():
            validate_supported(self.input_path)
            yield self.input_path
            return

        if not self.input_path.is_dir():
            raise FileNotFoundError(f"Input path does not exist: {self.input_path}")

        pattern = "**/*" if self.recursive else "*"
        for path in sorted(self.input_path.glob(pattern)):
            if path.is_file() and path.suffix.lower() in SUPPORTED_DOC_SUFFIXES:
                yield path

    def iter_documents(self) -> Iterator[DocumentInput]:
        for path in self.iter_paths():
            stat = path.stat()
            relative = safe_relative_path(self.input_path, path)
            yield DocumentInput(
                raw_bytes=path.read_bytes(),
                display_name=path.name,
                logical_source_id=str(relative),
                mime_type=detect_mime_type(path.name),
                source_metadata={
                    "absolute_path": str(path.resolve()),
                    "mtime": stat.st_mtime,
                    "extension": path.suffix.lower(),
                },
            )

    def discovered_paths(self, max_documents: int | None = None) -> list[Path]:
        paths = list(self.iter_paths())
        if max_documents is not None:
            return paths[:max_documents]
        return paths
