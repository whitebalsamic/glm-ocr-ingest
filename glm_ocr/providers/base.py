"""Provider contract definitions."""

from __future__ import annotations

from collections.abc import Iterable
from typing import Protocol

from ..models import DocumentInput, ExecutionBudget, OcrSettings, ProviderParseResult


class OcrProvider(Protocol):
    def validate_environment(self, settings: OcrSettings) -> list[str]:
        """Validate runtime prerequisites and return warnings."""

    def parse_document(
        self,
        document: DocumentInput,
        settings: OcrSettings,
        budget: ExecutionBudget,
    ) -> ProviderParseResult:
        """Run OCR for a document."""

    def parse_documents_batch(
        self,
        documents: Iterable[DocumentInput],
        settings: OcrSettings,
        budget: ExecutionBudget,
    ) -> list[ProviderParseResult]:
        """Run OCR for a batch of documents."""

    def begin_run(self, settings: OcrSettings, budget: ExecutionBudget) -> None:
        """Initialize any run-scoped resources."""

    def end_run(self) -> None:
        """Release any run-scoped resources."""

    def run_telemetry(self) -> dict[str, object]:
        """Return provider telemetry for the current run."""

    def provider_metadata(self) -> dict[str, object]:
        """Return provider metadata."""
