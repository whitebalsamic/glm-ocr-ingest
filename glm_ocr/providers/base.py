"""Provider contract definitions."""

from __future__ import annotations

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

    def provider_metadata(self) -> dict[str, object]:
        """Return provider metadata."""
