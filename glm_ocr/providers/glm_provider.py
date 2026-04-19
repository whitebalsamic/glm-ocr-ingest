"""GLM OCR provider adapter."""

from __future__ import annotations

import importlib
import importlib.metadata
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any
from urllib import error, request

from ..constants import SUPPORTED_GLMOCR_VERSION_RANGE
from ..models import DocumentInput, ExecutionBudget, OcrSettings, ProviderParseResult
from ..normalize import normalize_glm_payload


def _parse_version(version: str) -> tuple[int, ...]:
    parts: list[int] = []
    for item in version.split("."):
        digits = "".join(ch for ch in item if ch.isdigit())
        if digits:
            parts.append(int(digits))
    return tuple(parts)


def _version_supported(version: str) -> bool:
    parsed = _parse_version(version)
    return (0, 1, 5) <= parsed < (0, 2, 0)


@dataclass(slots=True)
class DeterminismStrategy:
    strict: bool = True

    def apply(self, parser: Any, seed: int) -> tuple[bool, str | None]:
        pipeline = getattr(parser, "_pipeline", None)
        ocr_client = getattr(pipeline, "ocr_client", None) if pipeline is not None else None
        original_convert = (
            getattr(ocr_client, "_convert_to_ollama_generate", None)
            if ocr_client is not None
            else None
        )
        if original_convert is None:
            if self.strict:
                raise RuntimeError(
                    "GLM-OCR determinism hook is unavailable in this SDK version. "
                    "Retry with --best-effort-determinism to bypass strict enforcement."
                )
            return False, "Determinism hook unavailable; continuing in best-effort mode."

        def patched_convert(request_data: dict[str, Any]) -> dict[str, Any]:
            ollama_request = original_convert(request_data)
            options = dict(ollama_request.get("options") or {})
            options["seed"] = seed
            ollama_request["options"] = options
            return ollama_request

        ocr_client._convert_to_ollama_generate = patched_convert
        return True, None


@dataclass(slots=True)
class GlmOcrProvider:
    _parser: Any | None = None
    _parser_warning: str | None = None
    _parser_applied: bool = False
    _parser_initializations: int = 0

    def validate_environment(self, settings: OcrSettings) -> list[str]:
        warnings: list[str] = []
        version = self.provider_metadata()["provider_version"]
        if not settings.allow_untested_provider and not _version_supported(str(version)):
            raise RuntimeError(
                f"glmocr {version} is outside the tested range {SUPPORTED_GLMOCR_VERSION_RANGE}. "
                "Pass --allow-untested-provider to continue."
            )
        self._check_ollama_reachability(settings.api_url)
        return warnings

    def provider_metadata(self) -> dict[str, object]:
        try:
            version = importlib.metadata.version("glmocr")
        except importlib.metadata.PackageNotFoundError as exc:  # pragma: no cover - env-dependent
            raise RuntimeError(
                "glmocr is not importable. Install the official SDK normally."
            ) from exc
        return {
            "provider_name": "glm",
            "provider_version": version,
            "provider_mode": "selfhosted",
            "supported_version_range": SUPPORTED_GLMOCR_VERSION_RANGE,
        }

    def begin_run(self, settings: OcrSettings, budget: ExecutionBudget) -> None:
        if self._parser is not None:
            return
        self._parser_initializations = 0
        module = self._import_glmocr()
        parser = module.GlmOcr(
            mode="selfhosted",
            model=settings.model,
            api_url=settings.api_url,
            layout_device=settings.layout_device,
            _dotted={
                "pipeline.maas.enabled": False,
                "pipeline.ocr_api.api_url": settings.api_url,
                "pipeline.ocr_api.api_mode": settings.api_mode,
                "pipeline.ocr_api.api_path": settings.api_path,
                "pipeline.ocr_api.model": settings.model,
                "pipeline.ocr_api.request_timeout": 300,
                "pipeline.ocr_api.connect_timeout": 30,
                "pipeline.ocr_api.connection_pool_size": max(budget.provider_max_workers, 32),
                "pipeline.page_loader.max_tokens": settings.page_loader_max_tokens,
                "pipeline.page_loader.temperature": settings.temperature,
                "pipeline.page_loader.top_p": settings.top_p,
                "pipeline.page_loader.top_k": settings.top_k,
                "pipeline.page_loader.repetition_penalty": settings.repeat_penalty,
                "pipeline.page_loader.pdf_dpi": settings.pdf_dpi,
                "pipeline.layout.use_polygon": settings.layout_use_polygon,
                "pipeline.max_workers": budget.provider_max_workers,
            },
        )
        strategy = DeterminismStrategy(strict=not settings.best_effort_determinism)
        applied, warning = strategy.apply(parser, settings.seed)
        self._parser = parser
        self._parser_applied = applied
        self._parser_warning = warning
        self._parser_initializations += 1

    def end_run(self) -> None:
        if self._parser is not None:
            self._parser.close()
        self._parser = None
        self._parser_warning = None
        self._parser_applied = False

    def run_telemetry(self) -> dict[str, object]:
        return {"parser_initializations": self._parser_initializations}

    def parse_document(
        self,
        document: DocumentInput,
        settings: OcrSettings,
        budget: ExecutionBudget,
    ) -> ProviderParseResult:
        return self.parse_documents_batch([document], settings, budget)[0]

    def parse_documents_batch(
        self,
        documents: list[DocumentInput],
        settings: OcrSettings,
        budget: ExecutionBudget,
    ) -> list[ProviderParseResult]:
        if not documents:
            return []
        self.begin_run(settings, budget)
        if self._parser is None:
            raise RuntimeError("GLM-OCR parser is not initialized.")

        started_at = datetime.now(UTC)
        results = self._parser.parse(
            [document.raw_bytes for document in documents],
            stream=True,
            save_layout_visualization=settings.save_layout_visualization,
            preserve_order=True,
        )
        provider_metadata = self.provider_metadata() | {
            "determinism_applied": self._parser_applied,
            "api_mode": settings.api_mode,
            "api_path": settings.api_path,
            "layout_use_polygon": settings.layout_use_polygon,
            "pdf_dpi": settings.pdf_dpi,
        }
        warnings = [self._parser_warning] if self._parser_warning else []
        provider_results: list[ProviderParseResult] = []
        for result in results:
            finished_at = datetime.now(UTC)
            raw_payload = self._raw_payload(result)
            canonical_result = normalize_glm_payload(raw_payload)
            provider_results.append(
                ProviderParseResult(
                    provider_name="glm",
                    provider_version=str(provider_metadata["provider_version"]),
                    provider_metadata=provider_metadata,
                    raw_payload=raw_payload,
                    canonical_result=canonical_result,
                    warnings=list(warnings),
                    started_at=started_at,
                    finished_at=finished_at,
                )
            )
        return provider_results

    def _import_glmocr(self) -> Any:
        try:
            return importlib.import_module("glmocr")
        except ImportError as exc:  # pragma: no cover - env-dependent
            raise RuntimeError(
                "glmocr is not importable. Install the official SDK normally."
            ) from exc

    def _check_ollama_reachability(self, api_url: str) -> None:
        req = request.Request(api_url, method="GET")
        try:
            with request.urlopen(req, timeout=5):
                return
        except error.HTTPError:
            return
        except OSError as exc:
            raise RuntimeError(f"Ollama endpoint is not reachable: {api_url}") from exc

    def _raw_payload(self, result: Any) -> dict[str, Any]:
        payload: dict[str, Any] = {}
        to_dict = getattr(result, "to_dict", None)
        if callable(to_dict):
            payload.update(to_dict())
        json_result = getattr(result, "json_result", None)
        if json_result is not None:
            payload["json_result"] = json_result
        for attribute in ("raw_result", "layout_result", "metadata"):
            value = getattr(result, attribute, None)
            if value is not None:
                payload[attribute] = value
        return payload
