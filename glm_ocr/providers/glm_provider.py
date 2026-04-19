"""GLM OCR provider adapter."""

from __future__ import annotations

import importlib
import importlib.metadata
import tempfile
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
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

    def parse_document(
        self,
        document: DocumentInput,
        settings: OcrSettings,
        budget: ExecutionBudget,
    ) -> ProviderParseResult:
        module = self._import_glmocr()
        parser = module.GlmOcr(
            mode="selfhosted",
            model=settings.model,
            layout_device=settings.layout_device,
            _dotted={
                "pipeline.ocr_api.api_url": settings.api_url,
                "pipeline.ocr_api.api_mode": "ollama_generate",
                "pipeline.ocr_api.request_timeout": 300,
                "pipeline.ocr_api.connect_timeout": 30,
                "pipeline.page_loader.max_tokens": settings.page_loader_max_tokens,
                "pipeline.page_loader.temperature": settings.temperature,
                "pipeline.page_loader.top_p": settings.top_p,
                "pipeline.page_loader.top_k": settings.top_k,
                "pipeline.page_loader.repetition_penalty": settings.repeat_penalty,
                "pipeline.max_workers": budget.provider_max_workers,
            },
        )
        strategy = DeterminismStrategy(strict=not settings.best_effort_determinism)
        applied, warning = strategy.apply(parser, settings.seed)
        started_at = datetime.now(UTC)
        try:
            with tempfile.NamedTemporaryFile(
                suffix=Path(document.display_name).suffix,
                delete=False,
            ) as handle:
                handle.write(document.raw_bytes)
                temp_path = Path(handle.name)
            result = parser.parse(temp_path, save_layout_visualization=False, preserve_order=True)
        finally:
            parser.close()
            if "temp_path" in locals():
                temp_path.unlink(missing_ok=True)
        finished_at = datetime.now(UTC)
        raw_payload = self._raw_payload(result)
        canonical_result = normalize_glm_payload(raw_payload)
        warnings = [warning] if warning else []
        provider_metadata = self.provider_metadata() | {"determinism_applied": applied}
        return ProviderParseResult(
            provider_name="glm",
            provider_version=str(provider_metadata["provider_version"]),
            provider_metadata=provider_metadata,
            raw_payload=raw_payload,
            canonical_result=canonical_result,
            warnings=warnings,
            started_at=started_at,
            finished_at=finished_at,
        )

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
