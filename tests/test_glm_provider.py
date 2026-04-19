from __future__ import annotations

import importlib.metadata

import pytest

from glm_ocr.models import OcrSettings
from glm_ocr.providers.glm_provider import DeterminismStrategy, GlmOcrProvider


class ParserNoHook:
    class Pipeline:
        ocr_client = object()

    _pipeline = Pipeline()


def _noop_reachability(_self, _api_url: str) -> None:
    return None


def test_determinism_strategy_strict_failure() -> None:
    with pytest.raises(RuntimeError):
        DeterminismStrategy(strict=True).apply(ParserNoHook(), seed=42)


def test_determinism_strategy_best_effort_warning() -> None:
    applied, warning = DeterminismStrategy(strict=False).apply(ParserNoHook(), seed=42)
    assert applied is False
    assert "best-effort" in warning


def test_validate_environment_rejects_untested_version(monkeypatch) -> None:
    provider = GlmOcrProvider()
    settings = OcrSettings(
        provider="glm",
        model="m",
        api_url="http://example.com",
        layout_device="cpu",
        page_loader_max_tokens=1,
        seed=42,
        temperature=0.0,
        top_p=0.0,
        top_k=1,
        repeat_penalty=1.0,
    )
    monkeypatch.setattr(importlib.metadata, "version", lambda _name: "0.2.1")
    monkeypatch.setattr(GlmOcrProvider, "_check_ollama_reachability", _noop_reachability)

    with pytest.raises(RuntimeError):
        provider.validate_environment(settings)


def test_validate_environment_allows_override(monkeypatch) -> None:
    provider = GlmOcrProvider()
    settings = OcrSettings(
        provider="glm",
        model="m",
        api_url="http://example.com",
        layout_device="cpu",
        page_loader_max_tokens=1,
        seed=42,
        temperature=0.0,
        top_p=0.0,
        top_k=1,
        repeat_penalty=1.0,
        allow_untested_provider=True,
    )
    monkeypatch.setattr(importlib.metadata, "version", lambda _name: "0.2.1")
    monkeypatch.setattr(GlmOcrProvider, "_check_ollama_reachability", _noop_reachability)

    assert provider.validate_environment(settings) == []
