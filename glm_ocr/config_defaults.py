"""Optional project-local defaults."""

from __future__ import annotations

from .constants import FALLBACK_LAYOUT_DEVICE

try:
    import config as project_config  # type: ignore
except ImportError:  # pragma: no cover - optional local file
    project_config = None


def default_layout_device() -> str:
    return (
        getattr(project_config, "DEFAULT_LAYOUT_DEVICE", FALLBACK_LAYOUT_DEVICE)
        or FALLBACK_LAYOUT_DEVICE
    )
