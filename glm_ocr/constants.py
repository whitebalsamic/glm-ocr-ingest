"""Project-wide constants."""

from __future__ import annotations

from pathlib import Path

SUPPORTED_DOC_SUFFIXES = {
    ".pdf",
    ".png",
    ".jpg",
    ".jpeg",
    ".webp",
    ".bmp",
    ".gif",
    ".tif",
    ".tiff",
}
DEFAULT_MODEL = "glm-ocr:q8_0"
DEFAULT_OLLAMA_API_URL = "http://127.0.0.1:11434/api/generate"
FALLBACK_LAYOUT_DEVICE = "cpu"
DEFAULT_PAGE_LOADER_MAX_TOKENS = 4096
DEFAULT_SEED = 42
DEFAULT_TEMPERATURE = 0.0
DEFAULT_TOP_P = 0.00001
DEFAULT_TOP_K = 1
DEFAULT_REPEAT_PENALTY = 1.1
DEFAULT_PROVIDER_NAME = "glm"
SUPPORTED_GLMOCR_VERSION_RANGE = ">=0.1.5,<0.2.0"
CANONICAL_SCHEMA_VERSION = 1
ARTIFACT_MANIFEST_VERSION = 1
PROVIDER_CONTRACT_VERSION = 1
SCHEMA_DIR = Path(__file__).resolve().parent / "sql"
