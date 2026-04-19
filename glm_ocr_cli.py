#!/usr/bin/env python3
"""Backward-compatible CLI shim."""

from glm_ocr.cli import main

if __name__ == "__main__":
    raise SystemExit(main())
