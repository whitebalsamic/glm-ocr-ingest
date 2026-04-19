from __future__ import annotations

from pathlib import Path

from glm_ocr.sources import LocalPathDocumentSource


def test_local_source_preserves_sorted_discovery(tmp_path: Path) -> None:
    (tmp_path / "b.pdf").write_bytes(b"b")
    (tmp_path / "a.png").write_bytes(b"a")
    (tmp_path / "skip.txt").write_text("x", encoding="utf-8")

    source = LocalPathDocumentSource(tmp_path)

    assert [path.name for path in source.discovered_paths()] == ["a.png", "b.pdf"]


def test_local_source_yields_document_bytes(tmp_path: Path) -> None:
    file_path = tmp_path / "nested.pdf"
    file_path.write_bytes(b"hello")

    source = LocalPathDocumentSource(file_path)
    document = next(source.iter_documents())

    assert document.raw_bytes == b"hello"
    assert document.logical_source_id == "nested.pdf"
