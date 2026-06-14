"""Unit tests for the Python-side ``TaskInfo.files`` attach surface (#336 P2).

Single concern: pin the public Python boundary a consumer crosses when
declaring the files a WORK task needs UPLOADED before it runs. The PyO3
bridge (where ``files`` becomes the Rust ``Vec<UploadFileRef>`` on
``required_files``, then the framework DEDUPS each unique ``(source, dest)``
into ONE upload setup task) is covered by the python-feature gated Rust
tests in ``crates/dynrunner-pyo3/src/pytypes/extract.rs::tests``; these
tests own the Python-side legal-shape set (default empty, verbatim storage
of bare-source and ``(source, dest)`` entries, ``to_dict`` projection).
"""

from __future__ import annotations

from pathlib import Path

from dynamic_runner._shared import BinaryIdentifier, TaskInfo


def _identifier() -> BinaryIdentifier:
    """Minimal stand-in identifier — values are not load-bearing here."""
    return BinaryIdentifier(
        binary_name="bin",
        platform="x86_64",
        compiler="gcc",
        version="12",
        opt_level="O2",
    )


def _make(files: tuple[str | tuple[str, str | None], ...]) -> TaskInfo:
    return TaskInfo(
        path=Path("/x"),
        size=1,
        identifier=_identifier(),
        task_id="build",
        files=files,
    )


class TestTaskInfoFilesShape:
    """``TaskInfo.files`` stores its entries verbatim (no eager coercion)."""

    def test_default_files_is_empty(self) -> None:
        # A task that declares no files behaves exactly as today (the common
        # pre-#336 case): the framework derives no upload setup tasks.
        info = TaskInfo(
            path=Path("/x"),
            size=1,
            identifier=_identifier(),
            task_id="plain",
        )
        assert info.files == ()

    def test_bare_source_entries_preserved(self) -> None:
        # A bare source path (the common case — destination derived).
        info = _make(("/src/a", "/src/b"))
        assert info.files == ("/src/a", "/src/b")

    def test_source_dest_pair_entries_preserved(self) -> None:
        # A (source, dest) pair for explicit placement of a shared resource.
        info = _make((("/src/a", "/dst/a"), ("/src/b", None)))
        assert info.files == (("/src/a", "/dst/a"), ("/src/b", None))

    def test_mixed_entries_preserved(self) -> None:
        # Bare source + (source, dest) pair mixed in one declaration.
        info = _make(("/src/a", ("/src/b", "/dst/b")))
        assert info.files[0] == "/src/a"
        assert info.files[1] == ("/src/b", "/dst/b")


class TestTaskInfoFilesToDict:
    """``TaskInfo.to_dict`` renders ``files`` JSON-friendly.

    A bare source stays a string; a ``(source, dest)`` tuple renders as a
    2-element list (JSON has no tuple). The on-disk projection mirrors the
    legal-shape set the PyO3 extractor accepts (attribute-based, not
    JSON-based — the two paths share shapes, not encoders).
    """

    def test_to_dict_renders_bare_and_pair_entries(self) -> None:
        info = _make(("/src/a", ("/src/b", "/dst/b"), ("/src/c", None)))
        as_dict = info.to_dict()
        assert as_dict["files"] == [
            "/src/a",
            ["/src/b", "/dst/b"],
            ["/src/c", None],
        ]

    def test_to_dict_empty_files_renders_empty_list(self) -> None:
        info = TaskInfo(
            path=Path("/x"),
            size=1,
            identifier=_identifier(),
            task_id="plain",
        )
        assert info.to_dict()["files"] == []
