"""Unit tests for the Python-side ``TaskInfo.is_secondary_affine`` kind surface (#497 P6).

Single concern: pin the public Python boundary a consumer crosses when
declaring a SecondaryAffine gate task. The PyO3 bridge (where
``is_secondary_affine`` maps to the Rust ``TaskKind::SecondaryAffine`` at the
single kind-selector site) is covered by the python-feature gated Rust tests in
``crates/dynrunner-pyo3/src/pytypes/task_info.rs::tests`` and
``extract.rs::tests``; these tests own the Python-side dataclass surface
(default ``False``, verbatim storage, ``to_dict`` projection).
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


class TestTaskInfoSecondaryAffineShape:
    """``TaskInfo.is_secondary_affine`` stores the kind bool verbatim."""

    def test_default_is_secondary_affine_is_false(self) -> None:
        # A task that declares nothing is an ordinary WORK task (the common
        # case): the kind bool defaults to False.
        info = TaskInfo(
            path=Path("/x"),
            size=1,
            identifier=_identifier(),
            task_id="plain",
        )
        assert info.is_secondary_affine is False

    def test_is_secondary_affine_true_stored(self) -> None:
        # A SecondaryAffine gate task: the per-secondary import GATE. It uses
        # setup_affinity=None and carries its deps (empty here — ready at
        # spawn).
        info = TaskInfo(
            path=Path("/x"),
            size=1,
            identifier=_identifier(),
            task_id="import-gate",
            is_secondary_affine=True,
        )
        assert info.is_secondary_affine is True
        assert info.setup_affinity is None
        assert info.task_depends_on == ()

    def test_is_secondary_affine_with_upload_dep_stored(self) -> None:
        # Compose with #336: the gate depends on an upload setup-task id via a
        # plain task_depends_on edge (no new dep machinery). Downstream builds
        # depend on this gate's task_id.
        info = TaskInfo(
            path=Path("/x"),
            size=1,
            identifier=_identifier(),
            task_id="import-gate",
            is_secondary_affine=True,
            task_depends_on=("upload-archive",),
        )
        assert info.is_secondary_affine is True
        assert info.task_depends_on == ("upload-archive",)


class TestTaskInfoSecondaryAffineToDict:
    """``TaskInfo.to_dict`` projects ``is_secondary_affine`` alongside ``is_setup``."""

    def test_to_dict_carries_is_secondary_affine_true(self) -> None:
        info = TaskInfo(
            path=Path("/x"),
            size=1,
            identifier=_identifier(),
            task_id="import-gate",
            is_secondary_affine=True,
        )
        as_dict = info.to_dict()
        assert as_dict["is_secondary_affine"] is True
        # Mutually exclusive with is_setup — the default stays False.
        assert as_dict["is_setup"] is False

    def test_to_dict_default_is_secondary_affine_false(self) -> None:
        info = TaskInfo(
            path=Path("/x"),
            size=1,
            identifier=_identifier(),
            task_id="plain",
        )
        assert info.to_dict()["is_secondary_affine"] is False
