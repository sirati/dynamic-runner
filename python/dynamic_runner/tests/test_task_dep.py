"""Unit tests for the Python-side ``TaskDep`` dataclass.

Single concern: pin the public Python boundary that consumers cross
when expressing structured task dependencies. The PyO3 bridge (where
``str`` and ``TaskDep`` instances become a Rust ``Vec<TaskDep>``) is
covered by the python-feature gated Rust tests in
``crates/dynrunner-pyo3/src/pytypes/extract.rs::tests``; these tests
own the Python-side legal-shape set (default ``inherit_outputs``,
frozen-dataclass equality, ``to_dict`` JSON projection).
"""

from __future__ import annotations

from pathlib import Path

import pytest

from dynamic_runner._shared import BinaryIdentifier, TaskDep, TaskInfo


def _identifier() -> BinaryIdentifier:
    """Minimal stand-in identifier — values are not load-bearing here."""
    return BinaryIdentifier(
        binary_name="bin",
        platform="x86_64",
        compiler="gcc",
        version="12",
        opt_level="O2",
    )


class TestTaskDepShape:
    """The Python-side dataclass contract.

    ``TaskDep`` is frozen + hashable so it can live in tuples or sets
    without losing referential stability. Default ``inherit_outputs``
    must remain ``False`` so a bare ``TaskDep("id")`` and a bare
    string ``"id"`` carry identical semantics through the bridge.
    """

    def test_default_inherit_outputs_is_false(self) -> None:
        dep = TaskDep(task_id="A")
        assert dep.task_id == "A"
        assert dep.inherit_outputs is False

    def test_inherit_outputs_explicit_true(self) -> None:
        dep = TaskDep(task_id="B", inherit_outputs=True)
        assert dep.inherit_outputs is True

    def test_default_phase_id_is_empty(self) -> None:
        # Empty phase_id == "same phase as the declaring task" (resolved
        # at the PyO3 boundary); a non-empty value names a cross-phase
        # prerequisite explicitly.
        dep = TaskDep(task_id="A")
        assert dep.phase_id == ""

    def test_explicit_cross_phase_phase_id(self) -> None:
        dep = TaskDep(task_id="A", phase_id="other-phase")
        assert dep.phase_id == "other-phase"

    def test_frozen_dataclass_rejects_mutation(self) -> None:
        # Frozen dataclasses must raise FrozenInstanceError on attempted
        # mutation; otherwise ``TaskDep`` could not safely be a hashable
        # set/dict key (and the consumer's tuple of deps could be
        # mutated under the framework's nose).
        from dataclasses import FrozenInstanceError

        dep = TaskDep(task_id="A")
        with pytest.raises(FrozenInstanceError):
            dep.task_id = "X"  # type: ignore[misc]
        with pytest.raises(FrozenInstanceError):
            dep.inherit_outputs = True  # type: ignore[misc]

    def test_equality_and_hash(self) -> None:
        # Same fields ⇒ equal AND share hash. Different fields ⇒
        # inequal. Hashing pins membership-test cost at O(1) for
        # set-of-TaskDep consumers.
        a1 = TaskDep("A")
        a2 = TaskDep("A", inherit_outputs=False)
        b = TaskDep("A", inherit_outputs=True)
        c = TaskDep("C")
        assert a1 == a2
        assert hash(a1) == hash(a2)
        assert a1 != b
        assert a1 != c
        assert {a1, a2, b, c} == {a1, b, c}


class TestTaskInfoTaskDependsOnShape:
    """TaskInfo.task_depends_on accepts mixed bare-string + TaskDep.

    The PyO3 extractor (``crates/dynrunner-pyo3/src/pytypes/extract.rs``)
    handles the duck-typed bridge; these tests pin the Python-side
    contract: a consumer can populate ``task_depends_on`` with either
    shape and the dataclass stores them verbatim (no eager coercion).
    """

    def _make(
        self, task_depends_on: tuple[TaskDep | str, ...]
    ) -> TaskInfo:
        return TaskInfo(
            path=Path("/x"),
            size=1,
            identifier=_identifier(),
            task_id="C",
            task_depends_on=task_depends_on,
        )

    def test_bare_string_entries_preserved(self) -> None:
        """The historical ``tuple[str, ...]`` shape still works."""
        info = self._make(("A", "B"))
        assert info.task_depends_on == ("A", "B")

    def test_taskdep_entries_preserved(self) -> None:
        """``TaskDep`` instances pass through unchanged."""
        deps = (
            TaskDep("A"),
            TaskDep("B", inherit_outputs=True),
        )
        info = self._make(deps)
        assert info.task_depends_on == deps
        assert info.task_depends_on[1].inherit_outputs is True

    def test_mixed_entries_preserved(self) -> None:
        """Bare-string + ``TaskDep`` mix in the same tuple.

        The most common consumer pattern — a downstream task that
        depends on its immediate predecessor (legacy bare-string) PLUS
        one ancestor it wants ``inherit_outputs`` from. Both arms must
        round-trip through the dataclass storage without coercion.
        """
        info = self._make(("A", TaskDep("B", inherit_outputs=True)))
        assert info.task_depends_on[0] == "A"
        assert info.task_depends_on[1] == TaskDep(
            "B", inherit_outputs=True
        )

    def test_to_dict_renders_struct_shape_for_taskdep(self) -> None:
        """``TaskInfo.to_dict`` emits the JSON-canonical struct shape.

        The Rust-side untagged ``TaskDepWire`` decoder accepts both
        bare-strings and ``{"task_id": ..., "inherit_outputs": ...}``
        dicts in the same array. Our to_dict projection picks the
        struct shape only when ``inherit_outputs`` matters
        (i.e. when the entry is a ``TaskDep``) so a wire snapshot
        produced from a Python TaskInfo round-trips through
        ``serde_json::from_value`` cleanly without losing the flag.
        """
        info = self._make(("A", TaskDep("B", inherit_outputs=True)))
        as_dict = info.to_dict()
        assert as_dict["task_depends_on"] == [
            "A",
            {"task_id": "B", "phase_id": "", "inherit_outputs": True},
        ]

    def test_to_dict_default_inherit_outputs_still_renders(self) -> None:
        """A ``TaskDep`` with default ``inherit_outputs=False`` still
        renders as the struct shape — keeping the on-disk JSON
        unambiguous about which entries used the dataclass form vs the
        legacy bare-string form. The untagged ``TaskDepWire`` decoder
        on the Rust side handles both arms equivalently.
        """
        info = self._make((TaskDep("A"),))
        as_dict = info.to_dict()
        assert as_dict["task_depends_on"] == [
            {"task_id": "A", "phase_id": "", "inherit_outputs": False},
        ]
