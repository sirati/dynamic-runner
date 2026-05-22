"""Unit tests for the keyed-outputs-inherit scenario.

Single concern: pin the scenario's argv shape and consumer topology so
a future refactor cannot silently drop the
``TaskDep(inherit_outputs=True)`` API exercise.

Tests that exercise the consumer module's ``discover_items`` skip
when the installed ``dynamic_runner`` predates the ``TaskDep`` export
(``ImportError`` at :func:`InheritSyntheticTask`'s module-level
import). The scenario-level structural pins do not import the
consumer module and therefore run uniformly.

Why a unit test in addition to the live e2e run:
the live run exercises the full distributed dispatch (primary spawn +
cluster state cache + secondary subprocess + transitive-ancestry
walker on the primary) and is the authoritative gate, but it requires
a working slurm-test-env. A unit test that drives ``prepare()`` and
inspects the argv + the consumer's declared topology pins the
scenario's intent at the API surface, so even when the live run is
skipped the regression sensitivity stays under version control.

Mirrors the placement and pattern of ``test_keyed_outputs_scenario.py``
and ``test_distributed_local_subprocess_scenario.py``.
"""

from __future__ import annotations

import sys
import tempfile
from pathlib import Path


# Bring repo root onto sys.path so the absolute imports work whether
# pytest is invoked from the repo root or from this dir. Mirrors the
# dance ``run_e2e.py`` itself does.
_REPO_ROOT = Path(__file__).resolve().parents[2]
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

import pytest  # noqa: E402

from tests.e2e.scenarios._base import DispatchEnv  # noqa: E402
from tests.e2e.scenarios.keyed_outputs_inherit import (  # noqa: E402
    SCENARIO,
)


def _taskdep_available() -> bool:
    """Probe whether the installed ``dynamic_runner`` exposes
    ``TaskDep`` from its public ``_shared`` re-export.

    Returns ``False`` against pre-feature installs (the venv may be
    pinned to a worktree without this change merged yet). The
    scenario-level structural tests do not depend on the consumer
    module and therefore run uniformly; the discover_items test skips
    when the symbol is absent.
    """
    try:
        from dynamic_runner._shared import TaskDep  # noqa: F401
        return True
    except ImportError:
        return False


_HAS_TASKDEP = _taskdep_available()


def _env(mode: str = "slurm") -> DispatchEnv:
    """Minimal DispatchEnv. The scenario respects whatever ``mode`` the
    harness passes (it's a vanilla single-plan scenario, no cross-mode
    overrides like distributed-local-subprocess does).
    """
    return DispatchEnv(
        instance_id="e2e",
        ssh_port=2222,
        slurm_root_folder="/home/e2e-user/dynrunner-e2e",
        workers=4,
        mode=mode,
        ssh_user="e2e-user",
    )


def test_scenario_name_and_registry_match() -> None:
    """Module's ``SCENARIO`` declares the expected name.

    The registry in ``tests/e2e/scenarios/__init__.py`` cross-checks
    this; the unit test pins it independently so a rename surfaces
    here even without importing the registry.
    """
    assert SCENARIO.name == "keyed-outputs-inherit"


def test_prepare_emits_one_plan() -> None:
    """``prepare()`` returns exactly one dispatch plan.

    Single A->B->C chain — the scenario doesn't fan out into multiple
    plans (no cross-mode replay, no rerun phase). Pinning the count
    catches any accidental fan-out that would mask which plan the
    failure actually came from.
    """
    with tempfile.TemporaryDirectory() as raw_tmp:
        tmp_root = Path(raw_tmp)
        plans = SCENARIO.prepare(_env(), tmp_root)
        assert len(plans) == 1, (
            f"expected exactly one plan, got {len(plans)}"
        )


def test_argv_points_at_inherit_consumer() -> None:
    """The dispatch invokes the dedicated inherit consumer module.

    The 3-task A->B->C topology + inherit_outputs assertion plumbing
    lives in ``tests.e2e.inherit_consumer``. A scenario that
    accidentally pointed at the plain 2-phase ``tests.e2e.test_consumer``
    would lose all coverage of the inherit_outputs path (that consumer
    has no C task and no inherit-flag edges).
    """
    with tempfile.TemporaryDirectory() as raw_tmp:
        tmp_root = Path(raw_tmp)
        plans = SCENARIO.prepare(_env(), tmp_root)
        plan = plans[0]
        joined = " ".join(plan.argv)
        assert "tests.e2e.inherit_consumer" in joined, (
            f"expected inherit consumer module in argv, got "
            f"{plan.argv!r}"
        )


def test_inputs_are_staged_under_tmp_root() -> None:
    """The scenario's plan paths point inside the per-scenario tmp root.

    Mirrors the sibling scenario tests' cleanup-safety pin: a scenario
    writing outside the driver-allocated tmpdir would leak.
    """
    with tempfile.TemporaryDirectory() as raw_tmp:
        tmp_root = Path(raw_tmp)
        plans = SCENARIO.prepare(_env(), tmp_root)
        plan = plans[0]
        for path in (
            plan.paths.source,
            plan.paths.output,
            plan.paths.publish_src,
            plan.paths.publish_dst,
        ):
            assert tmp_root in path.parents, (
                f"path {path!r} is not under tmp_root {tmp_root!r}"
            )


def test_staging_creates_a_b_c_input_files() -> None:
    """The scenario stages ``input-{a|b|c}.txt`` files for the chain.

    Unlike the plain ``stage_inputs`` helper (which uses integer-keyed
    ``input-{i}.txt``), the inherit consumer's task discovery expects
    task-id-keyed names. The staging helper in the scenario module
    owns this convention; this test pins the contract so a rename in
    the consumer's :func:`task._input_filename` doesn't silently
    desync from the scenario's staging.
    """
    with tempfile.TemporaryDirectory() as raw_tmp:
        tmp_root = Path(raw_tmp)
        plans = SCENARIO.prepare(_env(), tmp_root)
        source = plans[0].paths.source
        for task_id in ("a", "b", "c"):
            input_path = source / f"input-{task_id}.txt"
            assert input_path.exists(), (
                f"staging did not create {input_path}; the inherit "
                f"consumer's discover_items will _probe_size them as "
                "missing"
            )


@pytest.mark.skipif(
    not _HAS_TASKDEP,
    reason=(
        "installed dynamic_runner does not export TaskDep yet — "
        "the inherit consumer's task.py cannot import "
        "InheritSyntheticTask under a pre-feature framework wheel. "
        "Re-run after `maturin develop --release` in this worktree, "
        "or merge to main and refresh the venv."
    ),
)
def test_inherit_consumer_emits_inherit_outputs_edge() -> None:
    """The inherit consumer's discover_items emits the load-bearing
    ``TaskDep('a', inherit_outputs=True)`` edge on C's
    ``task_depends_on``.

    Closes the loop between the scenario (which points at the consumer
    module) and the framework (which reads C's edge at dispatch time).
    A regression that turned the edge back into a bare-string would
    drop ``inherit_outputs`` to its default ``False`` at the PyO3
    bridge — the e2e would still pass the file-presence check (C
    publishes), but the assertion would never gate inherit_outputs.
    """
    import argparse

    # Import TaskDep from the shared module rather than the top-level
    # ``dynamic_runner`` package — the latter eagerly imports the
    # compiled ``_native`` extension, so a unit-test environment that
    # has not run ``maturin develop`` cannot reach that surface. The
    # ``_shared`` re-export is the same dataclass; consumer code can
    # use either path interchangeably.
    from dynamic_runner._shared import TaskDep
    from tests.e2e.inherit_consumer.task import InheritSyntheticTask

    task = InheritSyntheticTask()
    parser = argparse.ArgumentParser()
    task.add_task_arguments(parser)
    args = parser.parse_args([])
    with tempfile.TemporaryDirectory() as raw_tmp:
        source_dir = Path(raw_tmp)
        # discover_items doesn't actually read the files — _probe_size
        # tolerates absent files — so an empty source_dir is fine.
        items = list(task.discover_items(source_dir, args))
    assert len(items) == 3, (
        f"expected 3 items in A->B->C chain, got {len(items)}"
    )
    by_id = {item.task_id: item for item in items}
    # The A task has no predecessors.
    assert by_id["a"].task_depends_on == ()
    # The B task has a single bare-string predecessor (legacy shape).
    assert by_id["b"].task_depends_on == ("a",), (
        f"B's task_depends_on shape changed: {by_id['b'].task_depends_on!r}"
    )
    # The C task carries the load-bearing inherit edge: a bare-string
    # entry for B (direct predecessor) AND a ``TaskDep`` for A with
    # ``inherit_outputs=True`` (transitive ancestor). Both shapes must
    # round-trip through the PyO3 extractor — and the inherit flag is
    # the whole point of this scenario.
    c_deps = by_id["c"].task_depends_on
    assert len(c_deps) == 2, (
        f"C's task_depends_on length changed: {c_deps!r}"
    )
    assert c_deps[0] == "b"
    assert c_deps[1] == TaskDep("a", inherit_outputs=True), (
        f"C's transitive ancestor edge shape changed: {c_deps[1]!r}"
    )
    assert c_deps[1].inherit_outputs is True
