"""Unit tests for the keyed-outputs scenario.

Single concern: pin the scenario's argv shape so a future refactor of
the dispatch builder, the consumer's ``--keyed-outputs`` flag, or the
scenario module's plan-emission contract cannot silently drop the
keyed-outputs API exercise.

Why a unit test in addition to the live e2e run:
the live run exercises the full dispatch (primary spawn + cluster
state cache + secondary subprocess + framework auto-stage) and is the
authoritative gate, but it requires a working slurm-test-env. A unit
test that drives ``prepare()`` and inspects the argv pins the
scenario's intent at the API surface, so even when the live run is
skipped the regression sensitivity stays under version control.

Mirrors the placement and pattern of
``test_distributed_local_subprocess_scenario.py`` and
``test_source_already_staged_scenario.py``.
"""

from __future__ import annotations

import sys
import tempfile
from pathlib import Path


# Bring repo root onto sys.path so the absolute import works whether
# pytest is invoked from the repo root or from this dir. Mirrors the
# dance ``run_e2e.py`` itself does.
_REPO_ROOT = Path(__file__).resolve().parents[2]
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

from tests.e2e.scenarios._base import DispatchEnv  # noqa: E402
from tests.e2e.scenarios.keyed_outputs import (  # noqa: E402
    SCENARIO,
)


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
    assert SCENARIO.name == "keyed-outputs"


def test_prepare_emits_one_plan() -> None:
    """``prepare()`` returns exactly one dispatch plan.

    Single A → B graph — the scenario doesn't fan out into multiple
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


def test_argv_carries_keyed_outputs_flag() -> None:
    """The load-bearing ``--keyed-outputs`` flag is on the plan's argv.

    Without ``--keyed-outputs`` the consumer's discover_items sets
    ``payload['keyed_outputs'] = False`` for every task, and the
    worker silently skips the publish_string call AND the
    predecessor_outputs assertion. The scenario then degrades to a
    plain cross-phase task-deps run (identical to ``task-deps-cross``)
    and provides zero coverage of the keyed-outputs round-trip — a
    silent regression vector this test exists to gate.
    """
    with tempfile.TemporaryDirectory() as raw_tmp:
        tmp_root = Path(raw_tmp)
        plans = SCENARIO.prepare(_env(), tmp_root)
        plan = plans[0]
        assert "--keyed-outputs" in plan.argv, (
            f"plan argv missing the load-bearing --keyed-outputs "
            f"flag: {plan.argv!r}"
        )


def test_argv_points_at_canonical_consumer() -> None:
    """The dispatch invokes the canonical synthetic consumer module.

    The keyed-outputs API exercise lives in the canonical consumer
    (see ``tests/e2e/test_consumer/worker.py::_produce`` and
    ``_consume``). A scenario that accidentally pointed at a
    different consumer would not exercise it.
    """
    with tempfile.TemporaryDirectory() as raw_tmp:
        tmp_root = Path(raw_tmp)
        plans = SCENARIO.prepare(_env(), tmp_root)
        plan = plans[0]
        joined = " ".join(plan.argv)
        assert "tests.e2e.test_consumer" in joined, (
            f"expected canonical consumer module in argv, got "
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


def test_consumer_payload_carries_keyed_outputs_flag() -> None:
    """The consumer's discover_items propagates --keyed-outputs into
    every task's payload.

    Closes the loop between the scenario (which sets the CLI flag) and
    the worker (which branches on the payload flag). A regression that
    parsed ``--keyed-outputs`` but failed to thread it onto payloads
    would make the consumer behave as if the flag were absent — the
    e2e would then be silently negative-equivalent to the plain
    task-deps-cross scenario. This unit test gates that drift without
    needing the live dispatch.
    """
    import argparse

    from tests.e2e.test_consumer.task import SyntheticTask

    task = SyntheticTask()
    parser = argparse.ArgumentParser()
    task.add_task_arguments(parser)
    args = parser.parse_args(["--num-tasks", "2", "--keyed-outputs"])
    with tempfile.TemporaryDirectory() as raw_tmp:
        source_dir = Path(raw_tmp)
        # discover_items doesn't actually read the files — _probe_size
        # tolerates absent files — so an empty source_dir is fine.
        items = list(task.discover_items(source_dir, args))
    assert items, "discover_items must emit at least one TaskInfo"
    for item in items:
        assert item.payload.get("keyed_outputs") is True, (
            f"item {item.task_id!r}: payload missing keyed_outputs flag "
            f"or set falsey; payload={item.payload!r}"
        )


def test_consumer_payload_default_off_keeps_flag_unset() -> None:
    """Without ``--keyed-outputs`` the payload flag is False.

    Pins the backwards-compat shape — existing scenarios using the
    canonical consumer without opting in must see the same task
    payloads they had pre-feature. A regression that flipped the
    default to True would silently make every scenario exercise the
    keyed-outputs path and slow down the suite (and potentially
    break scenarios whose consume tasks don't have keyed predecessors).
    """
    import argparse

    from tests.e2e.test_consumer.task import SyntheticTask

    task = SyntheticTask()
    parser = argparse.ArgumentParser()
    task.add_task_arguments(parser)
    args = parser.parse_args(["--num-tasks", "2"])
    with tempfile.TemporaryDirectory() as raw_tmp:
        source_dir = Path(raw_tmp)
        items = list(task.discover_items(source_dir, args))
    assert items, "discover_items must emit at least one TaskInfo"
    for item in items:
        assert item.payload.get("keyed_outputs") is False, (
            f"item {item.task_id!r}: payload['keyed_outputs'] must "
            f"default to False; payload={item.payload!r}"
        )
