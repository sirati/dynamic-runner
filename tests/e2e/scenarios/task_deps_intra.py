"""Scenario: 50+ intra-phase task dependencies.

Single concern: assert the framework respects intra-phase
``task_depends_on`` ordering at scale.

Each ``produce-{i}`` declares ``task_depends_on=("produce-{i-1}",)``
in the canonical consumer (see
:mod:`tests.e2e.test_consumer.task`), so a 50-task chain forces the
framework's PendingPool to process a deep linear graph. If any task
runs before its predecessor finished publishing, the consumer's
worker raises ``NonRecoverableError`` and the dispatch exits non-zero.

The consume phase doubles the task count, so we run 100 tasks total
to keep both the intra-phase chain (produce side) and the
phase barrier (consume side) under load at once.
"""

from __future__ import annotations

from pathlib import Path

from ._assertions import assert_files_present, expected_canonical_outputs
from ._base import DispatchEnv, Scenario, ScenarioPlan, ScenarioResult
from ._dispatch import build_dispatch_argv
from ._staging import stage_inputs


_NUM_TASKS_PER_PHASE = 50


class TaskDepsIntraScenario(Scenario):
    name = "task-deps-intra"
    description = (
        f"{_NUM_TASKS_PER_PHASE}+ tasks per phase with intra-phase "
        "task_depends_on chains. Asserts the framework's PendingPool "
        "processes a deep linear dependency graph correctly."
    )
    requires = ()

    def prepare(
        self, env: DispatchEnv, tmp_root: Path
    ) -> list[ScenarioPlan]:
        paths = stage_inputs(tmp_root, _NUM_TASKS_PER_PHASE)
        argv = build_dispatch_argv(
            env=env,
            source=paths.source,
            output=paths.output,
            num_tasks=_NUM_TASKS_PER_PHASE,
        )
        return [ScenarioPlan(argv=argv, paths=paths)]

    def assert_outputs(
        self, env: DispatchEnv, results: list[ScenarioResult]
    ) -> tuple[bool, list[str]]:
        del env
        result = results[0]
        return assert_files_present(
            result.plan.paths.publish_dst,
            expected_canonical_outputs(_NUM_TASKS_PER_PHASE),
        )


SCENARIO = TaskDepsIntraScenario()
