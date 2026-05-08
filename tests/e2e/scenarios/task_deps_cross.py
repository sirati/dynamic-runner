"""Scenario: cross-phase task dependencies.

Single concern: assert ``task_depends_on`` works across phase
boundaries. The phase barrier alone would suffice to keep
``consume-{i}`` from running before ``produce-{i}``, so this scenario
counts on the consumer's per-task ``expects_output`` payload check
at the worker side: if the framework dispatches ``consume-{i}``
before ``produce-{i}`` published, the worker raises
``NonRecoverableError`` (see :func:`tests.e2e.test_consumer.worker._consume`).

The produce phase intra-chain is disabled here in spirit (the
canonical consumer wires ``produce-{i}`` ← ``produce-{i-1}``
unconditionally; we don't override that — the cross-phase edge is
the focus, the intra-chain is incidental coverage). Bigger N pushes
the framework's blocked-map deeper.
"""

from __future__ import annotations

from pathlib import Path

from ._assertions import assert_files_present, expected_canonical_outputs
from ._base import DispatchEnv, Scenario, ScenarioPlan, ScenarioResult
from ._dispatch import build_dispatch_argv
from ._staging import stage_inputs


_NUM_TASKS_PER_PHASE = 16


class TaskDepsCrossScenario(Scenario):
    name = "task-deps-cross"
    description = (
        "Cross-phase task_depends_on edges (consume-{i} depends on "
        "produce-{i}). Asserts the worker never sees its predecessor's "
        "output missing — failure indicates the cross-phase blocked-map "
        "is broken."
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


SCENARIO = TaskDepsCrossScenario()
