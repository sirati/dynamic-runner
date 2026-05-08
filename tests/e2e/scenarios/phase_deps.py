"""Scenario: phase dependencies.

Single concern: the cross-phase ``PhaseSpec.depends_on`` edge.

Topology — see :mod:`tests.e2e.test_consumer.task` for the canonical
two-phase consumer used here:

    produce ──depends_on──▶ consume

Even though the consumer technically has two phases, the produce
phase has intra-phase task chains AND the consume phase has explicit
``task_depends_on`` edges, so this single scenario also incidentally
covers the basic shape of ``task-deps-intra`` and ``task-deps-cross``.
The dedicated scenarios for those exercise the *scaled* shape (50+
tasks, deeper chains) — this one is the smoke check that the
phase-dep barrier works at all.

This scenario is intentionally the smallest reasonable run (default
2 tasks per phase) so it doubles as a smoke check for the cluster
itself: if phase-deps fails, the cluster is broken before we even
look at heavier scenarios.
"""

from __future__ import annotations

from pathlib import Path

from ._assertions import assert_files_present, expected_canonical_outputs
from ._base import DispatchEnv, Scenario, ScenarioPlan, ScenarioResult
from ._dispatch import build_dispatch_argv
from ._staging import stage_inputs


_NUM_TASKS_PER_PHASE = 2


class PhaseDepsScenario(Scenario):
    name = "phase-deps"
    description = (
        "Smoke check: 2-phase consumer with explicit "
        "PhaseSpec(depends_on=...). Asserts produce/consume outputs "
        "land at the publish destination."
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


SCENARIO = PhaseDepsScenario()
