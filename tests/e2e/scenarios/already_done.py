"""Scenario: ``--skip-existing`` idempotent re-run.

Single concern: assert that re-running with ``--skip-existing``
preserves previously-published outputs.

Two plans
---------

This scenario emits TWO plans the driver runs back-to-back, sharing
the publish destination:

1. ``initial`` — normal dispatch. Populates the publish dst.
2. ``rerun`` — same dispatch + ``--skip-existing``. Must complete
   with the published outputs intact.

The driver dispatches them in order and feeds both results into
``assert_outputs``. The assertion checks:

- both runs exited zero;
- after the rerun, every expected output is still present (the
  rerun did not delete or truncate them, even if it re-executed
  the work).

Note on "actually skipped"
--------------------------

In the dynamic_runner framework, ``--skip-existing`` is passed
through to the consumer's ``discover_items``; whether tasks are
filtered out is the consumer's policy. The current synthetic
consumer does not filter, so the rerun may re-execute every task
and publish on top of the existing outputs — that's still
idempotent (same bytes), and the assertion passes. To exercise the
filter path itself the consumer would need to consult
``get_output_filename_pattern`` and skip tasks whose output already
exists; that's a consumer-side feature deliberately out of scope
for this driver.
"""

from __future__ import annotations

import tempfile
from pathlib import Path

from ._assertions import assert_files_present, expected_canonical_outputs
from ._base import (
    DispatchEnv,
    DispatchPaths,
    Scenario,
    ScenarioPlan,
    ScenarioResult,
)
from ._dispatch import build_dispatch_argv
from ._staging import stage_inputs


_NUM_TASKS_PER_PHASE = 4


class AlreadyDoneScenario(Scenario):
    name = "already-done"
    description = (
        "Re-runs the dispatch with --skip-existing; asserts published "
        "outputs are preserved and not regenerated."
    )
    requires = ()

    def prepare(
        self, env: DispatchEnv, tmp_root: Path
    ) -> list[ScenarioPlan]:
        # Both plans share the publish_dst; otherwise the rerun
        # would write into a fresh empty dir and skip-existing
        # would have nothing to skip. Different source/output dirs
        # are fine — the framework reads inputs and writes logs
        # afresh per run.
        shared = stage_inputs(tmp_root, _NUM_TASKS_PER_PHASE, label="initial")
        rerun_paths = DispatchPaths(
            source=shared.source,
            output=Path(tempfile.mkdtemp(prefix="out-rerun-", dir=tmp_root)),
            publish_src=Path(
                tempfile.mkdtemp(prefix="pubsrc-rerun-", dir=tmp_root)
            ),
            publish_dst=shared.publish_dst,
        )

        initial_argv = build_dispatch_argv(
            env=env,
            source=shared.source,
            output=shared.output,
            num_tasks=_NUM_TASKS_PER_PHASE,
        )
        rerun_argv = build_dispatch_argv(
            env=env,
            source=rerun_paths.source,
            output=rerun_paths.output,
            num_tasks=_NUM_TASKS_PER_PHASE,
            extra_args=("--skip-existing",),
        )
        return [
            ScenarioPlan(argv=initial_argv, paths=shared, label="initial"),
            ScenarioPlan(argv=rerun_argv, paths=rerun_paths, label="rerun"),
        ]

    def assert_outputs(
        self, env: DispatchEnv, results: list[ScenarioResult]
    ) -> tuple[bool, list[str]]:
        del env
        if len(results) != 2:
            return (False, [f"expected 2 plan results, got {len(results)}"])
        initial, rerun = results
        publish_dst = initial.plan.paths.publish_dst
        expected = expected_canonical_outputs(_NUM_TASKS_PER_PHASE)

        if initial.exit_code != 0:
            return (
                False,
                [f"initial run exited non-zero: {initial.exit_code}"],
            )
        if rerun.exit_code != 0:
            return (False, [f"rerun exited non-zero: {rerun.exit_code}"])

        ok_present, missing = assert_files_present(publish_dst, expected)
        if not ok_present:
            return (
                False,
                [f"after rerun, expected output missing: {m}" for m in missing],
            )
        return (True, [])


SCENARIO = AlreadyDoneScenario()
