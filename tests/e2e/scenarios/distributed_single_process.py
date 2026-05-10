"""Scenario: in-process distributed pipeline (--multi-computer single-process).

Single concern: pin the regression in which the in-process
distributed manager skipped initial StageFile staging, causing 100%
of tasks to fail with ``file_hash X not pre-staged at <name>;
expected StageFile notification first``.

Mechanic
--------

Force the dispatch into ``--multi-computer single-process`` mode
regardless of ``env.mode`` (the cluster is irrelevant for this code
path; the primary + N secondaries run in one process via in-memory
channels) and run the canonical produce/consume consumer with
``--jobs 2`` so the per-secondary fan-out of the staging walk
("two secondaries, one StageFile each per binary") is exercised, not
just a single-secondary edge case.

Why the override
----------------

The bug under test lives entirely in the in-process distributed
pipeline (``PyDistributedManager::run`` in ``dynrunner-pyo3``).
SLURM mode goes through a different staging entrypoint (the SLURM
pipeline's explicit ``coord.queue_initial_staging(...)`` pre-call).
Network-local mode (``--multi-computer local``) has its own
unrelated coverage gap that's tracked separately. So this scenario
runs single-process even when the operator passed ``--mode slurm``,
ensuring the gate fires regardless of the wider e2e harness mode.

Why ``--jobs 2``
----------------

The staging walk fans entries out across ``num_secondaries``. A
``--jobs 1`` scenario would exercise the loop body but not the
fan-out shape. ``--jobs 2`` is the smallest config that catches a
fan-out off-by-one (e.g. an accidental ``num_secondaries - 1``).
"""

from __future__ import annotations

import dataclasses
from pathlib import Path

from ._assertions import assert_files_present, expected_canonical_outputs
from ._base import DispatchEnv, Scenario, ScenarioPlan, ScenarioResult
from ._dispatch import build_dispatch_argv
from ._staging import stage_inputs


_NUM_TASKS_PER_PHASE = 2
_JOBS = 2


class DistributedSingleProcessScenario(Scenario):
    name = "distributed-single-process"
    description = (
        "In-process distributed pipeline with --jobs 2. Pins the "
        "StageFile-staging regression in PyDistributedManager::run "
        "(asm-tokenizer reported 100% task failures at HEAD 2f30920)."
    )
    requires = ()

    def prepare(
        self, env: DispatchEnv, tmp_root: Path
    ) -> list[ScenarioPlan]:
        paths = stage_inputs(tmp_root, _NUM_TASKS_PER_PHASE)
        # Force single-process mode for this scenario regardless of
        # the harness's --mode. The bug under test is exclusive to
        # the in-process distributed pipeline; running it under
        # SLURM would route through a different staging path and
        # mask the failure.
        local_env = dataclasses.replace(env, mode="single-process")
        argv = build_dispatch_argv(
            env=local_env,
            source=paths.source,
            output=paths.output,
            num_tasks=_NUM_TASKS_PER_PHASE,
            jobs=_JOBS,
        )
        # `build_dispatch_argv` only emits `--jobs` in slurm mode;
        # the in-process pipeline reads `args.jobs` to decide
        # `num_secondaries`, so we have to append it explicitly here.
        # See `python/dynamic_runner/run.py::_dispatch_single_process`
        # — `num_secondaries = args.jobs if args.jobs else 1`.
        argv += ["--jobs", str(_JOBS)]
        return [ScenarioPlan(argv=argv, paths=paths)]

    def assert_outputs(
        self, env: DispatchEnv, results: list[ScenarioResult]
    ) -> tuple[bool, list[str]]:
        del env
        result = results[0]
        if result.exit_code != 0:
            return (
                False,
                [
                    f"dispatch exited non-zero: {result.exit_code} "
                    f"(see {result.log_file})"
                ],
            )
        return assert_files_present(
            result.plan.paths.publish_dst,
            expected_canonical_outputs(_NUM_TASKS_PER_PHASE),
        )


SCENARIO = DistributedSingleProcessScenario()
