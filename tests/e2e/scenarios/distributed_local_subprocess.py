"""Scenario: network-primary + local-subprocess secondaries (--multi-computer local).

Single concern: pin the regression in which the Python caller of
``run_primary`` (in ``run.py::_dispatch_multi_computer_local``) forgot
to thread ``source_dir`` through to the Rust auto-stage pass, leaving
secondaries to reject every task with ``file_hash X not pre-staged at
<name>; expected StageFile notification first``.

Mechanic
--------

Force the dispatch into ``--multi-computer local`` mode regardless of
``env.mode`` (the slurm cluster is irrelevant for this code path; the
primary runs locally and spawns each secondary via
``build_subprocess_spawn`` as an in-process ``python -m
{secondary_module}`` ``subprocess.Popen``) and run the canonical
produce/consume consumer with ``--jobs 2`` so the per-secondary
fan-out of the staging walk ("two secondaries, one StageFile each per
binary") is exercised, not just a single-secondary edge case.

Why the override
----------------

The bug under test lives in the Python ``_dispatch_multi_computer_local``
caller (commit 04a3aef adds the missing ``source_dir=...`` kwarg). It
surfaces only when the dispatch goes through that exact branch —
``--multi-computer single-process`` routes through ``run_distributed``
and SLURM mode through ``run_slurm_pipeline``, neither of which touch
the ``run_primary`` staging path. So this scenario forces the local
dispatch even when the operator passed ``--mode slurm``, ensuring the
gate fires regardless of the wider e2e harness mode.

Why ``--jobs 2``
----------------

The staging walk fans entries out across ``num_secondaries``. A
``--jobs 1`` scenario would exercise the loop body but not the
fan-out shape. ``--jobs 2`` is the smallest config that catches a
fan-out off-by-one (e.g. an accidental ``num_secondaries - 1``) and
mirrors the ``--jobs 2`` smoke that asm-tokenizer ran when the
regression first surfaced (484/484 NonRecoverable at 55dbf15).

Prerequisites
-------------

The local-subprocess spawn shells out to ``python -m
tests.e2e.test_consumer`` for each secondary. That import resolves
against the same Python environment the dispatcher itself runs in;
no podman image build is required. The scenario inherits the
operator's PATH/PYTHONPATH via the dispatcher process, matching the
single-process scenario's lightweight-as-possible posture.
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


class DistributedLocalSubprocessScenario(Scenario):
    name = "distributed-local-subprocess"
    description = (
        "Network primary + local-subprocess secondaries with --jobs 2. "
        "Pins the source_dir-threading regression in "
        "_dispatch_multi_computer_local (asm-tokenizer reported "
        "484/484 NonRecoverable at 55dbf15; fixed in 04a3aef)."
    )
    requires = ()

    def prepare(
        self, env: DispatchEnv, tmp_root: Path
    ) -> list[ScenarioPlan]:
        paths = stage_inputs(tmp_root, _NUM_TASKS_PER_PHASE)
        # Force local mode for this scenario regardless of the
        # harness's --mode. The bug under test is exclusive to the
        # `_dispatch_multi_computer_local` Python caller; running it
        # under SLURM or single-process would route through a
        # different dispatch path and mask the failure.
        local_env = dataclasses.replace(env, mode="local")
        argv = build_dispatch_argv(
            env=local_env,
            source=paths.source,
            output=paths.output,
            num_tasks=_NUM_TASKS_PER_PHASE,
            jobs=_JOBS,
        )
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


SCENARIO = DistributedLocalSubprocessScenario()
