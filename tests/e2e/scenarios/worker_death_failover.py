"""Scenario: kill one worker mid-run, assert framework recovers.

Single concern: the requeue-on-failover code path.

Mechanic
--------

The scenario emits a normal dispatch plan. The driver fires the
scenario's :meth:`run_hook` in a side thread immediately after
spawning the dispatch; the hook waits a short ``_KILL_DELAY_S`` for
the dispatch to actually start work, then ``ssh``es into the gateway
and ``scancel``s the lowest-numbered job in the user's queue (which
is one of the framework-spawned secondaries).

Three healthy workers must then absorb the killed one's pending
tasks and the run must complete with all expected outputs landing
at the publish destination. If the framework doesn't requeue, we'll
see ``consume-{i}`` with no matching ``produce-{i}`` (because the
dead worker's produce never published) and the assertion fails on
missing outputs.

Why we kill via ``scancel`` not ``podman kill``
------------------------------------------------

``scancel`` exercises the SLURM-aware failover path. ``podman kill``
would also kill the underlying container but bypass SLURM's
job-state machinery, masking framework behaviour with infrastructure
behaviour. The framework's failover triggers on SLURM job-state
transitions, so we trigger one.
"""

from __future__ import annotations

import threading
import time
from pathlib import Path

from ._assertions import assert_files_present, expected_canonical_outputs
from ._base import DispatchEnv, Scenario, ScenarioPlan, ScenarioResult
from ._dispatch import build_dispatch_argv
from ._ssh import gateway_ssh
from ._staging import stage_inputs


_NUM_TASKS_PER_PHASE = 20
_KILL_DELAY_S = 30.0
"""How long to wait after dispatch start before scancel-ing.

Long enough for the framework to have submitted secondaries and at
least one task to be in flight; short enough that the kill genuinely
hits a running worker (not one already mid-shutdown). 30s is a
heuristic — bump if we observe the kill landing too early on a
slow-starting cluster.
"""


class WorkerDeathFailoverScenario(Scenario):
    name = "worker-death-failover"
    description = (
        "Mid-run scancel of one secondary; asserts the framework "
        "requeues the killed worker's tasks and the run completes."
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
            jobs=env.workers,
        )
        return [ScenarioPlan(argv=argv, paths=paths)]

    def run_hook(
        self, env: DispatchEnv, plan: ScenarioPlan, dispatch_pid: int
    ) -> None:
        del plan, dispatch_pid

        def kill_one() -> None:
            time.sleep(_KILL_DELAY_S)
            # squeue --me lists the user's jobs; we cancel the
            # lowest-numbered one. The primary driver runs OUTSIDE
            # the cluster so it isn't in the queue, only the
            # framework-submitted secondaries are.
            cmd = (
                "set -e; "
                "jobid=$(squeue --me --noheader -o '%i' | sort -n | head -1); "
                "[ -n \"$jobid\" ] && scancel --signal=TERM $jobid && "
                "echo killed:$jobid"
            )
            proc = gateway_ssh(env, cmd, timeout_s=20)
            # Hook output is informational — surface it on the
            # driver's stdout so a debugging operator sees what got
            # killed. We don't propagate failures: a failed scancel
            # just means the assertion will (correctly) fail on
            # missing outputs.
            print(
                f"[worker-death-failover] hook: rc={proc.returncode} "
                f"out={proc.stdout.strip()!r} err={proc.stderr.strip()!r}",
                flush=True,
            )

        threading.Thread(
            target=kill_one,
            name="worker-death-killer",
            daemon=True,
        ).start()

    def assert_outputs(
        self, env: DispatchEnv, results: list[ScenarioResult]
    ) -> tuple[bool, list[str]]:
        del env
        result = results[0]
        if result.exit_code != 0:
            return (
                False,
                [
                    f"dispatch exited non-zero (rc={result.exit_code}) — "
                    "framework didn't recover from worker death"
                ],
            )
        return assert_files_present(
            result.plan.paths.publish_dst,
            expected_canonical_outputs(_NUM_TASKS_PER_PHASE),
        )


SCENARIO = WorkerDeathFailoverScenario()
