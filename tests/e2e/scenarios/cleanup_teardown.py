"""Scenario: post-run worker /tmp cleanup.

Single concern: assert ``/tmp/asm-*`` (and other framework scratch
dirs) are empty on every worker after a successful run.

Mechanic
--------

Run a normal dispatch, wait for it to complete, then submit a
``srun`` job to each worker that lists any leftover scratch dirs
under ``/tmp``. The L4.9 fix wrapped the cleanup in
``podman unshare rm -rf`` so subuid-mapped state is reachable; if
that fix regresses, the cleanup leaves stale dirs behind and the
srun output catches it.

Dependency on L4.9
------------------

Pre-L4.9, ``rm -rf`` on subuid-mapped paths fails with EACCES and
the cleanup leaves leftovers. The scenario fails loudly there
(showing the leftover paths). Post-L4.9 cleanup succeeds and the
scenario passes.

Why srun rather than ssh-into-the-worker
----------------------------------------

The slurm-test-env's worker containers don't accept the operator's
SSH key for the dispatcher's user (the per-cluster keypair is
authorized on the gateway via ``provision-user``, but the
PubkeyAuthentication chain to workers via ProxyJump fails when the
gateway-side ssh client doesn't have access to the operator's
private key). srun, on the other hand, submits via the slurm
controller and runs on the worker as the dispatcher user — uses
the same auth path as the framework's actual job dispatch.
"""

from __future__ import annotations

from pathlib import Path

from ._assertions import assert_files_present, expected_canonical_outputs
from ._base import DispatchEnv, Scenario, ScenarioPlan, ScenarioResult
from ._dispatch import build_dispatch_argv
from ._ssh import gateway_ssh
from ._staging import stage_inputs


_NUM_TASKS_PER_PHASE = 4
# Patterns whose presence under /tmp on a worker indicates the
# framework forgot to clean up. Conservative — any of these surfacing
# is a fail.
_LEFTOVER_GLOBS = ("/tmp/asm-*", "/tmp/dynrunner-*")


class CleanupTeardownScenario(Scenario):
    name = "cleanup-teardown"
    description = (
        "Asserts /tmp/asm-* and /tmp/dynrunner-* directories are gone "
        "from every worker after a successful run. Catches a "
        "regression of the L4.9 podman-unshare cleanup fix."
    )
    requires = ("L4.9-cleanup-podman-unshare",)

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

    def assert_outputs(
        self, env: DispatchEnv, results: list[ScenarioResult]
    ) -> tuple[bool, list[str]]:
        result = results[0]
        ok_present, missing = assert_files_present(
            result.plan.paths.publish_dst,
            expected_canonical_outputs(_NUM_TASKS_PER_PHASE),
        )
        if not ok_present:
            return (False, missing)

        # One srun-per-worker via `sbatch --array` would interleave
        # output; use plain `srun` per worker so we can attribute
        # leftovers cleanly. The slurm-test-env's `slurm-worker[1..N]`
        # are the only nodes; we pin srun to each in turn.
        glob_args = " ".join(_LEFTOVER_GLOBS)
        # Single-line shell: list leftovers (silent on no-match), prefix
        # each line with the worker hostname so the assertion can
        # attribute it.
        srun_cmd = (
            f"sh -c 'ls -d {glob_args} 2>/dev/null | sed s,^,$(hostname): ,"
            f" || true'"
        )

        leftovers: list[str] = []
        for worker_idx in range(env.workers):
            worker_hostname = f"slurm-worker{worker_idx + 1}"
            # `srun -w <node>` pins the step to the named worker.
            # `--immediate` fails fast if the node is busy rather than
            # blocking; for a post-run check the node should be idle.
            full_cmd = (
                f"srun --partition=debug "
                f"--nodelist={worker_hostname} "
                f"--ntasks=1 --cpus-per-task=1 --time=00:01:00 "
                f"--quiet "
                f"{srun_cmd}"
            )
            proc = gateway_ssh(env, full_cmd, timeout_s=60)
            if proc.returncode != 0:
                leftovers.append(
                    f"worker{worker_idx}: srun failed (rc="
                    f"{proc.returncode}) — cannot verify cleanup; "
                    f"stderr: {proc.stderr.strip()!r}"
                )
                continue
            for line in proc.stdout.splitlines():
                line = line.strip()
                if line:
                    leftovers.append(line)
        if leftovers:
            return (False, leftovers)
        return (True, [])


SCENARIO = CleanupTeardownScenario()
