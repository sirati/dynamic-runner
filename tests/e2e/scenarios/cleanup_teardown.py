"""Scenario: post-run worker /tmp cleanup.

Single concern: assert ``/tmp/asm-*`` (and other framework scratch
dirs) are empty on every worker after a successful run.

Mechanic
--------

Run a normal dispatch, wait for it to complete, then ssh into each
worker (via the gateway's ProxyJump) and ``ls /tmp`` looking for any
``asm-*`` or ``dynrunner-*`` directories. The L4.9 fix wrapped the
cleanup in ``podman unshare rm -rf`` so subuid-mapped state is
reachable; if that fix regresses, the cleanup leaves stale dirs
behind and this scenario detects it.

Dependency on L4.9
------------------

Pre-L4.9, ``rm -rf`` on subuid-mapped paths fails with EACCES and
the cleanup leaves leftovers. The scenario fails loudly there
(showing the leftover paths). Post-L4.9 cleanup succeeds and the
scenario passes.

NOTE: the scenario uses ``ssh ... ls`` rather than ``podman exec``
because the test driver runs OUTSIDE the rootless-podman scope
that owns the workers; ssh as the cluster user is the only ingress
that exercises the same auth path the framework uses for upload.
"""

from __future__ import annotations

from pathlib import Path

from ._assertions import assert_files_present, expected_canonical_outputs
from ._base import DispatchEnv, Scenario, ScenarioPlan, ScenarioResult
from ._dispatch import build_dispatch_argv
from ._ssh import worker_ssh
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

        # One ssh per worker, listing all configured globs in a
        # single round trip. Each worker's stdout is a newline-list
        # of leftover paths; empty stdout = clean.
        glob_args = " ".join(_LEFTOVER_GLOBS)
        cmd = f"sh -c 'ls -d {glob_args} 2>/dev/null || true'"

        leftovers: list[str] = []
        for worker_idx in range(env.workers):
            proc = worker_ssh(env, worker_idx, cmd, timeout_s=20)
            if proc.returncode != 0:
                leftovers.append(
                    f"worker{worker_idx}: ssh failed (rc="
                    f"{proc.returncode}) — cannot verify cleanup; "
                    f"stderr: {proc.stderr.strip()!r}"
                )
                continue
            for line in proc.stdout.splitlines():
                line = line.strip()
                if line:
                    leftovers.append(f"worker{worker_idx}: {line}")
        if leftovers:
            return (False, leftovers)
        return (True, [])


SCENARIO = CleanupTeardownScenario()
