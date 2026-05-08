"""Run a single :class:`ScenarioPlan` end-to-end.

Single concern: spawn the dispatch process, tee its output to a log
file, return a :class:`ScenarioResult` describing what happened.

This module sits between the driver (which iterates scenarios) and
the heartbeat / scenario hooks (which observe a running dispatch).
The dispatch's child PID is exposed to a per-plan
``run_hook_pid_callback`` so scenarios with mid-run intervention
(worker-death-failover) can target the correct process.
"""

from __future__ import annotations

import os
import subprocess
import sys
import time
from pathlib import Path
from typing import Callable

from .scenarios._base import ScenarioPlan, ScenarioResult


def run_plan(
    plan: ScenarioPlan,
    *,
    log_file: Path,
    repo_root: Path,
    timeout_s: int,
    log_activity_callback: Callable[[float], None] | None = None,
    spawn_callback: Callable[[int], None] | None = None,
) -> ScenarioResult:
    """Spawn and supervise the dispatch process.

    Parameters
    ----------
    plan
        The plan to run. ``plan.argv`` is the full command;
        ``plan.extra_env`` is overlaid on the OS env.
    log_file
        Where to tee stdout/stderr. Created if absent.
    repo_root
        ``cwd`` for the child. Needed because the consumer is
        invoked as ``python -m tests.e2e.test_consumer`` and the
        ``-m`` import looks for ``tests/`` on ``sys.path``.
    timeout_s
        Hard wallclock cap. On exceed, returns a ScenarioResult with
        ``exit_code=124`` (matches ``timeout(1)`` convention).
    log_activity_callback
        Optional callback invoked once per output line with the
        monotonic timestamp of the line. The driver's heartbeat
        progress check uses this to know whether the dispatch is
        making progress.
    spawn_callback
        Optional callback invoked with the child PID immediately
        after spawn. Scenarios with run hooks (worker-death-failover)
        use this to start their side-thread.
    """
    env = os.environ.copy()
    env["DYNRUNNER_PUBLISH_SRC_ROOT"] = str(plan.paths.publish_src)
    env["DYNRUNNER_PUBLISH_DST_ROOT"] = str(plan.paths.publish_dst)
    env.setdefault("PYTHONPATH", str(repo_root))
    env.update(plan.extra_env)

    print(f"[dispatch] argv: {' '.join(plan.argv)}", flush=True)
    print(f"[dispatch] log:  {log_file}", flush=True)

    log_file.parent.mkdir(parents=True, exist_ok=True)
    started = time.monotonic()
    with log_file.open("w", buffering=1) as logf:
        proc = subprocess.Popen(
            plan.argv,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            env=env,
            cwd=str(repo_root),
        )
        if spawn_callback is not None:
            spawn_callback(proc.pid)

        deadline = started + timeout_s
        try:
            assert proc.stdout is not None
            for line in proc.stdout:
                logf.write(line)
                logf.flush()
                sys.stdout.write(line)
                sys.stdout.flush()
                if log_activity_callback is not None:
                    log_activity_callback(time.monotonic())
                if time.monotonic() > deadline:
                    print(
                        "[dispatch] TIMEOUT: dispatch exceeded budget; "
                        "killing child",
                        flush=True,
                    )
                    proc.kill()
                    return ScenarioResult(
                        plan=plan,
                        exit_code=124,
                        log_file=log_file,
                        duration_s=time.monotonic() - started,
                    )
            rc = proc.wait(timeout=max(1.0, deadline - time.monotonic()))
        except subprocess.TimeoutExpired:
            proc.kill()
            return ScenarioResult(
                plan=plan,
                exit_code=124,
                log_file=log_file,
                duration_s=time.monotonic() - started,
            )

    return ScenarioResult(
        plan=plan,
        exit_code=rc,
        log_file=log_file,
        duration_s=time.monotonic() - started,
    )


__all__ = ["run_plan"]
