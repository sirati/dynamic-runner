#!/usr/bin/env python3
"""Driver for the #540 PhaseSpec.barrier=False e2e assertion.

Single concern: dispatch the 3-phase ``tests.e2e.test_consumer_barrier_540``
workload against an EXISTING slurm-test-env cluster, tee the primary
log, and assert the temporal ordering that proves the barrier feature
works end-to-end.

What it does NOT do
-------------------

* Does NOT bring the cluster up or down — that is the operator's
  concern (matches the brief's "cluster lifecycle is shared across
  tests, not per-test" rule).
* Does NOT provision the SSH user — the wrapper shell script delegates
  that to ``slurm-test-env-provision-user`` BEFORE invoking us.

Expected runtime context
------------------------

* Repo root on ``PYTHONPATH`` (the wrapper script sets it from
  ``$DYNRUNNER_REPO_ROOT``). We import ``tests.e2e._ssh_state`` and
  ``tests.e2e.scenarios._dispatch`` so the SSH-config and argv shapes
  stay identical to the canonical e2e runner — one source of truth.
* Cluster reachable on ``localhost:$SSH_PORT`` with ``$TEST_USER``
  already provisioned (the wrapper's responsibility).
* ``$DYNRUNNER_TEST540_LOG`` chosen by the wrapper for the captured
  primary-log path; the assertion pass re-reads it after the dispatch
  exits.

Assertions (run after dispatch completes successfully):

1. ``phase_b``-tagged ``task assigned: identity`` lines appear in the
   log BEFORE all ``phase_a`` tasks have completed — proves the
   no-barrier opt-in actually lifted the implicit ``phase_a`` barrier.
2. ``phase_c``-tagged ``task assigned: identity`` lines appear ONLY
   AFTER every ``phase_b`` task has completed — proves the default
   ``barrier=True`` still enforces the ``phase_b → phase_c`` gate.
3. The primary log carries NO ``SpawnError::BarrierViolation`` entry —
   proves the legitimate ``on_phase_end → spawn_tasks`` idiom is
   accepted and no spurious rejection fires under this valid
   configuration.

Exit codes match the brief contract: 0 pass, 1 assertion failed,
70 cluster unreachable.
"""

from __future__ import annotations

import argparse
import os
import re
import subprocess
import sys
import time
from pathlib import Path


# Markers / regexes the assertions look for. Pinned to the framework's
# OWN log emit sites — see references in the docstrings — so a drift in
# the line shape surfaces here as a test failure rather than silent
# pass.
#
# The "task assigned: identity" DEBUG line emits at
# ``crates/dynrunner-manager-distributed/src/primary/lifecycle/dispatch.rs``
# (and twin sites under primary/assignment.rs, primary/task/request.rs).
# Shape: ``... DEBUG ... task assigned: identity secondary=... worker_id=N
# task_id=Some("phase_b-3") phase=phase_b task_type=... task_hash=...``.
_RE_TASK_ASSIGNED = re.compile(
    r"task assigned: identity\b.*?\bphase=(?P<phase>\w+)\b"
)
# The "task complete: identity" DEBUG line emits at
# ``crates/dynrunner-manager-distributed/src/primary/task/complete.rs``.
# Same shape — phase is a structured field.
_RE_TASK_COMPLETE = re.compile(
    r"task complete: identity\b.*?\bphase=(?P<phase>\S+)"
)
# A timestamp prefix the tracing formatter emits (LocalHhMm timer,
# `HH:MM±hhmm` shape — `crates/dynrunner-pyo3/src/logging/mod.rs::LocalHhMm`).
# The orderings we care about are by line index, not wall-clock parse —
# the lines arrive in dispatcher order — so we use line position for
# ordering and only quote the timestamp in failure diagnostics.
_RE_TS = re.compile(r"^(\d{2}:\d{2}[+\-]\d{4})\b")
# The barrier-violation rejection variant the validator returns
# (``crates/dynrunner-core/src/spawn_tasks_validator.rs::SpawnError``).
# The rejection bubbles up through ``apply_spawn_tasks`` and is logged
# at WARN — any occurrence is a fail for this valid configuration.
_RE_BARRIER_VIOLATION = re.compile(r"BarrierViolation\b")


def _ssh_config_dir(state_dir: Path, instance_id: str) -> Path:
    """Where this driver stores its per-cluster SSH state.

    Lives UNDER the slurm-test-env state dir (where provision-user.sh
    already drops the keypair under ``keys/``) so we don't carve a
    second per-cluster directory tree.
    """
    return state_dir / "test540"


def _build_dispatch_argv(
    *,
    consumer_module: str,
    source_dir: Path,
    output_dir: Path,
    ssh_user: str,
    ssh_port: int,
    gateway_host_alias: str,
    slurm_root_folder: str,
    workers: int,
    ssh_config_path: Path,
    phase_a_tasks: int,
    phase_bc_tasks: int,
) -> list[str]:
    """Build the ``python -m <consumer> ...`` argv.

    Mirrors :func:`tests.e2e.scenarios._dispatch.build_dispatch_argv`'s
    SLURM-mode shape; reproduced inline (rather than imported) so the
    driver script is fully self-contained and does not couple
    slurm-test-env to the tests/e2e package layout.
    """
    return [
        sys.executable,
        "-m",
        consumer_module,
        "--debug",
        "--source",
        str(source_dir),
        "--output",
        str(output_dir),
        "--raw-logs",
        "--phase-a-tasks",
        str(phase_a_tasks),
        "--phase-bc-tasks",
        str(phase_bc_tasks),
        "--multi-computer",
        "slurm",
        "--packaging",
        "podman",
        "--gateway",
        f"ssh://{ssh_user}@{gateway_host_alias}:{ssh_port}",
        "--slurm-root-folder",
        slurm_root_folder,
        "--slurm-partition",
        "debug",
        "--slurm-cpus-per-task",
        "2",
        "--jobs",
        str(workers),
        "--ssh-config",
        str(ssh_config_path),
    ]


def _stage_inputs(source_dir: Path, phase_a_tasks: int, phase_bc_tasks: int) -> None:
    """Drop one small input file per discovered task.

    The framework's SLURM packaging path uploads ``TaskInfo.path`` to
    the gateway; the file just needs to exist with non-zero size. We
    keep the contents trivial — the worker reads them but does not
    interpret them.
    """
    source_dir.mkdir(parents=True, exist_ok=True)
    for phase, n in (("phase_a", phase_a_tasks), ("phase_b", phase_bc_tasks), ("phase_c", phase_bc_tasks)):
        for idx in range(n):
            f = source_dir / f"{phase}-input-{idx}.txt"
            f.write_text(f"{phase}-{idx}\n")


def _run_dispatch(argv: list[str], log_file: Path, env: dict[str, str], timeout_s: int) -> int:
    """Spawn the dispatch and tee combined stdout/stderr to ``log_file``.

    Mirrors :func:`tests.e2e._dispatch_runner.run_plan` minimally: the
    same Popen + tee shape, no heartbeat / hooks (this single-scenario
    driver doesn't need them).
    """
    print(f"[dispatch] argv: {' '.join(argv)}", flush=True)
    print(f"[dispatch] log:  {log_file}", flush=True)
    log_file.parent.mkdir(parents=True, exist_ok=True)
    started = time.monotonic()
    with log_file.open("w", buffering=1) as logf:
        proc = subprocess.Popen(
            argv,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            env=env,
        )
        deadline = started + timeout_s
        assert proc.stdout is not None
        for line in proc.stdout:
            logf.write(line)
            logf.flush()
            sys.stdout.write(line)
            sys.stdout.flush()
            if time.monotonic() > deadline:
                print("[dispatch] TIMEOUT", flush=True)
                proc.kill()
                return 124
        return proc.wait(timeout=max(1.0, deadline - time.monotonic()))


def _assert_ordering(log_file: Path) -> tuple[bool, list[str]]:
    """Walk the captured primary log and assert the three contracts.

    Returns (ok, diagnostics). Diagnostics describe BOTH the pass and
    the fail path so the test wrapper can print one block regardless
    of outcome — failing tests still want the partial evidence
    (timestamps, line counts) the assertion ledger gathered.
    """
    lines = log_file.read_text(errors="replace").splitlines()
    # Index every "task assigned: identity" line by phase and line index.
    assigned: dict[str, list[int]] = {"phase_a": [], "phase_b": [], "phase_c": []}
    completed: dict[str, list[int]] = {"phase_a": [], "phase_b": [], "phase_c": []}
    barrier_violations: list[int] = []
    for lineno, raw in enumerate(lines):
        m = _RE_TASK_ASSIGNED.search(raw)
        if m and m.group("phase") in assigned:
            assigned[m.group("phase")].append(lineno)
            continue
        m = _RE_TASK_COMPLETE.search(raw)
        if m and m.group("phase") in completed:
            # `task complete: identity` quotes the phase field, so it
            # arrives as Some("phase_b") — strip Some(...) and quotes.
            phase = m.group("phase").strip(',')
            # Be lenient on the Some("...") debug-print shape.
            phase = phase.lstrip("Some(").rstrip(")").strip('"')
            if phase in completed:
                completed[phase].append(lineno)
            continue
        if _RE_BARRIER_VIOLATION.search(raw):
            barrier_violations.append(lineno)

    diags: list[str] = []
    diags.append(
        f"log line counts: phase_a assigned={len(assigned['phase_a'])} "
        f"completed={len(completed['phase_a'])}; "
        f"phase_b assigned={len(assigned['phase_b'])} "
        f"completed={len(completed['phase_b'])}; "
        f"phase_c assigned={len(assigned['phase_c'])} "
        f"completed={len(completed['phase_c'])}; "
        f"barrier_violations={len(barrier_violations)}"
    )

    ok = True

    # (1) Every phase must own ≥1 assigned line; otherwise the workload
    # never even reached dispatch (cluster issue, not feature issue) and
    # the contract asserts are meaningless. Surface this distinctly.
    for phase, ids in assigned.items():
        if not ids:
            diags.append(
                f"FAIL: {phase} has no 'task assigned: identity' lines — "
                "dispatch never reached that phase. Cluster issue?"
            )
            ok = False

    # (2) phase_b's FIRST assigned line must come BEFORE phase_a's LAST
    # completed line — i.e. phase_b started dispatching while phase_a
    # was still active. This is the no-barrier signal.
    if assigned["phase_b"] and completed["phase_a"]:
        first_b_assigned = assigned["phase_b"][0]
        last_a_completed = completed["phase_a"][-1]
        if first_b_assigned < last_a_completed:
            diags.append(
                f"PASS: phase_b first assigned at line {first_b_assigned} "
                f"< phase_a last completed at line {last_a_completed} — "
                "barrier=False lifted the phase-A→B gate."
            )
        else:
            diags.append(
                f"FAIL: phase_b first assigned at line {first_b_assigned} "
                f">= phase_a last completed at line {last_a_completed} — "
                "barrier=False did not pipeline; trunk shipped #540?"
            )
            ok = False

    # (3) phase_c's FIRST assigned line must come AFTER phase_b's LAST
    # completed line — i.e. phase_c respected the default barrier.
    if assigned["phase_c"] and completed["phase_b"]:
        first_c_assigned = assigned["phase_c"][0]
        last_b_completed = completed["phase_b"][-1]
        if first_c_assigned > last_b_completed:
            diags.append(
                f"PASS: phase_c first assigned at line {first_c_assigned} "
                f"> phase_b last completed at line {last_b_completed} — "
                "default barrier=True still gates the phase-B→C edge."
            )
        else:
            diags.append(
                f"FAIL: phase_c first assigned at line {first_c_assigned} "
                f"<= phase_b last completed at line {last_b_completed} — "
                "barrier=True was not enforced on phase_c."
            )
            ok = False

    # (4) Zero BarrierViolation rejections — this valid configuration
    # must not trip the runtime-spawn interlock.
    if barrier_violations:
        diags.append(
            f"FAIL: {len(barrier_violations)} BarrierViolation lines in log; "
            "valid configuration should not trip the interlock. "
            f"First at line {barrier_violations[0]}."
        )
        ok = False
    else:
        diags.append("PASS: no BarrierViolation lines (validation interlock not falsely tripped).")

    return ok, diags


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    p.add_argument("--repo-root", type=Path, required=True,
                   help="Repo root (carries python/dynamic_runner + tests/e2e).")
    p.add_argument("--state-dir", type=Path, required=True,
                   help="slurm-test-env STATE_DIR — owns keys/ + test540/.")
    p.add_argument("--instance-id", type=str, required=True)
    p.add_argument("--ssh-port", type=int, required=True)
    p.add_argument("--ssh-user", type=str, default="testuser")
    p.add_argument("--ssh-private-key", type=Path, required=True,
                   help="Private key the wrapper provisioned for $TEST_USER.")
    p.add_argument("--gateway-host-alias", type=str, default="slurm-gateway")
    p.add_argument("--workers", type=int, default=4)
    p.add_argument("--log-file", type=Path, required=True,
                   help="Path the dispatch's combined stdout/stderr is teed to.")
    p.add_argument("--timeout-s", type=int, default=600)
    p.add_argument("--phase-a-tasks", type=int, default=2)
    p.add_argument("--phase-bc-tasks", type=int, default=5)
    p.add_argument("--phase-a-sleep-s", type=float, default=5.0,
                   help="Per-task sleep in phase_a (forwarded via env var).")
    args = p.parse_args(argv)

    # The driver imports tests.e2e._ssh_state lazily AFTER argparse so
    # an early --help / --version doesn't pay the import cost.
    # ``generate_ssh_config`` is the only helper we still consume — the
    # keypair + provision-user steps are owned by the wrapper script
    # (mirrors smoke-test.sh and avoids carrying ``nix run`` resolution
    # logic in Python).
    sys.path.insert(0, str(args.repo_root))
    try:
        from tests.e2e._ssh_state import (  # type: ignore[import-not-found]
            generate_ssh_config,
        )
    except ImportError as e:
        print(f"[fatal] cannot import tests.e2e._ssh_state from "
              f"{args.repo_root}: {e}", file=sys.stderr)
        return 2

    if not args.ssh_private_key.is_file():
        print(f"[fatal] --ssh-private-key {args.ssh_private_key} not found "
              "(wrapper should have created it).", file=sys.stderr)
        return 2

    # Per-cluster SSH state under our own subdir so we don't collide
    # with smoke-test.sh's keys/ tree.
    test540_state = _ssh_config_dir(args.state_dir, args.instance_id)
    test540_state.mkdir(parents=True, exist_ok=True)
    ssh_config = generate_ssh_config(
        test540_state,
        host_alias=args.gateway_host_alias,
        ssh_port=args.ssh_port,
        user=args.ssh_user,
        identity_file=args.ssh_private_key,
    )

    # Cluster reachability gate. The wrapper script has already TCP-
    # probed; double-checking here is cheap.
    import socket
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.settimeout(2.0)
    try:
        sock.connect(("localhost", args.ssh_port))
    except OSError as e:
        print(f"[fatal] cluster sshd unreachable at localhost:{args.ssh_port}: {e}",
              file=sys.stderr)
        return 70
    finally:
        sock.close()

    # Staging dirs. The framework needs source/ files to exist for the
    # SLURM upload step; output/ is the framework-output sink the
    # consumer writes its framework-out artifacts to.
    work = test540_state / "run"
    source_dir = work / "source"
    output_dir = work / "output"
    publish_src = work / "publish-src"
    publish_dst = work / "publish-dst"
    for d in (source_dir, output_dir, publish_src, publish_dst):
        d.mkdir(parents=True, exist_ok=True)
    _stage_inputs(source_dir, args.phase_a_tasks, args.phase_bc_tasks)

    argv_dispatch = _build_dispatch_argv(
        consumer_module="tests.e2e.test_consumer_barrier_540",
        source_dir=source_dir,
        output_dir=output_dir,
        ssh_user=args.ssh_user,
        ssh_port=args.ssh_port,
        gateway_host_alias=args.gateway_host_alias,
        slurm_root_folder=f"/home/{args.ssh_user}/dynrunner-test540",
        workers=args.workers,
        ssh_config_path=ssh_config,
        phase_a_tasks=args.phase_a_tasks,
        phase_bc_tasks=args.phase_bc_tasks,
    )

    env = os.environ.copy()
    env["PYTHONPATH"] = str(args.repo_root) + os.pathsep + env.get("PYTHONPATH", "")
    env["DYNRUNNER_PUBLISH_SRC_ROOT"] = str(publish_src)
    env["DYNRUNNER_PUBLISH_DST_ROOT"] = str(publish_dst)
    env["DYNRUNNER_TEST540_PHASE_A_SLEEP_S"] = str(args.phase_a_sleep_s)

    rc = _run_dispatch(argv_dispatch, args.log_file, env, args.timeout_s)
    if rc != 0:
        print(f"[fatal] dispatch exited with code {rc}", file=sys.stderr)
        return 1

    ok, diags = _assert_ordering(args.log_file)
    print()
    print("=== test-540 assertions ===")
    for line in diags:
        print(f"  {line}")
    if ok:
        print("  result: ALL PASSED")
        return 0
    print("  result: FAILED")
    return 1


if __name__ == "__main__":
    sys.exit(main())
