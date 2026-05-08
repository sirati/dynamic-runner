"""End-to-end driver for the synthetic dynrunner consumer.

Runs the full dispatch flow (source-dir prep → framework SLURM
dispatch → publish → output assertion) against a running
``slurm-test-env`` cluster. Designed to be run by a coordinator that
monitors the dispatch log for liveness.

Coordinator contract
--------------------

The coordinator is expected to monitor the dispatch log for progress.
This driver intentionally stays simple — it does not implement its
own watchdog. The expected hang-detection protocol::

    coordinator: poll log mtime once per minute via Monitor/ScheduleWakeup.
    coordinator: if 5 min passes with no log activity, surface to user.
    coordinator: never auto-restart the cluster — call down/up manually.

The cluster stays UP between runs to amortise startup cost. This
driver does not run ``down.sh`` on success or failure.

Usage
-----

::

    python tests/e2e/run_e2e.py [--timeout 1800] [--num-tasks N]

Exit codes
----------

* ``0``   — all phases dispatched, all expected outputs landed.
* ``1``   — dispatch failed or expected outputs missing.
* ``2``   — argparse / setup error.
* ``124`` — overall timeout exceeded; cluster may be stuck. The
  message printed in this case explicitly tells the operator the
  cluster needs a manual ``down.sh && up.sh`` reset.

The cluster itself is left running on every exit path so a
follow-up iteration can run without paying the startup cost.
"""

from __future__ import annotations

import argparse
import os
import shutil
import subprocess
import sys
import tempfile
import time
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[2]
SLURM_TEST_ENV_DIR = REPO_ROOT / "slurm-test-env"
DEFAULT_INSTANCE_ID = "e2e"
DEFAULT_SSH_PORT = 2222
DEFAULT_TIMEOUT_S = 1800
DEFAULT_NUM_TASKS = 2


# ─── slurm-test-env lifecycle (idempotent) ──────────────────────────


def _gateway_container_name(instance_id: str) -> str:
    """Container name the slurm-test-env scripts produce.

    Mirrors the naming convention in ``slurm-test-env/deploy/env.sh``::

        GATEWAY_NAME="${GATEWAY_HOSTNAME}-${INSTANCE_ID}"
        GATEWAY_HOSTNAME="slurm-gateway"
    """
    return f"slurm-gateway-{instance_id}"


def _is_cluster_running(instance_id: str) -> bool:
    """Probe ``podman ps`` for the gateway container.

    Returns False on any error (podman missing, daemon unreachable,
    etc.) — the caller treats False as "needs bring-up", which is
    correct for the missing-podman case anyway (up.sh will surface
    a clearer error than this probe could).
    """
    name = _gateway_container_name(instance_id)
    try:
        result = subprocess.run(
            ["podman", "ps", "--format", "{{.Names}}"],
            check=False,
            capture_output=True,
            text=True,
            timeout=10,
        )
    except (FileNotFoundError, subprocess.TimeoutExpired):
        return False
    if result.returncode != 0:
        return False
    running = {line.strip() for line in result.stdout.splitlines() if line.strip()}
    return name in running


def _bring_cluster_up(instance_id: str, ssh_port: int) -> None:
    """Run ``slurm-test-env/deploy/up.sh`` with the configured instance.

    The bring-up is slow (multi-minute on first run because nix has to
    build the images). We run it inheriting stdout/stderr so the
    coordinator can see progress in real time.
    """
    up_sh = SLURM_TEST_ENV_DIR / "deploy" / "up.sh"
    if not up_sh.exists():
        raise RuntimeError(
            f"slurm-test-env up.sh not found at {up_sh} — is this the right repo?"
        )
    env = os.environ.copy()
    env.setdefault("INSTANCE_ID", instance_id)
    env.setdefault("SSH_PORT", str(ssh_port))
    print(
        f"[run_e2e] cluster not running; bringing up via {up_sh} "
        f"(instance={instance_id}, ssh_port={ssh_port})",
        flush=True,
    )
    subprocess.run([str(up_sh)], check=True, env=env, cwd=str(SLURM_TEST_ENV_DIR))


# ─── source / output staging ────────────────────────────────────────


def _prepare_source_dir(num_tasks: int) -> Path:
    """Materialise N small input files under a fresh tmpdir.

    The synthetic task's ``discover_items`` builds TaskInfos with paths
    like ``input-0.txt``; the framework needs those paths to exist on
    disk because the SLURM packaging path uploads them to the gateway.
    """
    src_dir = Path(tempfile.mkdtemp(prefix="dynrunner-e2e-src-"))
    for i in range(num_tasks):
        (src_dir / f"input-{i}.txt").write_bytes(
            f"input-{i}-payload\n".encode()
        )
    return src_dir


def _prepare_output_dir() -> Path:
    """Fresh output dir per-run. Caller decides cleanup policy."""
    out_dir = Path(tempfile.mkdtemp(prefix="dynrunner-e2e-out-"))
    return out_dir


# ─── dispatch ───────────────────────────────────────────────────────


def _build_dispatch_argv(
    source_dir: Path,
    output_dir: Path,
    num_tasks: int,
    mode: str,
    ssh_port: int,
    slurm_root_folder: str,
) -> list[str]:
    """Build the framework dispatch CLI for the synthetic consumer.

    The mode is a parameter so the driver can be pointed at the simple
    in-process ``run_local`` path for fast smoke checks during
    development without re-touching every flag. ``slurm`` is the
    canonical e2e mode the user-facing test runs against.
    """
    argv: list[str] = [
        sys.executable,
        "-m",
        "tests.e2e.test_consumer",
        "--source",
        str(source_dir),
        "--output",
        str(output_dir),
        "--num-tasks",
        str(num_tasks),
        "--raw-logs",
    ]
    if mode == "slurm":
        argv += [
            "--multi-computer",
            "slurm",
            "--packaging",
            "podman",
            "--gateway",
            f"ssh://testuser@localhost:{ssh_port}",
            "--slurm-root-folder",
            slurm_root_folder,
            "--jobs",
            "1",
        ]
    elif mode == "single-process":
        argv += ["--multi-computer", "single-process"]
    # mode == "in-process" → no --multi-computer flag, framework picks
    # the in-process run_local path.
    return argv


def _run_dispatch(
    argv: list[str],
    log_file: Path,
    publish_src_root: Path,
    publish_dst_root: Path,
    timeout_s: int,
) -> int:
    """Execute the framework dispatch and tee its output to ``log_file``.

    Reads stdout line-by-line so the coordinator's tail can observe
    progress in near real time. The publish env vars are set in the
    child process group so workers (in local mode) write+publish into
    the test-managed roots; in SLURM mode the wrapper overrides these
    with ``/app/out-{tmp,network}`` inside the container, but exporting
    them here is harmless.
    """
    env = os.environ.copy()
    env["DYNRUNNER_PUBLISH_SRC_ROOT"] = str(publish_src_root)
    env["DYNRUNNER_PUBLISH_DST_ROOT"] = str(publish_dst_root)
    # The consumer + worker live under tests/e2e/test_consumer; the
    # framework spawns workers as `python -m tests.e2e.test_consumer`.
    # Running with cwd=REPO_ROOT puts ``tests`` on sys.path so the
    # ``-m`` import succeeds without a sitecustomize/.pth dance.
    env.setdefault("PYTHONPATH", str(REPO_ROOT))

    print(f"[run_e2e] dispatch: {' '.join(argv)}", flush=True)
    print(f"[run_e2e] log file: {log_file}", flush=True)

    log_file.parent.mkdir(parents=True, exist_ok=True)
    with log_file.open("w", buffering=1) as logf:
        proc = subprocess.Popen(
            argv,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            env=env,
            cwd=str(REPO_ROOT),
        )
        deadline = time.monotonic() + timeout_s
        try:
            assert proc.stdout is not None
            for line in proc.stdout:
                logf.write(line)
                logf.flush()
                sys.stdout.write(line)
                sys.stdout.flush()
                if time.monotonic() > deadline:
                    print(
                        "[run_e2e] TIMEOUT: dispatch exceeded budget; "
                        "killing child",
                        flush=True,
                    )
                    proc.kill()
                    return 124
            return proc.wait(timeout=max(1, deadline - time.monotonic()))
        except subprocess.TimeoutExpired:
            proc.kill()
            return 124


# ─── output assertion ───────────────────────────────────────────────


def _assert_outputs_present(
    publish_dst_root: Path, num_tasks: int
) -> tuple[bool, list[str]]:
    """Verify each phase's expected outputs landed under the destination.

    Returns ``(ok, missing)``: ``ok=True`` when every expected file
    exists and is non-empty; ``missing`` is a list of human-readable
    descriptions of the missing / empty files (for the failure
    message).
    """
    missing: list[str] = []
    for i in range(num_tasks):
        for stem in (f"produce-{i}.out", f"consume-{i}.out"):
            p = publish_dst_root / stem
            if not p.exists():
                missing.append(f"{p} (missing)")
            elif p.stat().st_size == 0:
                missing.append(f"{p} (empty)")
    return (not missing, missing)


# ─── re-run for already-done detection ──────────────────────────────


def _assert_idempotent_rerun(
    argv: list[str],
    log_file: Path,
    publish_src_root: Path,
    publish_dst_root: Path,
    timeout_s: int,
) -> int:
    """Re-run the same dispatch with ``--skip-existing`` and assert the
    outputs are still present (i.e. not deleted by re-running).

    This is the deferred-bug check for "already-done detection": if the
    framework's skip-existing machinery is broken, this either re-runs
    everything (slow but harmless) or — worse — clobbers the publish
    destination and leaves the outputs in a half-state. The latter
    surfaces as a missing-output assertion failure.
    """
    rerun_argv = [*argv, "--skip-existing"]
    rerun_log = log_file.with_name(log_file.stem + "-rerun" + log_file.suffix)
    print("[run_e2e] re-running dispatch with --skip-existing", flush=True)
    return _run_dispatch(
        rerun_argv,
        rerun_log,
        publish_src_root,
        publish_dst_root,
        timeout_s,
    )


# ─── orchestration ──────────────────────────────────────────────────


def _print_timeout_help(instance_id: str) -> None:
    print(
        "\n[run_e2e] SLURM CLUSTER MAY BE STUCK — manual inspection required.\n"
        f"  cd {SLURM_TEST_ENV_DIR}\n"
        f"  INSTANCE_ID={instance_id} ./deploy/down.sh\n"
        f"  INSTANCE_ID={instance_id} ./deploy/up.sh\n",
        flush=True,
    )


def main() -> int:
    parser = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument(
        "--timeout",
        type=int,
        default=DEFAULT_TIMEOUT_S,
        help=f"Overall timeout in seconds (default {DEFAULT_TIMEOUT_S}).",
    )
    parser.add_argument(
        "--num-tasks",
        type=int,
        default=DEFAULT_NUM_TASKS,
        help=f"Tasks per phase (default {DEFAULT_NUM_TASKS}).",
    )
    parser.add_argument(
        "--instance-id",
        type=str,
        default=os.environ.get("INSTANCE_ID", DEFAULT_INSTANCE_ID),
        help=(
            "slurm-test-env INSTANCE_ID (default 'e2e' or "
            "$INSTANCE_ID if set)."
        ),
    )
    parser.add_argument(
        "--ssh-port",
        type=int,
        default=int(os.environ.get("SSH_PORT", DEFAULT_SSH_PORT)),
        help=f"slurm-test-env SSH port (default {DEFAULT_SSH_PORT}).",
    )
    parser.add_argument(
        "--mode",
        choices=("slurm", "single-process", "in-process"),
        default="slurm",
        help=(
            "Dispatch mode. 'slurm' is the canonical e2e mode against "
            "the slurm-test-env cluster. 'single-process' uses the "
            "framework's in-process distributed primary + 1 secondary. "
            "'in-process' uses the simplest run_local manager (one "
            "process, N workers) — fastest smoke check while iterating "
            "on the consumer or driver."
        ),
    )
    parser.add_argument(
        "--slurm-root-folder",
        type=str,
        default="/home/testuser/dynrunner-e2e",
        help="Gateway-side root folder for SLURM staging.",
    )
    parser.add_argument(
        "--keep-tmp",
        action="store_true",
        help="Keep the source / output / publish-staging tmpdirs after exit.",
    )
    parser.add_argument(
        "--skip-rerun",
        action="store_true",
        help=(
            "Skip the second --skip-existing dispatch. Useful when "
            "iterating on the consumer or driver itself."
        ),
    )
    args = parser.parse_args()

    # 1. Ensure the cluster is up (slurm mode only — the in-process /
    #    single-process modes don't touch the cluster at all).
    if args.mode == "slurm" and not _is_cluster_running(args.instance_id):
        try:
            _bring_cluster_up(args.instance_id, args.ssh_port)
        except subprocess.CalledProcessError as e:
            print(f"[run_e2e] up.sh failed (exit={e.returncode})", flush=True)
            return 1

    # 2. Stage source files + a fresh output dir + publish-staging tmpdirs.
    source_dir = _prepare_source_dir(args.num_tasks)
    output_dir = _prepare_output_dir()
    publish_src_root = Path(tempfile.mkdtemp(prefix="dynrunner-e2e-pubsrc-"))
    publish_dst_root = Path(tempfile.mkdtemp(prefix="dynrunner-e2e-pubdst-"))
    log_file = output_dir / "run_e2e.log"
    print(f"[run_e2e] source={source_dir}", flush=True)
    print(f"[run_e2e] output={output_dir}", flush=True)
    print(f"[run_e2e] publish_src={publish_src_root}", flush=True)
    print(f"[run_e2e] publish_dst={publish_dst_root}", flush=True)

    cleanup: list[Path] = [source_dir, output_dir, publish_src_root, publish_dst_root]

    rc = 0
    try:
        # 3. Dispatch.
        argv = _build_dispatch_argv(
            source_dir,
            output_dir,
            args.num_tasks,
            args.mode,
            args.ssh_port,
            args.slurm_root_folder,
        )
        rc = _run_dispatch(
            argv,
            log_file,
            publish_src_root,
            publish_dst_root,
            args.timeout,
        )
        if rc == 124:
            _print_timeout_help(args.instance_id)
            return 124
        if rc != 0:
            print(f"[run_e2e] dispatch exited non-zero: {rc}", flush=True)
            return 1

        # 4. Assert expected publish outputs landed.
        ok, missing = _assert_outputs_present(publish_dst_root, args.num_tasks)
        if not ok:
            print("[run_e2e] FAIL: missing or empty expected outputs:", flush=True)
            for m in missing:
                print(f"  - {m}", flush=True)
            return 1
        print(
            f"[run_e2e] OK: {2 * args.num_tasks} expected outputs present "
            f"under {publish_dst_root}",
            flush=True,
        )

        # 5. Re-run with --skip-existing to exercise idempotency.
        if not args.skip_rerun:
            rc = _assert_idempotent_rerun(
                argv,
                log_file,
                publish_src_root,
                publish_dst_root,
                args.timeout,
            )
            if rc == 124:
                _print_timeout_help(args.instance_id)
                return 124
            if rc != 0:
                print(f"[run_e2e] re-run dispatch exited non-zero: {rc}", flush=True)
                return 1
            ok, missing = _assert_outputs_present(publish_dst_root, args.num_tasks)
            if not ok:
                print(
                    "[run_e2e] FAIL: outputs missing after re-run "
                    "(skip-existing may have clobbered):",
                    flush=True,
                )
                for m in missing:
                    print(f"  - {m}", flush=True)
                return 1
            print("[run_e2e] OK: re-run preserved outputs.", flush=True)

        print("[run_e2e] PASS", flush=True)
        return 0
    finally:
        if not args.keep_tmp:
            for p in cleanup:
                shutil.rmtree(p, ignore_errors=True)


if __name__ == "__main__":
    sys.exit(main())
