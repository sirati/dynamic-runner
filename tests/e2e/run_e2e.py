"""End-to-end driver — orchestrates scenarios on the slurm-test-env.

Single concern: scenario selection, cluster lifecycle, hang
detection, exit-code reporting. Knows nothing about how each
scenario produces its dispatch argv or asserts its outputs — those
live behind the :class:`Scenario` API in
:mod:`tests.e2e.scenarios`.

Usage
-----

::

    run_e2e.py --scenario phase-deps [--workers 4] [--timeout 1800]
    run_e2e.py --scenario all [--down-on-success]

Scenarios available are enumerated from
:mod:`tests.e2e.scenarios`; see the per-scenario module docstrings
for what each one exercises.

Hang-detection contract
-----------------------

The driver writes a heartbeat file every 10s while the dispatch's
log is showing fresh activity. An outer watcher (coordinator-side
``Monitor`` poll, shell wrapper, cron) reads the file's mtime and
alerts if it goes stale beyond a threshold (typically 5 min).

On overall-timeout (``--timeout``, default 30 min), the driver
prints a multi-line "SLURM CLUSTER MAY BE STUCK — manual inspection
required" block and exits 124. The cluster is left UP across runs
so that follow-up iterations don't pay the bring-up cost; only
``--down-on-success`` flips that.

Exit codes
----------

* 0   — every requested scenario passed.
* 1   — at least one scenario failed (assertion or non-zero
        dispatch exit).
* 2   — argparse / setup error.
* 124 — overall timeout exceeded; cluster may be stuck.
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


# Support both ``python -m tests.e2e.run_e2e`` (used as a module) and
# ``python tests/e2e/run_e2e.py`` (used as a script). When invoked as
# a script ``__package__`` is empty and relative imports raise
# ImportError; we fall back to absolute imports after putting the
# repo root on sys.path. This matches the convention every other
# ``-m``-runnable script in the repo follows; centralising the dance
# would just reintroduce a meta-import that has the same problem.
if __package__:
    from ._cluster import bring_cluster_down, bring_cluster_up, is_cluster_running
    from ._dispatch_runner import run_plan
    from ._heartbeat import (
        HEARTBEAT_PERIOD_S,
        HeartbeatWriter,
        heartbeat_path_for_pid,
    )
    from .scenarios import Scenario, all_scenarios, scenario_names
    from .scenarios._base import DispatchEnv, ScenarioResult
else:
    sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
    from tests.e2e._cluster import (  # noqa: E402
        bring_cluster_down,
        bring_cluster_up,
        is_cluster_running,
    )
    from tests.e2e._dispatch_runner import run_plan  # noqa: E402
    from tests.e2e._heartbeat import (  # noqa: E402
        HEARTBEAT_PERIOD_S,
        HeartbeatWriter,
        heartbeat_path_for_pid,
    )
    from tests.e2e.scenarios import (  # noqa: E402
        Scenario,
        all_scenarios,
        scenario_names,
    )
    from tests.e2e.scenarios._base import (  # noqa: E402
        DispatchEnv,
        ScenarioResult,
    )


REPO_ROOT = Path(__file__).resolve().parents[2]
SLURM_TEST_ENV_DIR = REPO_ROOT / "slurm-test-env"

DEFAULT_INSTANCE_ID = "e2e"
DEFAULT_SSH_PORT = 2222
DEFAULT_TIMEOUT_S = 1800
DEFAULT_WORKERS = 4
# SSH user the dispatcher provisions and connects as. Per the
# slurm-test-env owner's contract: never reuse the operator's
# personal user; provision a dedicated test user via
# `nix run .#provision-user`. The default slurm root folder
# below mirrors this so the gateway can `mkdir -p` it (only
# the matching user owns the home directory).
DEFAULT_SSH_USER = "e2e-user"
DEFAULT_SLURM_ROOT_FOLDER = f"/home/{DEFAULT_SSH_USER}/dynrunner-e2e"
# Cluster-internal hostname workers use to reach the gateway over
# the podman bridge network (DNS-resolvable via the
# `--network-alias` slurm-test-env registers on the gateway
# container — see `slurm-test-env/deploy/lib.sh`). Threaded into
# the dispatcher's `--gateway` URL so the framework propagates it
# verbatim into the worker wrapper's `--secondary` argument.
GATEWAY_HOST_ALIAS = "slurm-gateway"

# How many seconds without log activity before the heartbeat thread
# stops touching its file (so an outer watcher knows the dispatch
# is hung). Independent of HEARTBEAT_PERIOD_S because progress
# detection and heartbeat tick can have different cadences.
LOG_QUIESCENT_THRESHOLD_S = 300.0


def _print_timeout_help(instance_id: str, heartbeat_file: Path) -> None:
    """The "stuck cluster" message the user explicitly asked for.

    Printed on overall-timeout (124) so the operator knows what to
    inspect manually. The exact wording is verbatim from the user
    spec because the message is part of the contract — if the
    operator greps logs for it, the string must match.
    """
    print(
        "\n"
        "============================================================\n"
        "SLURM CLUSTER MAY BE STUCK — manual inspection required\n"
        "============================================================\n"
        f"Last heartbeat: {heartbeat_file}\n"
        f"Live log dir:   /tmp/ (run_e2e log files are tee'd there)\n"
        "\n"
        "To diagnose:\n"
        f"  ssh into the gateway: ssh -p ${{SSH_PORT:-{DEFAULT_SSH_PORT}}} "
        "<user>@localhost\n"
        "  Inside: squeue -u kruppb; sacct -X\n"
        "  On each worker: ls /tmp/asm-* (should be empty after a clean run)\n"
        "  podman ps -a (orphan containers indicate the conmon-leak class)\n"
        "\n"
        "To reset:\n"
        f"  cd {SLURM_TEST_ENV_DIR}\n"
        f"  INSTANCE_ID={instance_id} ./deploy/down.sh && \\\n"
        f"  INSTANCE_ID={instance_id} ./deploy/up.sh\n"
        "============================================================\n",
        flush=True,
    )


def _fetch_published_outputs_from_gateway(
    env: DispatchEnv, publish_dst: Path
) -> None:
    """Mirror the gateway's published outputs into the local
    ``publish_dst`` so assertions can run unchanged.

    Why: the framework's worker is a container on a SLURM compute
    node. It publishes to ``/app/out-network`` which is bind-mounted
    to ``<slurm_root_folder>/out`` on the gateway. The scenario's
    assertions inspect a host-local ``publish_dst``. This function
    SCPs every regular file under the gateway's ``out`` dir back to
    ``publish_dst`` (top-level) so the host-side check finds them.

    Failures here are surfaced as warnings — the assertion will then
    fail loudly with a "missing X" message that's more actionable
    than a fetch error obscuring the real problem.
    """
    if env.ssh_config_path is None:
        return
    remote_out_dir = f"{env.slurm_root_folder}/out"
    cmd = [
        "scp",
        "-r",
        "-F", str(env.ssh_config_path),
        f"{GATEWAY_HOST_ALIAS}:{remote_out_dir}/.",
        str(publish_dst),
    ]
    rc = subprocess.run(
        cmd,
        check=False,
        stdin=subprocess.DEVNULL,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )
    if rc.returncode != 0:
        print(
            f"[run_e2e]   warning: scp from gateway '{remote_out_dir}' "
            f"failed (rc={rc.returncode}): {rc.stderr.decode().strip()}",
            flush=True,
        )


def _list_user_jobs_on_gateway(env: DispatchEnv) -> list[str]:
    """Return SLURM job IDs still present for our SSH user.

    Used as the post-scenario teardown gate: a healthy run must end
    with zero leftover SLURM jobs for the dispatcher's SSH user. Any
    job that's still in the queue (R, PD, CG) means a worker hasn't
    exited yet — usually a sign of a framework completion-signal
    regression.

    Polls for up to ``_TEARDOWN_GRACE_S`` seconds so the framework's
    `RunComplete` broadcast has time to propagate through the peer
    mesh and the wrapper scripts can finish their cleanup (podman
    container exit + ``podman unshare rm -rf $RNDTMP`` is the
    standard ~5-15s teardown). Returns the leftover job ids if the
    grace window expires with jobs still in the queue.

    Empty list on `squeue` failures (e.g. transient SSH hiccup) so
    the gate doesn't false-positive on flaky transport. The check is
    a guardrail, not a hard barrier — the actual scenario assertions
    are authoritative.
    """
    if env.ssh_config_path is None:
        return []
    deadline = time.monotonic() + _TEARDOWN_GRACE_S
    last: list[str] = []
    while True:
        rc = subprocess.run(
            [
                "ssh",
                "-F", str(env.ssh_config_path),
                GATEWAY_HOST_ALIAS,
                f"squeue --user={env.ssh_user} --noheader --format='%i'",
            ],
            check=False,
            stdin=subprocess.DEVNULL,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )
        if rc.returncode != 0:
            return []
        last = [j for j in rc.stdout.decode().split() if j.strip()]
        if not last:
            return []
        if time.monotonic() >= deadline:
            return last
        time.sleep(_TEARDOWN_POLL_S)


# Grace window for SLURM jobs to drain post-RunComplete. The wrapper
# script's `podman unshare rm -rf $RNDTMP` cleanup typically takes
# 5-15s, but on the slurm-test-env the CG (completing) state can
# linger up to ~60s before slurmstepd reaps the wrapper PID;
# 90s gives headroom without making flaky scenarios block forever.
_TEARDOWN_GRACE_S = 90.0
_TEARDOWN_POLL_S = 2.0


def _scancel_user_jobs_on_gateway(env: DispatchEnv) -> None:
    """Force-cancel everything owned by the dispatcher's SSH user.

    Called when the post-scenario teardown gate fails — the leftover
    jobs would block the NEXT scenario's slot allocation. SIGKILL via
    `scancel -s KILL` so we don't wait through `CG` (completing) state
    if the wrapper script is hung.
    """
    if env.ssh_config_path is None:
        return
    subprocess.run(
        [
            "ssh",
            "-F", str(env.ssh_config_path),
            GATEWAY_HOST_ALIAS,
            f"scancel -s KILL --user={env.ssh_user} 2>/dev/null; "
            f"scancel --user={env.ssh_user} 2>/dev/null; true",
        ],
        check=False,
        stdin=subprocess.DEVNULL,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )


def _ensure_ssh_master_alive(env: DispatchEnv) -> None:
    """Re-spawn the Python-managed SSH master if its control socket
    is gone.

    The SSH master under `DYNRUNNER_SSH_CONTROL_PATH` is supposed to
    live for the whole run, but empirically OpenSSH 10's master can
    die mid-run under sustained slurm-test-env load (suspected
    keepalive miss + MaxSessions contention). Without this guard, the
    next dispatch's reverse-forward setup silently no-ops and the
    secondaries hang on `Connection refused`. Called at every
    scenario-boundary AND per-plan dispatch boundary so a master that
    died mid-scenario still gets resurrected before the next plan
    runs.
    """
    if env.mode != "slurm" or env.ssh_config_path is None:
        return
    cp_str = os.environ.get("DYNRUNNER_SSH_CONTROL_PATH")
    if cp_str is None:
        return
    if Path(cp_str).exists():
        return
    print(
        f"[ssh] master socket missing at {cp_str}; respawning",
        flush=True,
    )
    from ._ssh_state import spawn_ssh_master  # noqa: E402
    instance_state_dir = env.ssh_config_path.parent
    new_cp = spawn_ssh_master(
        instance_state_dir,
        ssh_config_path=env.ssh_config_path,
        host_alias=GATEWAY_HOST_ALIAS,
    )
    os.environ["DYNRUNNER_SSH_CONTROL_PATH"] = str(new_cp)


def _select_scenarios(arg: str) -> list[Scenario]:
    """Resolve the ``--scenario`` CLI argument to a list of scenarios.

    ``all`` expands to every registered scenario in registry order.
    A single name resolves to a singleton list. Unknown names raise
    SystemExit(2) so argparse gets the right exit code.
    """
    available = all_scenarios()
    if arg == "all":
        return [available[name] for name in scenario_names()]
    if arg in available:
        return [available[arg]]
    print(
        f"unknown scenario: {arg!r}; available: "
        f"{', '.join(['all'] + list(available))}",
        file=sys.stderr,
    )
    raise SystemExit(2)


def _run_one_scenario(
    scenario: Scenario,
    env: DispatchEnv,
    *,
    log_dir: Path,
    timeout_s: int,
    heartbeat: HeartbeatWriter,
    keep_tmp: bool,
) -> tuple[bool, bool]:
    """Execute one scenario end-to-end.

    Returns ``(passed, timed_out)``: ``timed_out`` is True iff one of
    the scenario's plans hit the wallclock cap (rc=124 from
    dispatch_runner). The driver propagates ``timed_out`` to the
    overall return code so a hung dispatch surfaces as exit 124 and
    triggers the stuck-cluster help message.

    The driver allocates a per-scenario tmp_root, asks the scenario
    for plans, dispatches each plan, and feeds the results into the
    scenario's assertion. Cleanup of tmp_root is the driver's
    concern (per the Scenario API contract).
    """
    print(f"\n[run_e2e] scenario: {scenario.name} — {scenario.description}", flush=True)
    if scenario.requires:
        print(
            f"[run_e2e]   requires: {', '.join(scenario.requires)}",
            flush=True,
        )

    tmp_root = Path(tempfile.mkdtemp(prefix=f"dynrunner-e2e-{scenario.name}-"))
    try:
        plans = scenario.prepare(env, tmp_root)
        results: list[ScenarioResult] = []
        for plan in plans:
            # Per-plan SSH-master health check. Some scenarios (e.g.
            # already-done) emit multiple plans; the master can die
            # between them and the next plan would silently lose its
            # reverse-forward path.
            _ensure_ssh_master_alive(env)
            label_suffix = f"-{plan.label}" if plan.label else ""
            log_file = log_dir / f"{scenario.name}{label_suffix}.log"
            plan_timeout = (
                plan.timeout_s if plan.timeout_s is not None else timeout_s
            )

            # Reset the heartbeat's "last activity" baseline for this
            # plan so a long-running scenario doesn't carry stale
            # progress state from a prior plan.
            heartbeat_state = _ProgressTracker()
            heartbeat.start()

            def _on_log_activity(now: float) -> None:
                heartbeat_state.record(now)

            def _on_spawn(pid: int) -> None:
                scenario.run_hook(env, plan, pid)

            heartbeat.set_progress_check(
                lambda: heartbeat_state.is_recent(LOG_QUIESCENT_THRESHOLD_S)
            )

            result = run_plan(
                plan,
                log_file=log_file,
                repo_root=REPO_ROOT,
                timeout_s=plan_timeout,
                log_activity_callback=_on_log_activity,
                spawn_callback=_on_spawn,
            )
            results.append(result)
            if result.exit_code == 124:
                # Surface the timeout immediately and abort the
                # scenario; the assertion phase is moot. The
                # caller propagates this to the driver-level exit
                # code 124 with the stuck-cluster help message.
                return (False, True)
            if result.exit_code != 0:
                print(
                    f"[run_e2e]   plan{label_suffix} exited "
                    f"non-zero: {result.exit_code}",
                    flush=True,
                )
                # Continue into assertion — the assertion may
                # treat non-zero as expected (e.g. negative-test
                # scenarios). For currently-implemented scenarios
                # this is always a failure, and the assertion will
                # report it.

        # In SLURM mode the worker publishes to the gateway's
        # `<slurm_root_folder>/out` (bind-mounted as `/app/out-network`
        # in the container). The scenario's `publish_dst` lives on the
        # operator host. Bridge the two by SCP'ing the gateway's
        # published outputs back into each plan's `publish_dst` before
        # running assertions. In-process / single-process modes need
        # nothing; the worker writes locally.
        if env.mode == "slurm":
            for r in results:
                _fetch_published_outputs_from_gateway(env, r.plan.paths.publish_dst)

        ok, failures = scenario.assert_outputs(env, results)

        # Cluster-teardown verification: after the scenario's
        # assertions run, no worker job belonging to our SSH user may
        # remain on the gateway. A leftover means either the scenario
        # is buggy (didn't tell the framework to wind down) or the
        # framework has a teardown regression. Either way, the run
        # is NOT honestly green if SLURM slots are still occupied.
        if env.mode == "slurm":
            leftover = _list_user_jobs_on_gateway(env)
            if leftover:
                print(
                    f"[run_e2e]   FAIL: {scenario.name} — "
                    f"{len(leftover)} SLURM job(s) left running after run "
                    f"finished: {' '.join(leftover)}. The framework's run "
                    f"completion did not propagate to all secondaries (see "
                    f"docs/MIGRATION_2026_05_PYTHON_TO_RUST.md → "
                    f"'Known issues')",
                    flush=True,
                )
                # Cancel them so the next scenario isn't blocked by
                # this run's leftover slot occupation. We still report
                # the failure.
                _scancel_user_jobs_on_gateway(env)
                return (False, False)

        if ok:
            print(f"[run_e2e]   PASS: {scenario.name}", flush=True)
            return (True, False)
        print(f"[run_e2e]   FAIL: {scenario.name}", flush=True)
        for line in failures:
            print(f"    - {line}", flush=True)
        return (False, False)
    finally:
        if not keep_tmp:
            shutil.rmtree(tmp_root, ignore_errors=True)


class _ProgressTracker:
    """Stash for the last log-line timestamp.

    Local to the driver — the heartbeat-writer's
    ``progress_check`` reads ``is_recent`` once per period.
    """

    def __init__(self) -> None:
        self._last = time.monotonic()

    def record(self, now: float) -> None:
        self._last = now

    def is_recent(self, threshold_s: float) -> bool:
        return (time.monotonic() - self._last) < threshold_s


def _build_argparser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument(
        "--scenario",
        type=str,
        required=True,
        help=(
            "Scenario name or 'all'. Available: "
            + ", ".join(scenario_names())
        ),
    )
    parser.add_argument(
        "--workers",
        type=int,
        default=DEFAULT_WORKERS,
        help=f"Cluster worker count (default {DEFAULT_WORKERS}).",
    )
    parser.add_argument(
        "--timeout",
        type=int,
        default=DEFAULT_TIMEOUT_S,
        help=(
            f"Overall per-plan timeout in seconds (default {DEFAULT_TIMEOUT_S}). "
            "On exceed, exits 124 with the stuck-cluster help message."
        ),
    )
    parser.add_argument(
        "--mode",
        choices=("slurm", "single-process", "in-process"),
        default="slurm",
        help=(
            "Dispatch mode. 'slurm' is the canonical e2e mode against "
            "the slurm-test-env cluster. 'single-process' / 'in-process' "
            "skip the cluster entirely and are useful for iterating on "
            "the driver / scenarios."
        ),
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
        "--slurm-root-folder",
        type=str,
        default=DEFAULT_SLURM_ROOT_FOLDER,
        help="Gateway-side root folder for SLURM staging.",
    )
    parser.add_argument(
        "--keep-tmp",
        action="store_true",
        help="Keep per-scenario tmpdirs after exit.",
    )
    parser.add_argument(
        "--keep-cluster",
        action="store_true",
        default=True,
        help=(
            "Leave the cluster up after success (default). The driver "
            "never tears down on failure (the operator wants to "
            "inspect)."
        ),
    )
    parser.add_argument(
        "--down-on-success",
        action="store_true",
        help="Run ``down.sh`` after every scenario passes.",
    )
    parser.add_argument(
        "--heartbeat-file",
        type=Path,
        default=None,
        help=(
            "Path to write the heartbeat (default "
            "/tmp/dynrunner-e2e-heartbeat-<pid>). An external watcher "
            "polls the mtime; stale > 5 min indicates a hang."
        ),
    )
    parser.add_argument(
        "--log-dir",
        type=Path,
        default=None,
        help=(
            "Where per-plan logs are written (default a fresh "
            "tmpdir, also referenced in the timeout help message)."
        ),
    )
    return parser


def main() -> int:
    args = _build_argparser().parse_args()

    scenarios = _select_scenarios(args.scenario)

    # Cluster bring-up — only relevant in slurm mode. TCP-probe-based
    # check; bring up via `nix run .#up` per the slurm-test-env owner's
    # contract.
    ssh_config_path = None
    ssh_identity_path = None
    if args.mode == "slurm":
        if not is_cluster_running(args.ssh_port):
            try:
                bring_cluster_up(
                    SLURM_TEST_ENV_DIR, args.instance_id, args.ssh_port
                )
            except Exception as e:  # noqa: BLE001
                print(f"[run_e2e] cluster bring-up failed: {e}", flush=True)
                return 1

        # Per-cluster SSH state: keypair under tests/e2e/state/<id>/keys/,
        # provisioned dispatcher user (idempotent), generated ssh_config
        # pinning the slurm-test-env contract options.
        from ._ssh_state import (
            ensure_dispatcher_keypair,
            generate_ssh_config,
            provision_dispatcher_user,
            state_dir_for_instance,
        )
        state_root = Path(__file__).resolve().parent / "state"
        instance_state_dir = state_dir_for_instance(state_root, args.instance_id)
        priv, pub = ensure_dispatcher_keypair(instance_state_dir)
        ssh_user = DEFAULT_SSH_USER
        provision_dispatcher_user(
            SLURM_TEST_ENV_DIR, args.instance_id, ssh_user, pub
        )
        # `slurm-gateway` is the cluster-internal podman network
        # alias for the gateway container. Using it as the SSH Host
        # alias lets us:
        #   - dial via SSH from the operator host (HostName=localhost
        #     plus the forwarded port lands us on the gateway's sshd);
        #   - have the framework propagate `slurm-gateway` verbatim
        #     into the worker wrapper's
        #     `--secondary tcp://<gateway_host>:<port>` URL, where
        #     workers in the cluster's private podman network DNS-
        #     resolve it via the same `--network-alias` slurm-test-env
        #     registers in `deploy/lib.sh`.
        # Otherwise (host_alias=localhost) the wrapper would tell
        # workers to dial their OWN loopback (each worker container
        # has its own netns), which is exactly the connection-refused
        # storm `dynrunner-owner-slurm-test-env-owner` diagnosed.
        ssh_config_path = generate_ssh_config(
            instance_state_dir,
            host_alias=GATEWAY_HOST_ALIAS,
            ssh_port=args.ssh_port,
            user=ssh_user,
            identity_file=priv,
        )
        ssh_identity_path = priv
        print(f"[ssh] ssh_config: {ssh_config_path}", flush=True)
        print(f"[ssh] identity:   {ssh_identity_path}", flush=True)

        # Pre-spawn the SSH master from Python (which doesn't suffer
        # the Rust+tokio process-supervision interaction that kills
        # the framework's own master mid-run). The framework picks
        # up `DYNRUNNER_SSH_CONTROL_PATH` and reuses this master
        # instead of spawning its own; reverse forwards get added
        # dynamically via `ssh -O forward`.
        from ._ssh_state import spawn_ssh_master, stop_ssh_master  # noqa: E402
        ssh_master_cp = spawn_ssh_master(
            instance_state_dir,
            ssh_config_path=ssh_config_path,
            host_alias=GATEWAY_HOST_ALIAS,
        )
        os.environ["DYNRUNNER_SSH_CONTROL_PATH"] = str(ssh_master_cp)

        # Tear down the master on driver exit so the cluster doesn't
        # accumulate stale reverse forwards.
        import atexit  # noqa: E402
        atexit.register(
            stop_ssh_master,
            ssh_config_path=ssh_config_path,
            control_path=ssh_master_cp,
            host_alias=GATEWAY_HOST_ALIAS,
        )

    env = DispatchEnv(
        instance_id=args.instance_id,
        ssh_port=args.ssh_port,
        slurm_root_folder=args.slurm_root_folder,
        workers=args.workers,
        mode=args.mode,
        ssh_user=DEFAULT_SSH_USER,
        ssh_config_path=ssh_config_path,
        ssh_identity_path=ssh_identity_path,
        gateway_host_alias=GATEWAY_HOST_ALIAS,
    )

    heartbeat_file = args.heartbeat_file or heartbeat_path_for_pid()
    log_dir = args.log_dir or Path(
        tempfile.mkdtemp(prefix="dynrunner-e2e-logs-")
    )
    print(f"[run_e2e] heartbeat: {heartbeat_file}", flush=True)
    print(f"[run_e2e] logs: {log_dir}", flush=True)

    heartbeat = HeartbeatWriter(
        heartbeat_file,
        period_s=HEARTBEAT_PERIOD_S,
    )
    heartbeat.start()

    failures: list[str] = []
    timed_out = False
    try:
        for scenario in scenarios:
            _ensure_ssh_master_alive(env)
            ok, scenario_timed_out = _run_one_scenario(
                scenario,
                env,
                log_dir=log_dir,
                timeout_s=args.timeout,
                heartbeat=heartbeat,
                keep_tmp=args.keep_tmp,
            )
            if scenario_timed_out:
                # First timeout aborts the whole run — the cluster
                # is likely stuck, so subsequent scenarios would
                # waste their wallclock budget. The stuck-cluster
                # help lands at the bottom of main().
                timed_out = True
                failures.append(scenario.name)
                break
            if not ok:
                failures.append(scenario.name)
    finally:
        heartbeat.stop()

    if timed_out:
        _print_timeout_help(env.instance_id, heartbeat_file)
        return 124
    if failures:
        print(
            f"\n[run_e2e] FAIL: scenarios with failures: {', '.join(failures)}",
            flush=True,
        )
        return 1

    print(f"\n[run_e2e] PASS: {len(scenarios)} scenario(s)", flush=True)
    if args.down_on_success and env.mode == "slurm":
        try:
            bring_cluster_down(SLURM_TEST_ENV_DIR, env.instance_id)
        except Exception as e:  # noqa: BLE001
            print(
                f"[run_e2e] WARNING: down.sh failed (cluster left up): {e}",
                flush=True,
            )
    return 0


if __name__ == "__main__":
    sys.exit(main())
