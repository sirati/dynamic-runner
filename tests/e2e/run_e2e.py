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
from dataclasses import dataclass
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


# ── Worker-node leak gate ─────────────────────────────────────────────
#
# Single concern: after the gateway-side SLURM-job-list gate passes
# (zero jobs left), verify each compute node has no leaked artefacts
# that the wrapper script's cleanup() trap was responsible for. A job
# can finish (squeue empty) while leaving a detached podman container
# behind — the conmon-double-fork-escapes-cgroup case the wrapper's
# watchdog tries to mop up. If that mop-up missed, the leak surfaces
# here.
#
# Filterable signatures the wrapper sets (see
# crates/dynrunner-slurm/src/wrapper_script.rs):
#   - tempdir prefix `/tmp/asm-<hex8>`         (RNDTMP)
#   - test-wrapper tempdir prefix `/tmp/asm-test-<hex8>`
#   - container name prefix `asm-<hex8>-<secondary_id>`
#   - watchdog process: `setsid -f bash -c '...' watchdog ...` whose
#     argv contains the container name (so includes `asm-<hex8>`)
#
# Probe pattern `asm-[0-9a-f]{8}` (regex) discriminates framework
# state from any other operator's. `--user <ssh_user>` further
# narrows the pgrep scope so we never touch another operator's
# processes on a shared cluster.

# Eight lowercase hex chars after `asm-` — the wrapper's `rand_hex8`
# token. Used both as a literal grep substring (`asm-`) and a strict
# regex (`asm-[0-9a-f]{8}`) for discrimination from incidental matches.
_WRAPPER_TOKEN_PREFIX = "asm-"
# pgrep -f uses extended regular expressions (ERE), so `{8}` is the
# bounded-repetition operator — no backslash escaping. The pattern
# matches `asm-` followed by exactly 8 lowercase hex chars, the
# wrapper's `rand_hex8()` token. Tighter than the bare prefix to
# avoid spurious matches on unrelated `asm-*` names operators may
# have running on shared infrastructure.
_WRAPPER_TOKEN_REGEX = "asm-[0-9a-f]{8}"
# Tempdir glob prefixes, ordered most-specific-first so the test
# wrapper's `asm-test-*` is enumerated separately from secondary
# wrappers' `asm-<hex>-*`. Both classes are framework state and
# cleaned up the same way.
_LEAK_TEMPDIR_GLOBS: tuple[str, ...] = ("/tmp/asm-*",)


def _discover_worker_hostnames(env: DispatchEnv) -> list[str]:
    """Enumerate compute-node hostnames via the gateway's `sinfo`.

    Uses the slurm-test-env's slurmctld view so the leak gate adapts
    to whatever node count the cluster was brought up with — no need
    to thread `args.workers` through (which is the requested count,
    not necessarily what's currently configured).

    `sinfo -h -o '%N'` returns a compressed nodelist (e.g.
    `slurm-worker[1-4]`); `scontrol show hostnames` expands it. The
    pipeline runs on the gateway because slurm tooling isn't on the
    operator host.

    Returns ``[]`` on `sinfo` failure (transient SSH hiccup, gateway
    down) so the gate doesn't false-positive — symmetric with
    ``_list_user_jobs_on_gateway``'s behaviour on `squeue` failure.
    """
    if env.ssh_config_path is None:
        return []
    rc = subprocess.run(
        [
            "ssh",
            "-F", str(env.ssh_config_path),
            GATEWAY_HOST_ALIAS,
            "sinfo -h -o '%N' | xargs -I{} scontrol show hostnames {}",
        ],
        check=False,
        stdin=subprocess.DEVNULL,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )
    if rc.returncode != 0:
        return []
    return [
        line.strip()
        for line in rc.stdout.decode().splitlines()
        if line.strip()
    ]


@dataclass
class _WorkerLeak:
    """What a single worker leaked. Empty fields = clean.

    Container IDs are kept separate from `containers` lines so the
    forced-cleanup path can pass them straight to ``podman rm -f``
    without re-parsing.
    """

    hostname: str
    tempdirs: list[str]
    containers: list[str]
    container_ids: list[str]
    processes: list[str]
    process_pids: list[str]

    def is_empty(self) -> bool:
        return not (self.tempdirs or self.containers or self.processes)


# Per-worker probe shell. Single round-trip: lists each leak class
# under a labelled section header so the parser can split deterministi-
# cally. The `--user <ssh_user>` filter on pgrep matches `id -u <user>`
# at runtime so the probe doesn't depend on the caller knowing the
# worker's UID for the dispatcher account. Each section is silent on
# no-match (`|| true`) so the parser sees an empty body, not a
# spurious error line.
_PROBE_SHELL_TEMPLATE = (
    "set +e; "
    "echo '##TEMPDIRS##'; "
    "for g in {tempdir_globs}; do ls -d $g 2>/dev/null; done; "
    "echo '##CONTAINERS##'; "
    "podman ps -a "
    "--filter 'name={token_prefix}' "
    "--format '{{{{.ID}}}} {{{{.Names}}}} {{{{.Image}}}} {{{{.Status}}}}' "
    "2>/dev/null; "
    "echo '##PROCESSES##'; "
    "pgrep -u {user} -af '{token_regex}' 2>/dev/null; "
    "echo '##END##'; "
    "true"
)


def _build_probe_shell(env: DispatchEnv) -> str:
    return _PROBE_SHELL_TEMPLATE.format(
        tempdir_globs=" ".join(_LEAK_TEMPDIR_GLOBS),
        token_prefix=_WRAPPER_TOKEN_PREFIX,
        token_regex=_WRAPPER_TOKEN_REGEX,
        user=env.ssh_user,
    )


def _parse_probe_output(hostname: str, stdout: str) -> _WorkerLeak:
    """Split the probe's section-headered stdout into a _WorkerLeak.

    Sections are delimited by ``##NAME##`` markers. Anything between
    two markers is the section's body — empty body means no leak in
    that class.

    Container IDs are extracted from the first whitespace-separated
    token of each container line (matches the `{{.ID}}` format in
    `_PROBE_SHELL_TEMPLATE`). PIDs likewise from `pgrep -af` output's
    first column.
    """
    sections: dict[str, list[str]] = {
        "TEMPDIRS": [], "CONTAINERS": [], "PROCESSES": [],
    }
    current: str | None = None
    for raw in stdout.splitlines():
        line = raw.rstrip()
        if line.startswith("##") and line.endswith("##"):
            tag = line.strip("#")
            current = tag if tag in sections else None
            continue
        if current is None or not line.strip():
            continue
        sections[current].append(line)

    container_ids = [
        line.split()[0] for line in sections["CONTAINERS"] if line.split()
    ]
    process_pids = [
        line.split()[0] for line in sections["PROCESSES"] if line.split()
    ]
    return _WorkerLeak(
        hostname=hostname,
        tempdirs=sections["TEMPDIRS"],
        containers=sections["CONTAINERS"],
        container_ids=container_ids,
        processes=sections["PROCESSES"],
        process_pids=process_pids,
    )


def _run_probe_on_worker(
    env: DispatchEnv, hostname: str
) -> _WorkerLeak | None:
    """Submit one srun job to ``hostname`` and parse its output.

    Returns ``None`` on srun failure (the gate then logs a soft
    warning, like the gateway-side gate's behaviour on `squeue`
    failure — the check is a guardrail, not a hard barrier). Workers
    don't accept the operator's SSH key directly via ProxyJump on
    slurm-test-env (see ``scenarios/cleanup_teardown.py`` rationale),
    so we route the probe through `srun -w <node>` which authenticates
    via the slurm controller — same path the framework's actual job
    dispatch takes.
    """
    if env.ssh_config_path is None:
        return None
    probe = _build_probe_shell(env)
    srun_cmd = (
        f"srun --partition={env.slurm_partition} "
        f"--nodelist={hostname} "
        "--ntasks=1 --cpus-per-task=1 --time=00:01:00 "
        "--quiet "
        f"sh -c {_shell_quote(probe)}"
    )
    rc = subprocess.run(
        [
            "ssh",
            "-F", str(env.ssh_config_path),
            GATEWAY_HOST_ALIAS,
            srun_cmd,
        ],
        check=False,
        stdin=subprocess.DEVNULL,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        timeout=120,
    )
    if rc.returncode != 0:
        return None
    return _parse_probe_output(hostname, rc.stdout.decode())


def _shell_quote(s: str) -> str:
    """POSIX-safe single-quote wrap, mirroring shlex.quote.

    Inlined rather than imported so the helper has no extra import
    dependency at module top — and shlex.quote's exact behaviour
    (single-quote everything, escape internal `'` as `'\\''`) is
    short enough to express directly.
    """
    return "'" + s.replace("'", "'\\''") + "'"


def _list_worker_node_leaks(
    env: DispatchEnv, worker_hostnames: list[str]
) -> list[_WorkerLeak]:
    """Per-worker drain-and-check loop, mirroring the gateway gate.

    Submits the probe on every worker each iteration; clears the loop
    when every worker reports empty leaks. Polls for up to
    ``_TEARDOWN_GRACE_S`` seconds — the wrapper's
    ``podman unshare rm -rf $RNDTMP`` is the slowest cleanup step,
    and on slurm-test-env that runs ~5-15s after the SLURM job exits
    (the watchdog spawns from `setsid -f` so it survives wrapper EXIT
    by ~5s by design).

    Returns the list of non-empty ``_WorkerLeak`` snapshots from the
    final iteration. Empty list means clean.
    """
    if env.ssh_config_path is None or not worker_hostnames:
        return []
    deadline = time.monotonic() + _TEARDOWN_GRACE_S
    last: list[_WorkerLeak] = []
    while True:
        last = []
        for hostname in worker_hostnames:
            probe = _run_probe_on_worker(env, hostname)
            if probe is None:
                # Treat probe failure as "no signal this round". A
                # transient srun outage shouldn't false-positive the
                # gate; the next poll iteration retries. If every
                # iteration's probe fails we simply return [] which
                # is the same contract _list_user_jobs_on_gateway
                # follows on `squeue` failure.
                continue
            if not probe.is_empty():
                last.append(probe)
        if not last:
            return []
        if time.monotonic() >= deadline:
            return last
        time.sleep(_TEARDOWN_POLL_S)


def _force_cleanup_worker_leaks(
    env: DispatchEnv, leaks: list[_WorkerLeak]
) -> None:
    """Symmetric with ``_scancel_user_jobs_on_gateway``: kill leaks
    so the next scenario isn't poisoned.

    Per-worker srun that runs:
      - ``podman rm -f <id>...``  for each leaked container id
      - ``kill -KILL <pid>...``   for the specific PIDs pgrep matched
        (NOT a name-pattern pkill — see CLAUDE.md / the task spec's
        "PKILL CAVEAT": match by id-list to avoid touching another
        operator's processes on shared infrastructure)
      - ``podman unshare rm -rf -- <tempdir>``  for each leaked
        tempdir, mirroring the wrapper's own cleanup primitive
        (rootless podman writes subuid-mapped files unreachable via
        plain `rm`; per memory rule against `rm -rf $computed_path`
        in scripts, the user-namespaced primitive is the right tool).
    """
    if env.ssh_config_path is None or not leaks:
        return
    for leak in leaks:
        commands: list[str] = []
        if leak.container_ids:
            ids = " ".join(_shell_quote(c) for c in leak.container_ids)
            commands.append(f"podman rm -f {ids} 2>/dev/null || true")
        if leak.process_pids:
            pids = " ".join(_shell_quote(p) for p in leak.process_pids)
            commands.append(f"kill -KILL {pids} 2>/dev/null || true")
        for tempdir in leak.tempdirs:
            quoted = _shell_quote(tempdir)
            commands.append(
                f"podman unshare rm -rf -- {quoted} 2>/dev/null "
                f"|| rm -rf -- {quoted} 2>/dev/null || true"
            )
        if not commands:
            continue
        cleanup_shell = "; ".join(commands)
        srun_cmd = (
            f"srun --partition={env.slurm_partition} "
            f"--nodelist={leak.hostname} "
            "--ntasks=1 --cpus-per-task=1 --time=00:01:00 "
            "--quiet "
            f"sh -c {_shell_quote(cleanup_shell)}"
        )
        subprocess.run(
            [
                "ssh",
                "-F", str(env.ssh_config_path),
                GATEWAY_HOST_ALIAS,
                srun_cmd,
            ],
            check=False,
            stdin=subprocess.DEVNULL,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            timeout=120,
        )


def _format_worker_leak_report(
    scenario_name: str, leaks: list[_WorkerLeak]
) -> list[str]:
    """Multi-line FAIL message mirroring the gateway-gate style.

    Returns the lines so the caller can ``print`` them with the same
    flush cadence as the rest of the driver's output. Includes a
    pointer to the wrapper script for the operator's "Known issues"
    starting point.
    """
    lines: list[str] = [
        f"[run_e2e]   FAIL: {scenario_name} — worker-node leak(s) "
        f"detected after teardown gate. The wrapper script's cleanup "
        f"trap (or its watchdog fallback) did not finish — see "
        f"crates/dynrunner-slurm/src/wrapper_script.rs::cleanup()."
    ]
    for leak in leaks:
        lines.append(f"[run_e2e]     {leak.hostname}:")
        for path in leak.tempdirs:
            lines.append(f"[run_e2e]       tempdir leaked: {path}")
        for line in leak.containers:
            lines.append(f"[run_e2e]       container leaked: {line}")
        for line in leak.processes:
            lines.append(f"[run_e2e]       process leaked: {line}")
    return lines


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

            # Second teardown gate: the SLURM-jobs check above is
            # blind to detached state on the compute node (a job can
            # finish while leaving a podman container behind via the
            # conmon-double-fork-escapes-cgroup case). Probe each
            # worker for leaked tempdirs / containers / processes
            # the wrapper script was responsible for cleaning up.
            workers = _discover_worker_hostnames(env)
            leaks = _list_worker_node_leaks(env, workers)
            if leaks:
                for line in _format_worker_leak_report(scenario.name, leaks):
                    print(line, flush=True)
                # Force-cleanup so the next scenario starts with a
                # clean compute-node state. Mirrors the scancel path
                # of the SLURM-jobs gate.
                _force_cleanup_worker_leaks(env, leaks)
                return (False, False)
            print(
                f"[run_e2e]   teardown clean: {scenario.name} — "
                f"{len(workers)} worker node(s) free of framework state",
                flush=True,
            )

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
        # Note: the e2e driver does NOT auto-set DYNRUNNER_SSH_CONTROL_PATH.
        # The framework's own master spawn path (crates/dynrunner-gateway/
        # src/ssh.rs::connect()) handles the SSH master lifecycle. The
        # env-var hatch is preserved as an opt-in feature for harnesses
        # that want to manage their own master — operators set it
        # externally before invoking run_e2e if they need it.

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
