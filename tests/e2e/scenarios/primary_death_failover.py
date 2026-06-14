"""Scenario: kill the dispatcher's primary mid-run, assert peer election.

Single concern: the **primary-death** failover code path — R1
threshold-armed promotion (#13) + #28 peer-transport TaskAssignment
routing + #14 demoted-primary ClusterMutation arm composed under a
fully unobserved kill (the dispatcher itself dies, so the test has
NO live observer of the post-kill cluster; we verify via the
out-of-process artefact landscape).

Mode-aware: runs in both `--multi-computer local` (subprocess
secondaries on operator host) AND `--mode slurm` (SLURM-job
secondaries on remote nodes). Both share the SIGKILL-the-dispatcher
mechanic; the assertion path differs:
  - local: poll the local publish_dst tmpdir for output files
  - slurm: ssh into the gateway and `find` the gateway-side
    output_dir for output files

The framework's `local-as-non-candidate-observer` feature
(task #37) is NOT exercised here — that would require the
dispatcher process to SURVIVE the primary kill, which the SIGKILL
mechanic deliberately doesn't allow. Once #37 lands and the
dispatcher hosts an in-process observer, a separate scenario
(or a mode-3 branch here) can use a softer kill signal that
takes down only the primary task while leaving the observer
running on the dispatcher process.

Mechanic
--------

``--multi-computer local --jobs 3``: the dispatcher process owns the
primary; each secondary runs as a separate `subprocess.Popen`. The
subprocess secondaries are children of the dispatcher. When the
dispatcher is ``SIGKILL``-ed, those subprocesses get reparented to
init and continue running — their primary_transport (WSS to the now-
dead dispatcher) returns ``None``, R1 arms after the threshold,
election fires on the peer mesh, one of them is promoted, the new
primary takes over task dispatch (post-#28 the peer-routed
TaskAssignment reaches every peer's workers, not just its own).

Why ``--jobs 3`` (not 2)
-------------------------

The election picks a single winner; with N=2 surviving secondaries
post-kill the path is "minimum quorum" and we want at least 2 to
confirm the promoted secondary's authority via PromotionConfirm. With
N=3 secondaries total and ONE death (the dispatcher's local primary
doesn't run a secondary in --multi-computer local — only spawned
subprocesses do), we get N=3 surviving secondaries on a 3-peer mesh,
which is a comfortable election supermajority.

Kill timing
-----------

The SIGKILL is anchored to OBSERVED cluster progress, not a fixed
wall-clock delay: the kill fires once the first task output appears
(the cluster is operational with work in flight), bounded by
``_KILL_FLOOR_S`` / ``_KILL_CEILING_S`` — see
``_wait_until_operational_then``. A fixed delay cannot straddle the
SLURM bring-up variance (a cold container-image build vs a warm cache),
where a delay short enough for the warm path kills mid-build on a cold
cache and one long enough for the cold path lets the warm run finish
before the kill. The progress anchor lands the kill mid-operation in
both regimes, so SOME tasks have completed AND others are mid-flight.
The framework's promotion path must:
  - Detect primary disconnect via R1 (5-probe threshold + 30s window)
  - Promote a peer
  - Re-route in-flight tasks: any task that was dispatched to a
    secondary BEFORE primary death is still on that secondary's
    worker and completes locally. Any task that was dispatched to
    the primary's local pool (none in this mode — primary doesn't
    have workers in --multi-computer local) would need
    re-dispatch. Pending tasks the (dead) primary hadn't yet
    handed out get picked up by the promoted secondary via
    populate_primary_from_cluster_state().

Assertion strategy
------------------

The dispatcher is killed → its ``result.exit_code`` will be the
SIGKILL signal (negative on POSIX). We IGNORE that and instead
out-of-band-poll ``publish_dst`` for output files for up to
``_POST_KILL_DEADLINE_S`` after the dispatcher exits. The cluster
must converge on the expected output set within that window.

Why we don't ssh into anything for assertions: ``--multi-computer
local`` runs entirely on the operator host, so the publish_dst is
the canonical signal. SLURM-mode primary-failover (a separate
scenario for a future cycle) would add ssh-side SLURM-job-state
checks.
"""

from __future__ import annotations

import os
import signal
import threading
import time
from pathlib import Path

from ._assertions import assert_files_present, expected_canonical_outputs
from ._base import DispatchEnv, Scenario, ScenarioPlan, ScenarioResult
from ._dispatch import build_dispatch_argv
from ._ssh import gateway_ssh
from ._staging import stage_inputs


def _num_tasks_per_phase() -> int:
    """Per-phase task count, env-overridable for the #504 delta-pull
    corroboration.

    Defaults to 12 (the original distribution-shape rationale below). The
    ``DYNRUNNER_E2E_FAILOVER_NUM_TASKS`` env knob raises it to a NON-trivial
    ledger (a few hundred to low-thousands) so the post-failover convergence
    exercises the range-digest DELTA pull / anti-entropy machinery (#504): a
    buggy incremental range-digest memo would DROP CRDT entries on a delta
    convergence, surfacing here as a permanently-missing output. A single
    knob (not a new scenario) keeps the failover mechanic un-duplicated;
    absent → the historical 12-task shape, so every existing run is
    unchanged."""
    raw = os.environ.get("DYNRUNNER_E2E_FAILOVER_NUM_TASKS", "")
    if raw:
        try:
            n = int(raw)
            if n > 0:
                return n
        except ValueError:
            pass
    return 12


_NUM_TASKS_PER_PHASE = _num_tasks_per_phase()
"""12 tasks distributed across 3 secondaries → 4 per secondary in the
round-robin initial-assignment phase (post-#33). Enough work that
some completions happen pre-kill AND some assignments remain pending
when the dispatcher dies, so the promoted secondary's
populate_primary_from_cluster_state() path actually has tasks to
pull from the ledger."""

_JOBS = 3
"""3 subprocess secondaries. After dispatcher death N=3 survive on
the peer mesh — comfortable election quorum, not the minimum case.
Failover with N=2 is exercised separately in unit tests."""

_KILL_FLOOR_S = 5.0
"""Lower bound on time-to-kill: never SIGKILL in the first few seconds
even if (impossibly) an output appeared, so the kill always lands
after the dispatch process is fully up and the cluster has begun
forming — never racing the spawn itself."""

_KILL_CEILING_S = 180.0
"""Upper bound on time-to-kill. The kill is normally triggered by the
first observed task output (see :meth:`run_hook`); this ceiling is the
fallback for the pathological case where NO output ever appears (the
cluster never became operational — e.g. a bring-up hang). Killing at
the ceiling preserves the scenario's "fully unobserved kill" semantics
without letting the hook wait forever. Sized well above a cold-cache
SLURM bring-up (container image build + ssh-submit + queue + setup),
which a fixed wall-clock delay could not straddle: a delay short
enough for the warm path kills mid-build on a cold cache, and one long
enough for the cold path lets the warm run finish before the kill."""

_KILL_PROBE_INTERVAL_S = 1.0
"""How often the kill trigger polls for the first task output. Tight
so the SIGKILL lands close to the moment the cluster goes operational
— while the bulk of the work is still in flight to fail over."""

def _post_kill_deadline_s() -> float:
    """Convergence budget AFTER dispatcher exit. The base 90s covers the R1
    threshold + election + the original 12-task remaining-work tail. A large
    #504 delta-pull ledger (hundreds–thousands of tasks) adds a real
    post-failover execution tail (each task takes ~_TASK_SLEEP_S to publish,
    spread across the surviving secondaries), so the budget scales with the
    ledger above the original shape: +0.5s per task beyond 12, which
    comfortably covers the slowest-secondary tail at the test env's worker
    throughput. Absent the knob → the historical 90s, unchanged."""
    base = 90.0
    extra_tasks = max(0, _NUM_TASKS_PER_PHASE - 12)
    return base + 0.5 * extra_tasks


_POST_KILL_DEADLINE_S = _post_kill_deadline_s()
"""How long the test waits AFTER dispatcher exit for the cluster to
converge on the expected outputs.

Two slow phases live in this window:
  - R1 threshold trip: 5 probes × keepalive_interval (~5s) = ~25s
    worst case before failover arms.
  - Election + PromotePrimary broadcast + populate_primary_from_
    cluster_state + resumed dispatch + remaining task execution.

90s is conservative but bounded — the unit tests at
``crates/dynrunner-manager-distributed/src/secondary/election.rs``
pin the election path at <500ms once R1 has armed, so the
dominant cost is R1's threshold delay + the remaining work. For a large
#504 delta-pull ledger the budget scales with the ledger (see
``_post_kill_deadline_s``)."""

_POLL_INTERVAL_S = 2.0


def _outputs_present_count(
    env: DispatchEnv,
    plan: ScenarioPlan,
) -> int:
    """Count task outputs visible so far, mode-aware.

    Mirrors the convergence poll in :meth:`assert_outputs`: local-mode
    outputs land on the operator host's ``publish_dst``; SLURM-mode
    outputs land on the gateway's NFS-mounted ``output`` dir, reachable
    only via ssh. A return ``> 0`` means at least one task has completed
    and published — i.e. the cluster is OPERATIONAL with work in flight,
    the moment to land the SIGKILL. Any probe error counts as 0 (not yet
    operational) so a transient ssh hiccup just defers the kill toward
    the ceiling rather than firing it early on a false signal.
    """
    if env.mode == "slurm":
        # Count files under the gateway publish dir (cleared at prepare
        # time, so any file here is from THIS run). NOT the dispatcher's
        # `--output` arg — SLURM mode does not write there.
        cmd = (
            f"find {_gateway_out_dir(env)!s} -mindepth 1 -maxdepth 8 "
            "-type f 2>/dev/null | wc -l"
        )
        try:
            proc = gateway_ssh(env, cmd, timeout_s=10)
            if proc.returncode != 0:
                return 0
            return int(proc.stdout.strip() or "0")
        except Exception:
            return 0
    return sum(1 for _ in plan.paths.publish_dst.glob("*")) if (
        plan.paths.publish_dst.is_dir()
    ) else 0


def _wait_until_operational_then(
    env: DispatchEnv,
    plan: ScenarioPlan,
) -> float:
    """Block until the cluster is operational (first task output seen)
    or the ceiling elapses, then return the elapsed seconds.

    Anchoring the kill to observed progress — rather than a fixed
    wall-clock delay — is what makes the SLURM path robust to the wide
    bring-up-time variance (cold container build vs warm cache): the
    kill always lands just after the cluster goes operational, with the
    bulk of the work still in flight to fail over.
    """
    start = time.monotonic()
    while True:
        elapsed = time.monotonic() - start
        if elapsed >= _KILL_CEILING_S:
            return elapsed
        if elapsed >= _KILL_FLOOR_S and _outputs_present_count(env, plan) > 0:
            return elapsed
        time.sleep(_KILL_PROBE_INTERVAL_S)


class PrimaryDeathFailoverScenario(Scenario):
    name = "primary-death-failover"
    description = (
        "SIGKILL the dispatcher's primary mid-run (mode-aware: "
        "local subprocess secondaries OR SLURM-node secondaries); "
        "assert surviving secondaries elect a new primary and the "
        "cluster completes all outputs out-of-band."
    )
    requires = ()

    def prepare(
        self, env: DispatchEnv, tmp_root: Path
    ) -> list[ScenarioPlan]:
        paths = stage_inputs(tmp_root, _NUM_TASKS_PER_PHASE)
        # The gateway out dir (`<slurm_root_folder>/out`) is SHARED
        # across scenario runs. Clear it up-front so both the
        # operational-detection probe (run_hook) and the convergence
        # check (assert_outputs) see ONLY this run's outputs — a stale
        # output would make the probe think the cluster is operational
        # at t=0 and would let convergence pass on prior-run artefacts.
        if env.mode == "slurm":
            _clear_gateway_out_dir(env)
        # Honour the harness's --mode (was previously force-local).
        # In SLURM mode the dispatch goes through the full SLURM
        # packaging pipeline; in local mode the subprocess-secondary
        # path. SIGKILL works identically in both — only the
        # output-assertion path differs (see assert_outputs).
        argv = build_dispatch_argv(
            env=env,
            source=paths.source,
            output=paths.output,
            num_tasks=_NUM_TASKS_PER_PHASE,
            jobs=_JOBS,
        )
        # The dispatcher is SIGKILLed mid-run by design, so its exit
        # code is the signal, never 0 — declare that to the driver's
        # plan-exit gate; convergence is judged out-of-band in
        # assert_outputs (post-kill publish_dst polling).
        return [ScenarioPlan(argv=argv, paths=paths, allows_nonzero_exit=True)]

    def run_hook(
        self, env: DispatchEnv, plan: ScenarioPlan, dispatch_pid: int
    ) -> None:
        def kill_dispatcher() -> None:
            # Wait until the cluster is operational (first task output
            # observed) before killing — see
            # `_wait_until_operational_then`. A fixed wall-clock delay
            # cannot straddle the SLURM bring-up variance (cold image
            # build vs warm cache); the progress anchor lands the kill
            # mid-operation either way.
            waited = _wait_until_operational_then(env, plan)
            # SIGKILL not SIGTERM: we want the dispatcher gone
            # immediately with no cleanup hooks, exactly mirroring a
            # host crash / OOM-kill / scancel-with-immediate-signal.
            # SIGTERM would let the dispatcher's atexit / signal-
            # handler chain run, which masks the kernel-style death
            # we're trying to test.
            try:
                os.kill(dispatch_pid, signal.SIGKILL)
                print(
                    f"[primary-death-failover] hook: SIGKILL "
                    f"dispatcher pid={dispatch_pid} after {waited:.1f}s "
                    f"(cluster operational)",
                    flush=True,
                )
            except ProcessLookupError:
                # Dispatcher already exited (e.g. crashed before the
                # kill timer fired). The driver will catch the
                # non-zero exit and assert_outputs will handle it.
                print(
                    f"[primary-death-failover] hook: dispatcher "
                    f"pid={dispatch_pid} already gone — no kill needed",
                    flush=True,
                )

        threading.Thread(
            target=kill_dispatcher,
            name="primary-death-killer",
            daemon=True,
        ).start()

    def assert_outputs(
        self, env: DispatchEnv, results: list[ScenarioResult]
    ) -> tuple[bool, list[str]]:
        result = results[0]

        # The dispatcher was killed with SIGKILL, so its exit_code
        # is the signal (negative on POSIX `Popen.returncode`, or
        # 128 + signum if a shell intermediated). We DON'T fail on
        # non-zero exit — that's the EXPECTED post-kill state. The
        # real assertion is on the artefact landscape converging
        # out-of-band.
        expected = expected_canonical_outputs(_NUM_TASKS_PER_PHASE)
        publish_dst = result.plan.paths.publish_dst

        # Mode-aware polling. Local-mode outputs land on the
        # operator host's tmpdir; SLURM-mode outputs land on the
        # gateway's NFS-mounted output_dir, reachable only via ssh.
        # The poll loop is the same shape either way — different
        # "is the output set complete yet?" check.
        if env.mode == "slurm":
            ok, missing_or_err = _poll_outputs_slurm(
                env, _gateway_out_dir(env), expected
            )
        else:
            ok, missing_or_err = _poll_outputs_local(publish_dst, expected)

        if not ok:
            return (
                False,
                [
                    f"cluster failed to converge on expected outputs "
                    f"within {_POST_KILL_DEADLINE_S:.0f}s after "
                    f"dispatcher SIGKILL (exit_code={result.exit_code}). "
                    f"missing/error: {missing_or_err!r}. "
                    f"mode={env.mode}, publish_dst={publish_dst!s}. "
                    "Likely causes: R1 threshold-armed failover didn't "
                    "fire (check primary_link.rs failure_threshold), "
                    "election didn't reach quorum (check election.rs "
                    "PromotionVote/Confirm flow), promoted primary "
                    "didn't route TaskAssignment to peers (regression of #28), "
                    "or surviving secondaries crashed."
                ],
            )

        # Belt-and-braces: even after the convergence wait, surface
        # the dispatcher's exit code in the log so an operator
        # post-debugging the run can see what signal landed.
        # -9 / 137 is SIGKILL; anything else might suggest the
        # dispatcher died for a different reason (segfault, etc.)
        # before our SIGKILL fired.
        rc = result.exit_code
        if rc != -signal.SIGKILL and rc != 128 + signal.SIGKILL:
            print(
                f"[primary-death-failover] note: dispatcher exited "
                f"rc={rc}, expected SIGKILL ({-signal.SIGKILL} or "
                f"{128 + signal.SIGKILL}). Test still passes (outputs "
                "converged) but the kill timing may have raced with a "
                "natural exit.",
                flush=True,
            )

        return (True, [])


def _poll_outputs_local(
    publish_dst: Path,
    expected: set[str],
) -> tuple[bool, list[str]]:
    """Poll the operator-host publish_dst for the expected output
    files. Returns (True, []) on convergence or (False, <missing>)
    on timeout."""
    deadline = time.monotonic() + _POST_KILL_DEADLINE_S
    last_missing: list[str] = []
    while time.monotonic() < deadline:
        ok, missing = assert_files_present(publish_dst, expected)
        if ok:
            return (True, [])
        last_missing = missing
        time.sleep(_POLL_INTERVAL_S)
    return (False, last_missing)


def _poll_outputs_slurm(
    env: DispatchEnv,
    output_dir: str,
    expected: set[str],
) -> tuple[bool, list[str]]:
    """Poll the GATEWAY's output dir via ssh for the EXPECTED output
    filenames. The dispatcher is dead, so we can't rely on the
    framework's own publish pipeline — secondaries write directly to
    NFS-mounted /app/out-network (bind-mounted from the gateway's
    ``<slurm_root_folder>/out`` per the wrapper's
    ``-v "{out_network}:/app/out-network"``). Returns (True, []) on
    convergence or (False, <missing/error>) on timeout.

    We check for the SPECIFIC ``expected`` filenames (not a raw file
    count): the gateway out dir is shared across scenario runs, so a
    count would both undercount (wrong dir) and over-count (stale
    files from prior runs). The canonical names are deterministic, so
    a name-set check is the same shape the SCP-then-assert_files_present
    path uses for the live (non-failover) scenarios."""
    deadline = time.monotonic() + _POST_KILL_DEADLINE_S
    last_err = ""
    expected_set = set(expected)
    # One ls of the dir; membership-test the expected names locally.
    cmd = f"ls -1 {output_dir!s} 2>/dev/null"
    while time.monotonic() < deadline:
        try:
            proc = gateway_ssh(env, cmd, timeout_s=10)
        except Exception as e:
            last_err = f"gateway ssh failed: {e}"
            time.sleep(_POLL_INTERVAL_S)
            continue
        if proc.returncode != 0:
            last_err = (
                f"ssh rc={proc.returncode} "
                f"stderr={proc.stderr.strip()!r}"
            )
            time.sleep(_POLL_INTERVAL_S)
            continue
        present = {line.strip() for line in proc.stdout.splitlines() if line.strip()}
        missing = sorted(expected_set - present)
        if not missing:
            return (True, [])
        last_err = (
            f"{len(missing)} of {len(expected)} expected outputs still "
            f"missing on gateway under {output_dir!s}: {missing[:5]}"
            f"{'...' if len(missing) > 5 else ''}"
        )
        time.sleep(_POLL_INTERVAL_S)
    return (False, [last_err])


def _gateway_out_dir(env: DispatchEnv) -> str:
    """The gateway-side dir where SLURM workers publish their outputs:
    ``<slurm_root_folder>/out`` (bind-mounted into the worker container
    as ``/app/out-network``). This is the canonical publish location —
    the dispatcher's ``--output`` arg is a host-local path that SLURM
    mode does not write to. Mirrors
    ``run_e2e._fetch_published_outputs_from_gateway``."""
    return f"{env.slurm_root_folder}/out"


def _clear_gateway_out_dir(env: DispatchEnv) -> None:
    """Delete every regular file under the gateway out dir so this
    run's convergence + operational signals are not polluted by prior
    runs' artefacts. Best-effort: a clear failure surfaces later as a
    convergence timeout, which is more actionable than a clear error.

    Removes only regular files (``-type f``), never the dir itself —
    the framework bind-mounts the dir and would fail to start if it
    vanished."""
    out_dir = _gateway_out_dir(env)
    cmd = f"find {out_dir!s} -mindepth 1 -maxdepth 8 -type f -delete 2>/dev/null; true"
    try:
        gateway_ssh(env, cmd, timeout_s=15)
    except Exception:
        pass


SCENARIO = PrimaryDeathFailoverScenario()
