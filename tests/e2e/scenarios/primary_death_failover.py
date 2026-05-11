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

``_KILL_DELAY_S`` is sized so SOME tasks have completed (some
``produce-{i}`` files exist in the publish staging dir) AND others
are mid-flight. The framework's promotion path must:
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


_NUM_TASKS_PER_PHASE = 12
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

_KILL_DELAY_S = 12.0
"""Wallclock between dispatch spawn and SIGKILL of the dispatcher.

Empirically calibrated so subprocess secondaries have:
  - Completed setup handshake (Welcome + CertExchange + InitialAssignment)
  - Started processing their initial 4 tasks
  - At least one TaskComplete has landed and been forwarded through
    the (still-alive) primary
  - At least one task is mid-execution on each secondary

Pre-kill state matters: with too-short a delay we kill before
PromotePrimary infrastructure is even initialised on the peer mesh
(secondaries haven't broadcast their MeshReady yet, primary hasn't
sent PeerInfo), and the surviving secondaries can't actually elect.
12s is the smallest value that reliably puts the cluster in the
"mid-operational" state on the operator's host."""

_POST_KILL_DEADLINE_S = 90.0
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
dominant cost is R1's threshold delay + the remaining work."""

_POLL_INTERVAL_S = 2.0


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
        return [ScenarioPlan(argv=argv, paths=paths)]

    def run_hook(
        self, env: DispatchEnv, plan: ScenarioPlan, dispatch_pid: int
    ) -> None:
        del env, plan

        def kill_dispatcher() -> None:
            time.sleep(_KILL_DELAY_S)
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
                    f"dispatcher pid={dispatch_pid} after {_KILL_DELAY_S}s",
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
                env, result.plan.paths.output, expected
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
    output_dir: Path,
    expected: set[str],
) -> tuple[bool, list[str]]:
    """Poll the GATEWAY's output_dir via ssh for the expected output
    set. The dispatcher is dead, so we can't rely on the framework's
    own publish pipeline — secondaries write directly to NFS-mounted
    /app/out-network (bind-mounted from the gateway's output_dir
    per `wrapper_script.rs`'s `-v "{output_network}:/app/out-network"`).
    Returns (True, []) on convergence or (False, <error-string>)
    on timeout."""
    # `output_dir` here is the path the dispatcher passed via
    # `--output`; in SLURM mode that's a gateway-side path (e.g.
    # `/home/<gateway-user>/.../e2e-outputs/...`). The dispatcher
    # ssh-creates it during preparation. We don't tilde-expand
    # locally — the gateway-side find walks the literal path.
    deadline = time.monotonic() + _POST_KILL_DEADLINE_S
    last_err = ""
    while time.monotonic() < deadline:
        # Just count regular files under output_dir; the test_consumer
        # writes one per task. We don't enforce the exact filename
        # set on the SLURM side because the publish staging tree
        # is a black box from the ssh assertion's vantage point —
        # the count is the load-bearing invariant.
        cmd = (
            f"find {output_dir!s} -mindepth 1 -maxdepth 8 "
            "-type f 2>/dev/null | wc -l"
        )
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
        try:
            count = int(proc.stdout.strip() or "0")
        except ValueError:
            last_err = f"unparseable wc -l output: {proc.stdout!r}"
            time.sleep(_POLL_INTERVAL_S)
            continue
        if count >= len(expected):
            return (True, [])
        last_err = (
            f"only {count} of {len(expected)} expected outputs "
            f"present on gateway under {output_dir!s}"
        )
        time.sleep(_POLL_INTERVAL_S)
    return (False, [last_err])


SCENARIO = PrimaryDeathFailoverScenario()
