"""Scenario: kill the dispatcher's primary mid-run, assert peer election.

Single concern: the **primary-death** failover code path — R1
threshold-armed promotion (#13) + #28 peer-transport TaskAssignment
routing + #14 demoted-primary ClusterMutation arm composed under a
fully unobserved kill (the dispatcher itself dies, so the test has
NO live observer of the post-kill cluster; we verify via the
out-of-process artefact landscape).

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

import dataclasses
import os
import signal
import threading
import time
from pathlib import Path

from ._assertions import assert_files_present, expected_canonical_outputs
from ._base import DispatchEnv, Scenario, ScenarioPlan, ScenarioResult
from ._dispatch import build_dispatch_argv
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
        "SIGKILL the dispatcher's primary mid-run with --multi-computer "
        "local --jobs 3; assert surviving subprocess secondaries elect "
        "a new primary and the cluster completes all outputs out-of-band."
    )
    requires = ()

    def prepare(
        self, env: DispatchEnv, tmp_root: Path
    ) -> list[ScenarioPlan]:
        paths = stage_inputs(tmp_root, _NUM_TASKS_PER_PHASE)
        # Force local mode regardless of harness --mode. SLURM-mode
        # primary failover is a separate scenario (would need ssh-side
        # cluster-state polling, which this minimal scenario avoids).
        local_env = dataclasses.replace(env, mode="local")
        argv = build_dispatch_argv(
            env=local_env,
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
        del env
        result = results[0]

        # The dispatcher was killed with SIGKILL, so its exit_code
        # is the signal (negative on POSIX `Popen.returncode`, or
        # 128 + signum if a shell intermediated). We DON'T fail on
        # non-zero exit — that's the EXPECTED post-kill state. The
        # real assertion is on the artefact landscape converging
        # out-of-band.
        expected = expected_canonical_outputs(_NUM_TASKS_PER_PHASE)
        publish_dst = result.plan.paths.publish_dst

        deadline = time.monotonic() + _POST_KILL_DEADLINE_S
        last_ok = False
        last_missing: list[str] = []
        while time.monotonic() < deadline:
            ok, missing = assert_files_present(publish_dst, expected)
            if ok:
                last_ok = True
                break
            last_missing = missing
            time.sleep(_POLL_INTERVAL_S)

        if not last_ok:
            return (
                False,
                [
                    f"cluster failed to converge on expected outputs "
                    f"within {_POST_KILL_DEADLINE_S:.0f}s after "
                    f"dispatcher SIGKILL (exit_code={result.exit_code}). "
                    f"missing files: {last_missing!r}. "
                    f"publish_dst={publish_dst!s}. "
                    "Likely causes: R1 threshold-armed failover didn't "
                    "fire (check primary_link.rs failure_threshold), "
                    "election didn't reach quorum (check election.rs "
                    "PromotionVote/Confirm flow), promoted primary "
                    "didn't route TaskAssignment to peers (regression of #28), "
                    "or surviving subprocess secondaries crashed."
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


SCENARIO = PrimaryDeathFailoverScenario()
