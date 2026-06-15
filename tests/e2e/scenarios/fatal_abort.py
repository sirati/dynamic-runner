"""Scenario: a primary fatal-Err broadcasts ``RunAborted`` and the
cluster tears down honestly (no spurious failover, no post-latch
re-dispatch).

Single concern: the four #563 seams composed end-to-end against the
slurm-test-env cluster — Seam 0 (``run_pipeline`` broadcasts
``RunAborted`` on every fatal primary ``Err``), Seam 1 (secondary's
election arming consults the run-terminal latch), Seam 2
(``bootstrap_tail_dispatch`` adopts the latch pre-loop), Seam 3
(narrator suppresses failover / peer-lost / peer-rejoined under the
terminal latch). Pre-#563 these were four independent dishonest
behaviours; this scenario is the witness that they remain composed.

Repro shape
-----------

A ``TaskInfo`` whose ``path`` resolves under ``source_dir`` to a file
that does NOT exist on disk, with ``uses_file_based_items=True`` (the
default). The primary's ``queue_initial_staging`` walk inside
``coord.run`` reads the missing file and raises
``StagingError::SourceUnreadable``; the primary's run loop surfaces
this as ``RunError::Other`` (the catch-all variant). Pre-#563 the
``Other`` variant did NOT broadcast ``ClusterMutation::RunAborted``
(the broadcast was wired only for ``FatalPolicyExit`` /
``InvalidComposedGraph`` / the per-task #3a/#3b variants); the
cluster never learned the run was over, the observer's
``evaluate_exit`` blocked forever waiting for the verdict, and the
operator's stream sometimes carried a misleading "primary failed
over" line emitted by a peer that armed an election against the
dying primary's link drop. Post-#563 the verdict broadcast covers
every fatal ``Err`` and the receiver-side gates suppress the
failover-shaped narration / arming / re-dispatch.

This is the SLURM-mode counterpart of the in-process #562 test
``python/dynamic_runner/tests/test_distributed_err_propagation.py`` —
same trigger (a missing source file), same RunError variant
(``Other(StagingError)``), different transport (real SLURM mesh
instead of an in-process distributed primary). The in-process test
proves the broadcast reaches the in-process observer; this scenario
proves the broadcast + the three receiver gates compose under the
real multi-secondary SLURM topology.

What's asserted
---------------

The dispatcher's stdout (the operator-facing ``--important-stdio``
stream, captured into ``result.log_file`` by the e2e harness) MUST:

POSITIVE — verdict honesty (Seam 0):

  * carry exactly one ``"run aborted — shutting down"`` ERROR line —
    the authoritative narration the operator's eye should land on.
    Pre-#563 this line was absent (no broadcast on
    ``RunError::Other``).

NEGATIVE — receiver-side gates (Seams 1, 2, 3):

  * MUST NOT carry ``"primary failed over"`` (Seam 3 narrator
    suppression) — pre-#563 a peer that briefly armed an election
    against the dying primary's link drop would later emit this line,
    misleading the operator into reading the run as a failover-and-
    recovered case instead of an authored abort.
  * MUST NOT carry ``"primary death suspected"`` (Seam 1 election-
    arming gate) — pre-#563 a peer's election arm fires when the
    primary's link goes silent, regardless of whether the cluster's
    replicated verdict already says the run is over.
  * MUST NOT carry any ``"task assigned"`` line AFTER the
    ``"run aborted"`` line (Seam 2 ``bootstrap_tail_dispatch`` gate
    + Seam 0 broadcast preceding any promotion attempt) — if a peer
    DID briefly win an election against the dying primary, its
    bootstrap-tail check would short-circuit before re-dispatching
    any of the inherited ledger's Pending tasks. The temporal
    ordering check (after the verdict line) tolerates pre-verdict
    "task assigned" lines from a peer that started dispatching
    before the abort latched — those are legitimately concurrent;
    only the POST-LATCH re-dispatch is the bug.

Exit code:

  * The dispatcher MUST exit non-zero — the run failed and the Python
    caller MUST surface that to the operator. The in-process test
    asserts ``PyRuntimeError`` carrying the verbatim staging reason;
    this scenario only checks the exit code (the operator-stream
    assertion above already verifies the verdict line carries the
    reason). ``allows_nonzero_exit=True`` declares the plan's
    expected non-zero shape so the driver's plan-must-pass guard
    does not false-fail.
"""

from __future__ import annotations

import re
import tempfile
from pathlib import Path

from ._base import (
    DispatchEnv,
    DispatchPaths,
    Scenario,
    ScenarioPlan,
    ScenarioResult,
)
from ._dispatch import build_dispatch_argv


# Strip ANSI escape runs before substring matching: the operator-stdio
# sink sets `with_ansi(false)`, but the harness captures stdout+stderr
# and any other (non-gated) stream the child writes could still carry
# escapes — be robust to them rather than depend on a clean capture.
_ANSI_RE = re.compile(r"\x1b\[[0-9;]*m")

# The verbatim ``--important-stdio`` ERROR emit on the abort branch of
# the RunNarrator's terminal narration block, target
# ``IMPORTANT_TARGET``, level ``error``:
#   crates/dynrunner-manager-distributed/src/run_narrator.rs (terminal
#   summary): ``"run aborted — shutting down"``.
# Substring match keeps the assertion stable across the compact
# operator format (``HH:MM±hhmm LEVEL message reason=...``).
_VERDICT_LINE = "run aborted — shutting down"

# Bug fingerprints — strings the operator stream MUST NOT carry under
# a #563-compliant cluster. Each names ONE of the three receiver
# seams; presence indicates regression of that seam.
_SEAM3_FAILOVER_NARRATION = "primary failed over"
"""Seam 3 — narrator's failover line; suppressed under a converged
terminal latch (``run_aborted().is_some() || run_complete()``).
Presence under our scenario (where the run-terminal verdict
precedes any failover signal) means the suppression gate is broken
and the operator gets the misleading 'failover' read instead of
'aborted'."""

_SEAM1_ELECTION_ARM = "primary death suspected"
"""Seam 1 — the secondary's election-arming Suspecting transition
WARN line; under #563 the arming reads ``run_terminal_latched`` (=
``cluster_state.run_aborted().is_some() ||
cluster_state.run_complete()``) and skips the arm. Presence means
a peer armed against the dying primary's link-drop despite the
authored verdict — the same path that produces Seam 3's misleading
narration upstream."""

# Seam 2 evidence — the ``"task assigned"`` ledger line a primary
# emits at every TaskAssignment send (the framework's wire-side
# dispatch log,
# crates/dynrunner-manager-distributed/src/primary/assignment.rs).
# Pre-#563 a promoted primary's ``bootstrap_tail_dispatch`` re-walked
# the inherited Pending ledger and re-emitted these lines AFTER the
# RunAborted latch — the catastrophic post-finalize re-dispatch the
# seam 2 gate prevents. We assert no such line appears AFTER the
# verdict line; pre-verdict lines (from a primary that started
# dispatching before its fatal Err) are legitimate.
_TASK_ASSIGNED_LINE = "task assigned"


def _stage_empty_source(tmp_root: Path) -> DispatchPaths:
    """Allocate the source/output/publish-{src,dst} quartet WITHOUT
    seeding any input files.

    Mirrors :func:`tests.e2e.scenarios._staging.stage_inputs` field-
    for-field so the dispatcher reads the same canonical layout, but
    leaves ``source`` empty. The consumer's ``discover_items`` reads
    ``--num-tasks`` from ``args`` and emits ``TaskInfo`` with
    ``path=Path("input-0.txt")`` — a path that resolves under
    ``source`` to a non-existent file. That is the
    ``StagingError::SourceUnreadable`` repro shape.

    Inline (not a shared helper) on the principle that one-of
    helpers belong at the call site; ``stage_inputs`` is the canonical
    SEED, and "no seed" is this scenario's whole point.
    """
    source = Path(tempfile.mkdtemp(prefix="src-fatalabort-", dir=tmp_root))
    output = Path(tempfile.mkdtemp(prefix="out-fatalabort-", dir=tmp_root))
    publish_src = Path(
        tempfile.mkdtemp(prefix="pubsrc-fatalabort-", dir=tmp_root)
    )
    publish_dst = Path(
        tempfile.mkdtemp(prefix="pubdst-fatalabort-", dir=tmp_root)
    )
    return DispatchPaths(
        source=source,
        output=output,
        publish_src=publish_src,
        publish_dst=publish_dst,
    )


class FatalAbortScenario(Scenario):
    name = "fatal-abort"
    description = (
        "#563 four-seam composition contract: a primary fatal-Err "
        "(missing source file → RunError::Other(StagingError)) "
        "broadcasts RunAborted; the operator stream carries the "
        "verdict line and NO failover-shaped narration, NO election "
        "arming, NO post-latch re-dispatch."
    )
    requires = ("563-fatal-abort-broadcast",)

    def prepare(
        self, env: DispatchEnv, tmp_root: Path
    ) -> list[ScenarioPlan]:
        paths = _stage_empty_source(tmp_root)
        # ``--num-tasks 1`` makes the consumer's ``discover_items``
        # emit exactly one TaskInfo with a non-existent source path
        # (``input-0.txt`` under the empty ``source`` dir). One task
        # is sufficient: the staging walk fails on the first read.
        # ``--important-stdio-only`` gates the operator stream to the
        # wake-worthy emits — including the verdict line we assert
        # POSITIVE on; the bug-fingerprint strings (failover /
        # election / task-assigned) are ALSO important-target where
        # they exist (failover narration is an IMPORTANT-target emit,
        # election arming is WARN on the normal target but its
        # presence under important-only would only mean an
        # importance-gate leak — a strictly stronger assertion than
        # the regression we're guarding).
        argv = build_dispatch_argv(
            env=env,
            source=paths.source,
            output=paths.output,
            num_tasks=1,
            extra_args=("--important-stdio-only",),
        )
        return [
            ScenarioPlan(
                argv=argv,
                paths=paths,
                # The dispatcher MUST exit non-zero — that IS the
                # contract. Without this flag the driver fails the
                # plan on the non-zero exit regardless of what
                # ``assert_outputs`` decides, masking our success-
                # shape assertion. The exit-code IS one of our
                # checks below; ``allows_nonzero_exit`` just
                # decouples the driver's plan-pass guard from our
                # contract.
                allows_nonzero_exit=True,
            )
        ]

    def assert_outputs(
        self, env: DispatchEnv, results: list[ScenarioResult]
    ) -> tuple[bool, list[str]]:
        del env
        result = results[0]

        failures: list[str] = []

        # The dispatcher MUST exit non-zero. ``exit_code == 0`` means
        # the framework swallowed the fatal Err and exited rc=0,
        # masking a broken run from the operator (#562's #1
        # symptom). Negative exit codes (e.g. -9 SIGKILL from a
        # driver-side timeout) are equally non-success but are a
        # DIFFERENT failure shape — they mean we hung past the
        # driver timeout, which would be the pre-#563 wedge re-
        # asserting itself. Either non-zero is acceptable here; we
        # name the silent-success case explicitly.
        if result.exit_code == 0:
            failures.append(
                "dispatcher exited 0 despite the primary's fatal "
                "staging error; the Python caller has no signal the "
                "run failed (#562 silent-success)"
            )

        try:
            stream = _ANSI_RE.sub("", result.log_file.read_text())
        except OSError as e:
            failures.append(f"operator stream unreadable: {e}")
            return (False, failures)

        # POSITIVE — Seam 0: the verdict line MUST appear.
        verdict_count = stream.count(_VERDICT_LINE)
        if verdict_count == 0:
            failures.append(
                f"operator stream missing {_VERDICT_LINE!r}: "
                f"the primary's fatal RunError::Other did not "
                f"author a ClusterMutation::RunAborted, the observer "
                f"never read run_aborted() in its CRDT, and the "
                f"narrator's abort branch never fired (#563 Seam 0 "
                f"regression; see {result.log_file})"
            )
        elif verdict_count > 1:
            # The narrator's terminal-summary block is gated on a
            # local ``completion_emitted`` latch — multiple lines
            # mean the gate leaked. Strict-equality is the contract.
            failures.append(
                f"operator stream carries the verdict line "
                f"{_VERDICT_LINE!r} {verdict_count} times; the "
                f"narrator's ``completion_emitted`` latch is broken "
                f"(see {result.log_file})"
            )

        # NEGATIVE — Seam 3: no failover-shaped narration. The
        # pre-fix bug emits this WARN line from the narrator's
        # ``narrate_failover`` block when a peer wins an election
        # against the dying primary; post-fix the
        # ``terminal_latched`` gate at the top of the block
        # suppresses it.
        if _SEAM3_FAILOVER_NARRATION in stream:
            failures.append(
                f"operator stream carries {_SEAM3_FAILOVER_NARRATION!r}: "
                f"the narrator's failover-suppression gate (#563 "
                f"Seam 3) did not consult the run-terminal latch; "
                f"the operator's eye lands on the misleading "
                f"'failover' read instead of the authoritative "
                f"abort verdict (see {result.log_file})"
            )

        # NEGATIVE — Seam 1: no election arming. The pre-fix bug
        # emits this WARN from the secondary's election coordinator
        # when the primary's link goes silent; post-fix the
        # ``run_terminal_latched`` check skips the
        # ``Normal → Suspecting`` transition entirely.
        if _SEAM1_ELECTION_ARM in stream:
            failures.append(
                f"operator stream carries {_SEAM1_ELECTION_ARM!r}: "
                f"the secondary's election arm fired despite the "
                f"replicated run-terminal verdict (#563 Seam 1 "
                f"regression); see {result.log_file})"
            )

        # NEGATIVE — Seam 2: no post-latch re-dispatch. A
        # ``"task assigned"`` line BEFORE the verdict is the
        # legitimate concurrent dispatch from a primary that had
        # already started handing out work before its fatal Err;
        # a line AFTER the verdict is a promoted primary's
        # ``bootstrap_tail_dispatch`` re-dispatching the inherited
        # Pending ledger, which the Seam 2 ``run_aborted()`` check
        # at the top of ``bootstrap_tail_dispatch`` short-circuits.
        # ``stream.find`` returns -1 when missing, which is < any
        # real index — a post-verdict "task assigned" line is
        # only checked when the verdict line itself appears.
        if verdict_count >= 1:
            verdict_idx = stream.index(_VERDICT_LINE)
            tail = stream[verdict_idx:]
            if _TASK_ASSIGNED_LINE in tail:
                failures.append(
                    f"operator stream carries {_TASK_ASSIGNED_LINE!r} "
                    f"AFTER the verdict line: a promoted primary "
                    f"re-dispatched work post-RunAborted-latch (#563 "
                    f"Seam 2 regression — bootstrap_tail_dispatch "
                    f"did not consult the latch); see "
                    f"{result.log_file})"
                )

        return (not failures, failures)


SCENARIO = FatalAbortScenario()
