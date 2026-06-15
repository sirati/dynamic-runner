"""In-process distributed runner Err-propagation contract (#562).

Single concern: pin the contract that ``_rs.run_distributed`` MUST
return / raise within a bounded wall-clock budget when the in-process
distributed primary's run-loop returns a fatal ``RunError`` — instead
of wedging indefinitely while the setup-peer-observer waits for a
verdict no one broadcast and the other secondaries tick on
anti-entropy.

Repro shape (verbatim from the #558 RCA):

* A ``TaskInfo`` whose ``path`` resolves under ``source_dir`` to a file
  that does NOT exist on disk.
* The TaskDefinition declares ``uses_file_based_items = True``
  (the default the framework infers when the attribute is absent).
* ``_rs.run_distributed`` is invoked; the relocate-target's
  ``maybe_auto_stage_initial`` reads the missing file, raises
  ``StagingError::SourceUnreadable`` which the primary's run-loop
  surfaces as ``RunError::Other`` (the catch-all variant).

Pre-fix, the cluster-wide ``ClusterMutation::RunAborted`` broadcast
was wired only for specific ``RunError`` variants (#3a duplicate,
``FatalPolicyExit``, ``InvalidComposedGraph``, #3b invalidation);
``RunError::Other`` was NOT broadcast. The setup-peer-observer's
``evaluate_exit`` reads ``run_aborted()`` / ``run_complete()`` from
its CRDT — without the broadcast, neither flips and the observer
blocks forever. The orchestrator's ``node.run(inputs).await``
inherits the wedge and ``_rs.run_distributed`` never returns —
``mgr.run`` blocks the GIL-detached tokio runtime indefinitely and
the Python caller has no way to learn the run failed.

Post-fix (#563 widens the broadcast Seam set to cover ALL fatal
``RunError`` variants, including ``Other``), the broadcast lands in
every replica's CRDT, the observer reads ``run_aborted()`` and
returns ``ObserverTerminal::Aborted { reason }``, the orchestrator's
GIL-side tail maps it to a ``PyRuntimeError`` carrying the verbatim
reason ("queue_initial_staging: cannot read ..."), and the Python
caller catches an actionable error within seconds of the primary's
own ``primary exiting: run loop returned an error`` log line.

This test is the in-process E2E contract — orthogonal to #563's
Rust-side seam fix. It will be RED until #563's Seam 0 lands; once
#563 closes the missing broadcast on ``RunError::Other``, the test
flips GREEN automatically with no further changes here.

Per the user's "tests do not time out — if it locks up you have a
deadlock" rule, the hang is detected by running the framework call
in a worker thread and joining with a wall-clock deadline: if the
join hits the budget, the test FAILS (the bug fingerprint); if the
join returns, the captured exception is asserted to carry the
verbatim staging reason (the post-fix contract).
"""

from __future__ import annotations

import threading
from dataclasses import dataclass, field
from pathlib import Path
from types import SimpleNamespace

import pytest


pytest.importorskip(
    "dynamic_runner",
    reason=(
        "dynamic_runner not installed; run `maturin develop --release` "
        "in this worktree first."
    ),
)


# ── Synthetic TaskInfo + TaskDefinition shapes (the #558 RCA repro) ──


@dataclass
class _BinaryIdentifier:
    binary_name: str

    def identifier_key(self) -> str:
        return self.binary_name


@dataclass
class _TaskInfo:
    """Minimum field set the PyO3 ``extract_binaries`` extractor reads.

    Mirrors ``test_lifecycle_hooks._StubTaskInfo`` field-for-field so
    the synthetic binary survives the Python -> Rust crossing.
    """

    path: str
    size: int
    identifier: _BinaryIdentifier
    phase_id: str = ""
    type_id: str = ""
    affinity_id: str | None = None
    payload: dict = field(default_factory=dict)
    task_id: str = ""


class _FileBackedTask:
    """Minimal TaskDefinition: file-based items, one phase, no work.

    ``uses_file_based_items`` is the framework default (``True``) and
    is set explicitly here so the contract is legible at the test
    site — the run-time selector that drives ``maybe_auto_stage_initial``
    (the ``StagingError`` source).
    """

    uses_file_based_items = True

    def get_phases(self):
        from dynamic_runner.task_protocol import PhaseSpec, TaskTypeSpec

        return (
            PhaseSpec(
                phase_id="only-phase",
                types=(
                    TaskTypeSpec(
                        type_id="default",
                        worker_module=(
                            "dynamic_runner.tests._failover_stub_worker"
                        ),
                    ),
                ),
                # The phase is populated by the binaries below; no
                # may_be_empty relaxation is needed (the run aborts
                # BEFORE drain on the staging read failure).
            ),
        )

    def discover_items(self, source_dir, args):
        return []

    def estimate_memory(self, item) -> int:
        return 1024 * 1024

    def add_task_arguments(self, parser) -> None:
        pass

    def build_worker_command_args(
        self, type_id, args, source_dir, output_dir, skip_existing
    ):
        return []

    def get_output_filename_pattern(self, type_id, item) -> str:
        return f"{item.path}.done"

    def on_run_start(self, source_dir, output_dir, args, primary_handle=None):
        pass

    def on_run_end(self, success: bool) -> None:
        pass

    def on_phase_start(self, phase_id) -> None:
        pass

    def on_phase_end(self, phase_id, completed: int, failed: int) -> None:
        pass


# ── The contract test ───────────────────────────────────────────────


# Wall-clock budget for the bounded-return contract. The primary's
# natural ``RunError::Other`` exit fires within seconds of run-start
# (the staging walk is the first thing the primary does after fleet
# bring-up); 60s is generous slack for slow CI + the fleet-bring-up
# window, and is small enough that a true wedge (the bug fingerprint)
# is unmistakable. Per the brief: "if it blocks longer than 30s, the
# test fails the contract" — we use 60s for slow-CI tolerance.
_BOUNDED_RETURN_BUDGET_SECS = 60.0


def test_run_distributed_returns_on_primary_fatal_err(tmp_path: Path) -> None:
    """``_rs.run_distributed`` MUST return / raise within a bounded
    wall-clock budget when the in-process distributed primary's run
    loop returns a fatal ``RunError`` (here ``RunError::Other``
    wrapping ``StagingError`` from a missing source file).

    The bug fingerprint pre-#563: the call wedges forever because the
    setup-peer-observer's ``evaluate_exit`` never reads ``run_aborted``
    in its CRDT — no replica broadcast the verdict for the catch-all
    ``RunError::Other`` variant.

    Contract:
    1. The call returns / raises within ``_BOUNDED_RETURN_BUDGET_SECS``
       wall-clock (the deadlock guard — pre-fix this hangs).
    2. The captured exception carries the verbatim staging reason
       ("queue_initial_staging: cannot read ...") so the Python caller
       can surface an actionable diagnostic.

    The call is invoked on a worker thread (not the test thread) so a
    hang is OBSERVABLE via ``Thread.join(timeout=...)`` — joining the
    thread with a deadline is the bounded-time contract assertion; if
    the join hits the budget, the thread is still inside
    ``_rs.run_distributed`` and the test FAILS the contract.
    """
    import dynamic_runner as _rs

    source_dir = tmp_path / "src"
    output_dir = tmp_path / "out"
    source_dir.mkdir(parents=True, exist_ok=True)
    output_dir.mkdir(parents=True, exist_ok=True)

    # A binary whose relative ``path`` resolves under ``source_dir`` to
    # a file that does NOT exist on disk. The primary's staging walk
    # opens ``source_dir / "missing-input.bin"`` and hits ENOENT —
    # ``StagingError::SourceUnreadable``.
    missing_path = "missing-input.bin"
    binaries = [
        _TaskInfo(
            path=missing_path,
            size=100,
            identifier=_BinaryIdentifier(binary_name=missing_path),
            phase_id="only-phase",
            type_id="default",
            task_id="missing-input-1",
        ),
    ]

    primary_cfg = _rs.PrimaryConfig(num_secondaries=1)
    secondary_template = _rs.SecondaryConfig(
        secondary_id="<template>",
        num_workers=1,
        max_resources=_rs.ResourceMap({"memory": 64 * 1024 * 1024}),
    )
    args = SimpleNamespace()
    task = _FileBackedTask()

    # Capture either a normal return or any raised exception from the
    # worker thread. Both outcomes satisfy the bounded-time contract;
    # the test then asserts the captured exception carries the verbatim
    # reason. A normal return (no exception) would mean the framework
    # silently swallowed the fatal Err and the run reported success
    # despite never staging — surfaced as an explicit test failure
    # because the Python caller would otherwise get exit-0 on a
    # broken run.
    outcome: dict[str, object] = {"raised": None, "returned": None}

    def _invoke() -> None:
        try:
            outcome["returned"] = _rs.run_distributed(
                primary_cfg,
                secondary_template,
                task,
                args,
                str(source_dir),
                str(output_dir),
                binaries,
            )
        except BaseException as exc:  # noqa: BLE001 — capture every exit
            outcome["raised"] = exc

    worker = threading.Thread(
        target=_invoke,
        name="rs-run-distributed-562",
        daemon=True,
    )
    worker.start()
    worker.join(timeout=_BOUNDED_RETURN_BUDGET_SECS)

    # Contract 1: the call returned within the bounded wall-clock
    # budget. A still-alive thread here is the #562 deadlock
    # fingerprint — pre-#563 broadcast extension, the
    # setup-peer-observer's ``node.run`` blocks forever waiting for
    # ``run_aborted``/``run_complete`` in the CRDT.
    assert not worker.is_alive(), (
        f"_rs.run_distributed did not return within "
        f"{_BOUNDED_RETURN_BUDGET_SECS:.0f}s of the primary's fatal "
        f"Err — the in-process observer is wedged waiting for a "
        f"verdict that was never broadcast (#562 / #563)"
    )

    raised = outcome["raised"]
    returned = outcome["returned"]

    # Contract 2: the run reported failure to the Python caller. A
    # silent return would mean the framework swallowed the staging
    # error and exited rc=0 — masking a broken run.
    assert raised is not None, (
        f"_rs.run_distributed returned cleanly ({returned!r}) despite "
        f"the primary's fatal staging error — the Python caller would "
        f"have no signal the run failed (#562)"
    )

    # Contract 3: the raised exception carries the verbatim staging
    # reason. The primary's ``RunError`` reason field embeds the
    # ``StagingError::SourceUnreadable`` ``Display`` output verbatim;
    # the GIL-side tail wraps it in a ``PyRuntimeError`` whose message
    # MUST preserve the reason so the operator (e.g. asm-tokenizer's
    # error-grep predicates) can pin the cause.
    raised_text = str(raised)
    assert "queue_initial_staging: cannot read" in raised_text, (
        f"_rs.run_distributed raised but the message did not carry "
        f"the verbatim staging reason; got: {raised_text!r}"
    )
    assert missing_path in raised_text, (
        f"_rs.run_distributed raised but the message did not name "
        f"the missing file ({missing_path!r}); got: {raised_text!r}"
    )
