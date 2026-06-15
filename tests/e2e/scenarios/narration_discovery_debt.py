"""Scenario: #568 DiscoveryDebt narration (Owed -> Settled).

Single concern: pin the two ``DiscoveryDebt`` arms shipped by #568 — when
the dispatcher runs in mode-2 (``--source-already-staged``) the framework
defers task discovery to a setup-secondary and the run narrator emits an
``IMPORTANT`` INFO ``discovery owed — awaiting compute-peer primary to
seed task ledger`` once the debt is declared, then an INFO ``discovery
settled — task ledger fully seeded by compute-peer primary`` once the
secondary's seed lands. This scenario drives both edges end-to-end and
greps for both messages on the operator's ``--important-stdio-only``
stream.

Why a separate scenario from ``source-already-staged``?
-------------------------------------------------------

``source_already_staged`` already pins the load-bearing
``--source-already-staged`` behaviour but does NOT gate on the
``--important-stdio-only`` stream (it runs single-process / local modes
that don't need the IMPORTANT-target operator gate), so its log stream
carries every level and the narration arms would not be discriminated
from the firehose. This scenario runs SLURM-mode only with
``--important-stdio-only`` so the assertion targets the operator stream
exactly.

WindDownRequested deferral
--------------------------

The third #568 arm (``WindDownRequested`` WARN per (secondary, member_gen)
pair) requires a replacement secondary that the framework subsequently
winds down — a code path driven by the slurm-authoritative reversibility
recipe with a deterministic respawn-then-rescind trigger. There is no
single-flag CLI to drive that path against the slurm-test-env cluster
today; the orchestrator's framework test fixtures would be the natural
home (state-mutation level: ``WindDownRequested`` is an apply-target).
That arm is deferred — see ``test-568-570-narration.sh``'s exit message.
"""

from __future__ import annotations

import dataclasses
import re
from pathlib import Path

from ._base import DispatchEnv, Scenario, ScenarioPlan, ScenarioResult
from ._dispatch import build_dispatch_argv
from ._staging import stage_inputs


_NUM_TASKS_PER_PHASE = 2

# Strip ANSI escape runs before substring matching — the operator-stdio
# sink sets ``with_ansi(false)`` but the harness captures stdout+stderr
# and any non-gated stream the child writes could still carry escapes.
_ANSI_RE = re.compile(r"\x1b\[[0-9;]*m")

# Verbatim narration substrings shipped by #568 (a5685fa2). See
# ``crates/dynrunner-manager-distributed/src/run_narrator.rs`` — both
# emits are at the IMPORTANT target so the operator gate admits them.
_DISCOVERY_OWED = (
    "discovery owed — awaiting compute-peer primary to seed task ledger"
)
_DISCOVERY_SETTLED = (
    "discovery settled — task ledger fully seeded by compute-peer primary"
)


class NarrationDiscoveryDebtScenario(Scenario):
    name = "narration-discovery-debt"
    description = (
        "#568 DiscoveryDebt Owed/Settled narration: a mode-2 run "
        "(--source-already-staged) under --important-stdio-only "
        "surfaces both arms on the operator stream."
    )
    requires = ()

    def prepare(
        self, env: DispatchEnv, tmp_root: Path
    ) -> list[ScenarioPlan]:
        # SLURM-only: the discovery-debt narration only fires when the
        # primary relocates to a compute peer that performs discovery,
        # which is the SLURM dispatch shape (single-process / local
        # modes don't carry the compute-peer relocation seam the
        # narration arms key off of).
        if env.mode != "slurm":
            return []
        paths = stage_inputs(tmp_root, _NUM_TASKS_PER_PHASE)
        slurm_env = dataclasses.replace(env, mode="slurm")
        argv = build_dispatch_argv(
            env=slurm_env,
            source=paths.source,
            output=paths.output,
            num_tasks=_NUM_TASKS_PER_PHASE,
            extra_args=(
                "--important-stdio-only",
                "--source-already-staged",
                str(paths.source),
            ),
        )
        return [ScenarioPlan(argv=argv, paths=paths)]

    def assert_outputs(
        self, env: DispatchEnv, results: list[ScenarioResult]
    ) -> tuple[bool, list[str]]:
        del env
        if not results:
            # Non-slurm mode: nothing to assert (prepare returned []).
            return (True, [])
        result = results[0]

        if result.exit_code != 0:
            return (
                False,
                [
                    f"dispatch exited non-zero: {result.exit_code} "
                    f"(see {result.log_file})"
                ],
            )

        try:
            stream = _ANSI_RE.sub("", result.log_file.read_text())
        except OSError as e:
            return (False, [f"operator stream unreadable: {e}"])

        failures: list[str] = []
        for needle in (_DISCOVERY_OWED, _DISCOVERY_SETTLED):
            if needle not in stream:
                failures.append(
                    f"--important-stdio-only stream missing #568 "
                    f"DiscoveryDebt narration {needle!r}: the shipped "
                    f"IMPORTANT-target emit did not reach the operator's "
                    f"stdout end-to-end (see {result.log_file})"
                )
        return (not failures, failures)


SCENARIO = NarrationDiscoveryDebtScenario()
