"""Scenario: the ``--important-stdio-only`` operator-stream contract.

Single concern: end-to-end proof that ``--important-stdio-only`` surfaces
the shipped wake-worthy milestones on the dispatcher's stdout AND gates out
the routine per-task chatter ("only logs that wake an LLM"). It is the e2e
counterpart of the unit-level gate tests
(``crates/dynrunner-pyo3/src/logging/mod.rs`` and the per-emit-site
``bringup_milestone.rs`` / ``important_events.rs`` tests): those prove each
emit site keys the IMPORTANT target and the gate admits exactly that target;
THIS scenario proves the whole chain actually reaches the operator's stdout
when the real SLURM pipeline runs, which the unit tests cannot (the gate is
installed by the Python CLI after argparse, and the milestones fire from the
live preparation / coordinator / observer loops).

Topology — the canonical two-phase consumer (see
:mod:`tests.e2e.test_consumer.task`)::

    produce ──depends_on──▶ consume

run under ``--multi-computer slurm`` with ``--important-stdio-only`` added.
In SLURM mode the dispatcher process IS the operator-facing endpoint: it
boots as the bootstrap primary, relocates its primary role to a compute
peer, and then runs the observer coordinator IN THE SAME PROCESS — so its
stdout carries both the submitter-side bring-up milestones (gateway connect,
image transfer, jobs queued, secondaries connected) AND the observer's
RunNarrator phase / completion narration. The harness captures that stdout
verbatim into ``result.log_file``, which IS the operator stream this
scenario asserts against.

What's asserted
---------------

POSITIVE — every milestone class the operator must see surfaces at least
once (substring match, ANSI-stripped, so the compact operator format
``HH:MM±hhmm LEVEL message`` does not interfere):

  * bring-up: gateway connect, container image ready, all SLURM jobs queued,
    all secondaries connected;
  * phase narration: "starting job phase" and "phase complete";
  * the one-shot terminal: "run complete: ... — shutting down".

NEGATIVE — the routine per-task completion line (``task complete ...
secondary=...``, the firehose ``parallel-4-workers`` counts on) must NOT
appear on the gated stream. That line is emitted on the NORMAL tracing
target, so its presence would prove the gate leaks non-important events —
the exact "flood" regression #197's history warns about.

The publish outputs are also asserted (the run must actually do its work,
not just narrate) so a silently-broken run cannot pass on narration alone.
"""

from __future__ import annotations

import re
from pathlib import Path

from ._assertions import assert_files_present, expected_canonical_outputs
from ._base import DispatchEnv, Scenario, ScenarioPlan, ScenarioResult
from ._dispatch import build_dispatch_argv
from ._staging import stage_inputs


_NUM_TASKS_PER_PHASE = 2

# Strip ANSI escape runs before substring matching: the operator-stdio sink
# sets `with_ansi(false)`, but the harness captures stdout+stderr and any
# other (non-gated) stream the child writes could still carry escapes — be
# robust to them rather than depend on a clean capture.
_ANSI_RE = re.compile(r"\x1b\[[0-9;]*m")

# Wake-worthy milestone substrings the gated operator stream MUST carry.
# Each is the verbatim message fragment of an IMPORTANT-target emit:
#   - "Connecting to gateway..."          run_pipeline.rs
#   - "container image ready on gateway"  job_manager/images.rs
#   - "SLURM jobs queued"                 slurm/pipeline/preparation.rs (#418)
#   - "secondaries connected"            primary/connect.rs (#418; matches
#                                         both the full-fleet and quorum lines)
#   - "starting job phase"               run_narrator.rs (phase started)
#   - "phase complete"                   run_narrator.rs (phase complete)
#   - "run complete:"                    run_narrator.rs (terminal summary)
_REQUIRED_MILESTONES: tuple[str, ...] = (
    "Connecting to gateway...",
    "container image ready on gateway",
    "SLURM jobs queued",
    "secondaries connected",
    "starting job phase",
    "phase complete",
    "run complete:",
)

# Routine per-task chatter that MUST be gated OUT. The framework emits one
# such line per completed task on the NORMAL target (see
# `parallel_4_workers._TASK_COMPLETE_SECONDARY_RE`); its presence on the
# gated stream proves a leak.
_FORBIDDEN_FLOOD_RE = re.compile(r"task complete .*?secondary=")


class ImportantStdioScenario(Scenario):
    name = "important-stdio"
    description = (
        "--important-stdio-only contract: the dispatcher's stdout carries "
        "the bring-up / phase / completion milestones and is NOT flooded "
        "with per-task chatter (the 'only logs that wake an LLM' guarantee)."
    )
    requires = ()

    def prepare(
        self, env: DispatchEnv, tmp_root: Path
    ) -> list[ScenarioPlan]:
        paths = stage_inputs(tmp_root, _NUM_TASKS_PER_PHASE)
        argv = build_dispatch_argv(
            env=env,
            source=paths.source,
            output=paths.output,
            num_tasks=_NUM_TASKS_PER_PHASE,
            extra_args=("--important-stdio-only",),
        )
        return [ScenarioPlan(argv=argv, paths=paths)]

    def assert_outputs(
        self, env: DispatchEnv, results: list[ScenarioResult]
    ) -> tuple[bool, list[str]]:
        del env
        result = results[0]

        # The run must actually do its work — narration alone must not pass.
        ok_present, missing = assert_files_present(
            result.plan.paths.publish_dst,
            expected_canonical_outputs(_NUM_TASKS_PER_PHASE),
        )

        failures: list[str] = list(missing)

        try:
            stream = _ANSI_RE.sub("", result.log_file.read_text())
        except OSError as e:
            return (False, [f"operator stream unreadable: {e}"])

        # POSITIVE: every milestone class must appear at least once.
        for needle in _REQUIRED_MILESTONES:
            if needle not in stream:
                failures.append(
                    f"--important-stdio-only stream missing milestone "
                    f"{needle!r}: a shipped IMPORTANT-target emit did not "
                    f"reach the operator's stdout end-to-end (see "
                    f"{result.log_file})"
                )

        # NEGATIVE: the routine per-task firehose must be gated out.
        flood = _FORBIDDEN_FLOOD_RE.findall(stream)
        if flood:
            failures.append(
                f"--important-stdio-only stream FLOODED: {len(flood)} "
                f"per-task 'task complete ... secondary=' line(s) leaked "
                f"past the importance gate (the gate is admitting the "
                f"normal target — the 'only logs that wake an LLM' contract "
                f"is broken; see {result.log_file})"
            )

        return (not failures, failures)


SCENARIO = ImportantStdioScenario()
