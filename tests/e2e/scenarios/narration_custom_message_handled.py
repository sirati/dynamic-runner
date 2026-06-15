"""Scenario: #568 CustomMessagePosted + #570 CustomMessageOutcome Handled.

Single concern: pin the happy-path arms of the F5 custom-message
narration shipped by #568 and #570 — when a worker streams a custom
message and the primary's handler returns cleanly, the run narrator
emits:

* INFO ``custom message posted: <topic> from <origin> (seq <seq>)`` once
  per (origin, seq) the moment the message lands as ``Unhandled``
  (#568, a5685fa2 — :mod:`run_narrator`);
* INFO ``custom message handled: from <origin> (seq <seq>)`` the moment
  the event-driven outcome channel fires Handled (#570, 07275ef6 —
  :mod:`observer::custom_message_outcome_narrator`).

Both emits target IMPORTANT so ``--important-stdio-only`` admits them.

Wire-up
-------

The test consumer (``tests.e2e.test_consumer``) reads the run-wide opt-
in env var ``DYNRUNNER_E2E_CUSTOM_MESSAGE_MODE``. Setting it to ``ok``
binds the no-raise primary handler AND the secondary listener that
forwards worker customs to the primary (the framework does NOT auto-
forward; see :mod:`task_protocol` ``worker_message_listener`` /
``custom_message_handler`` contract). Each produce worker then calls
``Task.send_message`` once, so the assertion counts ≥1 Posted + ≥1
Handled line per run.
"""

from __future__ import annotations

import dataclasses
import re
from pathlib import Path

from ._base import DispatchEnv, Scenario, ScenarioPlan, ScenarioResult
from ._dispatch import build_dispatch_argv
from ._staging import stage_inputs


_NUM_TASKS_PER_PHASE = 2

# Strip ANSI escape runs — same rationale as :mod:`important_stdio`:
# the operator-stdio sink sets ``with_ansi(false)`` but the harness
# captures stdout+stderr and any non-gated stream the child writes
# could still carry escapes.
_ANSI_RE = re.compile(r"\x1b\[[0-9;]*m")

# Verbatim narration line shapes shipped by #568 (``run_narrator.rs``
# narrate_custom_messages) and #570
# (``observer/custom_message_outcome_narrator.rs`` narrate_live).
# The substrings here are the load-bearing fragments — they sit on the
# IMPORTANT target so the operator gate admits them — and they carry
# the topic / origin / seq parameters the live emit fills in. The
# regexes pin the SHAPE without locking the runtime-known origin id,
# so a re-numbered secondary doesn't false-negative.
_POSTED_RE = re.compile(
    r"custom message posted: [^ ]+ from [^ ]+ \(seq \d+\)"
)
_HANDLED_RE = re.compile(r"custom message handled: from [^ ]+ \(seq \d+\)")


class NarrationCustomMessageHandledScenario(Scenario):
    name = "narration-custom-message-handled"
    description = (
        "#568 CustomMessagePosted + #570 CustomMessageOutcome Handled "
        "narration: a run with --important-stdio-only and a worker that "
        "posts a custom message handled cleanly by the primary."
    )
    requires = ()

    def prepare(
        self, env: DispatchEnv, tmp_root: Path
    ) -> list[ScenarioPlan]:
        if env.mode != "slurm":
            return []
        paths = stage_inputs(tmp_root, _NUM_TASKS_PER_PHASE)
        slurm_env = dataclasses.replace(env, mode="slurm")
        argv = build_dispatch_argv(
            env=slurm_env,
            source=paths.source,
            output=paths.output,
            num_tasks=_NUM_TASKS_PER_PHASE,
            extra_args=("--important-stdio-only",),
        )
        # Drive both the discovery-side handler binding and the worker's
        # send via the env var (the consumer's task module reads it at
        # import time to gate the handler attribute, and discovery
        # stamps the mode on every produce payload so the worker's
        # send-or-skip branch matches without a second env-var read in
        # the worker container).
        return [
            ScenarioPlan(
                argv=argv,
                paths=paths,
                extra_env={"DYNRUNNER_E2E_CUSTOM_MESSAGE_MODE": "ok"},
            )
        ]

    def assert_outputs(
        self, env: DispatchEnv, results: list[ScenarioResult]
    ) -> tuple[bool, list[str]]:
        del env
        if not results:
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
        posted_hits = _POSTED_RE.findall(stream)
        if not posted_hits:
            failures.append(
                f"--important-stdio-only stream missing #568 "
                f"'custom message posted: <topic> from <origin> (seq N)' "
                f"INFO line — no posted-narration emit reached the "
                f"operator stdout (see {result.log_file})"
            )
        handled_hits = _HANDLED_RE.findall(stream)
        if not handled_hits:
            failures.append(
                f"--important-stdio-only stream missing #570 "
                f"'custom message handled: from <origin> (seq N)' INFO "
                f"line — the event-driven outcome channel did not fire "
                f"Handled (see {result.log_file})"
            )
        return (not failures, failures)


SCENARIO = NarrationCustomMessageHandledScenario()
