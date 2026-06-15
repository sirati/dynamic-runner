"""Scenario: #570 CustomMessageOutcome Failed (verbatim reason).

Single concern: pin the failure arm of the event-driven custom-message
outcome narrator shipped by #570 — when the primary's handler RAISES,
the run narrator emits an IMPORTANT ERROR line

    custom message handler FAILED (raised): from <origin> (seq <seq>)
      — <verbatim reason> — result discarded, no task mutations applied

(see ``observer/custom_message_outcome_narrator.rs`` narrate_live). The
``<verbatim reason>`` is the Python exception's stringified message —
this scenario pins that the reason TEXT rides through to the operator
stream verbatim (not stripped, not redacted), so a regression that
turns the reason into a generic placeholder surfaces here.

Wire-up
-------

The test consumer reads ``DYNRUNNER_E2E_CUSTOM_MESSAGE_MODE``. Setting
it to ``fail`` binds the SAME secondary listener as the ``ok`` arm
(forwarding worker customs to the primary) but the primary handler
raises ``RuntimeError("test-induced narration failure: synthetic
handler raise")``. The handler's raise transitions the message to
``Failed`` in the replicated ledger (per the
:data:`task_protocol.CustomMessageHandler` error contract) and fires the
outcome event with the exception text as ``reason``. The framework
classifies the handler's raise as a USER ERROR — the dispatch ALSO
terminates non-zero (every important message that fails is a terminal
on its own primary; the consumer asked the handler to raise, so this
is intended-by-test failure). The scenario therefore declares
``allows_nonzero_exit=True``: the assertion is the narration text on
the operator stream, not the exit code.
"""

from __future__ import annotations

import dataclasses
import re
from pathlib import Path

from ._base import DispatchEnv, Scenario, ScenarioPlan, ScenarioResult
from ._dispatch import build_dispatch_argv
from ._staging import stage_inputs


_NUM_TASKS_PER_PHASE = 2

_ANSI_RE = re.compile(r"\x1b\[[0-9;]*m")

# Verbatim reason substring the test handler raises (see
# ``tests/e2e/test_consumer/task.py::_narration_custom_message_handler``
# — its RuntimeError message). The #570 emit interpolates the reason
# between em-dashes; matching the inner substring is enough to prove
# verbatim propagation.
_VERBATIM_REASON = "test-induced narration failure: synthetic handler raise"

# Header substring from the #570 ERROR emit. Combined with the
# verbatim-reason check below this pins both the SHAPE (the
# ``FAILED (raised)`` header) and the ``<verbatim reason>``
# round-trip through the event channel + narrator.
_FAILED_HEADER = "custom message handler FAILED (raised):"


class NarrationCustomMessageFailedScenario(Scenario):
    name = "narration-custom-message-failed"
    description = (
        "#570 CustomMessageOutcome Failed narration: a worker posts a "
        "custom message handled by a raising primary handler; the "
        "verbatim exception reason rides through to the operator stream."
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
        return [
            ScenarioPlan(
                argv=argv,
                paths=paths,
                extra_env={"DYNRUNNER_E2E_CUSTOM_MESSAGE_MODE": "fail"},
                # The handler's deliberate raise terminates the dispatch
                # — the scenario asserts on narration text, not exit code.
                allows_nonzero_exit=True,
            )
        ]

    def assert_outputs(
        self, env: DispatchEnv, results: list[ScenarioResult]
    ) -> tuple[bool, list[str]]:
        del env
        if not results:
            return (True, [])
        result = results[0]

        try:
            stream = _ANSI_RE.sub("", result.log_file.read_text())
        except OSError as e:
            return (False, [f"operator stream unreadable: {e}"])

        failures: list[str] = []
        if _FAILED_HEADER not in stream:
            failures.append(
                f"--important-stdio-only stream missing #570 "
                f"{_FAILED_HEADER!r} ERROR line — the event-driven "
                f"outcome channel did not fire Failed for a raising "
                f"handler (see {result.log_file})"
            )
        if _VERBATIM_REASON not in stream:
            failures.append(
                f"--important-stdio-only stream missing the verbatim "
                f"exception reason {_VERBATIM_REASON!r} from the #570 "
                f"Failed ERROR line — the raise's message did not "
                f"round-trip through the outcome event channel + "
                f"narrator (see {result.log_file})"
            )
        return (not failures, failures)


SCENARIO = NarrationCustomMessageFailedScenario()
