"""Worker-runtime exception → wire mapping tests.

Covers the contract from plan §D6: every documented exception class
produces the documented wire response, and the loop terminates on
the documented terminal conditions (StopCommand, channel close,
mid-task interrupt, OOM).

Tests use a scripted ``CommunicationInterface`` stub so the loop
can be driven without spawning a real worker subprocess. The stub
records every response sent so assertions inspect the wire-level
sequence directly.
"""
from __future__ import annotations

import json
import subprocess
import unittest
from dataclasses import dataclass, field
from typing import Optional
from unittest.mock import patch

from dynamic_runner.comm import (
    Command,
    CommunicationInterface,
    DoneResponse,
    ErrorResponse,
    ErrorType,
    KeepaliveResponse,
    PhaseUpdateResponse,
    ProcessBinaryCommand,
    ReadyResponse,
    Response,
    StopCommand,
    WorkerExceptionResponse,
)
from dynamic_runner.worker import (
    NonRecoverableError,
    RecoverableError,
    Task,
    WorkerOutput,
    run,
    task_function,
)
from dynamic_runner.worker.runtime import _REGISTRY, _encode_done_payload


@dataclass
class ScriptedComm(CommunicationInterface):
    """Comm stub: feeds a pre-defined queue of commands to the
    worker, captures every response sent. ``None`` in the queue
    triggers a channel-close (mirrors ``readline()`` returning empty).
    """

    inbox: list[Optional[Command]] = field(default_factory=list)
    outbox: list[Response] = field(default_factory=list)
    closed: bool = False

    def send_command(self, command):
        return (True, None)

    def send_response(self, response):
        self.outbox.append(response)
        return (True, None)

    def receive_command(self, blocking=True):
        if not self.inbox:
            return None
        return self.inbox.pop(0)

    def receive_responses(self):
        return []

    def close(self):
        self.closed = True

    def set_blocking(self, blocking):
        pass


def _drive(handler, commands):
    """Helper: run the loop with a scripted comm and return outbox.

    `comm.exit_code` is populated with the SystemExit code that
    `run()` raised on the way out (0 if `run()` returned normally,
    or whatever code SystemExit was raised with — e.g. 1 on a
    NonRecoverableError-bearing run, per Bug A's exit-code
    contract).
    """
    # Reset registry between tests so registry-pollution from a
    # previous @task_function decoration doesn't leak in.
    _REGISTRY.default = None
    _REGISTRY.overwritten = False
    comm = ScriptedComm(inbox=list(commands))
    comm.exit_code = 0
    try:
        run(handler, comm=comm)
    except SystemExit as exc:
        # Mirror the OS-level exit code into the comm fixture so
        # tests can assert on it without catching SystemExit
        # themselves.
        comm.exit_code = exc.code if isinstance(exc.code, int) else 1
    return comm


def _process(path: str = "/some/binary", payload: Optional[str] = None):
    return ProcessBinaryCommand(relative_path=path, payload=payload)


class WorkerRuntimeTests(unittest.TestCase):
    """End-to-end loop behaviour: every command path, every error
    classification, both terminal and continuation cases.
    """

    def test_ready_then_done_on_clean_completion(self):
        def handle(task: Task) -> Optional[WorkerOutput]:
            return None

        comm = _drive(handle, [_process(), StopCommand()])

        kinds = [type(r).__name__ for r in comm.outbox]
        self.assertEqual(kinds, ["ReadyResponse", "DoneResponse"])
        self.assertTrue(comm.closed)

    def test_done_response_carries_warnings_and_filtered(self):
        def handle(task: Task) -> WorkerOutput:
            return WorkerOutput(warnings=3, filtered=7)

        comm = _drive(handle, [_process(), None])

        done = [r for r in comm.outbox if isinstance(r, DoneResponse)]
        self.assertEqual(len(done), 1)
        # The runtime JSON-encodes ``WorkerOutput`` counters into the
        # opaque ``result_data`` payload. The framework never inspects
        # those bytes; a consumer primary that wants the counters
        # decodes the JSON itself. The test mirrors that decode path
        # to verify the producer-side contract end-to-end.
        self.assertIsNotNone(done[0].result_data)
        decoded = json.loads(done[0].result_data.decode("utf-8"))
        self.assertEqual(decoded, {"warnings": 3, "filtered": 7})

    def test_recoverable_error_emits_recoverable_response(self):
        def handle(task: Task):
            raise RecoverableError("transient blip")

        comm = _drive(handle, [_process(), StopCommand()])

        errs = [r for r in comm.outbox if isinstance(r, ErrorResponse)]
        self.assertEqual(len(errs), 1)
        self.assertEqual(errs[0].error_type, ErrorType.RECOVERABLE)
        self.assertIn("transient blip", errs[0].error_message)

    def test_nonrecoverable_error_emits_nonrecoverable_response(self):
        def handle(task: Task):
            raise NonRecoverableError("config bad")

        comm = _drive(handle, [_process(), StopCommand()])

        errs = [r for r in comm.outbox if isinstance(r, ErrorResponse)]
        self.assertEqual(len(errs), 1)
        self.assertEqual(errs[0].error_type, ErrorType.NON_RECOVERABLE)
        self.assertIn("config bad", errs[0].error_message)
        # Bug A: a worker that emitted at least one
        # NonRecoverableError MUST exit with a non-zero process
        # exit code. The wire-protocol error reached the manager
        # synchronously (asserted above); the exit-code half of
        # the contract reaches the parent-process supervisor via
        # SystemExit propagation.
        self.assertEqual(
            comm.exit_code,
            1,
            "NonRecoverableError run must exit with code 1 so OS-level "
            "supervisors can discriminate clean shutdown from "
            "non-recoverable-error shutdown",
        )

    def test_clean_completion_exits_zero(self):
        # Inverse of the Bug A regression test: a run with no
        # NonRecoverableError raised must NOT exit non-zero.
        # Without this guard, a logic flip on the
        # `non_recoverable_emitted` flag could silently start
        # exit-1-ing every clean run and only surface as a
        # downstream supervisor classification regression.
        def handle(task: Task):
            return None  # Clean completion → DoneResponse

        comm = _drive(handle, [_process(), StopCommand()])

        dones = [r for r in comm.outbox if isinstance(r, DoneResponse)]
        self.assertEqual(len(dones), 1)
        self.assertEqual(
            comm.exit_code,
            0,
            "clean-completion run must exit with code 0",
        )

    def test_recoverable_error_exits_zero(self):
        # RecoverableError is "stay alive, retry the task" per the
        # NonRecoverableError class docstring contract — the
        # framework retries; the worker process keeps living for
        # the next task. The process should NOT exit non-zero on
        # a Recoverable failure; the exit-code contract applies
        # only to NonRecoverable.
        def handle(task: Task):
            raise RecoverableError("transient blip")

        comm = _drive(handle, [_process(), StopCommand()])

        errs = [r for r in comm.outbox if isinstance(r, ErrorResponse)]
        self.assertEqual(len(errs), 1)
        self.assertEqual(errs[0].error_type, ErrorType.RECOVERABLE)
        self.assertEqual(
            comm.exit_code,
            0,
            "Recoverable-only run must exit with code 0 — the worker "
            "stays alive for the next task per docstring contract",
        )

    def test_memory_error_emits_oom_and_exits(self):
        # MemoryError is process-level: the runtime must emit OOM
        # AND break out of the loop, even if subsequent commands
        # remain in the inbox.
        def handle(task: Task):
            raise MemoryError("blew the heap")

        comm = _drive(handle, [_process(), _process("/should/not/run"), StopCommand()])

        errs = [r for r in comm.outbox if isinstance(r, ErrorResponse)]
        self.assertEqual(len(errs), 1)
        self.assertEqual(errs[0].error_type, ErrorType.OUT_OF_MEMORY)
        # Only the first task ran; the runtime must not have looped.
        self.assertEqual(
            len([r for r in comm.outbox if isinstance(r, DoneResponse)]),
            0,
        )

    def test_called_process_error_emits_recoverable(self):
        def handle(task: Task):
            raise subprocess.CalledProcessError(returncode=2, cmd=["nix", "build"])

        comm = _drive(handle, [_process(), StopCommand()])

        errs = [r for r in comm.outbox if isinstance(r, ErrorResponse)]
        self.assertEqual(len(errs), 1)
        self.assertEqual(errs[0].error_type, ErrorType.RECOVERABLE)
        self.assertIn("CalledProcessError", errs[0].error_message)

    def test_unclassified_exception_emits_workerexception_recoverable(self):
        # Unknown exception type → WorkerExceptionResponse with a
        # full traceback and error_type=RECOVERABLE per plan D6.
        def handle(task: Task):
            raise ValueError("bad input")

        comm = _drive(handle, [_process(), StopCommand()])

        wx = [r for r in comm.outbox if isinstance(r, WorkerExceptionResponse)]
        self.assertEqual(len(wx), 1)
        self.assertEqual(wx[0].error_type, ErrorType.RECOVERABLE)
        self.assertEqual(wx[0].exception_type, "ValueError")
        self.assertIn("bad input", wx[0].exception_message)
        self.assertIn("ValueError", wx[0].traceback_str)

    def test_keyboard_interrupt_mid_task_emits_recoverable_and_exits(self):
        # SIGINT (KeyboardInterrupt) mid-task: report the task as
        # Recoverable so the framework retries it on a fresh worker,
        # then exit cleanly.
        def handle(task: Task):
            raise KeyboardInterrupt()

        comm = _drive(handle, [_process(), _process("/should/not/run"), StopCommand()])

        errs = [r for r in comm.outbox if isinstance(r, ErrorResponse)]
        self.assertEqual(len(errs), 1)
        self.assertEqual(errs[0].error_type, ErrorType.RECOVERABLE)
        self.assertIn("KeyboardInterrupt", errs[0].error_message)

    def test_payload_is_json_decoded(self):
        captured: list[Task] = []

        def handle(task: Task):
            captured.append(task)
            return None

        payload = json.dumps({"versions": ["1.0", "2.0"]})
        _drive(handle, [_process(payload=payload), StopCommand()])

        self.assertEqual(len(captured), 1)
        self.assertEqual(captured[0].payload, {"versions": ["1.0", "2.0"]})
        self.assertEqual(captured[0].payload_str, payload)

    def test_payload_falls_back_to_raw_string_when_invalid_json(self):
        captured: list[Task] = []

        def handle(task: Task):
            captured.append(task)
            return None

        _drive(handle, [_process(payload="not-json-at-all"), StopCommand()])

        self.assertEqual(captured[0].payload, "not-json-at-all")

    def test_stop_command_terminates_loop(self):
        called = []

        def handle(task: Task):
            called.append(task)
            return None

        comm = _drive(handle, [StopCommand(), _process("/should/not/run")])

        self.assertEqual(called, [])
        # Only the Ready response — no task ever dispatched.
        self.assertEqual(
            [type(r).__name__ for r in comm.outbox],
            ["ReadyResponse"],
        )

    def test_channel_close_terminates_loop(self):
        # ``None`` in the inbox stands in for ``readline()`` →
        # empty string, which the comm interface returns as None.
        called = []

        def handle(task: Task):
            called.append(task)
            return None

        comm = _drive(handle, [None])

        self.assertEqual(called, [])
        self.assertTrue(comm.closed)

    def test_task_function_decorator_is_used_when_handle_omitted(self):
        _REGISTRY.default = None

        @task_function
        def registered_handler(task: Task):
            return WorkerOutput(warnings=42)

        comm = ScriptedComm(inbox=[_process(), StopCommand()])
        run(comm=comm)

        done = [r for r in comm.outbox if isinstance(r, DoneResponse)]
        self.assertEqual(len(done), 1)
        self.assertIsNotNone(done[0].result_data)
        decoded = json.loads(done[0].result_data.decode("utf-8"))
        self.assertEqual(decoded["warnings"], 42)

    def test_run_without_handler_raises(self):
        _REGISTRY.default = None
        comm = ScriptedComm(inbox=[StopCommand()])
        with self.assertRaises(RuntimeError):
            run(comm=comm)

    def test_task_keepalive_sends_wire_msg(self):
        def handle(task: Task):
            task.keepalive()
            return None

        comm = _drive(handle, [_process(), StopCommand()])

        kinds = [type(r).__name__ for r in comm.outbox]
        self.assertEqual(
            kinds,
            ["ReadyResponse", "KeepaliveResponse", "DoneResponse"],
        )

    def test_task_set_phase_sends_wire_msg(self):
        def handle(task: Task):
            task.set_phase("tok")
            return None

        comm = _drive(handle, [_process(), StopCommand()])

        phases = [r for r in comm.outbox if isinstance(r, PhaseUpdateResponse)]
        self.assertEqual(len(phases), 1)
        self.assertEqual(phases[0].phase_name, "tok")
        # Phase update precedes the Done that closes the task.
        kinds = [type(r).__name__ for r in comm.outbox]
        self.assertEqual(
            kinds,
            ["ReadyResponse", "PhaseUpdateResponse", "DoneResponse"],
        )

    def test_task_emit_silent_when_no_hook(self):
        # Direct construction (no runtime loop) → calls are no-ops.
        # This is the unit-test-harness path: a consumer testing its
        # handler in isolation can build a Task by hand and call
        # task.keepalive() / task.set_phase() without crashing.
        t = Task(relative_path="/x")
        t.keepalive()
        t.set_phase("phase-a")


class KeyedOutputsTests(unittest.TestCase):
    """Phase 6b: keyed task outputs API on the worker `Task`.

    Covers the producer side (publish_string / publish(key=)),
    the encode-merge with WorkerOutput counters, and the
    predecessor_outputs read seam constructed from the
    `predecessor_outputs_json` PyO3 bridge field.
    """

    def test_publish_string_accumulates_inline_output(self):
        # `publish_string` records `{kind: inline, value: ...}` under
        # the requested key. The runtime flushes the accumulator into
        # DoneResponse.result_data on task return; here we inspect the
        # in-memory accumulator directly so the test stays free of
        # encode-path concerns (covered separately below).
        t = Task(relative_path="/x")
        t.publish_string("nonce", "xyz")
        self.assertEqual(
            t._outputs_accumulator,
            {"nonce": {"kind": "inline", "value": "xyz"}},
        )

    def test_publish_with_key_accumulates_file_output(self):
        # `publish(src, dst, key=k)` records the *post-publish*
        # destination under `k` so the downstream consumer reads the
        # path on the shared mount. The underlying file delivery is
        # owned by `dynamic_runner.worker.publish.publish`; mock it
        # out so the unit test doesn't depend on disk state.
        with patch("dynamic_runner.worker.publish.publish") as mock_pub:
            t = Task(relative_path="/x")
            t.publish("/staging/x.csv", "/network/out/x.csv", key="result")
            mock_pub.assert_called_once_with("/staging/x.csv", "/network/out/x.csv")
        self.assertEqual(
            t._outputs_accumulator,
            {"result": {"kind": "file", "value": "/network/out/x.csv"}},
        )

    def test_publish_without_key_leaves_accumulator_empty(self):
        # The keyed-outputs side-effect is opt-in: a caller that
        # publishes without `key=` gets the legacy file-delivery
        # behaviour and nothing else. This guards against accidentally
        # treating every published file as a keyed output.
        with patch("dynamic_runner.worker.publish.publish") as mock_pub:
            t = Task(relative_path="/x")
            t.publish("/staging/x.csv", "/network/out/x.csv")
            mock_pub.assert_called_once()
        self.assertEqual(t._outputs_accumulator, {})

    def test_encode_done_payload_empty_returns_none(self):
        # Byte-identical legacy wire shape: a task that uses neither
        # WorkerOutput counters nor the keyed-outputs accumulator
        # emits a bare `done` (None payload). This is the contract
        # the framework relies on to keep pre-feature tasks
        # wire-compatible.
        self.assertIsNone(
            _encode_done_payload(WorkerOutput(warnings=0, filtered=0), {})
        )

    def test_encode_done_payload_merges_counters_and_outputs(self):
        # The merge shape: warnings present, filtered absent (zero),
        # outputs present. The encoded JSON carries only the present
        # keys so a consumer that decodes the old shape and ignores
        # unknown keys (the existing `json.loads` + `.get` pattern)
        # sees no regression.
        accumulator = {"k": {"kind": "inline", "value": "v"}}
        encoded = _encode_done_payload(
            WorkerOutput(warnings=2, filtered=0), accumulator
        )
        self.assertIsNotNone(encoded)
        decoded = json.loads(encoded.decode("utf-8"))
        self.assertEqual(
            decoded,
            {
                "warnings": 2,
                "outputs": {"k": {"kind": "inline", "value": "v"}},
            },
        )

    def test_encode_done_payload_outputs_only(self):
        # WorkerOutput counters absent, accumulator populated → JSON
        # carries only `outputs`. Guards the "omit empty counters"
        # branch from regressing into emitting `warnings: 0`.
        accumulator = {"k": {"kind": "file", "value": "/network/out/k.csv"}}
        encoded = _encode_done_payload(WorkerOutput(), accumulator)
        decoded = json.loads(encoded.decode("utf-8"))
        self.assertEqual(decoded, {"outputs": accumulator})

    def test_predecessor_outputs_populated_from_dataclass(self):
        # Direct dataclass-construction path (no PyO3 round-trip): the
        # `predecessor_outputs` field stores the shape the dispatcher
        # emits verbatim, preserving `kind` so consumers can branch
        # without guessing whether a string happens to look like a
        # path. This is the contract the runtime's
        # `json.loads(command.predecessor_outputs_json)` call relies
        # on at `_process_one`.
        outputs = json.loads('{"A":{"nonce":{"kind":"inline","value":"xyz"}}}')
        t = Task(relative_path="/x", predecessor_outputs=outputs)
        self.assertEqual(
            t.predecessor_outputs["A"]["nonce"],
            {"kind": "inline", "value": "xyz"},
        )

    def test_predecessor_outputs_defaults_empty_for_no_deps(self):
        # Tasks with no predecessors see an empty dict, NOT None.
        # The dict shape is uniform so handlers can iterate without
        # special-casing the no-dep case.
        t = Task(relative_path="/x")
        self.assertEqual(t.predecessor_outputs, {})


if __name__ == "__main__":
    unittest.main()
