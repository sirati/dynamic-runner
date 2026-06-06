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
import signal
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
from dynamic_runner.worker import runtime as runtime_mod
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
        # out so the unit test doesn't depend on disk state. The
        # mock returns the resolved destination (matching the real
        # publish() contract — see `publish.py`) so `Task.publish`
        # has something correct to record in the accumulator.
        from pathlib import Path
        with patch("dynamic_runner.worker.publish.publish") as mock_pub:
            mock_pub.return_value = Path("/network/out/x.csv")
            t = Task(relative_path="/x")
            t.publish("/staging/x.csv", "/network/out/x.csv", key="result")
            mock_pub.assert_called_once_with("/staging/x.csv", "/network/out/x.csv")
        self.assertEqual(
            t._outputs_accumulator,
            {"result": {"kind": "file", "value": "/network/out/x.csv"}},
        )

    def test_publish_with_key_no_explicit_dst_records_resolved_path(self):
        # Regression for the `dst=None` bug: when the caller omits
        # `dst`, `publish.publish` derives it from src_root/dst_root
        # and returns the resolved path. `Task.publish` must record
        # *that* resolved path in the accumulator — not `str(None)`,
        # the literal user input. Mock the underlying publish to
        # return a known resolved path and assert the accumulator
        # captures it.
        from pathlib import Path
        resolved = Path("/network/out/auto/derived.csv")
        with patch("dynamic_runner.worker.publish.publish") as mock_pub:
            mock_pub.return_value = resolved
            t = Task(relative_path="/x")
            t.publish("/staging/auto/derived.csv", key="auto")
            # Underlying call still receives the original (None) dst —
            # destination resolution is publish.py's concern.
            mock_pub.assert_called_once_with("/staging/auto/derived.csv", None)
        self.assertEqual(
            t._outputs_accumulator,
            {"auto": {"kind": "file", "value": "/network/out/auto/derived.csv"}},
        )
        # The pre-fix code stored str(dst) == "None"; pin that the bug
        # cannot regress by asserting the value is not the literal
        # string the bug produced.
        self.assertNotEqual(
            t._outputs_accumulator["auto"]["value"], "None"
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


class KeyedOutputsEndToEndTests(unittest.TestCase):
    """End-to-end coverage of the keyed-outputs path through
    `_process_one` / `run()`. The Task-level unit tests above pin
    each surface (accumulator, encoder, decode) in isolation; these
    tests pin the *integration*: a real `ProcessBinaryCommand`
    dispatched into the runtime loop, a real handler that publishes
    or reads predecessor outputs, and a real `DoneResponse` observed
    on the wire (the `ScriptedComm` outbox).

    The plan's "Verification → Worker (Python)" subsection lists the
    full-completion DoneResponse byte-shape AND a handler that reads
    `task.predecessor_outputs[A][k]["value"]`. Those are integration
    contracts: they fail if the seam between `_encode_done_payload`,
    `task._outputs_accumulator`, `task.predecessor_outputs`, and the
    `ProcessBinaryCommand.predecessor_outputs_json` bridge ever
    breaks even when the individual unit tests still pass.
    """

    def _drive_with_predecessor_outputs(
        self, handler, predecessor_outputs_json: str = "{}"
    ):
        """Helper: drive `run()` with a single ProcessBinaryCommand
        carrying the given `predecessor_outputs_json`. Returns the
        comm fixture so the caller can inspect the outbox.

        The default `"{}"` mirrors `PyProcessBinaryCommand`'s own
        default — a task with no deps sees an empty dict on the
        Python side. Tests that exercise the consumer side pass a
        non-empty JSON object literal.
        """
        cmd = ProcessBinaryCommand(
            relative_path="/some/binary",
            payload=None,
            resolved_path=None,
            predecessor_outputs_json=predecessor_outputs_json,
        )
        return _drive(handler, [cmd, StopCommand()])

    def test_publish_string_flows_into_done_response_bytes(self):
        # Plan §"Verification → Worker (Python)" item 1, second half:
        # a handler that calls `task.publish_string(k, v)` must cause
        # the runtime to emit a `DoneResponse` whose `result_data`
        # decodes to `{"outputs": {k: {"kind":"inline","value":v}}}`.
        # The unit test pinned the encoder in isolation; this pins
        # the lifetime seam — `_process_one` reads the accumulator
        # off `task` *after* the handler returned, which only works
        # because Phase 6b hoisted `task` past the try-block.
        def handle(task: Task):
            task.publish_string("nonce", "xyz")
            return None

        comm = self._drive_with_predecessor_outputs(handle)

        done = [r for r in comm.outbox if isinstance(r, DoneResponse)]
        self.assertEqual(len(done), 1)
        self.assertIsNotNone(done[0].result_data)
        decoded = json.loads(done[0].result_data.decode("utf-8"))
        self.assertEqual(
            decoded,
            {"outputs": {"nonce": {"kind": "inline", "value": "xyz"}}},
        )

    def test_worker_output_and_publish_string_merge_in_done_response(self):
        # Plan §"Verification → Worker (Python)" item 4: a handler
        # that BOTH returns `WorkerOutput(warnings=2)` AND calls
        # `publish_string("k","v")` must emit
        # `{"warnings":2, "outputs":{"k":{"kind":"inline","value":"v"}}}`
        # on the wire. Unit-level merge is covered by
        # `test_encode_done_payload_merges_counters_and_outputs`;
        # this guards the integration with the loop's
        # `output = result if result is not None else _DEFAULT_OUTPUT`
        # plumbing.
        def handle(task: Task):
            task.publish_string("k", "v")
            return WorkerOutput(warnings=2)

        comm = self._drive_with_predecessor_outputs(handle)

        done = [r for r in comm.outbox if isinstance(r, DoneResponse)]
        self.assertEqual(len(done), 1)
        self.assertIsNotNone(done[0].result_data)
        decoded = json.loads(done[0].result_data.decode("utf-8"))
        self.assertEqual(
            decoded,
            {
                "warnings": 2,
                "outputs": {"k": {"kind": "inline", "value": "v"}},
            },
        )

    def test_predecessor_outputs_json_decodes_into_task(self):
        # Plan §"Verification → Worker (Python)" item 5, consumer
        # half: a handler reads
        # `task.predecessor_outputs["A"]["nonce"]["value"]` and gets
        # the value the dispatcher attached. The producer-side
        # contract is verified end-to-end above; here we pin the
        # JSON-string bridge → `Task.predecessor_outputs` dict step,
        # which lives at `runtime.py:_process_one` and is the seam
        # the PyO3 `predecessor_outputs_json` field feeds.
        captured: list[Task] = []

        def handle(task: Task):
            captured.append(task)
            return None

        outputs_json = json.dumps(
            {"A": {"nonce": {"kind": "inline", "value": "xyz"}}}
        )
        comm = self._drive_with_predecessor_outputs(handle, outputs_json)

        # The handler ran exactly once; its captured Task carries
        # the parsed predecessor outputs verbatim with `kind`
        # preserved so consumers can branch on inline vs file.
        self.assertEqual(len(captured), 1)
        self.assertEqual(
            captured[0].predecessor_outputs["A"]["nonce"],
            {"kind": "inline", "value": "xyz"},
        )
        # DoneResponse still fires cleanly (the consumer-side read
        # path is non-destructive — verifying no exception leaked).
        done = [r for r in comm.outbox if isinstance(r, DoneResponse)]
        self.assertEqual(len(done), 1)

    def test_default_predecessor_outputs_json_yields_empty_dict(self):
        # Regression-guard: `PyProcessBinaryCommand`'s constructor
        # defaults `predecessor_outputs_json` to `"{}"`. A handler
        # that never opts into deps must see an empty dict, not
        # crash on `None` or a missing attribute. The legacy test
        # helper `_process()` constructs the command without
        # passing `predecessor_outputs_json` explicitly, exercising
        # the PyO3 default path.
        captured: list[Task] = []

        def handle(task: Task):
            captured.append(task)
            return None

        _drive(handle, [_process(), StopCommand()])

        self.assertEqual(len(captured), 1)
        self.assertEqual(captured[0].predecessor_outputs, {})


class RunStartSweepTests(unittest.TestCase):
    """The worker reaps stale ``.publish-tmp`` leftovers exactly once
    at run-start, before processing any task.
    """

    def test_sweep_invoked_once_at_run_start(self):
        _REGISTRY.default = None
        with patch.object(runtime_mod, "_sweep_stale_publish_tmps") as sweep:
            comm = ScriptedComm(inbox=[StopCommand()])
            run(lambda task: None, comm=comm)
        sweep.assert_called_once_with()

    def test_helper_delegates_to_publish_sweep_with_dst_root(self):
        # The helper resolves the destination root and reaps via the
        # publish module's sweep. Patch the publish-module surface so
        # no real dest dir / native call is needed.
        with patch("dynamic_runner.worker.publish.dst_root",
                   return_value="/some/dst") as droot, \
             patch("dynamic_runner.worker.publish.sweep_stale_tmps",
                   return_value=0) as sweep:
            runtime_mod._sweep_stale_publish_tmps()
        droot.assert_called_once_with()
        sweep.assert_called_once_with("/some/dst")

    def test_helper_swallows_sweep_failure(self):
        # A sweep error at startup must not stop the worker.
        with patch("dynamic_runner.worker.publish.dst_root",
                   return_value="/some/dst"), \
             patch("dynamic_runner.worker.publish.sweep_stale_tmps",
                   side_effect=RuntimeError("boom")):
            runtime_mod._sweep_stale_publish_tmps()  # must not raise


class ExitSignalHandlerTests(unittest.TestCase):
    """SIGTERM and SIGHUP are installed as SystemExit-raising handlers
    for the duration of ``run()`` and restored on exit. SIGHUP is the
    new strand: it must be handled (not hit the default
    terminate-without-cleanup) so a publish-rename-deferred SIGHUP
    delivered post-unblock falls into the loop's clean SystemExit
    path.
    """

    def test_sighup_and_sigterm_installed_during_run_and_restored(self):
        _REGISTRY.default = None
        before = {
            signal.SIGHUP: signal.getsignal(signal.SIGHUP),
            signal.SIGTERM: signal.getsignal(signal.SIGTERM),
        }

        installed: dict[int, object] = {}

        def handle(task):
            # Snapshot the live handlers while run() is mid-loop.
            installed[signal.SIGHUP] = signal.getsignal(signal.SIGHUP)
            installed[signal.SIGTERM] = signal.getsignal(signal.SIGTERM)
            return None

        comm = ScriptedComm(inbox=[_process(), StopCommand()])
        run(handle, comm=comm)

        # During the run, both signals carried the runtime's handler —
        # a callable distinct from the prior disposition.
        for signum in (signal.SIGHUP, signal.SIGTERM):
            self.assertTrue(callable(installed[signum]))
            self.assertIsNot(installed[signum], before[signum])

        # After the run, both are restored to their prior disposition.
        self.assertEqual(signal.getsignal(signal.SIGHUP), before[signal.SIGHUP])
        self.assertEqual(signal.getsignal(signal.SIGTERM), before[signal.SIGTERM])

    def test_installed_handler_raises_systemexit(self):
        # The installed handler converts the signal into SystemExit so
        # the loop's KeyboardInterrupt/SystemExit branch owns shutdown.
        prev = runtime_mod._install_exit_signal_handlers()
        try:
            handler = signal.getsignal(signal.SIGHUP)
            self.assertTrue(callable(handler))
            with self.assertRaises(SystemExit):
                handler(signal.SIGHUP, None)
        finally:
            for signum, p in prev.items():
                signal.signal(signum, p)


if __name__ == "__main__":
    unittest.main()
