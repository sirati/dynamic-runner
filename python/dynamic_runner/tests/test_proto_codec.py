"""Manager-worker codec round-trip tests.

Covers the Python re-export shim over the Rust codec:

* Every ``Command`` / ``Response`` subclass survives serialise -> parse
  on the same side (Python -> Python) with byte-identical wire output.
* The Rust codec is the single source of truth — these tests exist to
  catch drift if someone re-introduces a hand-maintained Python codec.
  Rust-side codec tests live in
  ``crates/dynrunner-protocol-manager-worker/src/codec_tests.rs``.
"""
from __future__ import annotations

import unittest

from dynamic_runner.comm import (
    Command,
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
    parse_command,
    parse_response,
)


class CommandRoundTripTests(unittest.TestCase):
    """parse_command(cmd.serialize()) must produce a value
    structurally equal to the original `cmd`, for every variant."""

    def test_stop_command(self):
        cmd = StopCommand()
        wire = cmd.serialize()
        self.assertEqual(wire, b"stop\n")
        parsed = parse_command(wire.decode())
        self.assertIsInstance(parsed, StopCommand)
        self.assertIsInstance(parsed, Command)

    def test_process_binary_legacy_path_only(self):
        cmd = ProcessBinaryCommand("path/to/x")
        wire = cmd.serialize()
        # Legacy form: bare-path line, no JSON wrapper.
        self.assertEqual(wire, b"path/to/x\n")
        parsed = parse_command(wire.decode())
        self.assertIsInstance(parsed, ProcessBinaryCommand)
        self.assertEqual(parsed.relative_path, "path/to/x")
        self.assertIsNone(parsed.payload)
        self.assertIsNone(parsed.resolved_path)

    def test_process_binary_with_payload(self):
        cmd = ProcessBinaryCommand("path/to/x", payload='{"v":1}')
        wire = cmd.serialize()
        # `task:` prefix routes the JSON-wrapped form.
        self.assertTrue(wire.startswith(b"task:"))
        parsed = parse_command(wire.decode())
        self.assertIsInstance(parsed, ProcessBinaryCommand)
        self.assertEqual(parsed.relative_path, "path/to/x")
        self.assertEqual(parsed.payload, '{"v":1}')
        self.assertIsNone(parsed.resolved_path)

    def test_process_binary_with_resolved_path(self):
        cmd = ProcessBinaryCommand(
            "path/to/x",
            payload=None,
            resolved_path="/abs/local/x",
        )
        wire = cmd.serialize()
        self.assertTrue(wire.startswith(b"task:"))
        parsed = parse_command(wire.decode())
        self.assertEqual(parsed.relative_path, "path/to/x")
        self.assertIsNone(parsed.payload)
        self.assertEqual(parsed.resolved_path, "/abs/local/x")

    def test_process_binary_with_both_fields(self):
        cmd = ProcessBinaryCommand(
            "path/to/x",
            payload='{"v":2}',
            resolved_path="/abs/local/x",
        )
        parsed = parse_command(cmd.serialize().decode())
        self.assertEqual(parsed.relative_path, "path/to/x")
        self.assertEqual(parsed.payload, '{"v":2}')
        self.assertEqual(parsed.resolved_path, "/abs/local/x")


class ResponseRoundTripTests(unittest.TestCase):
    def test_ready(self):
        wire = ReadyResponse().serialize()
        self.assertEqual(wire, b"ready\n")
        parsed = parse_response(wire.decode())
        self.assertIsInstance(parsed, ReadyResponse)
        self.assertIsInstance(parsed, Response)

    def test_done_no_data(self):
        wire = DoneResponse().serialize()
        self.assertEqual(wire, b"done\n")
        parsed = parse_response(wire.decode())
        self.assertIsInstance(parsed, DoneResponse)
        self.assertIsNone(parsed.result_data)

    def test_done_with_data(self):
        wire = DoneResponse(result_data=b"3:7").serialize()
        self.assertEqual(wire, b"done:3:7\n")
        parsed = parse_response(wire.decode())
        self.assertIsInstance(parsed, DoneResponse)
        self.assertEqual(parsed.result_data, b"3:7")

    def test_keepalive(self):
        wire = KeepaliveResponse().serialize()
        self.assertEqual(wire, b"keepalive\n")
        parsed = parse_response(wire.decode())
        self.assertIsInstance(parsed, KeepaliveResponse)

    def test_phase_update(self):
        wire = PhaseUpdateResponse(phase_name="ANGR_1").serialize()
        self.assertEqual(wire, b"phase:ANGR_1\n")
        parsed = parse_response(wire.decode())
        self.assertIsInstance(parsed, PhaseUpdateResponse)
        self.assertEqual(parsed.phase_name, "ANGR_1")

    def test_error_oom(self):
        wire = ErrorResponse(ErrorType.OUT_OF_MEMORY, "ran out").serialize()
        self.assertEqual(wire, b"error:oom:ran out\n")
        parsed = parse_response(wire.decode())
        self.assertIsInstance(parsed, ErrorResponse)
        self.assertEqual(parsed.error_type, ErrorType.OUT_OF_MEMORY)
        self.assertEqual(parsed.error_message, "ran out")

    def test_error_non_recoverable(self):
        wire = ErrorResponse(ErrorType.NON_RECOVERABLE, "boom").serialize()
        self.assertEqual(wire, b"error:non_recoverable:boom\n")
        parsed = parse_response(wire.decode())
        self.assertEqual(parsed.error_type, ErrorType.NON_RECOVERABLE)

    def test_error_recoverable(self):
        wire = ErrorResponse(ErrorType.RECOVERABLE, "retry me").serialize()
        self.assertEqual(wire, b"error:recoverable:retry me\n")
        parsed = parse_response(wire.decode())
        self.assertEqual(parsed.error_type, ErrorType.RECOVERABLE)

    def test_worker_exception_no_error_type(self):
        wire = WorkerExceptionResponse(
            exception_type="ValueError",
            exception_message="bad input",
            traceback_str="Traceback (most recent call last):\n  ValueError: bad input",
        ).serialize()
        # Wire shape is `error:exception:<base64-json>\n`; structure
        # tested by parse round-trip rather than byte equality.
        self.assertTrue(wire.startswith(b"error:exception:"))
        parsed = parse_response(wire.decode())
        self.assertIsInstance(parsed, WorkerExceptionResponse)
        self.assertEqual(parsed.exception_type, "ValueError")
        self.assertEqual(parsed.exception_message, "bad input")
        self.assertIn("ValueError", parsed.traceback_str)
        self.assertIsNone(parsed.error_type)

    def test_worker_exception_with_error_type_recoverable(self):
        wire = WorkerExceptionResponse(
            exception_type="IndexError",
            exception_message="list index out of range",
            traceback_str="tb body",
            error_type=ErrorType.RECOVERABLE,
        ).serialize()
        parsed = parse_response(wire.decode())
        self.assertIsInstance(parsed, WorkerExceptionResponse)
        self.assertEqual(parsed.error_type, ErrorType.RECOVERABLE)


class ErrorTypeShimTests(unittest.TestCase):
    """The Python shim restores ``ErrorType(<wire-value>)``
    constructor behaviour around the PyO3 enum. The shim's three
    member constants must point at the same native enum members the
    string lookup returns.
    """

    def test_construct_by_wire_value(self):
        self.assertEqual(ErrorType("oom"), ErrorType.OUT_OF_MEMORY)
        self.assertEqual(ErrorType("non_recoverable"), ErrorType.NON_RECOVERABLE)
        self.assertEqual(ErrorType("recoverable"), ErrorType.RECOVERABLE)

    def test_invalid_value_raises(self):
        with self.assertRaises(ValueError):
            ErrorType("totally-unknown")

    def test_value_attribute(self):
        self.assertEqual(ErrorType.OUT_OF_MEMORY.value, "oom")
        self.assertEqual(ErrorType.NON_RECOVERABLE.value, "non_recoverable")
        self.assertEqual(ErrorType.RECOVERABLE.value, "recoverable")


if __name__ == "__main__":
    unittest.main()
