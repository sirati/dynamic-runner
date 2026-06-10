# =====================================================================
# WARNING — PYTHON BRIDGE ONLY. NO LOGIC HERE.
# =====================================================================
# This file is a thin PyO3 / CLI / config bridge. ALL business logic,
# lifecycle, state-tracking, async orchestration, and process management
# lives in Rust under
# `crates/dynrunner-protocol-manager-worker/src/codec.rs` and the PyO3
# adapter at `crates/dynrunner-pyo3/src/protocol_manager_worker.rs`.
# If you find yourself adding logic here — STOP. Put it in Rust and
# call it from this file via PyO3.
# =====================================================================
"""Re-export shim for the manager-worker wire codec.

The classes and parser functions below are imported verbatim from the
PyO3 submodule `dynamic_runner._native.protocol_manager_worker`. They
preserve the historical public-API surface of this module so existing
callers (`from dynamic_runner.comm.proto import Command, ...`) keep
working without changes.

A single Python-side helper is needed: ``ErrorType(<wire-value>)``.
The historical class was a stdlib ``enum.Enum`` which accepts its
member value as a constructor argument. PyO3 ``#[pyclass]`` enums
don't auto-implement that convenience, so this file wraps the native
class with a callable shim that delegates to its
``_from_value`` staticmethod. The wire encoding itself still happens
in Rust; the shim is pure dispatch.
"""

from dynamic_runner._native import protocol_manager_worker as _native_pmw

# noqa: F401 — re-export
Command = _native_pmw.Command
CustomMessageCommand = _native_pmw.CustomMessageCommand
CustomMessageResponse = _native_pmw.CustomMessageResponse
DoneResponse = _native_pmw.DoneResponse
ErrorResponse = _native_pmw.ErrorResponse
KeepaliveResponse = _native_pmw.KeepaliveResponse
PhaseUpdateResponse = _native_pmw.PhaseUpdateResponse
PickledErrorResponse = _native_pmw.PickledErrorResponse
ProcessBinaryCommand = _native_pmw.ProcessBinaryCommand
ReadyResponse = _native_pmw.ReadyResponse
Response = _native_pmw.Response
StopCommand = _native_pmw.StopCommand
WorkerExceptionResponse = _native_pmw.WorkerExceptionResponse
parse_command = _native_pmw.parse_command
parse_response = _native_pmw.parse_response
_NativeErrorType = _native_pmw.ErrorType


class ErrorType:
    """Thin shim restoring the historical ``ErrorType(<value>)``
    constructor convenience of the pre-refactor ``enum.Enum``-based
    class. All wire-value semantics live in
    ``dynamic_runner._native.protocol_manager_worker.ErrorType``;
    this shim only handles the Python-side ``"oom" -> member`` lookup.

    The three member constants are bound at class-definition time so
    ``ErrorType.OUT_OF_MEMORY is ErrorType("oom")`` holds.
    """

    OUT_OF_MEMORY = _NativeErrorType.OutOfMemory
    NON_RECOVERABLE = _NativeErrorType.NonRecoverable
    RECOVERABLE = _NativeErrorType.Recoverable

    def __new__(cls, value):
        # ``ErrorType("oom")`` returns the native enum member, not a
        # shim instance — same semantics the historical
        # ``enum.Enum`` constructor had. Idempotent for already-typed
        # values (``ErrorType(ErrorType.RECOVERABLE) is ErrorType.RECOVERABLE``),
        # matching the stdlib enum's lookup-by-member behaviour.
        if isinstance(value, _NativeErrorType):
            return value
        return _NativeErrorType._from_value(value)
