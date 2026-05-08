import base64
import json
import pickle
from dataclasses import dataclass
from enum import Enum
from typing import Optional


class ErrorType(Enum):
    OUT_OF_MEMORY = "oom"
    NON_RECOVERABLE = "non_recoverable"
    RECOVERABLE = "recoverable"


@dataclass
class Command:
    """Base class for commands sent from manager to worker."""

    def serialize(self) -> bytes:
        raise NotImplementedError


@dataclass
class StopCommand(Command):
    """Command to stop the worker."""

    def serialize(self) -> bytes:
        return b"stop\n"


@dataclass
class ProcessBinaryCommand(Command):
    """Command to process a binary file.

    `relative_path` is the worker-facing identifier the framework
    passed through verbatim — for file-based tasks it's a real path
    the worker opens; for `uses_file_based_items=False` tasks (FR-2)
    it's an opaque identifier the worker resolves however it wants.

    `payload` is the consumer's per-item data attached to the
    original `TaskInfo.payload`, serialised as a JSON string. `None`
    means "the task carries no payload" (legacy wire form). Workers
    that want the parsed value can `json.loads(cmd.payload)`.

    `resolved_path` is the secondary's locally-resolved on-disk
    location for distributed-mode dispatches where the file lives
    outside the worker's configured source dir (extraction-cache
    hit / pre-staged shared mount). `None` means "open
    `relative_path` against the configured source dir" — the
    legacy behaviour. `Some(p)` means "open `p` directly; treat
    `relative_path` purely as the wire identifier used for
    output-tree mirroring and task identity".
    """

    relative_path: str
    payload: str | None = None
    resolved_path: str | None = None

    def serialize(self) -> bytes:
        if self.payload is None and self.resolved_path is None:
            return f"{self.relative_path}\n".encode("utf-8")
        # Wrap path + optional fields as a JSON object on the new
        # `task:`-prefixed wire form. `json.dumps` emits a single
        # line (no newlines) so this is safe to embed in the
        # line-delimited protocol. Absent fields are omitted so
        # legacy parsers that don't know about them don't trip on
        # a `null`.
        wrapper: dict[str, str] = {"path": self.relative_path}
        if self.payload is not None:
            wrapper["payload"] = self.payload
        if self.resolved_path is not None:
            wrapper["resolved_path"] = self.resolved_path
        return f"task:{json.dumps(wrapper)}\n".encode("utf-8")


@dataclass
class Response:
    """Base class for responses sent from worker to manager."""

    def serialize(self) -> bytes:
        raise NotImplementedError


@dataclass
class DoneResponse(Response):
    """Response indicating successful completion."""

    warnings: int = 0
    filtered: int = 0

    def serialize(self) -> bytes:
        if self.warnings == 0 and self.filtered == 0:
            return b"done\n"
        return f"done:{self.warnings}:{self.filtered}\n".encode("utf-8")


@dataclass
class ErrorResponse(Response):
    """Response indicating an error occurred."""

    error_type: ErrorType
    error_message: str

    def serialize(self) -> bytes:
        return f"error:{self.error_type.value}:{self.error_message}\n".encode("utf-8")


@dataclass
class PickledErrorResponse(Response):
    """Response with pickled error information including traceback.

    Legacy form. Prefer ``WorkerExceptionResponse`` for new code:
    pickle round-trips opaquely on the Rust runner (which doesn't
    deserialise Python objects), so traceback fidelity is lost on the
    wire. The runner currently surfaces this as a generic
    ``LegacyPickledException`` with the pickle bytes as message.
    """

    exception_type: str
    exception_message: str
    traceback_str: str

    def serialize(self) -> bytes:
        error_info = {
            "type": self.exception_type,
            "message": self.exception_message,
            "traceback": self.traceback_str,
        }
        pickled_data = pickle.dumps(error_info)
        return f"error:pickle:{pickled_data.decode('latin-1')}\n".encode("utf-8")


@dataclass
class WorkerExceptionResponse(Response):
    """Response with full exception details (type + message + traceback).

    Wire-compatible with the Rust runner's modern ``error:exception:``
    shape: a base64-encoded JSON object that survives newlines and
    colons in tracebacks. Use this instead of ``ErrorResponse`` when
    you want a Python traceback to reach the framework's WARN log
    without losing fidelity to the line-delimited transport.

    ``error_type`` controls the runner's restart-vs-recover decision:

    * ``None`` (the default) — runner treats this as
      ``NON_RECOVERABLE`` and restarts the worker process. This
      matches the historical "an unhandled exception in the worker
      means the worker is corrupt" semantic.
    * ``ErrorType.RECOVERABLE`` — runner treats the failure as a
      recoverable task error AND keeps the worker alive. This is the
      shape consumers want for user-task exceptions
      (``IndexError`` mid-task, KeyError on a single item, etc.) where
      the worker process itself is fine but the specific task should
      be retried. The runner concatenates
      ``f"{exception_type}: {message}\\n{traceback}"`` into the
      ``TaskFailed.error_message`` field so framework-side WARN logs
      surface the full stack — useful when downstream cleanup
      (e.g. SLURM wrapper trap) wipes worker-local log files before
      they can be inspected.
    * ``ErrorType.NON_RECOVERABLE`` — explicit form of the default;
      worker is restarted.
    * ``ErrorType.OUT_OF_MEMORY`` — categorise as OOM; the runner
      surfaces it with the same OOM bookkeeping as
      ``ErrorResponse(error_type=OUT_OF_MEMORY, ...)``.
    """

    exception_type: str
    exception_message: str
    traceback_str: str
    error_type: Optional[ErrorType] = None

    def serialize(self) -> bytes:
        wire = {
            "type": self.exception_type,
            "message": self.exception_message,
            "traceback": self.traceback_str,
        }
        if self.error_type is not None:
            wire["error_type"] = self.error_type.value
        encoded = base64.b64encode(json.dumps(wire).encode("utf-8")).decode("ascii")
        return f"error:exception:{encoded}\n".encode("utf-8")


@dataclass
class PhaseUpdateResponse(Response):
    """Response indicating phase transition."""

    phase_name: str

    def serialize(self) -> bytes:
        return f"phase:{self.phase_name}\n".encode("utf-8")


@dataclass
class KeepaliveResponse(Response):
    """Response indicating worker is still alive."""

    def serialize(self) -> bytes:
        return b"keepalive\n"


@dataclass
class ReadyResponse(Response):
    """Response indicating worker is ready to receive commands."""

    def serialize(self) -> bytes:
        return b"ready\n"


def parse_command(data: str) -> Command | None:
    """Parse a command string into a Command object."""
    data = data.strip()

    if not data:
        return None

    if data == "stop":
        return StopCommand()

    if data.startswith("task:"):
        # New wire form: `task:<json {path, payload?, resolved_path?}>`.
        # Falls back to legacy interpretation if the JSON is
        # malformed (treat the whole line as a literal path) —
        # defensive, symmetric with the Rust codec's behaviour.
        # Missing optional fields deserialise to `None`, preserving
        # wire compatibility with senders that omit either.
        try:
            wrapper = json.loads(data[len("task:"):])
            return ProcessBinaryCommand(
                relative_path=wrapper.get("path", ""),
                payload=wrapper.get("payload"),
                resolved_path=wrapper.get("resolved_path"),
            )
        except (json.JSONDecodeError, AttributeError):
            pass

    return ProcessBinaryCommand(relative_path=data)


def parse_response(data: str) -> Response | None:
    """Parse a response string into a Response object."""
    data = data.strip()

    if not data:
        return None

    if data == "keepalive":
        return KeepaliveResponse()

    if data == "ready":
        return ReadyResponse()

    if data == "done":
        return DoneResponse()

    if data.startswith("done:"):
        parts = data.split(":", 2)
        warnings = int(parts[1]) if len(parts) > 1 else 0
        filtered = int(parts[2]) if len(parts) > 2 else 0
        return DoneResponse(warnings=warnings, filtered=filtered)

    if data.startswith("phase:"):
        phase_name = data.split(":", 1)[1]
        return PhaseUpdateResponse(phase_name=phase_name)

    if data.startswith("error:exception:"):
        # Modern shape mirroring the Rust runner's ``error:exception:``
        # wire form. JSON is the source of truth; any deserialisation
        # failure falls through to a generic error response so a
        # malformed line doesn't crash the parser.
        try:
            json_bytes = base64.b64decode(data[len("error:exception:"):])
            wire = json.loads(json_bytes.decode("utf-8"))
            err_type_str = wire.get("error_type")
            err_type = ErrorType(err_type_str) if err_type_str else None
            return WorkerExceptionResponse(
                exception_type=wire.get("type", "Unknown"),
                exception_message=wire.get("message", ""),
                traceback_str=wire.get("traceback", ""),
                error_type=err_type,
            )
        except Exception:
            return ErrorResponse(
                error_type=ErrorType.RECOVERABLE,
                error_message="Failed to decode exception response",
            )

    if data.startswith("error:pickle:"):
        try:
            pickled_data = data[13:].encode("latin-1")
            error_info = pickle.loads(pickled_data)
            return PickledErrorResponse(
                exception_type=error_info.get("type", "Unknown"),
                exception_message=error_info.get("message", "No message"),
                traceback_str=error_info.get("traceback", "No traceback"),
            )
        except Exception:
            return ErrorResponse(
                error_type=ErrorType.RECOVERABLE,
                error_message="Failed to unpickle error",
            )

    if data.startswith("error:"):
        parts = data.split(":", 2)
        if len(parts) >= 3:
            error_type_str = parts[1]
            error_message = parts[2]
            try:
                error_type = ErrorType(error_type_str)
            except ValueError:
                error_type = ErrorType.RECOVERABLE
            return ErrorResponse(error_type=error_type, error_message=error_message)

    return None
