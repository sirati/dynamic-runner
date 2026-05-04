import json
import pickle
from dataclasses import dataclass
from enum import Enum


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
    """

    relative_path: str
    payload: str | None = None

    def serialize(self) -> bytes:
        if self.payload is None:
            return f"{self.relative_path}\n".encode("utf-8")
        # Wrap path + payload as a JSON object on the new
        # `task:`-prefixed wire form. `json.dumps` emits a single
        # line (no newlines) so this is safe to embed in the
        # line-delimited protocol.
        wrapper = json.dumps({"path": self.relative_path, "payload": self.payload})
        return f"task:{wrapper}\n".encode("utf-8")


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
    """Response with pickled error information including traceback."""

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
        # New wire form (FR-3): `task:<json {path, payload}>`. Falls
        # back to legacy interpretation if the JSON is malformed
        # (treat the whole line as a literal path) — defensive,
        # symmetric with the Rust codec's behaviour.
        try:
            wrapper = json.loads(data[len("task:"):])
            return ProcessBinaryCommand(
                relative_path=wrapper.get("path", ""),
                payload=wrapper.get("payload"),
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
