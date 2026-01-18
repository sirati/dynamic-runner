import logging
import pickle
import time
from dataclasses import dataclass
from enum import Enum

from .models import ErrorType, TaskResult, WorkerState
from .task_handler import parse_response


class WorkerCommunicationError(Enum):
    SOCKET_ERROR = "socket_error"
    SOCKET_CLOSED = "socket_closed"
    NO_ERROR = "no_error"


@dataclass
class WorkerMessage:
    success: bool
    error_type: WorkerCommunicationError
    error_message: str | None = None
    parsed_responses: list[str | TaskResult] | None = None
    pickled_error_info: dict | None = None


def send_worker_command(worker: WorkerState, command: str) -> tuple[bool, str | None]:
    """Send a command to a worker via socket.

    Returns:
        (success, error_message)
    """
    try:
        worker.socket.sendall(f"{command}\n".encode("utf-8"))
        return (True, None)
    except (BrokenPipeError, ConnectionResetError, OSError) as e:
        return (False, str(e))


def handle_pickled_error(response: str) -> dict | None:
    """Extract and unpickle error information from a response.

    Returns:
        Dictionary with error info or None if unpickling fails
    """
    if not response.startswith("error:pickle:"):
        return None

    try:
        pickled_data = response[13:].encode("latin-1")
        error_info = pickle.loads(pickled_data)
        return error_info
    except Exception:
        return None


def receive_worker_messages(worker: WorkerState) -> WorkerMessage:
    """Receive and parse messages from a worker.

    Handles socket errors and parses responses including pickled errors.
    """
    try:
        worker.socket.setblocking(False)
    except (BrokenPipeError, ConnectionResetError, OSError) as e:
        return WorkerMessage(
            success=False,
            error_type=WorkerCommunicationError.SOCKET_ERROR,
            error_message=str(e),
        )

    try:
        data = worker.socket.recv(1024)

        if not data:
            return WorkerMessage(
                success=False,
                error_type=WorkerCommunicationError.SOCKET_CLOSED,
                error_message="Worker socket closed",
            )

        responses = data.decode("utf-8").strip().split("\n")
        worker.last_keepalive = time.time()

        parsed_responses = []
        pickled_error = None

        for response in responses:
            if response.startswith("error:pickle:"):
                pickled_error = handle_pickled_error(response)
                if pickled_error:
                    return WorkerMessage(
                        success=True,
                        error_type=WorkerCommunicationError.NO_ERROR,
                        pickled_error_info=pickled_error,
                    )
            else:
                parsed = parse_response(response)
                if isinstance(parsed, (str, TaskResult)):
                    parsed_responses.append(parsed)

        return WorkerMessage(
            success=True,
            error_type=WorkerCommunicationError.NO_ERROR,
            parsed_responses=parsed_responses,
        )

    except BlockingIOError:
        return WorkerMessage(
            success=True,
            error_type=WorkerCommunicationError.NO_ERROR,
            parsed_responses=[],
        )
    except (BrokenPipeError, ConnectionResetError, OSError) as e:
        return WorkerMessage(
            success=False,
            error_type=WorkerCommunicationError.SOCKET_ERROR,
            error_message=str(e),
        )
    finally:
        try:
            worker.socket.setblocking(True)
        except (BrokenPipeError, ConnectionResetError, OSError):
            pass


def log_pickled_error(worker_id: int, error_info: dict, logger: logging.Logger) -> str:
    """Format and log a pickled error from a worker.

    Returns:
        Formatted error message string
    """
    error_msg = f"[Worker {worker_id}] Pickled error received:\n"
    error_msg += f"  Exception Type: {error_info.get('type', 'Unknown')}\n"
    error_msg += f"  Exception Message: {error_info.get('message', 'No message')}\n"
    error_msg += f"  Traceback:\n{error_info.get('traceback', 'No traceback')}"
    logger.error(error_msg)
    return error_msg
