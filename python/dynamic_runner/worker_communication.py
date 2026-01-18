import logging
import time
from dataclasses import dataclass
from enum import Enum

from .comm import (
    DoneResponse,
    ErrorResponse,
    KeepaliveResponse,
    PhaseUpdateResponse,
    PickledErrorResponse,
    ProcessBinaryCommand,
    ReadyResponse,
)
from .models import TaskResult, WorkerState


class WorkerCommunicationError(Enum):
    COMMUNICATION_ERROR = "communication_error"
    CONNECTION_CLOSED = "connection_closed"
    NO_ERROR = "no_error"


@dataclass
class WorkerMessage:
    success: bool
    error_type: WorkerCommunicationError
    error_message: str | None = None
    parsed_responses: list[str | TaskResult] | None = None
    pickled_error_info: dict | None = None


def send_worker_command(worker: WorkerState, binary_relative_path: str) -> tuple[bool, str | None]:
    """Send a command to a worker to process a binary.

    Returns:
        (success, error_message)
    """
    command = ProcessBinaryCommand(relative_path=binary_relative_path)
    return worker.comm.send_command(command)


def receive_worker_messages(worker: WorkerState) -> WorkerMessage:
    """Receive and parse messages from a worker.

    Handles communication errors and parses responses including pickled errors.
    """
    worker.comm.set_blocking(False)

    try:
        responses = worker.comm.receive_responses()

        if not responses:
            return WorkerMessage(
                success=True,
                error_type=WorkerCommunicationError.NO_ERROR,
                parsed_responses=[],
            )

        worker.last_keepalive = time.time()

        parsed_responses = []
        pickled_error = None

        for response in responses:
            if isinstance(response, PickledErrorResponse):
                pickled_error = {
                    "type": response.exception_type,
                    "message": response.exception_message,
                    "traceback": response.traceback_str,
                }
                return WorkerMessage(
                    success=True,
                    error_type=WorkerCommunicationError.NO_ERROR,
                    pickled_error_info=pickled_error,
                )
            elif isinstance(response, PhaseUpdateResponse):
                parsed_responses.append(response.phase_name)
            elif isinstance(response, DoneResponse):
                task_result = TaskResult(
                    success=True,
                    warnings=response.warnings,
                    filtered=response.filtered,
                )
                parsed_responses.append(task_result)
            elif isinstance(response, ErrorResponse):
                task_result = TaskResult(
                    success=False,
                    error_type=response.error_type,
                    error_message=response.error_message,
                )
                parsed_responses.append(task_result)
            elif isinstance(response, KeepaliveResponse):
                pass
            elif isinstance(response, ReadyResponse):
                parsed_responses.append("ready")

        return WorkerMessage(
            success=True,
            error_type=WorkerCommunicationError.NO_ERROR,
            parsed_responses=parsed_responses,
        )

    except Exception as e:
        return WorkerMessage(
            success=False,
            error_type=WorkerCommunicationError.COMMUNICATION_ERROR,
            error_message=str(e),
        )
    finally:
        worker.comm.set_blocking(True)


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
