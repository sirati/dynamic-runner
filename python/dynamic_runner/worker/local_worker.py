"""Local worker implementation for subprocess-based workers.

This wraps the existing worker_lifecycle functions to provide a clean
worker abstraction for local subprocess workers.
"""

import logging
import time
from pathlib import Path
from typing import Any

from ..binary_info import BinaryInfo
from ..comm import ErrorType
from ..models import TaskResult, WorkerState
from ..task import TaskDefinition
from ..worker_communication import receive_worker_messages, send_worker_command
from ..worker_lifecycle import check_worker_timeout, start_worker
from ..worker_lifecycle import restart_worker as lifecycle_restart
from .base_worker import BaseWorker

try:
    import psutil
except ImportError:
    psutil = None

logger = logging.getLogger(__name__)


class LocalWorker(BaseWorker):
    """Local subprocess-based worker implementation."""

    def __init__(
        self,
        worker_id: int,
        memory_budget: int,
        source_dir: Path,
        output_dir: Path,
        log_path: Path,
        task_definition: TaskDefinition,
        task_args: Any,
        skip_existing: bool = False,
        manual_start: bool = False,
        connection_mode: str = "socketpair",
        socket_path: Path | None = None,
    ):
        super().__init__(worker_id, memory_budget)
        self.source_dir = source_dir
        self.output_dir = output_dir
        self.log_path = log_path
        self.task_definition = task_definition
        self.task_args = task_args
        self.skip_existing = skip_existing
        self.manual_start = manual_start
        self.connection_mode = connection_mode
        self.socket_path = socket_path
        self.worker_state: WorkerState | None = None

    def start(self) -> bool:
        """Start the local worker subprocess."""
        try:
            self.worker_state = start_worker(
                self.worker_id,
                self.source_dir,
                self.output_dir,
                self.log_path,
                self.task_definition,
                self.task_args,
                self.skip_existing,
                self.manual_start,
                self.connection_mode,
                self.socket_path,
            )
            return True
        except Exception as e:
            logger.error(f"Failed to start worker {self.worker_id}: {e}")
            return False

    def assign_task(self, binary: BinaryInfo, estimated_memory: int) -> tuple[bool, str | None]:
        """Assign a task to this local worker."""
        if not self.worker_state:
            return False, "Worker not started"

        try:
            relative_path = binary.path.relative_to(self.source_dir)
        except ValueError:
            relative_path = binary.path

        success, error_msg = send_worker_command(self.worker_state, str(relative_path))
        if success:
            self.mark_busy(binary, estimated_memory)
        return success, error_msg

    def check_status(self) -> tuple[bool, TaskResult | None]:
        """Check status and receive messages from local worker."""
        if not self.worker_state:
            return False, None

        msg = receive_worker_messages(self.worker_state)

        if not msg.success:
            # Communication error
            if msg.error_type.value == "connection_closed":
                return True, TaskResult(
                    success=False,
                    error_type=ErrorType.NON_RECOVERABLE,
                    error_message="Worker connection closed",
                )
            return False, None

        # Process responses
        if msg.parsed_responses:
            for response in msg.parsed_responses:
                if response == "ready":
                    self.ready = True
                elif isinstance(response, TaskResult):
                    # Task completed
                    return True, response

        # Update phase tracking
        if self.worker_state.phase:
            self.phase = self.worker_state.phase
            self.phase_start_time = self.worker_state.phase_start_time

        # Update keepalive
        if self.worker_state.last_keepalive:
            self.last_keepalive = self.worker_state.last_keepalive

        return False, None

    def terminate(self) -> None:
        """Terminate the local worker subprocess."""
        if self.worker_state and self.worker_state.process:
            try:
                self.worker_state.process.terminate()
                self.worker_state.comm.close()
            except Exception as e:
                logger.debug(f"Error terminating worker {self.worker_id}: {e}")

    def restart(self) -> bool:
        """Restart the local worker after failure."""
        if not self.worker_state:
            return self.start()

        try:
            self.worker_state = lifecycle_restart(
                self.worker_state,
                self.source_dir,
                self.output_dir,
                self.log_path,
                self.task_definition,
                self.task_args,
                self.skip_existing,
                self.manual_start,
                self.connection_mode,
                self.socket_path,
            )
            self.ready = False
            self.has_received_initial_assignment = False
            return True
        except Exception as e:
            logger.error(f"Failed to restart worker {self.worker_id}: {e}")
            return False

    def is_alive(self) -> bool:
        """Check if local worker process is alive."""
        if not self.worker_state or not self.worker_state.process:
            return False

        return self.worker_state.process.poll() is None

    def get_actual_memory_usage(self) -> int:
        """Get actual memory usage of local worker process."""
        if not self.worker_state or not self.worker_state.process or not psutil:
            return 0

        try:
            if self.worker_state.process.poll() is None:
                process = psutil.Process(self.worker_state.process.pid)
                return process.memory_info().rss
        except (psutil.NoSuchProcess, psutil.AccessDenied, psutil.ZombieProcess):
            pass

        return 0

    def check_timeout(self) -> bool:
        """Check if worker has timed out."""
        if not self.worker_state:
            return False

        return check_worker_timeout(self.worker_state, self.task_definition)
