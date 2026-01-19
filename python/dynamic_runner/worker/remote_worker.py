"""Remote worker implementation for SLURM distributed mode.

This worker delegates task execution to a secondary node over the network.
The primary coordinator uses RemoteWorker to communicate with workers
running on SLURM secondary nodes.
"""

import asyncio
import logging
import time
from pathlib import Path
from typing import Any

from ..binary_info import BinaryInfo
from ..comm import ErrorType
from ..models import TaskResult
from .base_worker import BaseWorker

logger = logging.getLogger(__name__)


class RemoteWorker(BaseWorker):
    """Remote worker implementation that communicates with SLURM secondary."""

    def __init__(
        self,
        worker_id: int,
        memory_budget: int,
        secondary_id: str,
        message_router: Any,
    ):
        super().__init__(worker_id, memory_budget)
        self.secondary_id = secondary_id
        self.message_router = message_router
        self.task_hash: str | None = None
        self.task_started_time: float | None = None
        self._started = False

    def start(self) -> bool:
        """Mark remote worker as started (already running on secondary)."""
        self._started = True
        self.ready = True
        return True

    def assign_task(self, binary: BinaryInfo, estimated_memory: int) -> tuple[bool, str | None]:
        """Assign a task to remote worker via secondary."""
        if not self._started:
            return False, "Remote worker not started"

        if not self.message_router:
            return False, "No message router available"

        # Mark worker as busy
        self.mark_busy(binary, estimated_memory)

        # Task assignment is handled asynchronously via coordinator
        # The actual message sending happens in the coordinator's _send_initial_assignment
        # or when responding to task_request messages
        self.task_started_time = time.time()

        return True, None

    def send_task_assignment(self, binary: BinaryInfo, zip_file: str | None, local_path: str, file_hash: str) -> None:
        """Send task assignment message to secondary (called by coordinator).

        Args:
            binary: Binary info
            zip_file: ZIP file name containing the binary (or None if already extracted)
            local_path: Path within ZIP file
            file_hash: SHA256 hash of binary
        """
        msg = {
            "type": "task_assignment",
            "secondary_id": self.secondary_id,
            "worker_id": self.worker_id,
            "zip_file": zip_file,
            "local_path": local_path,
            "file_hash": file_hash,
            "binary_info": {
                "path": str(binary.path),
                "size": binary.size,
                "binary_name": binary.binary_name,
                "platform": binary.platform,
                "compiler": binary.compiler,
                "version": binary.version,
                "opt_level": binary.opt_level,
            },
        }

        # Send via message router (async)
        asyncio.create_task(self.message_router.send_to_secondary(self.secondary_id, msg))

    def check_status(self) -> tuple[bool, TaskResult | None]:
        """Check remote worker status (results arrive via callbacks).

        For remote workers, task completion is communicated via messages
        from the secondary, so this just checks if we have a pending task.

        Returns:
            (False, None) - results come via coordinator callbacks
        """
        # Remote workers receive status updates via messages handled by coordinator
        # This method is just for compatibility with the BaseWorker interface
        return False, None

    def handle_task_complete(self, warnings: int, filtered: int) -> TaskResult:
        """Handle task completion message from secondary.

        Args:
            warnings: Number of warnings
            filtered: Number of filtered items

        Returns:
            TaskResult for this task
        """
        result = TaskResult(
            success=True,
            error_type=None,
            error_message=None,
            warnings=warnings,
            filtered=filtered,
        )

        # Clear task state
        self.clear_task()
        self.ready = True
        self.task_started_time = None

        return result

    def handle_task_failed(self, error_type_str: str, error_message: str) -> TaskResult:
        """Handle task failure message from secondary.

        Args:
            error_type_str: Error type as string
            error_message: Error message

        Returns:
            TaskResult for this failed task
        """
        # Map error type string to ErrorType
        error_type_map = {
            "worker_crashed": ErrorType.WORKER_CRASHED,
            "worker_timeout": ErrorType.WORKER_TIMEOUT,
            "oom_killed": ErrorType.OOM_KILLED,
            "communication_error": ErrorType.COMMUNICATION_ERROR,
            "task_error": ErrorType.TASK_ERROR,
        }

        error_type = error_type_map.get(error_type_str.lower(), ErrorType.TASK_ERROR)

        result = TaskResult(
            success=False,
            error_type=error_type,
            error_message=error_message,
        )

        # Clear task state
        self.clear_task()
        self.ready = True
        self.task_started_time = None

        return result

    def terminate(self) -> None:
        """Terminate remote worker (handled by secondary)."""
        # Remote workers are managed by the secondary
        # Just mark as not ready
        self._started = False
        self.ready = False

    def restart(self) -> bool:
        """Restart remote worker (handled by secondary).

        Returns:
            True if restart successful
        """
        # Remote workers are managed by the secondary
        # Just reset state
        self._started = True
        self.ready = True
        self.has_received_initial_assignment = False
        self.clear_task()
        return True

    def is_alive(self) -> bool:
        """Check if remote worker connection is alive.

        Returns:
            True if worker is marked as started
        """
        return self._started

    def get_actual_memory_usage(self) -> int:
        """Get actual memory usage (not available for remote workers).

        Returns:
            0 (memory tracking happens on secondary)
        """
        return 0

    def get_elapsed_time(self) -> float | None:
        """Get elapsed time since task started.

        Returns:
            Elapsed time in seconds, or None if no task running
        """
        if self.task_started_time and self.current_binary:
            return time.time() - self.task_started_time
        return None
