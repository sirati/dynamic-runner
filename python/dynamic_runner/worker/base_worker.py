"""Base worker abstraction for dynamic batch processing.

This module defines the abstract base class for workers that can be either
local (subprocess) or remote (via network to SLURM secondary).
"""

from abc import ABC, abstractmethod
from pathlib import Path
from typing import Any

from ..binary_info import BinaryInfo
from ..comm import ErrorType
from ..models import TaskResult


class BaseWorker(ABC):
    """Abstract base class for workers (local or remote)."""

    def __init__(self, worker_id: int, memory_budget: int):
        self.worker_id = worker_id
        self.memory_budget = memory_budget
        self.current_binary: BinaryInfo | None = None
        self.estimated_memory: int = 0
        self.idle: bool = False
        self.opportunistic: bool = False
        self.reserved_budget: int = memory_budget
        self.has_received_initial_assignment: bool = False
        self.ready: bool = False
        self.phase: str | None = None
        self.phase_start_time: float | None = None
        self.last_keepalive: float | None = None
        self.last_printed_minute: int | None = None

    @abstractmethod
    def start(self) -> bool:
        """Start the worker.

        Returns:
            True if worker started successfully, False otherwise.
        """
        pass

    @abstractmethod
    def assign_task(self, binary: BinaryInfo, estimated_memory: int) -> tuple[bool, str | None]:
        """Assign a task to this worker.

        Args:
            binary: Binary to process
            estimated_memory: Estimated memory needed

        Returns:
            (success, error_message)
        """
        pass

    @abstractmethod
    def check_status(self) -> tuple[bool, TaskResult | None]:
        """Check worker status and return any completed task result.

        Returns:
            (has_result, task_result) - has_result is True if task completed
        """
        pass

    @abstractmethod
    def terminate(self) -> None:
        """Terminate the worker."""
        pass

    @abstractmethod
    def restart(self) -> bool:
        """Restart the worker after failure.

        Returns:
            True if restart successful, False otherwise.
        """
        pass

    @abstractmethod
    def is_alive(self) -> bool:
        """Check if worker process/connection is alive.

        Returns:
            True if worker is alive, False otherwise.
        """
        pass

    @abstractmethod
    def get_actual_memory_usage(self) -> int:
        """Get actual memory usage of worker.

        Returns:
            Memory usage in bytes, or 0 if unavailable.
        """
        pass

    def mark_idle(self) -> None:
        """Mark worker as idle."""
        self.idle = True
        self.current_binary = None
        self.estimated_memory = 0

    def mark_busy(self, binary: BinaryInfo, estimated_memory: int, opportunistic: bool = False) -> None:
        """Mark worker as busy with a task."""
        self.idle = False
        self.current_binary = binary
        self.estimated_memory = estimated_memory
        self.opportunistic = opportunistic
        self.has_received_initial_assignment = True

    def clear_task(self) -> None:
        """Clear current task from worker."""
        self.current_binary = None
        self.estimated_memory = 0
        self.idle = False
