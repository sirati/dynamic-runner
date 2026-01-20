"""Base class for authoritive worker managers.

Authoritive managers perform task assignment decisions but do NOT perform OOM checking.
They can work locally or over network, managing workers that may be local or remote.
"""

from abc import ABC, abstractmethod
from pathlib import Path

from ..binary_info import BinaryInfo
from ..task import TaskDefinition
from ..worker.base_worker import BaseWorker
from .base import WorkerManagerBase


class AuthoritiveManagerBase(WorkerManagerBase, ABC):
    """Base class for authoritive worker managers.

    Authoritive managers:
    - Perform task assignments using the standard algorithm
    - Do NOT perform OOM checking (delegated to submissive managers)
    - Can manage local or remote workers
    """

    def __init__(
        self,
        num_workers: int,
        max_memory: int,
        log_dir: Path,
        task_definition: TaskDefinition,
        workers: list[BaseWorker] | None = None,
        enable_logging: bool = True,
    ):
        # Workers can be provided externally (RemoteWorker or LocalWorker instances)
        # Set this BEFORE calling super().__init__() so _create_workers() can access it
        self._external_workers = workers or []

        super().__init__(
            num_workers=num_workers,
            max_memory=max_memory,
            log_dir=log_dir,
            task_definition=task_definition,
            always_restart_worker=False,
            enable_logging=enable_logging,
        )

    def _create_workers(self) -> list[BaseWorker]:
        """Return externally provided workers (can be RemoteWorker or LocalWorker instances)."""
        return self._external_workers

    def _check_memory_pressure_and_kill(self) -> None:
        """No OOM checking in authoritive manager - delegated to submissive managers."""
        # Authoritive manager does not perform OOM checking
        # This is handled by SubmissiveManager on each worker node
        pass

    @abstractmethod
    def _handle_oom_killed_task(self, worker: BaseWorker, binary: BinaryInfo, reason: str) -> None:
        """Handle OOM killed task.

        For authoritive managers, this should forward the OOM notification
        to the appropriate handler (local queue or remote notification).
        Must be implemented by subclasses.
        """
        pass

    def get_worker_assignments(self) -> dict[int, tuple[BinaryInfo, int] | None]:
        """Get current task assignments for all workers.

        Returns:
            Dictionary mapping worker_id to (binary, estimated_memory) or None if idle
        """
        assignments = {}
        for worker in self.workers:
            if worker.current_binary and worker.estimated_memory is not None:
                assignments[worker.worker_id] = (worker.current_binary, worker.estimated_memory)
            else:
                assignments[worker.worker_id] = None
        return assignments

    def assign_task_to_worker(self, worker_id: int, binary: BinaryInfo, estimated_memory: int) -> bool:
        """Assign a specific task to a specific worker.

        This is used by submissive managers to delegate assignment decisions
        to the authoritive manager while handling the actual worker interaction locally.

        Args:
            worker_id: Worker ID to assign to
            binary: BinaryInfo to process
            estimated_memory: Estimated memory needed

        Returns:
            True if assignment decision is valid, False otherwise
        """
        if worker_id >= len(self.workers):
            self.manager_logger.error(f"Invalid worker_id {worker_id}")
            return False

        worker = self.workers[worker_id]

        # For authoritive manager, we just track the assignment decision
        # The actual worker interaction is handled by the submissive manager
        # So we mark the worker as busy in our tracking
        worker.mark_busy(binary, estimated_memory)

        size_mb = binary.size / (1024 * 1024)
        estimated_mb = estimated_memory / (1024 * 1024)
        self.manager_logger.info(
            f"[Worker {worker_id}] Assignment decision: {binary.path.name} "
            f"(size: {size_mb:.2f}MB, est: {estimated_mb:.2f}MB)"
        )

        return True
