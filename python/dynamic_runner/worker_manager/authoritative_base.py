"""Base class for authoritative worker managers.

Authoritative managers perform task assignment decisions but do NOT perform OOM checking.
They can work locally or over network, managing workers that may be local or remote.
"""

from abc import ABC, abstractmethod
from pathlib import Path

from ..binary_info import BinaryInfo
from ..task import TaskDefinition
from ..worker.base_worker import BaseWorker
from .base import WorkerManagerBase


class AuthoritativeBase(WorkerManagerBase, ABC):
    """Base class for authoritative worker managers.

    Authoritative managers:
    - Perform task assignments using the standard algorithm
    - Do NOT perform OOM checking (delegated to submissive managers)
    - Can manage local or remote workers
    - Provide API for submissive managers to request tasks
    """

    def __init__(
        self,
        num_workers: int,
        max_memory: int,
        log_dir: Path,
        task_definition: TaskDefinition,
        workers: list[BaseWorker] | None = None,
        always_restart_worker: bool = False,
        enable_logging: bool = True,
        **kwargs,
    ):
        # Workers can be provided externally (RemoteWorker or LocalWorker instances)
        # Set this BEFORE calling super().__init__() so _create_workers() can access it
        self._external_workers = workers or []

        super().__init__(
            num_workers=num_workers,
            max_memory=max_memory,
            log_dir=log_dir,
            task_definition=task_definition,
            always_restart_worker=always_restart_worker,
            enable_logging=enable_logging,
            **kwargs,
        )

    def _create_workers(self) -> list[BaseWorker]:
        """Return externally provided workers (can be RemoteWorker or LocalWorker instances)."""
        return self._external_workers

    def _check_memory_pressure_and_kill(self) -> None:
        """No OOM checking in authoritative manager - delegated to submissive managers."""
        # Authoritative manager does not perform OOM checking
        # This is handled by ActualSubmissiveWorkerManager on each worker node
        pass

    @abstractmethod
    def _handle_oom_killed_task(self, worker: BaseWorker, binary: BinaryInfo, reason: str) -> None:
        """Handle OOM killed task.

        For authoritative managers, this should forward the OOM notification
        to the appropriate handler (local queue or remote notification).
        Must be implemented by subclasses.
        """
        pass

    def _assign_binary_to_worker_initial_phase(self, worker: BaseWorker) -> bool:
        """Stub implementation for authoritative base.

        Authoritative managers that have decision logic should override this via
        DecisionWorkerManMixin. Pure authoritative bases provide stub implementation.
        """
        return False

    def _assign_binary_to_worker_normal(self, worker: BaseWorker, retry_attempt: bool = False) -> bool:
        """Stub implementation for authoritative base.

        Authoritative managers that have decision logic should override this via
        DecisionWorkerManMixin. Pure authoritative bases provide stub implementation.
        """
        return False

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
        to the authoritative manager while handling the actual worker interaction locally.

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

        # For authoritative manager, we just track the assignment decision
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

    def set_pending_binaries(self, binaries: list[BinaryInfo]) -> None:
        """Set pending binaries for the authoritative manager without full processing.

        This is used in test-master-slave mode where the authoritative manager
        needs the task list but doesn't run the full processing pipeline.

        Args:
            binaries: List of binaries to process
        """
        self.pending_binaries = binaries.copy()
        self.stats["total"] = len(binaries)
        self.stats["completed"] = 0
        self.stats["errored"] = 0

    def handle_task_request(self, worker_id: int) -> tuple[BinaryInfo, int] | None:
        """Handle a task request from a submissive manager.

        This uses the standard assignment algorithm to pick the next task.

        Args:
            worker_id: Worker ID requesting a task

        Returns:
            Tuple of (binary, estimated_memory) if task assigned, None otherwise
        """
        if worker_id >= len(self.workers):
            self.manager_logger.error(f"Invalid worker_id {worker_id}")
            return None

        worker = self.workers[worker_id]

        # Use the normal assignment logic
        with self.lock:
            if not self.pending_binaries:
                return None

            # Use worker's current budget (may have been adjusted)
            budget = worker.get_available_memory()

            # Find task that fits budget
            for i, binary in enumerate(self.pending_binaries):
                estimated = self.task_definition.estimate_memory(binary.size)

                if estimated > budget:
                    continue

                # Assign the task
                self.pending_binaries.pop(i)

                # Track assignment in authoritative manager
                worker.mark_busy(binary, estimated)

                size_mb = binary.size / (1024 * 1024)
                estimated_mb = estimated / (1024 * 1024)
                budget_mb = budget / (1024 * 1024)
                self.manager_logger.info(
                    f"[Worker {worker_id}] Authoritative assigned: {binary.path.name} "
                    f"(size: {size_mb:.2f}MB, est: {estimated_mb:.2f}MB, budget: {budget_mb:.2f}MB)"
                )

                return (binary, estimated)

            # No task fits
            return None
