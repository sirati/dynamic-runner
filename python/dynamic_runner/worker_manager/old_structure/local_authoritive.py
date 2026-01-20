"""Local authoritive worker manager for local testing of master/slave architecture.

This manager works with LocalSubmissiveManager instances to test the distributed
assignment algorithm locally without networking.
"""

from pathlib import Path
from typing import TYPE_CHECKING

from ..binary_info import BinaryInfo
from ..task import TaskDefinition
from ..worker.base_worker import BaseWorker
from .authoritive_base import AuthoritiveManagerBase

if TYPE_CHECKING:
    from .local_submissive import LocalSubmissiveManager


class LocalAuthoritiveManager(AuthoritiveManagerBase):
    """Local authoritive worker manager for testing master/slave locally.

    This manager:
    - Performs task assignments using the standard algorithm
    - Does NOT perform OOM checking (delegated to submissive managers)
    - Manages LocalWorker instances via local submissive managers
    """

    def __init__(
        self,
        num_workers: int,
        max_memory: int,
        log_dir: Path,
        task_definition: TaskDefinition,
        submissive_managers: list["LocalSubmissiveManager"],
    ):
        # Create worker proxies for each worker in each submissive
        workers: list[BaseWorker] = []
        for submissive in submissive_managers:
            workers.extend(submissive.workers)

        super().__init__(
            num_workers=num_workers,
            max_memory=max_memory,
            log_dir=log_dir,
            task_definition=task_definition,
            workers=workers,
        )

        self.submissive_managers = submissive_managers

    def _handle_oom_killed_task(self, worker: BaseWorker, binary: BinaryInfo, reason: str) -> None:
        """Handle OOM killed task by requeueing locally for retry."""
        # In local mode, we requeue the task at the front of pending_binaries
        self.pending_binaries.insert(0, binary)

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

                # Track assignment in authoritive manager
                worker.mark_busy(binary, estimated)

                size_mb = binary.size / (1024 * 1024)
                estimated_mb = estimated / (1024 * 1024)
                budget_mb = budget / (1024 * 1024)
                self.manager_logger.info(
                    f"[Worker {worker_id}] Authoritive assigned: {binary.path.name} "
                    f"(size: {size_mb:.2f}MB, est: {estimated_mb:.2f}MB, budget: {budget_mb:.2f}MB)"
                )

                return (binary, estimated)

            # No task fits
            return None
