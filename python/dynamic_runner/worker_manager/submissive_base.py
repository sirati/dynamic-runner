"""Base class for submissive worker managers.

Submissive managers perform OOM checking but delegate task assignment decisions
to an authoritive manager. They can work locally or over network.
"""

from abc import abstractmethod
from pathlib import Path
from typing import Any

from ..binary_info import BinaryInfo
from ..comm import ErrorType
from ..models import FailedTask
from ..task import TaskDefinition
from ..worker.base_worker import BaseWorker
from .local_base import LocalWorkerManagerBase


class SubmissiveManagerBase(LocalWorkerManagerBase):
    """Base class for submissive worker managers.

    Submissive managers:
    - Create and manage LocalWorker instances (subprocess-based)
    - Perform OOM checking and worker killing
    - Do NOT autonomously assign tasks beyond initial phase
    - Request tasks from authoritive manager (local or remote)
    """

    def __init__(
        self,
        num_workers: int,
        max_memory: int,
        source_dir: Path,
        output_dir: Path,
        task_definition: TaskDefinition,
        task_args: Any,
        skip_existing: bool,
        manual_start_worker: bool = False,
        connection_mode: str = "socketpair",
        socket_dir: Path | None = None,
    ):
        super().__init__(
            num_workers=num_workers,
            max_memory=max_memory,
            source_dir=source_dir,
            output_dir=output_dir,
            task_definition=task_definition,
            task_args=task_args,
            skip_existing=skip_existing,
            always_restart_worker=False,
            manual_start_worker=manual_start_worker,
            connection_mode=connection_mode,
            socket_dir=socket_dir,
        )

    def _handle_oom_killed_task(self, worker: BaseWorker, binary: BinaryInfo, reason: str) -> None:
        """Handle OOM killed task by reporting to authoritive manager (add to oom_tasks)."""
        self.oom_tasks.append(
            FailedTask(
                binary=binary,
                error_type=ErrorType.OUT_OF_MEMORY,
                error_message=reason,
            )
        )

    @abstractmethod
    def _request_task_from_authoritive(self, worker_id: int) -> None:
        """Request a task from the authoritive manager for the given worker.

        Must be implemented by subclasses (local or remote).
        """
        pass

    def _handle_worker_without_task(
        self,
        worker: BaseWorker,
        worker_id: int,
        active_workers: set[int],
        allow_stop: bool,
        is_initial_phase: bool = False,
    ) -> bool:
        """Handle a worker that has no current task.

        For submissive manager, after initial phase, request tasks from authoritive
        instead of autonomously assigning from local pending queue.
        """
        if is_initial_phase:
            # During initial phase, use base behavior
            return super()._handle_worker_without_task(worker, worker_id, active_workers, allow_stop, is_initial_phase)
        else:
            # After initial phase, request task from authoritive
            if worker.ready and not worker.current_binary:
                self.manager_logger.debug(f"[Worker {worker_id}] Requesting task from authoritive")
                self._request_task_from_authoritive(worker_id)
                # Keep worker active, waiting for authoritive assignment
                return True

            if not self.pending_binaries:
                if allow_stop:
                    worker.terminate()
                    self.manager_logger.info(f"[Worker {worker_id}] Stopping (no more tasks)")
                active_workers.discard(worker_id)
                return False

            # Worker is idle but binaries remain - keep it in the loop
            return True

    def assign_task_from_authoritive(self, worker_id: int, binary: BinaryInfo, estimated_memory: int) -> bool:
        """Assign a task to a worker from authoritive manager.

        Args:
            worker_id: Worker ID to assign to
            binary: BinaryInfo to process
            estimated_memory: Estimated memory needed

        Returns:
            True if assignment successful, False otherwise
        """
        if worker_id >= len(self.workers):
            self.manager_logger.error(f"Invalid worker_id {worker_id}")
            return False

        worker = self.workers[worker_id]

        if not worker.ready or worker.current_binary is not None:
            self.manager_logger.warning(f"[Worker {worker_id}] Cannot assign task - worker not ready or busy")
            return False

        success, error_msg = worker.assign_task(binary, estimated_memory)
        if success:
            worker.mark_busy(binary, estimated_memory)
            size_mb = binary.size / (1024 * 1024)
            estimated_mb = estimated_memory / (1024 * 1024)
            self.manager_logger.info(
                f"[Worker {worker_id}] Assigned from authoritive: {binary.path.name} "
                f"(size: {size_mb:.2f}MB, est: {estimated_mb:.2f}MB)"
            )
        else:
            self.manager_logger.error(f"[Worker {worker_id}] Assignment failed: {error_msg}")

        return success
