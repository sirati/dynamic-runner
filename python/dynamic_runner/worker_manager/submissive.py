"""Submissive worker manager for secondary nodes.

This manager performs OOM checking but does NOT do task assignment.
Instead, it requests tasks from the primary coordinator.
"""

from pathlib import Path
from typing import Any, Callable

from ..binary_info import BinaryInfo
from ..comm import ErrorType
from ..models import FailedTask
from ..task import TaskDefinition
from ..worker.base_worker import BaseWorker
from .local_base import LocalWorkerManagerBase


class SubmissiveManager(LocalWorkerManagerBase):
    """Submissive worker manager for secondary nodes.

    This manager:
    - Creates and manages LocalWorker instances (subprocess-based)
    - Performs OOM checking and worker killing
    - Does NOT autonomously assign tasks beyond initial phase
    - Requests tasks from primary coordinator via callback
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
        request_task_callback: Callable[[int], None],
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

        self.request_task_callback = request_task_callback

    def _handle_oom_killed_task(self, worker: BaseWorker, binary: BinaryInfo, reason: str) -> None:
        """Handle OOM killed task by reporting to primary (add to oom_tasks)."""
        self.oom_tasks.append(
            FailedTask(
                binary=binary,
                error_type=ErrorType.OUT_OF_MEMORY,
                error_message=reason,
            )
        )

    def _handle_worker_without_task(
        self,
        worker: BaseWorker,
        worker_id: int,
        active_workers: set[int],
        allow_stop: bool,
        is_initial_phase: bool = False,
    ) -> bool:
        """Handle a worker that has no current task.

        For submissive manager, after initial phase, request tasks from primary
        instead of autonomously assigning from local pending queue.
        """
        if is_initial_phase:
            # During initial phase, use base behavior
            return super()._handle_worker_without_task(worker, worker_id, active_workers, allow_stop, is_initial_phase)
        else:
            # After initial phase, request task from primary
            if worker.ready and not worker.current_binary:
                self.manager_logger.debug(f"[Worker {worker_id}] Requesting task from primary")
                self.request_task_callback(worker_id)
                # Keep worker active, waiting for primary assignment
                return True

            if not self.pending_binaries:
                if allow_stop:
                    worker.terminate()
                    self.manager_logger.info(f"[Worker {worker_id}] Stopping (no more tasks)")
                active_workers.discard(worker_id)
                return False

            # Worker is idle but binaries remain - keep it in the loop
            return True

    def assign_task_from_primary(self, worker_id: int, binary: BinaryInfo, estimated_memory: int) -> bool:
        """Assign a task to a worker from primary coordinator.

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
                f"[Worker {worker_id}] Assigned from primary: {binary.path.name} "
                f"(size: {size_mb:.2f}MB, est: {estimated_mb:.2f}MB)"
            )
        else:
            self.manager_logger.error(f"[Worker {worker_id}] Assignment failed: {error_msg}")

        return success
