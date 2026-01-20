"""Base class for local worker management.

This module provides shared logic for managers that create and manage
LocalWorker instances (subprocess-based workers).
"""

from pathlib import Path
from typing import Any

from ..comm import ErrorType
from ..models import FailedTask
from ..task import TaskDefinition
from ..worker.base_worker import BaseWorker
from ..worker.local_worker import LocalWorker
from .base import WorkerManagerBase


class LocalWorkerManagerBase(WorkerManagerBase):
    """Base class for managers that create local subprocess workers.

    This class provides:
    - Local worker creation logic
    - Socket path management
    - OOM checking with customizable task handling via hooks
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
        always_restart_worker: bool = False,
        manual_start_worker: bool = False,
        connection_mode: str = "socketpair",
        socket_dir: Path | None = None,
        enable_logging: bool = True,
    ):
        super().__init__(
            num_workers=num_workers,
            max_memory=max_memory,
            log_dir=output_dir / "logs",
            task_definition=task_definition,
            always_restart_worker=always_restart_worker,
            enable_logging=enable_logging,
        )

        self.source_dir = source_dir
        self.output_dir = output_dir
        self.task_args = task_args
        self.skip_existing = skip_existing
        self.manual_start_worker = manual_start_worker
        self.connection_mode = connection_mode
        self.socket_dir = socket_dir

        # Validate socket_dir for named mode
        if self.connection_mode == "named":
            if self.socket_dir is None:
                raise ValueError("socket_dir is required when connection_mode is 'named'")
            self.socket_dir.mkdir(parents=True, exist_ok=True)

    def _get_socket_path(self, worker_id: int) -> Path | None:
        """Get socket path for a worker in named mode."""
        if self.connection_mode != "named":
            return None
        return self.socket_dir / f"worker_{worker_id}_{self.session_id}.sock"

    def _create_workers(self) -> list[BaseWorker]:
        """Create local worker instances."""
        workers = []
        for worker_id in range(self.num_workers):
            worker_log_path = self.log_dir / f"worker_{worker_id}.log"
            socket_path = self._get_socket_path(worker_id)

            worker = LocalWorker(
                worker_id=worker_id,
                memory_budget=0,  # Will be set by _initialize_workers
                source_dir=self.source_dir,
                output_dir=self.output_dir,
                log_path=worker_log_path,
                task_definition=self.task_definition,
                task_args=self.task_args,
                skip_existing=self.skip_existing,
                manual_start=self.manual_start_worker,
                connection_mode=self.connection_mode,
                socket_path=socket_path,
            )
            workers.append(worker)

        return workers

    def _check_memory_pressure_and_kill(self) -> None:
        """Check memory pressure and kill workers according to specification."""
        actual_usage = self._get_worker_actual_memory_usage()
        threshold = min(500 * 1024 * 1024, self.max_memory // self.num_workers)

        # Check if we should kill opportunistic workers
        if actual_usage > (self.max_memory - threshold):
            opportunistic_workers = [w for w in self.workers if w.opportunistic and w.current_binary is not None]

            if opportunistic_workers:
                # Kill median opportunistic worker
                sorted_opp = sorted(opportunistic_workers, key=lambda w: w.estimated_memory)
                median_idx = len(sorted_opp) // 2
                victim = sorted_opp[median_idx]

                binary_name = victim.current_binary.path.name if victim.current_binary else "unknown"
                usage_mb = actual_usage / (1024 * 1024)
                self.manager_logger.warning(
                    f"[OOM] Killing median opportunistic worker {victim.worker_id} "
                    f"({binary_name}, usage: {usage_mb:.0f}MB)"
                )

                # Handle killed task via hook (requeue locally or report to primary)
                if victim.current_binary and not self.in_oom_phase:
                    self._handle_oom_killed_task(victim, victim.current_binary, "Opportunistic worker OOM killed")

                victim.current_binary = None
                victim.estimated_memory = 0
                victim.restart()
                return

        # Check if we exceeded limit (no opportunistic workers)
        if actual_usage > self.max_memory:
            active_workers = [w for w in self.workers if w.current_binary is not None]

            if not active_workers:
                return

            # Kill smallest worker
            smallest = min(active_workers, key=lambda w: w.estimated_memory)

            # Special handling for worker 0
            if smallest.worker_id == 0:
                binary_name = smallest.current_binary.path.name if smallest.current_binary else "unknown"
                self.manager_logger.error(f"[OOM] Worker 0 killed: {binary_name}")
                if smallest.current_binary:
                    self.oom_tasks.append(
                        FailedTask(
                            binary=smallest.current_binary,
                            error_type=ErrorType.OUT_OF_MEMORY,
                            error_message="Worker 0 exceeded memory",
                        )
                    )
                smallest.current_binary = None
                smallest.estimated_memory = 0
                smallest.restart()
            else:
                binary_name = smallest.current_binary.path.name if smallest.current_binary else "unknown"
                usage_mb = actual_usage / (1024 * 1024)
                self.manager_logger.warning(
                    f"[OOM] Killing smallest worker {smallest.worker_id} "
                    f"({binary_name}, usage: {usage_mb:.0f}MB), marking as opportunistic"
                )

                # Handle killed task via hook (requeue locally or report to primary)
                if smallest.current_binary and not self.in_oom_phase:
                    self._handle_oom_killed_task(
                        smallest, smallest.current_binary, "Worker OOM killed and marked opportunistic"
                    )

                smallest.current_binary = None
                smallest.estimated_memory = 0
                smallest.opportunistic = True
                smallest.restart()
