"""Actual submissive worker manager implementation.

This manager performs execution responsibilities (worker lifecycle, OOM checking)
and provides submissive API, but does NOT perform assignment decisions
(requests tasks from authoritative manager).
"""

from pathlib import Path
from typing import Any, Callable

from ..task import TaskDefinition
from .execution_impl import ExecutionWorkerManBaseImpl
from .submissive_base import SubmissiveBase


class ActualSubmissiveWorkerManager(ExecutionWorkerManBaseImpl, SubmissiveBase):
    """Actual submissive worker manager for secondary nodes.

    This manager:
    - Creates and manages LocalWorker instances (subprocess-based)
    - Performs OOM checking and worker killing (execution responsibility)
    - Does NOT autonomously assign tasks beyond initial phase
    - Requests tasks from authoritative manager via callback
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
        enable_logging: bool = True,
    ):
        # Store callback before calling super().__init__
        self.request_task_callback = request_task_callback

        # Call super().__init__ which will follow MRO:
        # ActualSubmissiveWorkerManager -> ExecutionWorkerManBaseImpl -> SubmissiveBase -> WorkerManagerBase
        super().__init__(
            num_workers=num_workers,
            max_memory=max_memory,
            log_dir=output_dir / "logs",
            task_definition=task_definition,
            source_dir=source_dir,
            output_dir=output_dir,
            task_args=task_args,
            skip_existing=skip_existing,
            always_restart_worker=False,
            manual_start_worker=manual_start_worker,
            connection_mode=connection_mode,
            socket_dir=socket_dir,
            enable_logging=enable_logging,
        )

    def _request_task_from_authoritative(self, worker_id: int) -> None:
        """Request a task from the authoritative manager via callback."""
        self.request_task_callback(worker_id)
