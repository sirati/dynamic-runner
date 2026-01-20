"""Local worker manager implementation for subprocess-based workers.

This manager maintains the exact behavior of the original WorkerManager,
managing local subprocess workers with full OOM checking and task assignment.
"""

from pathlib import Path
from typing import Any

from ..binary_info import BinaryInfo
from ..task import TaskDefinition
from ..worker.base_worker import BaseWorker
from .local_base import LocalWorkerManagerBase


class LocalManager(LocalWorkerManagerBase):
    """Local worker manager with full local subprocess management.

    This manager:
    - Creates and manages LocalWorker instances (subprocess-based)
    - Performs task assignments
    - Performs OOM checking and worker killing
    - Maintains exact behavior of original WorkerManager
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
        print_pid: bool,
        always_restart_worker: bool = False,
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
            always_restart_worker=always_restart_worker,
            manual_start_worker=manual_start_worker,
            connection_mode=connection_mode,
            socket_dir=socket_dir,
        )

        self.print_pid = print_pid

    def _handle_oom_killed_task(self, worker: BaseWorker, binary: BinaryInfo, reason: str) -> None:
        """Handle OOM killed task by requeueing locally for retry."""
        self.pending_binaries.insert(0, binary)
