"""Authoritive worker manager for primary coordinator.

This manager performs task assignments but does NOT do OOM checking.
It's used by the primary to manage workers across multiple secondaries over network.
"""

from pathlib import Path

from ..binary_info import BinaryInfo
from ..task import TaskDefinition
from ..worker.base_worker import BaseWorker
from .authoritive_base import AuthoritiveManagerBase


class AuthoritiveManager(AuthoritiveManagerBase):
    """Authoritive worker manager for primary coordinator (remote over network).

    This manager:
    - Performs task assignments
    - Does NOT perform OOM checking (delegated to secondaries)
    - Used by primary to manage RemoteWorker instances over network
    """

    def __init__(
        self,
        num_workers: int,
        max_memory: int,
        log_dir: Path,
        task_definition: TaskDefinition,
        workers: list[BaseWorker] | None = None,
    ):
        super().__init__(
            num_workers=num_workers,
            max_memory=max_memory,
            log_dir=log_dir,
            task_definition=task_definition,
            workers=workers,
        )

    def _handle_oom_killed_task(self, worker: BaseWorker, binary: BinaryInfo, reason: str) -> None:
        """Handle OOM killed task by requeueing for retry.

        In distributed mode, OOM notifications come from secondaries.
        We requeue the task at the front of pending_binaries.
        """
        self.pending_binaries.insert(0, binary)
