"""Authoritive worker manager for primary coordinator.

This manager performs task assignments but does NOT do OOM checking.
It's used by the primary to manage workers across multiple secondaries.
"""

from pathlib import Path

from ..task import TaskDefinition
from ..worker.base_worker import BaseWorker
from .base import WorkerManagerBase


class AuthoritiveManager(WorkerManagerBase):
    """Authoritive worker manager for primary coordinator.

    This manager:
    - Performs task assignments
    - Does NOT perform OOM checking (delegated to secondaries)
    - Used by primary to manage RemoteWorker instances
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
            always_restart_worker=False,
        )

        # Workers can be provided externally (RemoteWorker instances)
        self._external_workers = workers or []

    def _create_workers(self) -> list[BaseWorker]:
        """Return externally provided workers (RemoteWorker instances)."""
        return self._external_workers

    def _check_memory_pressure_and_kill(self) -> None:
        """No OOM checking in authoritive manager - delegated to secondaries."""
        # Authoritive manager does not perform OOM checking
        # This is handled by SubmissiveManager on each secondary
        pass
