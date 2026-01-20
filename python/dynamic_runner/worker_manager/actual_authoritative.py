"""Actual authoritative worker manager implementation.

This manager performs decision-making responsibilities and provides authoritative API,
but does NOT perform OOM checking (delegated to submissive managers).
"""

from pathlib import Path
from typing import TYPE_CHECKING

from ..binary_info import BinaryInfo
from ..task import TaskDefinition
from ..worker.base_worker import BaseWorker
from .authoritative_base import AuthoritativeBase
from .decision_impl import DecisionWorkerManMixin

if TYPE_CHECKING:
    from .actual_submissive import ActualSubmissiveWorkerManager


class ActualAuthoritativeWorkerManager(DecisionWorkerManMixin, AuthoritativeBase):
    """Actual authoritative worker manager for primary coordinator.

    This manager:
    - Performs task assignments using the standard algorithm (decision responsibility via mixin)
    - Does NOT perform OOM checking (delegated to submissive managers)
    - Manages RemoteWorker instances (workers provided externally)
    - Provides authoritative API for submissive managers to request tasks
    """

    def __init__(
        self,
        num_workers: int,
        max_memory: int,
        log_dir: Path,
        task_definition: TaskDefinition,
        workers: list[BaseWorker] | None = None,
        submissive_managers: list["ActualSubmissiveWorkerManager"] | None = None,
        enable_logging: bool = True,
    ):
        # If submissive_managers provided, extract workers from them
        if submissive_managers and workers is None:
            workers = []
            for submissive in submissive_managers:
                workers.extend(submissive.workers)

        super().__init__(
            num_workers=num_workers,
            max_memory=max_memory,
            log_dir=log_dir,
            task_definition=task_definition,
            workers=workers,
            always_restart_worker=False,
            enable_logging=enable_logging,
        )

        self.submissive_managers = submissive_managers or []

    def _handle_oom_killed_task(self, worker: BaseWorker, binary: BinaryInfo, reason: str) -> None:
        """Handle OOM killed task by requeueing locally for retry.

        In distributed mode, OOM notifications come from submissive managers.
        We requeue the task at the front of pending_binaries.
        """
        self.pending_binaries.insert(0, binary)
