"""Submissive worker manager for secondary nodes.

This manager performs OOM checking but does NOT do task assignment.
Instead, it requests tasks from the primary coordinator over network.
"""

from pathlib import Path
from typing import Any, Callable

from ..task import TaskDefinition
from .submissive_base import SubmissiveManagerBase


class SubmissiveManager(SubmissiveManagerBase):
    """Submissive worker manager for secondary nodes (remote over network).

    This manager:
    - Creates and manages LocalWorker instances (subprocess-based)
    - Performs OOM checking and worker killing
    - Does NOT autonomously assign tasks beyond initial phase
    - Requests tasks from primary coordinator via callback (over network)

    Note: This is a network relay manager. Logging is disabled by default
    since it only forwards messages and doesn't perform the actual work.
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
            manual_start_worker=manual_start_worker,
            connection_mode=connection_mode,
            socket_dir=socket_dir,
            enable_logging=False,
        )

        self.request_task_callback = request_task_callback

    def _request_task_from_authoritive(self, worker_id: int) -> None:
        """Request a task from the primary coordinator via network callback."""
        self.request_task_callback(worker_id)
