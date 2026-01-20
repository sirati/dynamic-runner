"""Local submissive worker manager for local testing of master/slave architecture.

This manager works with a LocalAuthoritiveManager to test the distributed
assignment algorithm locally without networking.
"""

from pathlib import Path
from typing import Any, Callable

from ..binary_info import BinaryInfo
from ..task import TaskDefinition
from .submissive_base import SubmissiveManagerBase


class LocalSubmissiveManager(SubmissiveManagerBase):
    """Local submissive worker manager for testing master/slave locally.

    This manager:
    - Creates and manages LocalWorker instances (subprocess-based)
    - Performs OOM checking and worker killing
    - Requests tasks from a local authoritive manager (no network)
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
        )

        self.request_task_callback = request_task_callback

    def _request_task_from_authoritive(self, worker_id: int) -> None:
        """Request a task from the local authoritive manager via callback."""
        self.request_task_callback(worker_id)
