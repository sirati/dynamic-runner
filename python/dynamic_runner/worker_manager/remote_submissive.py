"""Remote submissive worker manager (relay only).

This manager is a network relay that forwards submissive API calls
over the network. It does not perform any execution logic itself.
"""

from pathlib import Path
from typing import Callable

from ..binary_info import BinaryInfo
from ..task import TaskDefinition
from ..worker.base_worker import BaseWorker
from .submissive_base import SubmissiveBase


class RemoteSubmissiveWorkerManager(SubmissiveBase):
    """Remote submissive worker manager (relay only).

    This manager:
    - Forwards submissive API calls over network (relay only)
    - Does NOT perform execution logic itself
    - Does NOT create or manage workers
    - Logging disabled by default (it's just a relay)
    """

    def __init__(
        self,
        num_workers: int,
        max_memory: int,
        log_dir: Path,
        task_definition: TaskDefinition,
        request_task_callback: Callable[[int], None],
        assign_task_callback: Callable[[int, BinaryInfo, int], bool] | None = None,
    ):
        super().__init__(
            num_workers=num_workers,
            max_memory=max_memory,
            log_dir=log_dir,
            task_definition=task_definition,
            always_restart_worker=False,
            enable_logging=False,  # Relay only, no logging
        )

        self.request_task_callback = request_task_callback
        self.assign_task_callback = assign_task_callback

    def _create_workers(self) -> list[BaseWorker]:
        """Return empty list - remote submissive doesn't create workers locally.

        Workers are created by the actual submissive manager on the remote node.
        """
        return []

    def _check_memory_pressure_and_kill(self) -> None:
        """No OOM checking in relay - handled by actual submissive manager."""
        pass

    def _request_task_from_authoritative(self, worker_id: int) -> None:
        """Relay task request to authoritative manager over network."""
        self.request_task_callback(worker_id)

    def _assign_binary_to_worker_initial_phase(self, worker: BaseWorker) -> bool:
        """Stub implementation - relay does not assign tasks directly.

        In relay mode, assignments are forwarded via network protocol.
        """
        return False

    def _assign_binary_to_worker_normal(self, worker: BaseWorker, retry_attempt: bool = False) -> bool:
        """Stub implementation - relay does not assign tasks directly.

        In relay mode, assignments are forwarded via network protocol.
        """
        return False

    def assign_task_from_authoritative(self, worker_id: int, binary: BinaryInfo, estimated_memory: int) -> bool:
        """Relay task assignment to actual submissive manager over network.

        Args:
            worker_id: Worker ID to assign to
            binary: BinaryInfo to process
            estimated_memory: Estimated memory needed

        Returns:
            True if assignment successful, False otherwise
        """
        if self.assign_task_callback:
            return self.assign_task_callback(worker_id, binary, estimated_memory)

        # Fallback to local implementation if no callback provided
        return super().assign_task_from_authoritative(worker_id, binary, estimated_memory)
