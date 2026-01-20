"""Remote authoritative worker manager (relay only).

This manager is a network relay that forwards authoritative API calls
over the network. It does not perform any decision-making logic itself.
"""

from pathlib import Path
from typing import Callable

from ..binary_info import BinaryInfo
from ..task import TaskDefinition
from ..worker.base_worker import BaseWorker
from .authoritative_base import AuthoritativeBase


class RemoteAuthoritativeWorkerManager(AuthoritativeBase):
    """Remote authoritative worker manager (relay only).

    This manager:
    - Forwards authoritative API calls over network (relay only)
    - Does NOT perform decision logic itself
    - Does NOT perform OOM checking
    - Logging disabled by default (it's just a relay)
    """

    def __init__(
        self,
        num_workers: int,
        max_memory: int,
        log_dir: Path,
        task_definition: TaskDefinition,
        workers: list[BaseWorker] | None = None,
        handle_task_request_callback: Callable[[int], tuple[BinaryInfo, int] | None] | None = None,
    ):
        super().__init__(
            num_workers=num_workers,
            max_memory=max_memory,
            log_dir=log_dir,
            task_definition=task_definition,
            workers=workers,
            always_restart_worker=False,
            enable_logging=False,  # Relay only, no logging
        )

        self.handle_task_request_callback = handle_task_request_callback

    def _create_workers(self) -> list[BaseWorker]:
        """Return externally provided workers (RemoteWorker instances)."""
        return self._external_workers

    def _handle_oom_killed_task(self, worker: BaseWorker, binary: BinaryInfo, reason: str) -> None:
        """Relay OOM notification over network.

        In relay mode, this should send the OOM notification to the actual
        authoritative manager over the network.
        """
        # In relay mode, this would be forwarded via network protocol
        # For now, requeue locally (same behavior as actual authoritative)
        self.pending_binaries.insert(0, binary)

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

    def handle_task_request(self, worker_id: int) -> tuple[BinaryInfo, int] | None:
        """Relay task request to actual authoritative manager over network.

        Args:
            worker_id: Worker ID requesting a task

        Returns:
            Tuple of (binary, estimated_memory) if task assigned, None otherwise
        """
        if self.handle_task_request_callback:
            return self.handle_task_request_callback(worker_id)

        # Fallback to local implementation if no callback provided
        return super().handle_task_request(worker_id)
