import asyncio
import logging
import time
from typing import TYPE_CHECKING, Any

from ...worker_manager.submissive import SubmissiveManager

if TYPE_CHECKING:
    from .coordinator import SecondaryCoordinator

logger = logging.getLogger(__name__)


class WorkerManager:
    """Manages worker processes and their lifecycle"""

    def __init__(self, coordinator: "SecondaryCoordinator"):
        self.coordinator = coordinator

    async def start_workers(self) -> None:
        """Start worker processes using SubmissiveManager"""
        logger.info(f"Starting {self.coordinator.num_workers} workers via SubmissiveManager")

        # Create SubmissiveManager to handle all worker lifecycle
        self.coordinator.worker_manager = SubmissiveManager(
            num_workers=self.coordinator.num_workers,
            max_memory=self.coordinator.ram_bytes,
            source_dir=self.coordinator.src_tmp,
            output_dir=self.coordinator.out_tmp,
            task_definition=self.coordinator.task_definition,
            task_args=self.coordinator.task_args,
            skip_existing=self.coordinator.skip_existing,
            request_task_callback=self._request_task_callback,
            manual_start_worker=False,
            connection_mode="named",
            socket_dir=self.coordinator.socket_dir,
        )

        # Initialize workers (this starts the processes and waits for ready)
        self.coordinator.worker_manager._initialize_workers()

        # Send worker ready messages to primary for each worker
        for worker in self.coordinator.worker_manager.workers:
            worker_id = worker.worker_id
            memory_budget = worker.reserved_budget
            await self._send_worker_ready(worker_id, memory_budget)

        logger.info(f"All {len(self.coordinator.worker_manager.workers)} workers initialized and reported to primary")

    def _request_task_callback(self, worker_id: int) -> None:
        """Callback for SubmissiveManager to request tasks from primary.

        This is called synchronously, so we need to schedule the async work.
        """
        asyncio.create_task(self._request_new_task(worker_id))

    async def _send_worker_ready(self, worker_id: int, memory_budget: int) -> None:
        """Send worker ready message to primary"""
        msg = {
            "type": "worker_ready",
            "secondary_id": self.coordinator.secondary_id,
            "worker_id": worker_id,
            "ram_bytes": memory_budget,
            "from": self.coordinator.secondary_id,
        }

        await self.coordinator.send_to_primary_ws(msg)

    async def _request_new_task(self, worker_id: int) -> None:
        """Request a new task from primary"""
        if self.coordinator.connection_closing or not self.coordinator.message_router.primary_connection:
            logger.debug(f"Cannot request task for worker {worker_id}: not connected to primary")
            return

        if not self.coordinator.worker_manager or worker_id >= len(self.coordinator.worker_manager.workers):
            logger.error(f"Invalid worker_id {worker_id}")
            return

        worker = self.coordinator.worker_manager.workers[worker_id]

        msg = {
            "type": "task_request",
            "secondary_id": self.coordinator.secondary_id,
            "worker_id": worker_id,
            "from": self.coordinator.secondary_id,
        }

        await self.coordinator.send_to_primary_ws(msg)
        logger.debug(f"Requested new task for worker {worker_id}")

    def process_worker_updates(self) -> None:
        """Process worker completion and status updates using WorkerManager"""
        if not self.coordinator.worker_manager:
            return

        manager = self.coordinator.worker_manager

        # Check for ready messages from workers
        for worker in manager.workers:
            if not worker.ready:
                has_result, _ = worker.check_status()

        # Process each worker
        for worker_id, worker in enumerate(manager.workers):
            # Skip workers that aren't ready
            if not worker.ready:
                continue

            # Check if worker has a current task
            if worker.current_binary is not None:
                # Save the binary info before completion clears it
                current_binary = worker.current_binary

                # Check worker status
                has_result, result = worker.check_status()

                if has_result:
                    # Handle the result using base class logic
                    manager._worker_completed(worker, result)

                    # Notify primary of task completion
                    if result.success and current_binary:
                        # Compute task hash
                        import hashlib

                        hash_input = f"{current_binary.path}:{current_binary.binary_name}:{current_binary.platform}:{current_binary.compiler}:{current_binary.version}:{current_binary.opt_level}"
                        task_hash = hashlib.sha256(hash_input.encode()).hexdigest()[:16]

                        # Schedule async notification
                        import asyncio

                        task_handler = self.coordinator.task_handler
                        asyncio.create_task(task_handler.notify_task_complete(worker_id, task_hash))

                    # After completion, worker is ready for new task
                    # In submissive mode, request task from primary
                    if worker.ready and not worker.current_binary:
                        logger.debug(f"[Worker {worker_id}] Completed task, requesting new task from primary")
                        self.request_task_from_primary(worker_id)

        # Check memory pressure and kill workers if needed
        manager._check_memory_pressure_and_kill()

    async def send_keepalive(self) -> None:
        """Send keepalive to all peers"""
        active_count = 0
        if self.coordinator.worker_manager:
            active_count = len([w for w in self.coordinator.worker_manager.workers if w.current_binary is not None])

        msg = {
            "type": "keepalive",
            "secondary_id": self.coordinator.secondary_id,
            "active_workers": active_count,
        }

        # Broadcast to all peers (secondaries)
        if len(self.coordinator.message_router.secondary_connections) > 0:
            await self.coordinator.message_router.broadcast_to_secondaries(msg)
        else:
            logger.debug("No peer connections yet, skipping keepalive broadcast")

    def check_peer_timeouts(self) -> None:
        """Check for peer timeouts"""
        current_time = time.time()
        timeout_threshold = 120.0  # 2 minutes

        for peer_id, last_seen in self.coordinator.last_keepalives.items():
            if current_time - last_seen > timeout_threshold:
                logger.warning(f"Timeout detected for peer: {peer_id}")
                self._handle_timeout(peer_id)

    def _handle_timeout(self, peer_id: str) -> None:
        """Handle detected peer timeout"""
        # TODO: Query other peers for last keepalive
        # TODO: Mark peer as dead if consensus reached
        pass
