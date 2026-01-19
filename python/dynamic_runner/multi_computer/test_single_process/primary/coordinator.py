"""Single-process primary coordinator for testing without network overhead.

This coordinator runs primary and secondary in the same async context, using
Python message passing (asyncio queues) instead of network communication.
Worker managers use direct local_submissive to local_authoritive connection.

IMPORTANT: This coordinator follows the proper multi-computer protocol:
1. Prepare (create secondaries)
2. Connect (wait for secondaries - simulated)
3. Initial assignment (via _initial_assignment_phase)
4. File transfer (paths only, no actual transfer)
5. Notify transfer complete
6. Promote primary (one secondary becomes authoritive)
7. Send full task list
8. Monitor mode (workers process, request tasks dynamically)
"""

import asyncio
import logging
from pathlib import Path
from typing import Any

from ....binary_info import BinaryInfo
from ....multi_computer import ConnectionResult, FileTransferMode, PreparationResult
from ....multi_computer.primary.coordinator import BaseCoordinator
from ....task import TaskDefinition
from ....worker_manager import LocalAuthoritiveManager, LocalSubmissiveManager

logger = logging.getLogger(__name__)


class SingleProcessPrimaryCoordinator(BaseCoordinator):
    """Single-process primary coordinator.

    Runs primary and secondary in same async context with message passing.
    Uses local_submissive + local_authoritive for worker management.
    Follows the proper multi-computer protocol for testing.
    """

    def __init__(
        self,
        binaries: list[BinaryInfo],
        task_definition: TaskDefinition,
        task_args: Any,
        run_id: str = "default",
        source_dir: Path | None = None,
        output_dir: Path | None = None,
        num_workers_per_secondary: int = 2,
    ):
        super().__init__(binaries, task_definition, task_args, run_id, source_dir)

        self.output_dir = output_dir or Path.cwd() / "out"
        self.num_workers_per_secondary = num_workers_per_secondary

        # In-process secondaries (for message passing simulation)
        self.secondary_tasks: list[asyncio.Task] = []

        # Message queues for each secondary (primary -> secondary, secondary -> primary)
        self.to_secondary_queues: dict[str, asyncio.Queue] = {}
        self.from_secondary_queues: dict[str, asyncio.Queue] = {}

        # Worker managers
        self.submissive_managers: list[LocalSubmissiveManager] = []
        self.authoritive_manager: LocalAuthoritiveManager | None = None

        # Track which secondary is promoted to authoritive
        self.promoted_secondary_id: str | None = None

    async def prepare(self, num_secondaries: int, quic_port: int) -> PreparationResult:
        """Prepare single-process mode - create in-process secondaries.

        Args:
            num_secondaries: Number of secondaries to spawn
            quic_port: Not used in single-process mode

        Returns:
            PreparationResult with single-process data
        """
        logger.info("Phase 1: Single-Process Preparation")
        logger.info(f"Creating {num_secondaries} in-process secondaries")

        # Create submissive managers for each secondary
        total_workers = 0
        for i in range(num_secondaries):
            secondary_id = f"secondary-{i}"

            # Create submissive manager with placeholder callback
            submissive = LocalSubmissiveManager(
                num_workers=self.num_workers_per_secondary,
                max_memory=16 * 1024 * 1024 * 1024,  # 16GB default per secondary
                source_dir=self.source_dir or Path.cwd(),
                output_dir=self.output_dir,
                task_definition=self.task_definition,
                task_args=self.task_args,
                skip_existing=False,
                request_task_callback=lambda wid: None,  # Placeholder, will be replaced
            )

            self.submissive_managers.append(submissive)
            total_workers += self.num_workers_per_secondary

            logger.info(f"Created submissive manager for {secondary_id} with {self.num_workers_per_secondary} workers")

        # Create authoritive manager that manages all submissive managers
        self.authoritive_manager = LocalAuthoritiveManager(
            num_workers=total_workers,
            max_memory=16 * 1024 * 1024 * 1024 * num_secondaries,
            log_dir=self.output_dir,
            task_definition=self.task_definition,
            submissive_managers=self.submissive_managers,
        )

        logger.info(f"Created authoritive manager for {total_workers} total workers")

        # Set up callbacks for each submissive to request from authoritive
        for i, submissive in enumerate(self.submissive_managers):
            secondary_id = f"secondary-{i}"

            # Use default parameter to capture values in closure
            def make_callback(sec_id: str = secondary_id, sec_idx: int = i):
                def callback(worker_id: int):
                    # Request task from authoritive manager
                    global_worker_id = self._get_global_worker_id(sec_id, worker_id)
                    result = self.authoritive_manager.handle_task_request(global_worker_id)
                    if result:
                        binary, estimated_memory = result
                        submissive_for_sec = self.submissive_managers[sec_idx]
                        submissive_for_sec.assign_task_from_authoritive(worker_id, binary, estimated_memory)

                return callback

            # Replace the placeholder callback with the real one
            submissive.request_task_callback = make_callback()

        # Create message queues for protocol simulation (not heavily used in local mode)
        for i in range(num_secondaries):
            secondary_id = f"secondary-{i}"
            self.to_secondary_queues[secondary_id] = asyncio.Queue()
            self.from_secondary_queues[secondary_id] = asyncio.Queue()

        # Register secondary info (simulated)
        for i in range(num_secondaries):
            secondary_id = f"secondary-{i}"
            self.secondaries[secondary_id] = {
                "secondary_id": secondary_id,
                "num_workers": self.num_workers_per_secondary,
                "ram_bytes": 16 * 1024 * 1024 * 1024,
                "quic_port": 0,
                "cert_fingerprint": None,
                "connection": None,
                "message_queue": self.to_secondary_queues[secondary_id],
            }

        mode_specific_data = {
            "secondary_queues": self.to_secondary_queues,
        }

        return PreparationResult(
            num_secondaries=num_secondaries,
            run_id=self.run_id,
            cert_dir=self.cert_dir,
            primary_entropy=self.primary_entropy,
            mode_specific_data=mode_specific_data,
        )

    async def connect(self, prep_result: PreparationResult) -> ConnectionResult:
        """Wait for secondaries to connect (simulated for single-process).

        Args:
            prep_result: Result from preparation phase

        Returns:
            ConnectionResult with connection data
        """
        logger.info("Phase 2: Connection (single-process - simulated)")

        # In single-process mode, connection is immediate
        # All secondaries are already registered in self.secondaries during prepare()
        logger.info(f"All {prep_result.num_secondaries} in-process secondaries ready")

        return ConnectionResult(
            secondaries=self.secondaries,
            peer_connections_ready=set(self.secondaries.keys()),
        )

    async def transfer_files(self, conn_result: ConnectionResult) -> None:
        """Transfer files phase - for single-process, just copy initial assignments.

        In single-process mode, we copy the initial assignments from the authoritive
        manager's workers to the submissive managers' workers. No actual file transfer
        is needed since they share the same filesystem.

        This follows the protocol where:
        1. Primary assigns initial tasks to workers
        2. Primary notifies secondaries which files to process
        3. Secondaries receive file paths (not file data in local mode)

        Args:
            conn_result: Result from connection phase
        """
        logger.info("Phase 4: File transfer (single-process - copying assignments)")

        # The authoritive manager has already done initial assignments
        # Now we copy those assignments to the submissive workers
        for i, submissive in enumerate(self.submissive_managers):
            base_worker_id = i * self.num_workers_per_secondary
            for local_worker_id in range(len(submissive.workers)):
                global_worker_id = base_worker_id + local_worker_id
                if global_worker_id >= len(self.authoritive_manager.workers):
                    continue

                auth_worker = self.authoritive_manager.workers[global_worker_id]
                sub_worker = submissive.workers[local_worker_id]

                # Copy assignment from authoritive to submissive
                if auth_worker.current_binary is not None:
                    sub_worker.current_binary = auth_worker.current_binary
                    sub_worker.estimated_memory = auth_worker.estimated_memory
                    sub_worker.opportunistic = getattr(auth_worker, "opportunistic", False)
                    sub_worker.has_received_initial_assignment = True
                    logger.debug(
                        f"Copied initial assignment to submissive {i} worker {local_worker_id}: "
                        f"{auth_worker.current_binary.path.name}"
                    )

        logger.info("File transfer complete - initial assignments copied")

    def get_file_transfer_mode(self) -> FileTransferMode:
        """Get file transfer mode for single-process (direct copy, no network transfer)."""
        return FileTransferMode.SKIP_TRANSFER

    async def _preliminary_assignment(self) -> None:
        """Perform preliminary assignment using existing authoritive manager.

        Override base implementation to use the authoritive manager we already created
        in prepare() instead of creating new AuthoritiveManager instances.
        """
        logger.info("Phase 5: Preliminary assignment (single-process)")

        if len(self.secondaries) == 0:
            logger.warning("No secondaries connected")
            return

        if len(self.binaries) == 0:
            logger.warning("No binaries to assign")
            return

        # Use the existing authoritive manager we created
        # Populate pending_binaries and perform initial assignment
        self.authoritive_manager.pending_binaries = self.binaries.copy()
        self.authoritive_manager._initialize_workers()
        self.authoritive_manager._run_initial_assignments()

        # Track assignments in task_assignments for protocol compatibility
        for i, submissive in enumerate(self.submissive_managers):
            secondary_id = f"secondary-{i}"
            base_worker_id = i * self.num_workers_per_secondary
            assigned_count = 0

            for local_worker_id in range(len(submissive.workers)):
                global_worker_id = base_worker_id + local_worker_id
                if global_worker_id >= len(self.authoritive_manager.workers):
                    continue

                auth_worker = self.authoritive_manager.workers[global_worker_id]
                if auth_worker.current_binary is not None:
                    task_hash = self._compute_task_hash(auth_worker.current_binary)
                    self.task_assignments[task_hash] = secondary_id
                    assigned_count += 1

            logger.info(f"  {secondary_id}: {assigned_count} initial tasks assigned")

        total_assigned = len(self.task_assignments)
        logger.info(f"Total initial assignments: {total_assigned}/{len(self.binaries)} tasks")

    async def _wait_for_workers(self) -> None:
        """Wait for workers - in single-process mode, directly populate remote_workers.

        In single-process mode, workers are already created in the submissive managers.
        We just need to populate the remote_workers dict that the base coordinator uses
        to track worker readiness and budgets.
        """
        logger.info("Phase 4: Waiting for workers (single-process - simulated)")

        # Populate remote_workers directly from submissive managers
        for i, submissive in enumerate(self.submissive_managers):
            secondary_id = f"secondary-{i}"
            base_worker_id = i * self.num_workers_per_secondary

            # Initialize remote_workers list for this secondary
            if secondary_id not in self.remote_workers:
                self.remote_workers[secondary_id] = []

            # Add each worker's info
            for local_worker_id, worker in enumerate(submissive.workers):
                global_worker_id = base_worker_id + local_worker_id

                worker_info = {
                    "worker_id": local_worker_id,
                    "global_worker_id": global_worker_id,
                    "budget_mb": worker.budget / (1024 * 1024),
                }
                self.remote_workers[secondary_id].append(worker_info)
                logger.debug(
                    f"Registered worker {secondary_id}:{local_worker_id} "
                    f"(global {global_worker_id}) with budget {worker_info['budget_mb']:.2f}MB"
                )

        expected_workers = sum(len(workers) for workers in self.remote_workers.values())
        logger.info(f"All {expected_workers} workers ready")

    async def _monitor_mode(self) -> None:
        """Monitor mode - start processing binaries with submissive managers.

        This is called after the full protocol (initial assignment, file transfer,
        promote primary, send full task list). Now workers start processing and
        dynamically request new tasks.

        Since submissive.process_binaries is synchronous (blocking),
        we run each in a thread executor to avoid blocking the async event loop.
        """
        logger.info("Phase 8: Monitor mode - starting worker processing")

        import concurrent.futures

        loop = asyncio.get_event_loop()

        # Create tasks for each submissive manager - run in executor since they're blocking
        tasks = []
        with concurrent.futures.ThreadPoolExecutor(max_workers=len(self.submissive_managers)) as executor:
            for i, submissive in enumerate(self.submissive_managers):
                # Give each submissive the full list of binaries
                # The authoritive manager will coordinate which ones get assigned
                # but the submissive needs the list for stats tracking
                binaries_for_submissive = self.binaries.copy()

                # Run the blocking process_binaries in executor
                task = loop.run_in_executor(executor, submissive.process_binaries, binaries_for_submissive)
                tasks.append(task)

            # Wait for all to complete
            await asyncio.gather(*tasks)

        logger.info("All submissive managers completed processing")

    async def _cleanup(self) -> None:
        """Cleanup single-process resources."""
        logger.info("Cleaning up single-process resources...")

        # Cancel secondary tasks
        for task in self.secondary_tasks:
            if not task.done():
                task.cancel()
                try:
                    await task
                except asyncio.CancelledError:
                    pass

        # Cleanup base coordinator resources
        await super()._cleanup()

        logger.info("Single-process cleanup complete")

    def _get_submissive_for_secondary(self, secondary_id: str) -> LocalSubmissiveManager | None:
        """Get submissive manager for a given secondary ID."""
        secondary_index = int(secondary_id.split("-")[1])
        if 0 <= secondary_index < len(self.submissive_managers):
            return self.submissive_managers[secondary_index]
        return None

    def _get_base_worker_id_for_secondary(self, secondary_id: str) -> int:
        """Get the base global worker ID for a secondary."""
        secondary_index = int(secondary_id.split("-")[1])
        return secondary_index * self.num_workers_per_secondary

    def _get_global_worker_id(self, secondary_id: str, local_worker_id: int) -> int:
        """Convert local worker ID to global worker ID."""
        return self._get_base_worker_id_for_secondary(secondary_id) + local_worker_id
