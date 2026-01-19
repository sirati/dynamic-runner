"""Base primary coordinator for multi-computer coordination.

This module provides the common coordination logic for orchestrating work
across multiple secondary nodes, independent of the specific execution mode
(SLURM, local, etc.).
"""

import asyncio
import hashlib
import json
import logging
import secrets
from pathlib import Path
from typing import Any

from ...binary_info import BinaryInfo
from ...task import TaskDefinition
from ...worker.remote_worker import RemoteWorker
from ...worker_manager import WorkerManager
from ...worker_manager.authoritive import AuthoritiveManager
from .. import ConnectionResult, CoordinationPhase, FileTransferMode, PreparationResult
from ..message_router import MessageRouter
from ..quic_transport import QuicTransport

logger = logging.getLogger(__name__)


class BaseCoordinator:
    """Base coordinator for multi-computer orchestration.

    This class contains the common coordination logic that is independent
    of the specific execution mode (SLURM, local, etc.). Subclasses implement
    mode-specific preparation and file transfer logic.
    """

    def __init__(
        self,
        binaries: list[BinaryInfo],
        task_definition: TaskDefinition,
        task_args: Any,
        run_id: str = "default",
        source_dir: Path | None = None,
    ):
        self.binaries = binaries
        self.task_definition = task_definition
        self.task_args = task_args
        self.run_id = run_id
        self.source_dir = source_dir

        # Certificate persistence directory (local, in run-specific directory)
        self.cert_dir = Path.cwd() / "run" / run_id / "certificates"

        self.secondaries: dict[str, dict[str, Any]] = {}
        self.worker_managers: dict[str, WorkerManager] = {}  # One WorkerManager per secondary
        self.remote_workers: dict[str, list[RemoteWorker]] = {}  # Remote workers per secondary
        self.task_assignments: dict[str, str] = {}  # task_hash -> secondary_id
        self.completed_tasks: set[str] = set()
        self.failed_tasks: set[str] = set()
        self.discovered_binaries: dict[str, dict[str, Any]] = {}  # hash -> {zip_name, local_path, binary_info}
        self.peer_connections_ready: set[str] = set()  # Track which secondaries have completed peer connections

        self.primary_entropy = secrets.token_bytes(32)
        self.peer_info: list[dict[str, Any]] = []

        self.running = True
        self.transfer_complete = False
        self.slurm_primary_id: str | None = None

        # Message router for communication
        self.message_router = MessageRouter("primary", "primary")

        # QUIC transport for all connections (primary-secondary and secondary-secondary)
        self.quic_transport: QuicTransport | None = None
        self.primary_quic_port = 0  # Let OS allocate a free port

        # Track active connections (stores StreamWriter, Server, and control paths)
        self.active_connections: dict[str, Any] = {}

    def run(self, num_secondaries: int, quic_port: int = 5000) -> None:
        """Main execution loop for primary coordinator

        Args:
            num_secondaries: Number of secondaries to spawn
            quic_port: Base port for QUIC connections
        """
        logger.info("=" * 60)
        logger.info("PRIMARY COORDINATOR")
        logger.info("=" * 60)
        logger.info(f"Total binaries to process: {len(self.binaries)}")
        logger.info(f"Spawning {num_secondaries} secondaries")
        logger.info("")

        # Run async coordinator
        asyncio.run(self._run_async(num_secondaries, quic_port))

    async def _run_async(self, num_secondaries: int, quic_port: int) -> None:
        """Async main execution loop"""
        try:
            # Phase 0: Setup QUIC transport and certificates
            await self._setup_quic_transport()

            # Phase 1: Preparation (mode-specific)
            prep_result = await self.prepare(num_secondaries, quic_port)

            # Phase 2: Connection - Wait for secondaries to connect
            conn_result = await self.connect(prep_result)

            # Phase 3: Initial assignment
            await self._initial_assignment_phase(conn_result)

            # Phase 4: File transfer (mode-specific)
            await self.transfer_files(conn_result)

            # Phase 5: Notify transfer complete
            await self._notify_transfer_complete()

            # Phase 6: Promote SLURM-primary (if applicable)
            await self._promote_primary()

            # Phase 7: Send full task list
            await self._send_full_task_list()

            # Phase 8: Monitor until user disconnects
            await self._monitor_mode()

        except KeyboardInterrupt:
            logger.info("Received interrupt signal")
        except Exception as e:
            logger.error(f"Primary coordinator error: {e}", exc_info=True)
        finally:
            await self._cleanup()

    async def _setup_quic_transport(self) -> None:
        """Setup QUIC transport and generate certificates"""
        logger.info("Phase 0: Setting up QUIC transport")

        # Create certificate directory
        self.cert_dir.mkdir(parents=True, exist_ok=True)
        logger.info(f"Certificate directory: {self.cert_dir}")

        # Initialize QUIC transport - listen only on localhost, let OS choose port
        self.quic_transport = QuicTransport("primary", listen_port=0, bind_address="127.0.0.1")

        # Try to load existing certificates from local disk
        primary_cert_path = self.cert_dir / "primary_cert.pem"
        primary_key_path = self.cert_dir / "primary_key.pem"

        # Check if certificates exist locally
        if primary_cert_path.exists() and primary_key_path.exists():
            logger.info("Loading existing primary certificates from local disk...")

            self.quic_transport.cert_path = primary_cert_path
            self.quic_transport.key_path = primary_key_path
            self.quic_transport.cert_fingerprint = self.quic_transport._compute_cert_fingerprint(primary_cert_path)

            logger.info(f"Loaded certificates with fingerprint: {self.quic_transport.cert_fingerprint}")
        else:
            logger.info("Generating new primary certificates...")
            await self.quic_transport.generate_certificates()

            # Save certificates to local disk for persistence
            if not self.quic_transport.cert_path or not self.quic_transport.key_path:
                raise RuntimeError("Certificates not generated properly")

            cert_content = self.quic_transport.cert_path.read_text()
            key_content = self.quic_transport.key_path.read_text()

            primary_cert_path.write_text(cert_content)
            primary_key_path.write_text(key_content)
            primary_key_path.chmod(0o600)

            logger.info(f"Saved certificates to {self.cert_dir}")

        # Start QUIC server listening only on localhost
        await self.quic_transport.start_server()
        # Update primary_quic_port with the OS-allocated port
        self.primary_quic_port = self.quic_transport.listen_port
        logger.info(f"Primary QUIC server listening on 127.0.0.1:{self.primary_quic_port}")

        # Register message handlers with both message_router and QUIC transport
        self.message_router.register_handler("secondary_welcome", self._handle_secondary_welcome)
        self.message_router.register_handler("cert_exchange", self._handle_cert_exchange)
        self.message_router.register_handler("secondary_error", self._handle_secondary_error)
        self.message_router.register_handler("secondary_log", self._handle_secondary_log)
        self.message_router.register_handler("peer_connections_ready", self._handle_peer_connections_ready)
        self.message_router.register_handler("worker_ready", self._handle_worker_ready)
        self.message_router.register_handler("source_discovered", self._handle_source_discovered)
        self.message_router.register_handler("task_complete", self._handle_task_complete)
        self.message_router.register_handler("task_failed", self._handle_task_failed)
        self.message_router.register_handler("task_request", self._handle_task_request)

        self.quic_transport.register_handler("secondary_welcome", self._handle_secondary_welcome)
        self.quic_transport.register_handler("cert_exchange", self._handle_cert_exchange)
        self.quic_transport.register_handler("secondary_error", self._handle_secondary_error)
        self.quic_transport.register_handler("secondary_log", self._handle_secondary_log)
        self.quic_transport.register_handler("peer_connections_ready", self._handle_peer_connections_ready)
        self.quic_transport.register_handler("worker_ready", self._handle_worker_ready)
        self.quic_transport.register_handler("task_complete", self._handle_task_complete)
        self.quic_transport.register_handler("task_failed", self._handle_task_failed)

        logger.info("QUIC transport ready")

    # Abstract methods to be implemented by subclasses

    async def prepare(self, num_secondaries: int, quic_port: int) -> PreparationResult:
        """Prepare for multi-computer execution (mode-specific).

        For SLURM: Build docker image, submit jobs, setup SSH tunnels
        For local: Spawn local processes

        Args:
            num_secondaries: Number of secondaries to spawn
            quic_port: Base port for QUIC connections

        Returns:
            PreparationResult with preparation details
        """
        raise NotImplementedError("Subclasses must implement prepare()")

    async def connect(self, prep_result: PreparationResult) -> ConnectionResult:
        """Wait for secondaries to connect and establish peer connections.

        This is largely mode-independent, but subclasses can override for
        specific connection handling.

        Args:
            prep_result: Result from preparation phase

        Returns:
            ConnectionResult with connection details
        """
        logger.info("Phase 2: Connecting to secondaries")

        # Wait for secondaries to connect
        await self._wait_for_secondaries(prep_result.num_secondaries)

        # Exchange certificates
        await self._exchange_certificates()

        # Wait for peer connections
        await self._wait_for_peer_connections()

        return ConnectionResult(self.secondaries, self.peer_connections_ready)

    async def transfer_files(self, conn_result: ConnectionResult) -> None:
        """Transfer source files to secondaries (mode-specific).

        For SLURM: Transfer files via QUIC
        For local: Skip transfer, files already available

        Args:
            conn_result: Result from connection phase
        """
        raise NotImplementedError("Subclasses must implement transfer_files()")

    def get_file_transfer_mode(self) -> FileTransferMode:
        """Get the file transfer mode for this coordinator."""
        raise NotImplementedError("Subclasses must implement get_file_transfer_mode()")

    # Common coordination methods

    async def _wait_for_secondaries(self, num_secondaries: int) -> None:
        """Wait for all secondaries to connect"""
        logger.info(f"Waiting for {num_secondaries} secondaries to connect...")

        timeout = 600  # 10 minutes
        start_time = asyncio.get_event_loop().time()

        while len(self.secondaries) < num_secondaries:
            if asyncio.get_event_loop().time() - start_time > timeout:
                logger.error(f"Timeout waiting for secondaries. Connected: {len(self.secondaries)}/{num_secondaries}")
                raise TimeoutError("Failed to connect to all secondaries")

            await asyncio.sleep(1)

        logger.info(f"All {num_secondaries} secondaries connected")

    async def _handle_secondary_welcome(self, message: dict[str, Any], connection: Any) -> None:
        """Handle secondary welcome message"""
        secondary_id = message.get("secondary_id")
        num_workers = message.get("num_workers", 0)
        ram_bytes = message.get("ram_bytes", 0)
        quic_port = message.get("quic_port", 0)
        quic_cert = message.get("quic_cert")

        logger.info(f"Secondary {secondary_id} connected: {num_workers} workers, {ram_bytes / (1024**3):.2f}GB RAM")

        self.secondaries[secondary_id] = {
            "num_workers": num_workers,
            "ram_bytes": ram_bytes,
            "quic_port": quic_port,
            "quic_cert": quic_cert,
            "ready": False,
        }

    async def _handle_cert_exchange(self, message: dict[str, Any], connection: Any) -> None:
        """Handle certificate exchange message"""
        secondary_id = message.get("secondary_id")
        quic_cert = message.get("public_cert_pem")
        ipv4 = message.get("ipv4_address")
        ipv6 = message.get("ipv6_address")
        quic_port = message.get("quic_port")

        if secondary_id in self.secondaries:
            self.secondaries[secondary_id]["quic_cert"] = quic_cert
            self.secondaries[secondary_id]["ipv4"] = ipv4
            self.secondaries[secondary_id]["ipv6"] = ipv6
            if quic_port:
                self.secondaries[secondary_id]["quic_port"] = quic_port
            logger.debug(f"Updated certificate for {secondary_id}")

    async def _handle_secondary_error(self, message: dict[str, Any], connection: Any) -> None:
        """Handle secondary error message"""
        secondary_id = message.get("secondary_id", "unknown")
        error = message.get("error", "Unknown error")
        logger.error(f"Secondary {secondary_id} error: {error}")

    async def _handle_secondary_log(self, message: dict[str, Any], connection: Any) -> None:
        """Handle secondary log message"""
        secondary_id = message.get("secondary_id", "unknown")
        level = message.get("level", "INFO")
        log_message = message.get("message", "")

        # Forward log with secondary ID prefix
        log_func = getattr(logger, level.lower(), logger.info)
        log_func(f"[{secondary_id}] {log_message}")

    async def _exchange_certificates(self) -> None:
        """Exchange certificates with all secondaries for peer connections"""
        logger.info("Phase 3: Exchanging certificates for peer connections")

        # Build peer info list
        self.peer_info = []
        for secondary_id, info in self.secondaries.items():
            self.peer_info.append(
                {
                    "peer_id": secondary_id,
                    "cert_pem": info["quic_cert"],
                    "port": info["quic_port"],
                    "ipv4": info.get("ipv4"),
                    "ipv6": info.get("ipv6"),
                    "cert_fingerprint": "",
                }
            )

        # Send peer list to all secondaries
        for secondary_id, info in self.secondaries.items():
            peer_list_msg = {
                "type": "peer_list",
                "peers": self.peer_info,
                "primary_entropy": self.primary_entropy.hex(),
            }
            await self._send_to_secondary(secondary_id, peer_list_msg)

        logger.info("Certificate exchange complete")

    async def _wait_for_peer_connections(self) -> None:
        """Wait for all secondaries to complete peer connections"""
        logger.info("Phase 3.5: Waiting for peer connections")

        timeout = 300  # 5 minutes
        start_time = asyncio.get_event_loop().time()
        num_secondaries = len(self.secondaries)

        while len(self.peer_connections_ready) < num_secondaries:
            if asyncio.get_event_loop().time() - start_time > timeout:
                logger.error(
                    f"Timeout waiting for peer connections. Ready: {len(self.peer_connections_ready)}/{num_secondaries}"
                )
                raise TimeoutError("Failed to establish all peer connections")

            await asyncio.sleep(1)

        logger.info(f"All {num_secondaries} secondaries completed peer connections")

    async def _handle_peer_connections_ready(self, message: dict[str, Any], connection: Any) -> None:
        """Handle peer connections ready message"""
        secondary_id = message.get("secondary_id")
        self.peer_connections_ready.add(secondary_id)
        logger.info(f"Secondary {secondary_id} completed peer connections")

    async def _initial_assignment_phase(self, conn_result: ConnectionResult) -> None:
        """Phase 3: Initial assignment and worker readiness"""
        logger.info("Phase 3: Initial assignment")

        # Wait for workers to be ready
        await self._wait_for_workers()

        # Preliminary assignment
        await self._preliminary_assignment()

        # Source discovery (if needed)
        if self.get_file_transfer_mode() == FileTransferMode.FULL_TRANSFER:
            await self._source_discovery()

    async def _wait_for_workers(self) -> None:
        """Wait for all workers to be ready"""
        logger.info("Phase 4: Waiting for workers")

        # Send discover-sources to first secondary (for file transfer modes)
        if self.get_file_transfer_mode() == FileTransferMode.FULL_TRANSFER and self.secondaries:
            first_secondary_id = next(iter(self.secondaries.keys()))
            first_secondary = self.secondaries[first_secondary_id]

            discover_msg = {"type": "discover_sources"}
            await self._send_to_secondary(first_secondary_id, discover_msg)
            logger.info(f"Sent discover-sources to {first_secondary_id}")

        timeout = 300  # 5 minutes
        start_time = asyncio.get_event_loop().time()

        # Calculate expected workers
        expected_workers = sum(info["num_workers"] for info in self.secondaries.values())

        while True:
            ready_workers = sum(len(workers) for workers in self.remote_workers.values())

            if ready_workers >= expected_workers:
                break

            if asyncio.get_event_loop().time() - start_time > timeout:
                logger.error(f"Timeout waiting for workers. Ready: {ready_workers}/{expected_workers}")
                raise TimeoutError("Failed to start all workers")

            await asyncio.sleep(1)

        logger.info(f"All {expected_workers} workers ready")

    async def _handle_worker_ready(self, message: dict[str, Any], connection: Any) -> None:
        """Handle worker ready message"""
        secondary_id = message.get("secondary_id")
        worker_id = message.get("worker_id")
        ram_bytes = message.get("ram_bytes", 0)

        logger.info(f"Worker {worker_id} ready on {secondary_id}: {ram_bytes / (1024**3):.2f}GB RAM")

        # Create remote worker
        remote_worker = RemoteWorker(
            worker_id=worker_id,
            memory_budget=ram_bytes,
            secondary_id=secondary_id,
            message_router=self.message_router,
        )

        # Start the remote worker (marks it as ready)
        remote_worker.start()

        if secondary_id not in self.remote_workers:
            self.remote_workers[secondary_id] = []

        self.remote_workers[secondary_id].append(remote_worker)

    async def _preliminary_assignment(self) -> None:
        """Phase 5: Preliminary task assignment

        This phase assigns initial tasks to workers on each secondary.
        Assignment is done using the AuthoritiveManager for memory-aware scheduling.
        """
        logger.info("Phase 5: Preliminary assignment")

        if len(self.secondaries) == 0:
            logger.warning("No secondaries connected")
            return

        if len(self.binaries) == 0:
            logger.warning("No binaries to assign")
            return

        # Create WorkerManager for each secondary and perform initial assignment
        for secondary_id, info in self.secondaries.items():
            workers = self.remote_workers.get(secondary_id, [])
            logger.debug(f"Creating manager for {secondary_id} with {len(workers)} workers")
            for w in workers:
                logger.debug(f"  Worker {w.worker_id}: ready={w.ready}")

            # Create log directory for this secondary
            log_dir = Path.cwd() / "run" / self.run_id / "logs" / secondary_id
            log_dir.mkdir(parents=True, exist_ok=True)

            manager = AuthoritiveManager(
                num_workers=info["num_workers"],
                max_memory=info["ram_bytes"],
                log_dir=log_dir,
                task_definition=self.task_definition,
                workers=workers,
            )

            self.worker_managers[secondary_id] = manager

            # Initialize workers (this calls _create_workers() and sets up budgets)
            manager._initialize_workers()

            # Manually perform initial assignment by setting up pending binaries
            # and running initial assignment phase
            logger.info(f"Performing initial assignment for {secondary_id} ({info['num_workers']} workers)")
            logger.debug(f"  Manager has {len(manager.workers)} workers in its list")
            logger.debug(f"  Available binaries: {len(self.binaries)}")

            # Copy binaries to manager's pending list
            manager.pending_binaries = list(self.binaries)

            # Sort by size descending for better packing
            manager.pending_binaries.sort(key=lambda b: b.size, reverse=True)

            # Perform initial assignment to all workers
            assigned_count = 0
            for worker in manager.workers:
                logger.debug(
                    f"  Worker {worker.worker_id}: ready={worker.ready}, has_initial={worker.has_received_initial_assignment}"
                )
                if worker.ready:
                    success = manager._assign_binary_to_worker_initial_phase(worker)
                    logger.debug(
                        f"    Assignment success={success}, current_binary={worker.current_binary is not None}"
                    )
                    if success and worker.current_binary:
                        task_hash = self._compute_task_hash(worker.current_binary)
                        self.task_assignments[task_hash] = secondary_id
                        assigned_count += 1

            logger.info(f"  {secondary_id}: {assigned_count} initial tasks assigned to {info['num_workers']} workers")

        total_assigned = len(self.task_assignments)
        logger.info(f"Total initial assignments: {total_assigned}/{len(self.binaries)} tasks")
        logger.info(f"Created {len(self.worker_managers)} worker managers")

    async def _source_discovery(self) -> None:
        """Phase 6: Source discovery from first secondary"""
        logger.info("Phase 6: Source discovery")

        # Wait for source discovery to complete
        timeout = 300  # 5 minutes
        start_time = asyncio.get_event_loop().time()

        while len(self.discovered_binaries) < len(self.binaries):
            if asyncio.get_event_loop().time() - start_time > timeout:
                logger.error(
                    f"Timeout waiting for source discovery. Discovered: {len(self.discovered_binaries)}/{len(self.binaries)}"
                )
                raise TimeoutError("Failed to discover all sources")

            await asyncio.sleep(1)

        logger.info(f"Discovered {len(self.discovered_binaries)} binaries")

    async def _handle_source_discovered(self, message: dict[str, Any], connection: Any) -> None:
        """Handle source discovered message"""
        file_hash = message.get("hash")
        zip_name = message.get("zip_name")
        binary_info_dict = message.get("binary_info")

        # Reconstruct BinaryInfo
        binary_info = BinaryInfo.from_dict(binary_info_dict)

        self.discovered_binaries[file_hash] = {
            "zip_name": zip_name,
            "local_path": binary_info.path,
            "binary_info": binary_info,
        }

        logger.debug(f"Discovered: {zip_name}")

    async def _notify_transfer_complete(self) -> None:
        """Phase 8: Notify all secondaries that transfer is complete"""
        logger.info("Phase 8: Notifying transfer complete")

        for secondary_id, info in self.secondaries.items():
            transfer_msg = {"type": "transfer_complete"}
            await self._send_to_secondary(secondary_id, transfer_msg)

        self.transfer_complete = True
        logger.info("All secondaries notified of transfer completion")

    async def _promote_primary(self) -> None:
        """Phase 9: Promote one secondary to SLURM-primary role"""
        logger.info("Phase 9: Promoting SLURM-primary")

        # Pick first secondary as SLURM-primary
        if self.secondaries:
            self.slurm_primary_id = next(iter(self.secondaries.keys()))
            logger.info(f"Promoting {self.slurm_primary_id} to SLURM-primary")

            promote_msg = {"type": "promote_primary"}
            await self._send_to_secondary(self.slurm_primary_id, promote_msg)
        else:
            logger.warning("No secondaries to promote")

    async def _send_full_task_list(self) -> None:
        """Phase 10: Send full task list to SLURM-primary"""
        logger.info("Phase 10: Sending full task list")

        if not self.slurm_primary_id:
            logger.warning("No SLURM-primary to send task list to")
            return

        # Convert binaries to task list
        tasks = []
        for binary in self.binaries:
            task_hash = self._compute_task_hash(binary)
            tasks.append({"hash": task_hash, "binary_info": binary.to_dict()})

        task_list_msg = {
            "type": "full_task_list",
            "all_tasks": tasks,
            "completed_tasks": list(self.completed_tasks),
        }
        await self._send_to_secondary(self.slurm_primary_id, task_list_msg)

        logger.info(f"Sent {len(tasks)} tasks to {self.slurm_primary_id}")

    async def _monitor_mode(self) -> None:
        """Phase 11: Monitor until user disconnects"""
        logger.info("Phase 11: Monitor mode")
        logger.info("Press Ctrl+C to disconnect")

        try:
            while self.running:
                await asyncio.sleep(10)

                # Log progress
                completed = len(self.completed_tasks)
                failed = len(self.failed_tasks)
                total = len(self.binaries)
                logger.info(f"Progress: {completed}/{total} completed, {failed} failed")

        except KeyboardInterrupt:
            logger.info("User requested disconnect")

    async def _cleanup(self) -> None:
        """Cleanup resources"""
        logger.info("Cleaning up...")

        self.running = False

        # Close QUIC transport
        if self.quic_transport:
            await self.quic_transport.stop()

        logger.info("Cleanup complete")

    async def _send_to_secondary(self, secondary_id: str, message: dict[str, Any]) -> None:
        """Send message to secondary via WebSocket.

        Args:
            secondary_id: ID of secondary to send to
            message: Message dict to send
        """
        if secondary_id not in self.secondaries:
            logger.error(f"Unknown secondary: {secondary_id}")
            return

        # Get WebSocket connection from QUIC transport's wss_connections
        if secondary_id not in self.quic_transport.wss_connections:
            logger.error(f"No WebSocket connection for secondary: {secondary_id}")
            return

        connection = self.quic_transport.wss_connections[secondary_id]

        # Connection is a WebSocket connection
        try:
            await connection.send(json.dumps(message))
            logger.debug(f"Sent {message.get('type')} to {secondary_id}")
        except Exception as e:
            logger.error(f"Failed to send message to {secondary_id}: {e}")

    def _compute_task_hash(self, binary: BinaryInfo) -> str:
        """Compute unique hash for a task"""
        hash_input = f"{binary.path}:{binary.binary_name}:{binary.platform}:{binary.compiler}:{binary.version}:{binary.opt_level}"
        return hashlib.sha256(hash_input.encode()).hexdigest()[:16]

    def _handle_task_complete(self, message: dict[str, Any], connection: Any) -> None:
        """Handle task complete message"""
        task_hash = message.get("task_hash")
        secondary_id = message.get("secondary_id")
        worker_id = message.get("worker_id")

        logger.debug(f"Task complete: hash={task_hash}, secondary={secondary_id}, worker={worker_id}")

        if task_hash:
            self.completed_tasks.add(task_hash)
            logger.info(f"Task {task_hash} completed by {secondary_id} worker {worker_id}")
        else:
            logger.warning(f"Task complete message missing task_hash: {message}")

    async def _handle_task_failed(self, message: dict[str, Any], connection: Any) -> None:
        """Handle task failed message"""
        task_hash = message.get("task_hash")
        self.failed_tasks.add(task_hash)

    async def _handle_task_request(self, message: dict[str, Any], connection: Any) -> None:
        """Handle task request message"""
        # Task assignment is handled by the promoted primary in distributed mode
        pass
