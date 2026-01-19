import asyncio
import hashlib
import logging
import socket
import subprocess
import sys
import time
import zipfile
from pathlib import Path
from typing import Any
from urllib.parse import urlparse

from shared.binary_info import BinaryIdentifier

from ..binary_info import BinaryInfo
from ..models import WorkerState
from ..task import TaskDefinition
from ..worker_manager.submissive import SubmissiveManager
from .message_router import MessageRouter
from .quic_transport import QuicPeerInfo, QuicTransport

logger = logging.getLogger(__name__)


class PrimaryLogHandler(logging.Handler):
    """Custom logging handler that sends warnings and errors to primary"""

    def __init__(self, secondary_mode: "SecondaryMode"):
        super().__init__()
        self.secondary_mode = secondary_mode
        self.setLevel(logging.WARNING)
        self._in_emit = False  # Prevent recursive logging

    def emit(self, record: logging.LogRecord) -> None:
        """Send log record to primary if it's a warning or error"""
        # Prevent recursive calls if sending to primary fails and generates another log
        if self._in_emit:
            return

        try:
            self._in_emit = True
            if record.levelno >= logging.WARNING and self.secondary_mode.message_router.primary_connection:
                # Don't send logs about message router failures to avoid infinite recursion
                if record.module == "message_router" or "send_to_primary" in record.message:
                    return

                msg = {
                    "type": "secondary_log",
                    "secondary_id": self.secondary_mode.secondary_id,
                    "level": record.levelname,
                    "message": self.format(record),
                    "module": record.module,
                    "funcName": record.funcName,
                    "lineno": record.lineno,
                }
                asyncio.create_task(self.secondary_mode.message_router.send_to_primary(msg))
        except Exception:
            pass  # Don't let logging errors crash the application
        finally:
            self._in_emit = False


class SecondaryMode:
    """Handles secondary node execution in SLURM distributed mode"""

    def __init__(
        self,
        primary_url: str,
        secondary_id: str,
        num_workers: int,
        ram_bytes: int,
        src_tmp: Path,
        out_tmp: Path,
        log_tmp: Path,
        src_network: Path,
        out_network: Path,
        log_network: Path,
        socket_dir: Path,
        task_definition: TaskDefinition,
        task_args: Any,
        skip_existing: bool = False,
    ):
        self.primary_url = primary_url
        self.secondary_id = secondary_id
        self.num_workers = num_workers
        self.ram_bytes = ram_bytes

        self.src_tmp = src_tmp
        self.out_tmp = out_tmp
        self.log_tmp = log_tmp
        self.src_network = src_network
        self.out_network = out_network
        self.log_network = log_network
        self.socket_dir = socket_dir
        self.task_definition = task_definition
        self.task_args = task_args
        self.skip_existing = skip_existing
        self.task_definition = task_definition
        self.task_args = task_args
        self.skip_existing = skip_existing

        # Create directories
        self.src_tmp.mkdir(parents=True, exist_ok=True)
        self.out_tmp.mkdir(parents=True, exist_ok=True)
        self.log_tmp.mkdir(parents=True, exist_ok=True)

        self.peers: dict[str, Any] = {}
        self.primary_connection: Any = None
        self.peer_connections: dict[str, Any] = {}

        # SubmissiveManager handles all worker lifecycle and task assignment
        self.worker_manager: SubmissiveManager | None = None
        self.extracted_binaries: dict[str, Path] = {}  # hash -> extracted path

        self.completed_tasks: set[str] = set()
        self.failed_tasks: set[str] = set()
        self.all_tasks: list[Any] = []

        self.is_slurm_primary = False
        self.last_keepalives: dict[str, float] = {}

        self.running = True
        self.setup_complete = False  # Track if initialization is complete
        self.peer_list_received = asyncio.Event()  # Event to signal peer list processing is complete
        self.connection_closing = False  # Prevent sending messages during shutdown

        # Message router for communication
        self.message_router = MessageRouter(secondary_id, "secondary")

        # QUIC transport for peer-to-peer communication
        self.quic_transport = QuicTransport(secondary_id, listen_port=0)  # Let OS pick port

        # Parse primary URL
        parsed = urlparse(primary_url)
        self.primary_host = parsed.hostname or "localhost"
        self.primary_port = parsed.port or 6000

    def run(self) -> None:
        """Main execution loop for secondary mode"""
        logger.info(f"Starting secondary mode: {self.secondary_id}")

        # Add handler to send warnings/errors to primary
        root_logger = logging.getLogger()
        primary_handler = PrimaryLogHandler(self)
        root_logger.addHandler(primary_handler)

        try:
            # Run async secondary mode
            asyncio.run(self._run_async())
        finally:
            # Remove handler on exit
            root_logger.removeHandler(primary_handler)

    async def _run_async(self) -> None:
        """Async main execution loop"""
        try:
            # Connect to primary
            await self._connect_to_primary()

            # Phase 2: Send welcome message
            await self._send_welcome()

            # Phase 3: Certificate exchange
            await self._setup_certificates()

            # Phase 4: Register handlers
            self.message_router.register_handler("peer_list", self._handle_peer_list)
            self.message_router.register_handler("initial_assignment", self._handle_initial_assignment)
            self.message_router.register_handler("task_assignment", self._handle_task_assignment)
            self.message_router.register_handler("discover_sources", self._handle_discover_sources)

            # Phase 5: Connect to peers (wait for peer_list from primary)
            await self._connect_to_peers()

            # Phase 5: Start workers
            await self._start_workers()

            # Mark setup as complete
            self.setup_complete = True
            logger.info("Setup complete, entering main processing loop")

            # Phase 6: Main processing loop
            await self._main_loop()

        except KeyboardInterrupt:
            logger.info("Received interrupt signal")
        except Exception as e:
            logger.error(f"Secondary mode error: {e}", exc_info=True)
            await self._send_error_to_primary(e)
        finally:
            await self._cleanup()

    async def _connect_to_primary(self) -> None:
        """Establish connection to primary via gateway with retry logic (up to 60 seconds total)"""
        logger.info(f"Connecting to primary: {self.primary_host}:{self.primary_port}")
        logger.info("Will retry once per second for up to 60 seconds if primary is not ready yet...")

        timeout = 60.0  # seconds - total time to keep trying
        retry_delay = 1.0  # seconds - delay between retries
        start_time = time.time()
        attempt = 0

        while True:
            attempt += 1
            elapsed = time.time() - start_time

            if elapsed > timeout:
                logger.error(f"Failed to connect to primary after {timeout:.0f} seconds ({attempt} attempts)")
                logger.error("Primary may not have set up SSH tunnel yet, or connection info file was not found")
                raise TimeoutError(
                    f"Could not connect to primary at {self.primary_host}:{self.primary_port} within {timeout:.0f}s"
                )

            try:
                logger.info(f"Connection attempt {attempt} (elapsed: {elapsed:.1f}s)...")
                reader, writer = await asyncio.open_connection(self.primary_host, self.primary_port)

                self.message_router.set_primary_connection(writer)

                logger.info(f"Connected to primary successfully after {elapsed:.1f}s ({attempt} attempts)")

                # Start receive loop in background with connection monitoring
                asyncio.create_task(self._monitor_primary_connection(reader))
                return  # Success!

            except (ConnectionRefusedError, OSError) as e:
                error_type = type(e).__name__
                error_msg = str(e)
                remaining = timeout - elapsed
                if remaining > 0:
                    logger.info(
                        f"Connection failed (attempt {attempt}): {error_type}: {error_msg}. "
                        f"Retrying in {retry_delay}s..."
                    )
                    await asyncio.sleep(retry_delay)
                else:
                    logger.error(f"Failed to connect to primary after {timeout:.0f} seconds: {error_type}: {error_msg}")
                    raise
            except Exception as e:
                error_type = type(e).__name__
                error_msg = str(e)
                logger.error(f"Unexpected error connecting to primary: {error_type}: {error_msg}")
                raise

    async def _send_welcome(self) -> None:
        """Send welcome message with capabilities to primary"""
        logger.info("Sending welcome message to primary...")

        hostname = socket.gethostname()

        msg = {
            "type": "secondary_welcome",
            "secondary_id": self.secondary_id,
            "ram_bytes": self.ram_bytes,
            "worker_count": self.num_workers,
            "hostname": hostname,
        }

        await self.message_router.send_to_primary(msg)
        logger.info("Welcome message sent")

    async def _send_error_to_primary(self, error: Exception) -> None:
        """Send error message with traceback to primary"""
        import traceback

        if self.connection_closing:
            return

        try:
            error_msg = {
                "type": "secondary_error",
                "secondary_id": self.secondary_id,
                "error_type": type(error).__name__,
                "error_message": str(error),
                "traceback": traceback.format_exc(),
            }
            await self.message_router.send_to_primary(error_msg)
            logger.info("Sent error report to primary")
        except Exception as e:
            logger.error(f"Failed to send error to primary: {e}")

    async def _monitor_primary_connection(self, reader: asyncio.StreamReader) -> None:
        """Monitor primary connection and handle disconnect"""
        try:
            await self.message_router.receive_loop(reader, "primary")
        finally:
            # Mark that connection is closing to prevent sending more messages
            self.connection_closing = True

            # Connection closed
            if not self.setup_complete:
                logger.error("Primary connection closed before setup was complete!")
                logger.error("Aborting secondary - setup incomplete")
                self.running = False
                # Don't try to send error to primary - connection is already closed
                # Exit the process
                import sys

                sys.exit(1)
            else:
                logger.warning("Primary connection closed after setup was complete")
                self.running = False

    async def _setup_certificates(self) -> None:
        """Generate certificates and exchange with primary"""
        logger.info("Setting up QUIC certificates")

        # Generate certificates
        await self.quic_transport.generate_certificates()

        # Start QUIC server to accept peer connections (must start before sending cert to get actual port)
        await self.quic_transport.start_server()

        # Get local IP addresses
        ipv4, ipv6 = self.quic_transport.get_local_addresses()

        # Get public certificate
        cert_pem = self.quic_transport.get_public_cert().decode("utf-8")

        # Send certificate exchange message to primary
        msg = {
            "type": "cert_exchange",
            "secondary_id": self.secondary_id,
            "public_cert_pem": cert_pem,
            "ipv4_address": ipv4,
            "ipv6_address": ipv6,
            "quic_port": self.quic_transport.listen_port,
        }

        await self.message_router.send_to_primary(msg)
        logger.info(f"Sent certificate exchange: {ipv4}:{self.quic_transport.listen_port}")

    async def _handle_peer_list(self, message: dict[str, Any], sender_id: str | None) -> None:
        """Handle peer_list message from primary"""
        peers = message.get("peers", [])
        logger.info(f"Received peer list with {len(peers)} peers from primary")

        # Add all peers to QUIC transport
        for peer_info in peers:
            peer_id = peer_info.get("peer_id")
            if peer_id == self.secondary_id:
                # Skip self
                continue

            quic_peer = QuicPeerInfo(
                peer_id=peer_id,
                ipv4=peer_info.get("ipv4"),
                ipv6=peer_info.get("ipv6"),
                port=peer_info.get("port"),
                cert_pem=peer_info.get("cert_pem", ""),
                cert_fingerprint=peer_info.get("cert_fingerprint", ""),
            )
            self.quic_transport.add_peer(quic_peer)

        logger.info(f"Added {len(self.quic_transport.peers)} peers, connecting...")

        # Connect to all peers (primary controls the peer list, so we know this is complete)
        await self.quic_transport.connect_to_peers()

        logger.info(f"Peer connections established: {len(self.quic_transport.connections)} peers connected")

        # Signal that peer list has been processed
        self.peer_list_received.set()

    async def _connect_to_peers(self) -> None:
        """Wait for peer list from primary and establish connections"""
        logger.info("Waiting for peer list from primary...")

        # Wait for the peer_list message to be received and processed
        # The primary controls when this happens by sending the peer_list message
        await self.peer_list_received.wait()

        logger.info("Peer list received and connections established, continuing...")

    async def _start_workers(self) -> None:
        """Start worker processes using SubmissiveManager"""
        logger.info(f"Starting {self.num_workers} workers via SubmissiveManager")

        # Create SubmissiveManager to handle all worker lifecycle
        self.worker_manager = SubmissiveManager(
            num_workers=self.num_workers,
            max_memory=self.ram_bytes,
            source_dir=self.src_tmp,
            output_dir=self.out_tmp,
            task_definition=self.task_definition,
            task_args=self.task_args,
            skip_existing=self.skip_existing,
            request_task_callback=self._request_task_callback,
            manual_start_worker=False,
            connection_mode="named",
            socket_dir=self.socket_dir,
        )

        # Initialize workers (this starts the processes and waits for ready)
        self.worker_manager._initialize_workers()

        # Send worker ready messages to primary for each worker
        for worker in self.worker_manager.workers:
            worker_id = worker.worker_id
            memory_budget = worker.memory_budget
            await self._send_worker_ready(worker_id, memory_budget)

        logger.info(f"All {len(self.worker_manager.workers)} workers initialized and reported to primary")

    def _request_task_callback(self, worker_id: int) -> None:
        """Callback for SubmissiveManager to request tasks from primary.

        This is called synchronously, so we need to schedule the async work.
        """
        asyncio.create_task(self._request_new_task(worker_id))

    async def _send_worker_ready(self, worker_id: int, memory_budget: int) -> None:
        """Send worker ready message to primary"""
        if self.connection_closing or not self.message_router.primary_connection:
            logger.warning(f"Cannot send worker_ready for worker {worker_id}: not connected to primary")
            return

        msg = {
            "type": "worker_ready",
            "secondary_id": self.secondary_id,
            "worker_id": worker_id,
            "memory_budget": memory_budget,
        }
        try:
            await self.message_router.send_to_primary(msg)
        except Exception as e:
            logger.warning(f"Failed to send worker_ready for worker {worker_id}: {e}")

    async def _main_loop(self) -> None:
        """Main processing loop with keepalive"""
        logger.info("Entering main processing loop...")

        last_keepalive = time.time()
        keepalive_interval = 1.0  # 1 second

        while self.running:
            current_time = time.time()

            # Send keepalive
            if current_time - last_keepalive >= keepalive_interval:
                await self._send_keepalive()
                last_keepalive = current_time

            # Check for timeouts
            self._check_peer_timeouts()

            # Process worker completions
            self._process_worker_updates()

            # Sleep briefly
            await asyncio.sleep(0.1)

    async def _send_keepalive(self) -> None:
        """Send keepalive to all peers"""
        active_count = 0
        if self.worker_manager:
            active_count = len([w for w in self.worker_manager.workers if w.current_binary is not None])

        msg = {
            "type": "keepalive",
            "secondary_id": self.secondary_id,
            "active_workers": active_count,
        }

        # TODO: Broadcast to all peers (secondaries)
        # For now, skip if no peer connections
        if len(self.message_router.secondary_connections) > 0:
            await self.message_router.broadcast_to_secondaries(msg)
        else:
            logger.debug("No peer connections yet, skipping keepalive broadcast")

    def _check_peer_timeouts(self) -> None:
        """Check for peer timeouts"""
        current_time = time.time()
        timeout_threshold = 120.0  # 2 minutes

        for peer_id, last_seen in self.last_keepalives.items():
            if current_time - last_seen > timeout_threshold:
                logger.warning(f"Timeout detected for peer: {peer_id}")
                self._handle_timeout(peer_id)

    def _handle_timeout(self, peer_id: str) -> None:
        """Handle detected peer timeout"""
        # TODO: Query other peers for last keepalive
        # TODO: Mark peer as dead if consensus reached
        pass

    def _process_worker_updates(self) -> None:
        """Process worker completion and status updates using WorkerManager"""
        if not self.worker_manager:
            return

        # WorkerManager handles worker polling internally
        # We just need to check for completed tasks and request new ones from primary
        # Note: In SLURM mode, we don't auto-reassign - we ask primary for tasks
        pass

    def _process_messages(self) -> None:
        """Process incoming messages from primary and peers"""
        # TODO: Read messages from QUIC connections
        # TODO: Dispatch to appropriate handlers
        pass

    async def _cleanup(self) -> None:
        """Clean up resources"""
        logger.info("Cleaning up secondary resources...")
        # TODO: Stop workers
        # Stop message router
        self.message_router.stop()
        # TODO: Save state

    def _execute_host_command(self, command: str) -> tuple[int, str, str]:
        """Execute command on host via socket"""
        # TODO: Send command through Unix socket to host wrapper
        # TODO: Wait for result
        return 0, "", ""

    def _move_completed_files(self, worker_id: int, task_hash: str) -> None:
        """Move completed files from tmp to network storage"""
        task_info = self.worker_tasks.get(worker_id)
        if not task_info:
            logger.warning(f"No task info for worker {worker_id}")
            return

        binary_info = task_info.get("binary_info")
        if not binary_info:
            return

        # Move output files from out_tmp to out_network
        # Output files are named based on the binary
        output_pattern = f"{binary_info.path.stem}*"
        for output_file in self.out_tmp.glob(output_pattern):
            target = self.out_network / output_file.name
            try:
                output_file.rename(target)
                logger.debug(f"Moved output: {output_file.name} -> {target}")
            except Exception as e:
                logger.error(f"Failed to move output file {output_file}: {e}")

    def _rotate_worker_log(self, worker_id: int, force: bool = False) -> None:
        """Rotate worker log file if needed (at least 1 minute elapsed or forced)"""
        current_time = time.time()
        last_move = self.worker_last_log_move.get(worker_id, 0)

        if not force and (current_time - last_move) < 60:
            # Less than 1 minute elapsed, don't rotate
            return

        current_increment = self.worker_log_increments.get(worker_id, 0)
        old_log = self._get_worker_log_path(worker_id, current_increment)

        if old_log.exists():
            # Move to network storage
            target = self.log_network / old_log.name
            try:
                old_log.rename(target)
                logger.debug(f"Rotated log: {old_log.name} -> {target}")
            except Exception as e:
                logger.error(f"Failed to rotate log {old_log}: {e}")
                return

        # Increment and update worker with new log path
        new_increment = current_increment + 1
        self.worker_log_increments[worker_id] = new_increment
        self.worker_last_log_move[worker_id] = current_time

        new_log = self._get_worker_log_path(worker_id, new_increment)

        # Send command to worker to switch log file
        if worker_id < len(self.workers):
            try:
                # TODO: Implement log switching via comm interface
                # For now, just log the intention
                logger.debug(f"Would switch worker {worker_id} to new log file: {new_log}")
            except Exception as e:
                logger.warning(f"Failed to send log switch command to worker {worker_id}: {e}")

    def _extract_binary_from_zip(self, zip_name: str, local_path: str, file_hash: str) -> Path | None:
        """Extract a binary from a ZIP file in src_network to src_tmp"""
        # Check if already extracted
        if file_hash in self.extracted_binaries:
            return self.extracted_binaries[file_hash]

        zip_path = self.src_network / zip_name
        if not zip_path.exists():
            logger.error(f"ZIP file not found: {zip_path}")
            return None

        try:
            with zipfile.ZipFile(zip_path, "r") as zf:
                # Extract specific file
                extracted_path = self.src_tmp / Path(local_path).name

                # Read from ZIP and write to target
                with zf.open(local_path) as source:
                    with open(extracted_path, "wb") as target:
                        target.write(source.read())

                # Verify hash
                computed_hash = self._compute_file_hash(extracted_path)
                if computed_hash != file_hash:
                    logger.error(f"Hash mismatch for {local_path}: expected {file_hash}, got {computed_hash}")
                    extracted_path.unlink()
                    return None

                # Cache the extraction
                self.extracted_binaries[file_hash] = extracted_path
                logger.debug(f"Extracted {local_path} from {zip_name} to {extracted_path}")
                return extracted_path

        except Exception as e:
            logger.error(f"Failed to extract {local_path} from {zip_name}: {e}")
            return None

    def _compute_file_hash(self, path: Path) -> str:
        """Compute SHA256 hash of a file"""
        sha256 = hashlib.sha256()
        with open(path, "rb") as f:
            for chunk in iter(lambda: f.read(8192), b""):
                sha256.update(chunk)
        return sha256.hexdigest()

    async def _handle_initial_assignment(self, message: dict[str, Any], sender_id: str | None) -> None:
        """Handle initial assignment message from primary"""
        logger.info("Received initial assignment from primary")

        zip_files = message.get("zip_files", [])

        # Extract all binaries from ZIPs
        for zip_info in zip_files:
            zip_name = zip_info.get("zip_name")
            binaries = zip_info.get("binaries", [])

            for binary_entry in binaries:
                local_path = binary_entry.get("local_path")
                file_hash = binary_entry.get("hash")

                # Extract the binary
                extracted_path = self._extract_binary_from_zip(zip_name, local_path, file_hash)
                if not extracted_path:
                    logger.error(f"Failed to extract {local_path} from {zip_name}")
                    continue

                # Store task info (will assign to workers later)
                # For now, just log that we have it ready
                logger.debug(f"Ready to assign: {extracted_path.name}")

        logger.info(f"Extracted {len(self.extracted_binaries)} binaries from {len(zip_files)} ZIP files")

    async def _handle_task_assignment(self, message: dict[str, Any], sender_id: str | None) -> None:
        """Handle task assignment message from primary"""
        worker_id = message.get("worker_id")
        zip_file = message.get("zip_file")
        local_path = message.get("local_path")
        file_hash = message.get("file_hash")
        binary_info_dict = message.get("binary_info")
        estimated_memory = message.get("estimated_memory", 0)

        if not isinstance(worker_id, int) or worker_id >= len(self.worker_manager.workers):
            logger.error(f"Invalid worker_id {worker_id}")
            return

        # Extract binary if from ZIP
        if zip_file and local_path and file_hash:
            extracted_path = self._extract_binary_from_zip(zip_file, local_path, file_hash)
            if not extracted_path:
                logger.error(f"Failed to extract binary for worker {worker_id}")
                return
        elif file_hash:
            # Already extracted
            extracted_path = self.extracted_binaries.get(file_hash)
            if not extracted_path:
                logger.error(f"Binary not found for hash {file_hash}")
                return
        else:
            logger.error(f"Missing file information for worker {worker_id}")
            return

        # Create BinaryInfo from dict
        try:
            identifier = BinaryIdentifier(
                binary_name=binary_info_dict.get("binary_name", ""),
                platform=binary_info_dict.get("platform", ""),
                compiler=binary_info_dict.get("compiler", ""),
                version=binary_info_dict.get("version", ""),
                opt_level=binary_info_dict.get("opt_level", ""),
            )
            binary_info = BinaryInfo(
                path=extracted_path,
                size=binary_info_dict.get("size", extracted_path.stat().st_size),
                identifier=identifier,
            )
        except Exception as e:
            logger.error(f"Failed to create BinaryInfo: {e}")
            return

        # Assign to worker via SubmissiveManager
        success = self.worker_manager.assign_task_from_primary(worker_id, binary_info, estimated_memory)
        if success:
            logger.info(f"Assigned task to worker {worker_id}: {extracted_path.name}")
        else:
            logger.error(f"Failed to assign task to worker {worker_id}")

    async def _notify_task_complete(self, worker_id: int, task_hash: str) -> None:
        """Notify all peers that a task completed"""
        msg = {
            "type": "task_complete",
            "secondary_id": self.secondary_id,
            "worker_id": worker_id,
            "task_hash": task_hash,
            "warnings": 0,
            "filtered": 0,
        }

        # Broadcast to all peers
        try:
            if len(self.message_router.secondary_connections) > 0:
                await self.message_router.broadcast_to_secondaries(msg)
        except Exception as e:
            logger.warning(f"Failed to notify task complete: {e}")

        logger.info(f"Task completed: worker {worker_id}, hash {task_hash}")

    async def _request_new_task(self, worker_id: int) -> None:
        """Request a new task from primary or SLURM-primary"""
        if self.connection_closing or not self.message_router.primary_connection:
            logger.debug(f"Cannot request task for worker {worker_id}: not connected to primary")
            return

        if worker_id >= len(self.worker_manager.workers):
            logger.error(f"Invalid worker_id {worker_id}")
            return

        worker = self.worker_manager.workers[worker_id]

        msg = {
            "type": "task_request",
            "secondary_id": self.secondary_id,
            "worker_id": worker_id,
            "available_memory": worker.memory_budget,
        }

        # Send to primary (or SLURM-primary if promoted)
        try:
            await self.message_router.send_to_primary(msg)
            logger.debug(f"Requested new task for worker {worker_id}")
        except Exception as e:
            logger.warning(f"Failed to request new task for worker {worker_id}: {e}")

    async def _handle_discover_sources(self, message: dict[str, Any], sender_id: str | None) -> None:
        """Handle source discovery request from primary"""
        logger.info("Starting source discovery in /app/src-network")

        discovered_count = 0

        try:
            # Scan src_network directory for ZIP files
            for zip_path in self.src_network.glob("*.zip"):
                # Check for corresponding .hash file
                hash_file = zip_path.with_suffix(".zip.hash")
                if not hash_file.exists():
                    logger.debug(f"Skipping {zip_path.name} (no .hash file)")
                    continue

                # Read expected hash
                try:
                    with open(hash_file, "r") as f:
                        expected_hash = f.read().strip()
                except Exception as e:
                    logger.warning(f"Failed to read hash file {hash_file}: {e}")
                    continue

                # Verify ZIP hash
                actual_hash = self._compute_file_hash(zip_path)
                if actual_hash != expected_hash:
                    logger.warning(f"Hash mismatch for {zip_path.name}: expected {expected_hash}, got {actual_hash}")
                    continue

                # Open ZIP and report contents
                try:
                    with zipfile.ZipFile(zip_path, "r") as zf:
                        for info in zf.infolist():
                            if info.is_dir():
                                continue

                            # Extract file content to compute hash
                            with zf.open(info.filename) as f:
                                file_data = f.read()
                                file_hash = hashlib.sha256(file_data).hexdigest()

                            # Report to primary
                            msg = {
                                "type": "source_discovered",
                                "zip_name": zip_path.name,
                                "local_path": info.filename,
                                "hash": file_hash,
                                "binary_info": {
                                    "size": info.file_size,
                                    "path": info.filename,
                                },
                            }

                            await self.message_router.send_to_primary(msg)
                            discovered_count += 1
                            logger.debug(f"Discovered: {info.filename} in {zip_path.name}")

                except Exception as e:
                    logger.error(f"Failed to process ZIP {zip_path.name}: {e}")

            logger.info(f"Source discovery complete: reported {discovered_count} binaries")

        except Exception as e:
            logger.error(f"Source discovery failed: {e}")
