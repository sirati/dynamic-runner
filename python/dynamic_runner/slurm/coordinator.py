import asyncio
import hashlib
import logging
import secrets
import time
import zipfile
from pathlib import Path
from typing import Any

from ..binary_info import BinaryInfo
from ..task import TaskDefinition
from ..worker.remote_worker import RemoteWorker
from ..worker_manager import WorkerManager
from ..worker_manager.authoritive import AuthoritiveManager
from .message_router import MessageRouter
from .quic_transport import QuicTransport

logger = logging.getLogger(__name__)


class PrimaryCoordinator:
    """Coordinates primary orchestration in SLURM distributed mode"""

    def __init__(
        self,
        binaries: list[BinaryInfo],
        slurm_config: Any,
        job_manager: Any,
        gateway: Any,
        task_definition: TaskDefinition,
        task_args: Any,
        use_reverse_connection: bool = False,
        run_id: str = "default",
        source_dir: Path | None = None,
    ):
        self.binaries = binaries
        self.slurm_config = slurm_config
        self.job_manager = job_manager
        self.gateway = gateway
        self.task_definition = task_definition
        self.task_args = task_args
        self.use_reverse_connection = use_reverse_connection
        self.run_id = run_id
        self.source_dir = source_dir

        # Create run-specific log directory
        base_log_dir = self.slurm_config.get_log_dir()
        self.run_log_dir = f"{base_log_dir}/{run_id}"

        # Certificate persistence directory (local, in run-specific directory)
        self.cert_dir = Path.cwd() / "run" / run_id / "certificates"

        self.secondaries: dict[str, dict[str, Any]] = {}
        self.secondary_port_map: dict[str, int] = {}  # Map secondary_id to allocated port
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
        self.primary_quic_port = 5000

        # Track active connections (stores StreamWriter, Server, and control paths)
        self.active_connections: dict[str, Any] = {}

    def run(self, num_secondaries: int, quic_port: int = 5000) -> None:
        """Main execution loop for primary coordinator

        Args:
            num_secondaries: Number of SLURM secondaries to spawn
            quic_port: Base port for QUIC connections
        """
        logger.info("=" * 60)
        logger.info("PRIMARY COORDINATOR")
        logger.info("=" * 60)
        logger.info(f"Total binaries to process: {len(self.binaries)}")
        logger.info(f"Spawning {num_secondaries} SLURM secondaries")
        logger.info("")

        # Run async coordinator
        asyncio.run(self._run_async(num_secondaries, quic_port))

    async def _run_async(self, num_secondaries: int, quic_port: int) -> None:
        """Async main execution loop"""
        try:
            # Phase 0: Setup QUIC transport and generate certificates
            await self._setup_quic_transport()

            # Phase 1: Submit SLURM jobs
            self._submit_slurm_jobs(num_secondaries, quic_port)

            # Phase 2: Wait for secondaries to connect
            await self._wait_for_secondaries(num_secondaries)

            # Phase 3: Certificate exchange
            await self._exchange_certificates()

            # Phase 3.5: Wait for peer connections
            await self._wait_for_peer_connections()

            # Phase 4: Wait for workers ready
            await self._wait_for_workers()

            # Phase 5: Preliminary assignment
            await self._preliminary_assignment()

            # Phase 6: Source discovery from first secondary
            await self._source_discovery()

            # Phase 7: Intelligent file distribution
            await self._distribute_files()

            # Phase 8: Notify transfer complete
            await self._notify_transfer_complete()

            # Phase 9: Promote SLURM-primary
            await self._promote_slurm_primary()

            # Phase 10: Send full task list
            await self._send_full_task_list()

            # Phase 11: Monitor until user disconnects
            await self._monitor_mode()

        except KeyboardInterrupt:
            logger.info("Received interrupt signal")
        except Exception as e:
            logger.error(f"Primary coordinator error: {e}", exc_info=True)
        finally:
            await self._cleanup()

    async def _setup_quic_transport(self) -> None:
        """Setup QUIC transport, generate/load certificates, and start server"""
        logger.info("Setting up QUIC transport for all connections...")

        # Create certificate directory locally if it doesn't exist
        self.cert_dir.mkdir(parents=True, exist_ok=True)

        # Initialize QUIC transport - listen only on localhost
        self.quic_transport = QuicTransport("primary", listen_port=self.primary_quic_port, bind_address="127.0.0.1")

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
        # The SSH reverse tunnel will forward from compute nodes to this local port
        await self.quic_transport.start_server()
        logger.info(f"Primary QUIC server listening on 127.0.0.1:{self.quic_transport.listen_port}")

        # Register message handlers with both message_router (for current TCP) and QUIC transport (for future)
        self.message_router.register_handler("secondary_welcome", self._handle_secondary_welcome)
        self.message_router.register_handler("cert_exchange", self._handle_cert_exchange)
        self.message_router.register_handler("secondary_error", self._handle_secondary_error)
        self.message_router.register_handler("secondary_log", self._handle_secondary_log)
        self.message_router.register_handler("worker_ready", self._handle_worker_ready)
        self.message_router.register_handler("source_discovered", self._handle_source_discovered)
        self.message_router.register_handler("peer_connections_ready", self._handle_peer_connections_ready)

        self.quic_transport.register_handler("secondary_welcome", self._handle_secondary_welcome)
        self.quic_transport.register_handler("cert_exchange", self._handle_cert_exchange)
        self.quic_transport.register_handler("secondary_error", self._handle_secondary_error)
        self.quic_transport.register_handler("secondary_log", self._handle_secondary_log)
        self.quic_transport.register_handler("worker_ready", self._handle_worker_ready)

    # Old TCP connection handler - no longer used with QUIC
    # async def _handle_secondary_connection(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
    #     """Handle incoming connection from secondary"""
    #     # Now handled by QUIC protocol callbacks

    async def _handle_secondary_welcome(self, message: dict[str, Any], sender_id: str | None) -> None:
        """Handle secondary welcome message"""
        logger.debug(f"Received secondary_welcome message: {message}")
        secondary_id = message.get("secondary_id")
        ram_bytes = message.get("ram_bytes") or 0
        worker_count = message.get("worker_count") or 0
        hostname = message.get("hostname") or "unknown"

        if not secondary_id:
            logger.error("Received welcome message without secondary_id")
            return

        logger.info(
            f"Secondary {secondary_id} connected: {hostname}, {ram_bytes / (1024**3):.1f}GB, {worker_count} workers"
        )

        self.secondaries[secondary_id] = {
            "id": secondary_id,
            "ram_bytes": ram_bytes,
            "worker_count": worker_count,
            "hostname": hostname,
        }

        # Move writer from temp storage to permanent secondary connections
        # Find the temp connection and move it
        for temp_id, writer in list(self.active_connections.items()):
            if temp_id.startswith("temp_"):
                self.active_connections[secondary_id] = writer
                del self.active_connections[temp_id]
                # Add to message router for sending messages
                self.message_router.add_secondary_connection(secondary_id, writer)
                break

        logger.info(f"Secondary {secondary_id} registered and ready (total: {len(self.secondaries)} secondaries)")

    async def _handle_cert_exchange(self, message: dict[str, Any], sender_id: str | None) -> None:
        """Handle certificate exchange from secondary"""
        secondary_id = message.get("secondary_id")
        public_cert_pem = message.get("public_cert_pem")
        ipv4_address = message.get("ipv4_address")
        ipv6_address = message.get("ipv6_address")
        quic_port = message.get("quic_port")

        if not secondary_id:
            logger.error("Received cert_exchange without secondary_id")
            return

        logger.info(f"Received certificate from {secondary_id}: {ipv4_address}:{quic_port}")

        # Store peer info
        self.peer_info.append(
            {
                "peer_id": secondary_id,
                "ipv4": ipv4_address,
                "ipv6": ipv6_address,
                "port": quic_port,
                "cert_pem": public_cert_pem,
            }
        )

        # Save secondary certificate to local disk for persistence
        secondary_cert_path = self.cert_dir / f"{secondary_id}_cert.pem"
        secondary_info_path = self.cert_dir / f"{secondary_id}_info.json"

        # Save certificate
        if public_cert_pem:
            secondary_cert_path.write_text(public_cert_pem)

        # Save connection info as JSON
        import json

        info = {
            "secondary_id": secondary_id,
            "ipv4": ipv4_address,
            "ipv6": ipv6_address,
            "port": quic_port,
        }
        secondary_info_path.write_text(json.dumps(info, indent=2))

        logger.debug(f"Stored peer info for {secondary_id} ({len(self.peer_info)}/{len(self.secondaries)} peers)")
        logger.debug(f"Saved certificate to {secondary_cert_path}")

    async def _handle_secondary_error(self, message: dict[str, Any], sender_id: str | None) -> None:
        """Handle error message from secondary"""
        secondary_id = message.get("secondary_id")
        error_type = message.get("error_type")
        error_message = message.get("error_message")
        traceback_str = message.get("traceback")

        logger.error("=" * 80)
        logger.error(f"SECONDARY ERROR from {secondary_id}")
        logger.error("=" * 80)
        logger.error(f"Error Type: {error_type}")multi-computer slurm instead")
        
            # Validate multi-computer arguments
            if args.multi_computer:
                if args.multi_computer == "slurm":
                    if not args.gateway:
                        logger.error("--gateway is required when --multi-computer slurm is enabled")
                        return
                    if not args.packaging:
                        logger.error("--packaging is required when --multi-computer slurm is enabled")
                        return
                    if not args.slurm_root_folder:
                        home = Path.home()
                        suggestions = [home / "slurm", home / "BIG" / "slurm"]
                        logger.error(f"--slurm-root-folder is required when --multi-computer slurm is enabled")
                        logger.error(f"Suggested locations: {'
        logger.error(f"Error Message: {error_message}")
        logger.error("")
        logger.error("Traceback:")
        logger.error(traceback_str)
        logger.error("=" * 80)

    async def _handle_secondary_log(self, message: dict[str, Any], sender_id: str | None) -> None:
        """Handle log message from secondary"""
        secondary_id = message.get("secondary_id")
        level = message.get("level")
        log_message = message.get("message")
        module = message.get("module")
        func_name = message.get("funcName")
        lineno = message.get("lineno")

        # Format log message with secondary ID prefix
        formatted = f"[{secondary_id}] {module}.{func_name}:{lineno} - {log_message}"

        # Log at appropriate level
        if level == "CRITICAL":
            logger.critical(formatted)
        elif level == "ERROR":
            logger.error(formatted)
        elif level == "WARNING":
            logger.warning(formatted)
        else:
            logger.info(formatted)

    def _submit_slurm_jobs(self, num_secondaries: int, base_port: int) -> None:
        """Submit SLURM jobs for secondaries"""
        logger.info("Submitting SLURM jobs...")

        # Gateway hostname for secondaries to connect to
        # For SSH gateway, detect the actual hostname that compute nodes can reach
        if hasattr(self.gateway, "host") and self.gateway.host:
            # SSH gateway - get the actual FQDN from the gateway
            logger.info("Detecting gateway hostname for compute nodes...")
            returncode, stdout, stderr = self.gateway.execute_command("hostname -f")
            if returncode == 0 and stdout.strip():
                gateway_host = stdout.strip()
                logger.info(f"Using gateway FQDN: {gateway_host}")
            else:
                # Fallback to SSH host
                gateway_host = self.gateway.host
                logger.warning(f"Could not detect gateway FQDN, using SSH host: {gateway_host}")
        else:
            # Local gateway - use localhost
            gateway_host = "localhost"
            logger.info(f"Using local gateway host: {gateway_host}")

        # Gateway port is no longer used - using QUIC instead
        # gateway_port = self.gateway_port

        for i in range(num_secondaries):
            secondary_id = f"secondary-{i}"
            job_name = f"asm-tokenizer-{secondary_id}"

            # Generate wrapper script
            image_dir = self.slurm_config.get_image_dir()
            if isinstance(image_dir, str):
                image_path = f"{image_dir}/asm-tokenizer-docker.tar"
            else:
                image_path = image_dir / "asm-tokenizer-docker.tar"

            wrapper = self.job_manager.generate_wrapper_script(
                image_path=image_path,
                secondary_id=secondary_id,
                gateway_host=gateway_host,
                gateway_port=self.primary_quic_port,
                reverse_connection=self.use_reverse_connection,
                run_log_dir=self.run_log_dir,
            )

            # Submit job
            job_id = self.job_manager.submit_job(wrapper, job_name, run_log_dir=self.run_log_dir)
            logger.info(f"Submitted job {job_id} for {secondary_id}")

        logger.info(f"All {num_secondaries} jobs submitted")

    async def _connect_to_secondaries_reverse(self, expected_count: int) -> None:
        """Connect to secondaries in reverse mode (they listen, we connect via ProxyJump)"""
        import asyncio

        logger.info("Reverse connection mode: polling for secondary connection info files...")

        # Create run-specific log directory
        self.gateway.create_directory(self.run_log_dir)

        connection_info_dir = f"{self.run_log_dir}/connection_info"
        self.gateway.create_directory(connection_info_dir)

        connected = set()
        timeout = 600  # 10 minutes
        start_time = time.time()

        while len(connected) < expected_count:
            if time.time() - start_time > timeout:
                raise TimeoutError(f"Timeout waiting for secondaries. Got {len(connected)}/{expected_count}")

            # List connection info files
            returncode, stdout, stderr = self.gateway.execute_command(
                f"ls {connection_info_dir}/*.info 2>/dev/null || true"
            )

            if returncode == 0 and stdout.strip():
                files = stdout.strip().split("\n")

                for file_path in files:
                    if not file_path or file_path in connected:
                        continue

                    # Read connection info
                    returncode, content, stderr = self.gateway.execute_command(f"cat {file_path}")
                    if returncode != 0 or not content.strip():
                        continue

                    # Parse: secondary_id,hostname,port
                    try:
                        parts = content.strip().split(",")
                        if len(parts) != 3:
                            continue

                        secondary_id, hostname, port_str = parts
                        port = int(port_str)

                        if secondary_id in connected:
                            continue

                        # Only try to connect if we haven't already tried this secondary
                        # (avoid creating duplicate tunnels on retry)
                        if secondary_id in self.secondary_port_map:
                            logger.debug(f"Secondary {secondary_id} already has tunnel, skipping tunnel creation")
                            continue

                        logger.info(f"Found secondary {secondary_id} at {hostname}:{port}, connecting...")

                        # Connect via SSH ProxyJump
                        success = await self._connect_to_secondary_ssh(secondary_id, hostname, port)

                        if success:
                            connected.add(secondary_id)
                            logger.info(
                                f"Tunnel establishes to {secondary_id} - ready for connection ({len(connected)}/{expected_count})"
                            )
                        else:
                            logger.warning(f"Failed to establish tunnel to {secondary_id}, will retry")

                    except Exception as e:
                        logger.warning(f"Error processing connection info {file_path}: {e}")

            await asyncio.sleep(2)

        logger.info(f"All {expected_count} secondaries have an established tunnel")

    async def _connect_to_secondary_ssh(self, secondary_id: str, hostname: str, port: int) -> bool:
        """Connect to a secondary via SSH ProxyJump through the gateway"""
        import subprocess
        import tempfile

        try:
            # Use SSH with ProxyJump to create a local port forward tunnel
            gateway_host = self.gateway.host if hasattr(self.gateway, "host") else "localhost"

            # Get gateway username - use gateway.user if set, otherwise query the gateway
            if hasattr(self.gateway, "user") and self.gateway.user:
                gateway_user = self.gateway.user
            else:
                # Query the gateway to get the username
                returncode, stdout, stderr = self.gateway.execute_command("whoami")
                if returncode == 0 and stdout.strip():
                    gateway_user = stdout.strip()
                    logger.debug(f"Detected gateway username: {gateway_user}")
                else:
                    logger.warning("Could not determine gateway username, using 'unknown'")
                    gateway_user = "unknown"

            # Start listening on an automatically allocated free port for the secondary to connect via the tunnel
            logger.info(f"Allocating free port on primary for {secondary_id}...")
            server = await asyncio.start_server(
                lambda r, w: self._handle_proxyjump_connection(r, w, secondary_id), "localhost", 0
            )

            # Get the allocated port
            local_port = server.sockets[0].getsockname()[1]
            logger.info(f"Allocated port {local_port} for {secondary_id}")

            # Store the port allocation for this secondary
            self.secondary_port_map[secondary_id] = local_port

            # Store server so we can close it later
            self.active_connections[f"{secondary_id}_server"] = server

            # Create control socket for this tunnel
            control_dir = tempfile.mkdtemp(prefix=f"ssh-tunnel-{secondary_id}-")
            control_path = f"{control_dir}/control-socket"

            # Start SSH tunnel with ControlMaster using -R (remote forward)
            # This creates port 6000 on the secondary node that forwards to localhost:local_port on primary
            # Secondary connects to localhost:6000, which gets forwarded back to primary's local_port
            tunnel_cmd = [
                "ssh",
                "-M",  # ControlMaster
                "-N",  # Don't execute remote command
                "-f",  # Go to background
                "-R",
                f"{port}:localhost:{local_port}",
                "-J",
                gateway_host,
                "-o",
                f"ControlPath={control_path}",
                "-o",
                "ControlMaster=auto",
                "-o",
                "ControlPersist=yes",
                "-o",
                "StrictHostKeyChecking=no",
                "-o",
                "UserKnownHostsFile=/dev/null",
                "-o",
                "ExitOnForwardFailure=yes",
                f"{gateway_user}@{hostname}" if gateway_user else hostname,
            ]

            logger.info(
                f"Creating SSH ProxyJump tunnel: {hostname}:{port} -> localhost:{local_port} "
                f"(via gateway {gateway_host})"
            )
            logger.info(f"SSH tunnel command: {' '.join(tunnel_cmd)}")

            result = subprocess.run(tunnel_cmd, capture_output=True, text=True)
            if result.returncode != 0:
                logger.error(f"Failed to start SSH tunnel: {result.stderr}")
                return False

            # Store control path for cleanup
            self.active_connections[f"{secondary_id}_tunnel_control"] = control_path

            # Wait for tunnel to establish
            await asyncio.sleep(1)

            logger.info(
                f"Tunnel ready: {secondary_id} can connect to localhost:{port} -> "
                f"tunnels to primary localhost:{local_port}"
            )
            return True

        except Exception as e:
            logger.error(f"Error connecting to {secondary_id} at {hostname}:{port}: {e}")
            import traceback

            logger.debug(traceback.format_exc())
            return False

    async def _handle_proxyjump_connection(
        self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter, secondary_id: str
    ) -> None:
        """Handle incoming connection from secondary through SSH ProxyJump tunnel"""
        addr = writer.get_extra_info("peername")
        logger.info(f"Secondary {secondary_id} connected through ProxyJump tunnel from {addr}")

        # Store writer with temp ID first (will be moved to secondary_id after welcome message)
        temp_id = f"temp_{secondary_id}"
        self.active_connections[temp_id] = writer

        # Start receive loop with secondary_id so handlers can identify the sender
        asyncio.create_task(self.message_router.receive_loop(reader, secondary_id))

    async def _wait_for_secondaries(self, expected_count: int) -> None:
        """Wait for secondaries to connect and send welcome"""
        if self.use_reverse_connection:
            logger.info(f"Waiting for {expected_count} secondaries to start and write connection info...")
            await self._connect_to_secondaries_reverse(expected_count)
        else:
            logger.info(f"Waiting for {expected_count} secondaries to connect...")

        timeout = 600  # 10 minutes
        start_time = time.time()
        last_count = 0

        while len(self.secondaries) < expected_count:
            if time.time() - start_time > timeout:
                raise TimeoutError(f"Timeout waiting for secondaries. Got {len(self.secondaries)}/{expected_count}")

            # Log progress
            current_count = len(self.secondaries)
            if current_count != last_count:
                logger.info(f"Progress: {current_count}/{expected_count} secondaries connected")
                last_count = current_count

            # Wait a bit for connections
            await asyncio.sleep(1)

        logger.info(f"All {expected_count} secondaries connected")

    async def _exchange_certificates(self) -> None:
        """Exchange certificates with secondaries"""
        logger.info("Performing certificate exchange...")

        # Wait for all secondaries to send their certificates
        timeout = 60.0
        start_time = time.time()

        while len(self.peer_info) < len(self.secondaries):
            if time.time() - start_time > timeout:
                raise TimeoutError(
                    f"Timeout waiting for certificates. Got {len(self.peer_info)}/{len(self.secondaries)}"
                )
            await asyncio.sleep(0.5)

        logger.info(f"Received all {len(self.peer_info)} certificates")

        # Broadcast peer info to all secondaries
        for secondary_id in self.secondaries:
            if secondary_id not in self.active_connections:
                logger.warning(f"No connection to {secondary_id}, skipping peer info")
                continue

            msg = {
                "type": "peer_list",
                "peers": self.peer_info,
            }

            writer = self.active_connections[secondary_id]
            await self.message_router._send_message(writer, msg)
            logger.debug(f"Sent peer info to {secondary_id}")

        logger.info("Certificate exchange complete")

    async def _wait_for_peer_connections(self) -> None:
        """Wait for all secondaries to establish peer-to-peer connections"""
        logger.info("Waiting for secondaries to build peer-to-peer network...")

        expected_secondaries = len(self.secondaries)
        timeout = 300  # 5 minutes for peer connections
        start_time = time.time()

        while len(self.peer_connections_ready) < expected_secondaries:
            if time.time() - start_time > timeout:
                raise TimeoutError(
                    f"Timeout waiting for peer connections. Got {len(self.peer_connections_ready)}/{expected_secondaries}"
                )
            await asyncio.sleep(0.5)

        logger.info(f"All {expected_secondaries} secondaries have established peer connections")

    async def _handle_peer_connections_ready(self, message: dict[str, Any], sender_id: str | None) -> None:
        """Handle peer connections ready notification from secondary"""
        secondary_id = message.get("secondary_id")

        if not secondary_id:
            logger.warning("Received peer_connections_ready without secondary_id")
            return

        if secondary_id not in self.peer_connections_ready:
            self.peer_connections_ready.add(secondary_id)
            logger.info(
                f"[{secondary_id}] Peer connections ready ({len(self.peer_connections_ready)}/{len(self.secondaries)})"
            )

    async def _wait_for_workers(self) -> None:
        """Wait for all workers to report ready"""
        logger.info("Waiting for workers to report ready...")

        expected_workers = sum(s["worker_count"] for s in self.secondaries.values())
        timeout = 300  # 5 minutes
        start_time = time.time()

        total_workers = sum(len(workers) for workers in self.remote_workers.values())
        while total_workers < expected_workers:
            if time.time() - start_time > timeout:
                raise TimeoutError(f"Timeout waiting for workers. Got {total_workers}/{expected_workers}")

            await asyncio.sleep(0.5)
            total_workers = sum(len(workers) for workers in self.remote_workers.values())

        logger.info(f"All {expected_workers} workers ready")

    async def _handle_worker_ready(self, message: dict[str, Any], sender_id: str | None) -> None:
        """Handle worker ready message from secondary"""
        secondary_id = message.get("secondary_id")
        worker_id = message.get("worker_id")
        memory_budget = message.get("memory_budget")

        if not secondary_id or not isinstance(worker_id, int):
            logger.warning(f"Invalid worker_ready message: secondary_id={secondary_id}, worker_id={worker_id}")
            return

        # Create RemoteWorker instance
        if secondary_id not in self.remote_workers:
            self.remote_workers[secondary_id] = []

        remote_worker = RemoteWorker(
            worker_id=worker_id,
            memory_budget=memory_budget or 0,
            secondary_id=secondary_id,
            message_router=self.message_router,
        )
        remote_worker.start()
        self.remote_workers[secondary_id].append(remote_worker)

        # Initialize WorkerManager for this secondary if not exists
        if secondary_id not in self.worker_managers:
            secondary_info = self.secondaries.get(secondary_id, {})
            ram_bytes = secondary_info.get("ram_bytes", 0)

            # Create temp directories (not actually used for remote workers)
            from tempfile import mkdtemp

            temp_dir = Path(mkdtemp(prefix=f"remote_{secondary_id}_"))

            self.worker_managers[secondary_id] = WorkerManager(
                num_workers=secondary_info.get("worker_count", 1),
                max_memory=ram_bytes,
                source_dir=temp_dir / "src",
                output_dir=temp_dir / "out",
                task_definition=self.task_definition,
                task_args=self.task_args,
                skip_existing=False,
                print_pid=False,
                always_restart_worker=False,
                manual_start_worker=False,
                connection_mode="socketpair",
            )

            # Replace the WorkerManager's workers list with our RemoteWorker instances
            # This allows WorkerManager to use its logic with remote workers
            self.worker_managers[secondary_id].workers = []

        # Track worker info in secondaries dict
        if secondary_id in self.secondaries:
            if "workers" not in self.secondaries[secondary_id]:
                self.secondaries[secondary_id]["workers"] = {}

            self.secondaries[secondary_id]["workers"][worker_id] = {
                "memory_budget": memory_budget,
                "ready": True,
            }

        budget_gb = memory_budget / (1024**3) if memory_budget else 0
        logger.debug(f"Worker ready: {secondary_id} worker {worker_id} (budget: {budget_gb:.2f}GB)")

    async def _preliminary_assignment(self) -> None:
        """Assign initial tasks to secondaries - one per worker with OOM checking"""
        logger.info("Performing preliminary task assignment...")

        if len(self.secondaries) == 0:
            logger.warning("No secondaries connected, skipping preliminary assignment")
            return

        if len(self.binaries) == 0:
            logger.warning("No binaries to assign")
            return

        # Distribute binaries evenly across secondaries
        binaries_per_secondary = len(self.binaries) // len(self.secondaries)

        # Track binaries for assignment
        pending_binaries = self.binaries.copy()

        for idx, (secondary_id, secondary_info) in enumerate(self.secondaries.items()):
            # Get RemoteWorker instances for this secondary
            remote_workers = self.remote_workers.get(secondary_id, [])

            if not remote_workers:
                logger.warning(f"No workers registered for {secondary_id}, skipping")
                continue

            # Get slice of binaries for this secondary
            start_idx = idx * binaries_per_secondary
            end_idx = start_idx + binaries_per_secondary if idx < len(self.secondaries) - 1 else len(self.binaries)
            secondary_binaries = pending_binaries[start_idx:end_idx]

            # Create AuthoritiveManager for this secondary
            secondary_memory_limit = secondary_info.get("ram_bytes", 0)
            manager = AuthoritiveManager(
                num_workers=len(remote_workers),
                max_memory=secondary_memory_limit,
                log_dir=Path.cwd() / "run" / self.run_id / "logs",
                task_definition=self.task_definition,
                workers=sorted(remote_workers, key=lambda w: w.worker_id),
            )

            # Set reserved_budget from the budget sent by secondary (don't recalculate)
            for worker in manager.workers:
                worker.reserved_budget = worker.memory_budget

            # Set pending binaries and run initial assignment
            manager.pending_binaries = secondary_binaries
            manager._run_initial_assignments()

            # Collect assignments for sending to secondary
            assignments = []
            for worker in manager.workers:
                if worker.current_binary:
                    task_hash = self._compute_task_hash(worker.current_binary)
                    self.task_assignments[task_hash] = secondary_id

                    assignments.append(
                        {
                            "worker_id": worker.worker_id,
                            "binary": worker.current_binary,
                            "estimated_memory": worker.estimated_memory,
                            "opportunistic": worker.opportunistic,
                        }
                    )

            # Store assignments for sending after transfer complete
            secondary_info["initial_assignments"] = assignments

        logger.info("Preliminary assignment complete")

    async def _source_discovery(self) -> None:
        """First secondary discovers and reports source binaries"""
        logger.info("Starting source discovery phase...")

        if len(self.secondaries) == 0:
            logger.warning("No secondaries connected, skipping source discovery")
            return

        if len(self.binaries) == 0:
            logger.warning("No binaries to process, skipping source discovery")
            return

        # Get first secondary
        first_secondary = next(iter(self.secondaries.keys()))
        logger.info(f"Using {first_secondary} for source discovery")

        # Send source discovery request to first secondary
        msg = {
            "type": "discover_sources",
            "sender_id": "primary",
            "timestamp": time.time(),
        }

        # Register handler for discovered binaries
        self.message_router.register_handler("source_discovered", self._handle_source_discovered)

        # Send request
        await self.message_router.send_to_secondary(first_secondary, msg)

        # Wait for discovery to complete (timeout after 30 seconds)
        timeout = 30
        start_time = time.time()
        discovery_complete = False

        while not discovery_complete and (time.time() - start_time) < timeout:
            await asyncio.sleep(0.5)
            # Check if we received discovery complete message
            # For now, just wait a bit for discoveries to come in
            if (time.time() - start_time) > 5:  # Give 5 seconds for discovery
                discovery_complete = True

        logger.info(f"Source discovery complete: found {len(self.discovered_binaries)} existing binaries")

    async def _distribute_files(self) -> None:
        """Distribute files to secondaries with intelligent deduplication"""
        logger.info("Starting file distribution...")

        if len(self.secondaries) == 0:
            logger.warning("No secondaries connected, skipping file distribution")
            return

        if len(self.binaries) == 0:
            logger.info("No binaries to distribute, skipping file distribution")
            return

        total_size = 0
        total_files = 0

        # Get srcbins directory on gateway
        srcbins_dir = self.slurm_config.get_srcbins_dir()

        # Ensure srcbins directory exists
        self.gateway.create_directory(str(srcbins_dir))

        for secondary_id in self.secondaries:
            # Get assigned tasks for this secondary
            assigned_binaries = [
                binary
                for binary in self.binaries
                if self.task_assignments.get(self._compute_task_hash(binary)) == secondary_id
            ]

            if not assigned_binaries:
                logger.info(f"No binaries assigned to {secondary_id}")
                continue

            logger.info(f"Distributing {len(assigned_binaries)} binaries to {secondary_id}")

            # Group binaries into batches
            batches = self._create_zip_batches(assigned_binaries)

            # Create ZIPs and upload to gateway
            zip_files_info = []
            for batch_idx, batch in enumerate(batches):
                # Create unique ZIP name
                import secrets

                random_suffix = secrets.token_hex(8)
                zip_name = f"{secondary_id}_batch_{batch_idx}_{random_suffix}.zip"
                # Use string path joining for remote paths
                zip_path = f"{srcbins_dir}/{zip_name}" if not srcbins_dir.endswith("/") else f"{srcbins_dir}{zip_name}"

                # Create ZIP with batch binaries
                zip_info = await self._create_and_upload_zip(zip_path, batch)
                if zip_info:
                    zip_files_info.append(zip_info)
                    total_files += len(batch)
                    total_size += sum(b.size for b in batch)

            # Send initial assignment to secondary
            await self._send_initial_assignment(secondary_id, zip_files_info)

        logger.info(f"Distribution complete: {total_files} files, {total_size / (1024**3):.2f}GB")

    def _create_zip_batches(self, binaries: list[BinaryInfo]) -> list[list[BinaryInfo]]:
        """Create batches of binaries for ZIP files (20MB uncompressed target)"""
        target_size = 20 * 1024 * 1024  # 20MB
        batches = []
        current_batch = []
        current_size = 0

        for binary in binaries:
            # Check if already discovered (skip if so)
            binary_hash = self._compute_file_hash(binary.path)
            if binary_hash in self.discovered_binaries:
                logger.debug(f"Skipping {binary.path.name} (already on secondary)")
                continue

            # Check if adding this would exceed target
            if current_size > 0 and current_size + binary.size > target_size:
                # Start new batch
                batches.append(current_batch)
                current_batch = [binary]
                current_size = binary.size
            else:
                current_batch.append(binary)
                current_size += binary.size

        # Add remaining batch
        if current_batch:
            batches.append(current_batch)

        return batches

    async def _create_and_upload_zip(self, zip_path: str | Path, binaries: list[BinaryInfo]) -> dict[str, Any] | None:
        """Create ZIP file with binaries and upload to gateway"""
        try:
            # Convert to Path for local operations
            if isinstance(zip_path, str):
                zip_path = Path(zip_path).expanduser()

            # Ensure parent directory exists
            zip_path.parent.mkdir(parents=True, exist_ok=True)

            # Create ZIP without compression (store only)
            with zipfile.ZipFile(zip_path, "w", zipfile.ZIP_STORED) as zf:
                binaries_info = []
                for binary in binaries:
                    # Compute hash
                    file_hash = self._compute_file_hash(binary.path)

                    # Add to ZIP with relative path
                    arcname = binary.path.name
                    zf.write(binary.path, arcname)

                    binaries_info.append(
                        {
                            "local_path": arcname,
                            "binary_info": {
                                "path": str(binary.path),
                                "size": binary.size,
                                "binary_name": binary.binary_name,
                                "platform": binary.platform,
                                "compiler": binary.compiler,
                                "version": binary.version,
                                "opt_level": binary.opt_level,
                            },
                            "hash": file_hash,
                        }
                    )

            logger.info(f"Created ZIP: {zip_path.name} with {len(binaries)} binaries")

            return {
                "zip_name": zip_path.name,
                "binaries": binaries_info,
            }

        except Exception as e:
            logger.error(f"Failed to create ZIP {zip_path}: {e}")
            return None

    def _compute_file_hash(self, path: Path) -> str:
        """Compute SHA256 hash of a file"""
        sha256 = hashlib.sha256()
        with open(path, "rb") as f:
            for chunk in iter(lambda: f.read(8192), b""):
                sha256.update(chunk)
        return sha256.hexdigest()

    async def _send_initial_assignment(self, secondary_id: str, zip_files_info: list[dict[str, Any]]) -> None:
        """Send initial assignment with ZIP locations to secondary"""

        # Get the initial assignments prepared during preliminary_assignment
        secondary_info = self.secondaries.get(secondary_id, {})
        initial_assignments = secondary_info.get("initial_assignments", [])

        # Build worker assignments with full details
        worker_assignments = []
        for assignment in initial_assignments:
            worker_id = assignment["worker_id"]
            binary = assignment["binary"]
            estimated_memory = assignment["estimated_memory"]
            opportunistic = assignment["opportunistic"]

            # Compute hash to find which ZIP contains this binary
            task_hash = self._compute_task_hash(binary)

            # Find ZIP containing this binary
            zip_name = None
            local_path = None

            for zip_info in zip_files_info:
                for binary_entry in zip_info.get("binaries", []):
                    if binary_entry.get("hash") == task_hash:
                        zip_name = zip_info.get("zip_name")
                        local_path = binary_entry.get("local_path")
                        break
                if zip_name:
                    break

            if not zip_name:
                logger.warning(f"Could not find ZIP for binary {binary.path.name}, skipping assignment")
                continue

            worker_assignments.append(
                {
                    "worker_id": worker_id,
                    "zip_file": zip_name,
                    "local_path": local_path,
                    "file_hash": task_hash,
                    "estimated_memory": estimated_memory,
                    "opportunistic": opportunistic,
                    "binary_info": {
                        "path": str(binary.path.relative_to(self.source_dir)) if self.source_dir else str(binary.path),
                        "size": binary.size,
                        "binary_name": binary.binary_name,
                        "platform": binary.platform,
                        "compiler": binary.compiler,
                        "version": binary.version,
                        "opt_level": binary.opt_level,
                    },
                }
            )

        msg = {
            "type": "initial_assignment",
            "secondary_id": secondary_id,
            "zip_files": zip_files_info,
            "worker_assignments": worker_assignments,
        }

        await self.message_router.send_to_secondary(secondary_id, msg)
        logger.info(
            f"Sent initial assignment to {secondary_id}: {len(zip_files_info)} ZIP files, {len(worker_assignments)} worker assignments"
        )

    async def _handle_source_discovered(self, message: dict[str, Any], sender_id: str | None) -> None:
        """Handle discovered source binary report from first secondary"""
        zip_name = message.get("zip_name")
        local_path = message.get("local_path")
        file_hash = message.get("hash")
        binary_info = message.get("binary_info")

        if file_hash and file_hash not in self.discovered_binaries:
            self.discovered_binaries[file_hash] = {
                "zip_name": zip_name,
                "local_path": local_path,
                "binary_info": binary_info,
            }
            logger.debug(f"Discovered existing binary: {local_path} (hash: {file_hash[:8]})")

    async def _notify_transfer_complete(self) -> None:
        """Notify all secondaries that transfer is complete"""
        logger.info("Notifying secondaries: transfer complete")

        if len(self.secondaries) == 0:
            logger.info("No secondaries to notify, skipping transfer complete notification")
            self.transfer_complete = True
            return

        transfer_msg = {
            "type": "transfer_complete",
            "sender_id": "primary",
            "timestamp": time.time(),
            "total_files": len(self.binaries),
            "total_bytes": 0,
        }

        for secondary_id in self.secondaries:
            await self.message_router.send_to_secondary(secondary_id, transfer_msg)
            logger.debug(f"Sent transfer complete to {secondary_id}")

        self.transfer_complete = True
        logger.info("Transfer complete notification sent")

    async def _promote_slurm_primary(self) -> None:
        """Promote a random secondary to SLURM-primary role"""
        import random

        if len(self.secondaries) == 0:
            logger.info("No secondaries to promote, skipping SLURM-primary promotion")
            return

        self.slurm_primary_id = random.choice(list(self.secondaries.keys()))
        logger.info(f"Promoting {self.slurm_primary_id} to SLURM-primary")

        promote_msg = {
            "type": "promote_primary",
            "sender_id": "primary",
            "timestamp": time.time(),
            "new_primary_id": self.slurm_primary_id,
        }

        for secondary_id in self.secondaries:
            await self.message_router.send_to_secondary(secondary_id, promote_msg)
            logger.debug(f"Sent promotion to {secondary_id}")

        logger.info("SLURM-primary promotion complete")

    async def _send_full_task_list(self) -> None:
        """Send complete task list to all secondaries"""
        logger.info("Sending full task list to all secondaries...")

        if len(self.secondaries) == 0:
            logger.info("No secondaries to send task list to, skipping")
            return

        all_tasks = [
            {
                "hash": self._compute_task_hash(binary),
                "binary_info": {
                    "path": str(binary.path.relative_to(self.source_dir)) if self.source_dir else str(binary.path),
                    "size": binary.size,
                    "binary_name": binary.binary_name,
                    "platform": binary.platform,
                    "compiler": binary.compiler,
                    "version": binary.version,
                    "opt_level": binary.opt_level,
                },
            }
            for binary in self.binaries
        ]

        task_list_msg = {
            "type": "full_task_list",
            "sender_id": "primary",
            "timestamp": time.time(),
            "all_tasks": all_tasks,
            "completed_tasks": list(self.completed_tasks),
        }

        for secondary_id in self.secondaries:
            await self.message_router.send_to_secondary(secondary_id, task_list_msg)
            logger.debug(f"Sent task list to {secondary_id}")

        logger.info("Full task list sent")

    async def _monitor_mode(self) -> None:
        """Monitor mode - primary can be safely disconnected"""
        logger.info("")
        logger.info("=" * 60)
        logger.info("PRIMARY CAN NOW BE SAFELY CLOSED (Ctrl+C)")
        logger.info("=" * 60)
        logger.info(f"SLURM-primary: {self.slurm_primary_id}")
        logger.info("Secondaries will continue processing autonomously")
        logger.info("")

        # Keep running to monitor status updates
        while self.running:
            # TODO: Process status updates from secondaries
            # TODO: Display progress
            await asyncio.sleep(5)

    async def _cleanup(self) -> None:
        """Clean up resources"""
        logger.info("Cleaning up primary coordinator resources")

        # Clean up SSH tunnels
        import shutil
        import subprocess

        for key in list(self.active_connections.keys()):
            if key.endswith("_tunnel_control"):
                control_path = self.active_connections[key]
                try:
                    # Close SSH tunnel via ControlMaster
                    control_dir = str(Path(control_path).parent)
                    logger.debug(f"Closing SSH tunnel with control path: {control_path}")
                    subprocess.run(
                        ["ssh", "-O", "exit", "-o", f"ControlPath={control_path}", "lmu"],
                        capture_output=True,
                    )
                    # Clean up control directory
                    shutil.rmtree(control_dir, ignore_errors=True)
                except Exception as e:
                    logger.debug(f"Error cleaning up SSH tunnel {key}: {e}")

        # Close all secondary connections
        for secondary_id, writer in self.active_connections.items():
            if not secondary_id.endswith("_tunnel_control") and not secondary_id.endswith("_server"):
                try:
                    writer.close()
                    await writer.wait_closed()
                except Exception as e:
                    logger.debug(f"Error closing connection to {secondary_id}: {e}")

        # Close servers
        for key, server in self.active_connections.items():
            if key.endswith("_server"):
                try:
                    server.close()
                    await server.wait_closed()
                except Exception as e:
                    logger.debug(f"Error closing server {key}: {e}")

        # Stop QUIC transport
        if self.quic_transport:
            await self.quic_transport.stop()

        # Stop message router
        self.message_router.stop()

    def _compute_task_hash(self, binary: BinaryInfo) -> str:
        """Compute unique hash for task"""
        import hashlib

        data = f"{binary.path}|{binary.platform}|{binary.compiler}"
        return hashlib.sha256(data.encode()).hexdigest()[:16]

    def _handle_task_complete(self, secondary_id: str, task_hash: str) -> None:
        """Handle task completion notification"""
        self.completed_tasks.add(task_hash)
        logger.info(f"Task complete: {task_hash} (by {secondary_id})")

    async def _handle_task_failed(self, secondary_id: str, task_hash: str, error: str) -> None:
        """Handle task failure notification"""
        self.failed_tasks.add(task_hash)
        logger.warning(f"Task failed: {task_hash} (by {secondary_id}): {error}")

    async def _handle_task_request(self, secondary_id: str, worker_id: int, available_memory: int) -> None:
        """Handle request for new task from secondary"""
        # TODO: Find unassigned task that fits memory budget
        # TODO: Send TaskAssignmentMessage
        logger.debug(f"Task request from {secondary_id} worker {worker_id}")
