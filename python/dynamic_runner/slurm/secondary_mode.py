import asyncio
import logging
import secrets
import socket
import time
from pathlib import Path
from typing import Any
from urllib.parse import urlparse

from ..binary_info import BinaryInfo
from .message_router import MessageRouter
from .protocol import (
    CertExchangeMessage,
    KeepaliveMessage,
    SecondaryWelcomeMessage,
    TaskCompleteMessage,
    TaskFailedMessage,
    TaskRequestMessage,
)
from .quic_transport import QuicPeerInfo, QuicTransport

logger = logging.getLogger(__name__)


class PrimaryLogHandler(logging.Handler):
    """Custom logging handler that sends warnings and errors to primary"""

    def __init__(self, secondary_mode: "SecondaryMode"):
        super().__init__()
        self.secondary_mode = secondary_mode
        self.setLevel(logging.WARNING)

    def emit(self, record: logging.LogRecord) -> None:
        """Send log record to primary if it's a warning or error"""
        try:
            if record.levelno >= logging.WARNING and self.secondary_mode.message_router.primary_connection:
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

        self.peers: dict[str, Any] = {}
        self.primary_connection: Any = None
        self.peer_connections: dict[str, Any] = {}

        self.workers: list[Any] = []
        self.completed_tasks: set[str] = set()
        self.failed_tasks: set[str] = set()
        self.all_tasks: list[Any] = []

        self.is_slurm_primary = False
        self.last_keepalives: dict[str, float] = {}

        self.running = True
        self.setup_complete = False  # Track if initialization is complete
        self.peer_list_received = asyncio.Event()  # Event to signal peer list processing is complete

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

            # Phase 4: Register peer_list handler
            self.message_router.register_handler("peer_list", self._handle_peer_list)

            # Phase 5: Connect to peers (wait for peer_list from primary)
            await self._connect_to_peers()

            # Phase 5: Start workers
            self._start_workers()

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
                        f"Connection failed (attempt {attempt}): {error_type}: {error_msg}. Retrying in {retry_delay}s..."
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
            # Connection closed
            if not self.setup_complete:
                logger.error("Primary connection closed before setup was complete!")
                logger.error("Aborting secondary - setup incomplete")
                self.running = False
                # Send error to primary if possible (connection might still work briefly)
                try:
                    error_msg = {
                        "type": "secondary_error",
                        "secondary_id": self.secondary_id,
                        "error_type": "PrimaryDisconnectedError",
                        "error_message": "Primary connection closed before setup was complete",
                        "traceback": "Connection closed during initialization phase",
                    }
                    await self.message_router.send_to_primary(error_msg)
                except Exception:
                    pass
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

    def _start_workers(self) -> None:
        """Start worker processes"""
        logger.info(f"Starting {self.num_workers} workers")
        # TODO: Create worker processes
        # For now, just create placeholder worker structures
        for i in range(self.num_workers):
            self.workers.append(
                {
                    "id": i,
                    "status": "ready",
                    "active": False,
                }
            )
        logger.info(f"Created {len(self.workers)} worker placeholders")
        # TODO: Send ready messages to primary
        pass

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
        msg = {
            "type": "keepalive",
            "secondary_id": self.secondary_id,
            "active_workers": len([w for w in self.workers if isinstance(w, dict) and w.get("active", False)]),
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
        """Process worker completion and status updates"""
        # TODO: Check worker status
        # TODO: Move completed files from tmp to network storage
        # TODO: Rotate logs if needed
        # TODO: Request new tasks from SLURM-primary
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

    def _move_completed_files(self, worker_id: int) -> None:
        """Move completed files from tmp to network storage"""
        # TODO: Move files from out_tmp to out_network
        pass

    def _rotate_worker_log(self, worker_id: int, increment: int) -> None:
        """Rotate worker log file"""
        old_log = self.log_tmp / f"worker_{self.secondary_id}_{worker_id}.{increment}.log"
        if old_log.exists():
            new_log = self.log_network / old_log.name
            old_log.rename(new_log)
            logger.debug(f"Rotated log: {old_log.name}")
