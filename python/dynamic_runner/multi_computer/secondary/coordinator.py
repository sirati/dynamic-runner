import asyncio
import json
import logging
from pathlib import Path
from typing import Any

from ...models import WorkerState
from ...task import TaskDefinition
from ...worker_manager import ActualAuthoritativeWorkerManager, ActualSubmissiveWorkerManager
from ..message_router import MessageRouter
from ..quic_transport import QuicTransport
from .connection import ConnectionManager
from .logging_handler import PrimaryLogHandler
from .task_handling import TaskHandler
from .worker_management import WorkerManager

logger = logging.getLogger(__name__)


class SecondaryCoordinator:
    """Coordinates secondary node execution in multi-computer distributed mode"""

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
        task_definition: Any,
        task_args: Any,
        skip_existing: bool = False,
        quic_port: int = 0,
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

        # Create directories
        self.src_tmp.mkdir(parents=True, exist_ok=True)
        self.out_tmp.mkdir(parents=True, exist_ok=True)
        self.log_tmp.mkdir(parents=True, exist_ok=True)
        self.socket_dir.mkdir(parents=True, exist_ok=True)

        # Worker management
        self.worker_manager: ActualSubmissiveWorkerManager | None = None
        self.authoritative_manager: ActualAuthoritativeWorkerManager | None = None
        self.extracted_binaries: dict[str, Path] = {}  # hash -> extracted path

        # Task tracking
        self.completed_tasks: set[str] = set()
        self.failed_tasks: set[str] = set()
        self.all_tasks: list[Any] = []
        self.pending_worker_assignments: list[dict[str, Any]] = []

        # State flags
        self.is_slurm_primary = False
        self.slurm_primary_id: str | None = None
        self.transfer_complete = False
        self.last_keepalives: dict[str, float] = {}
        self.running = True
        self.setup_complete = False
        self.connection_closing = False

        # Message router for communication
        self.message_router = MessageRouter(secondary_id, "secondary")

        # QUIC transport for peer-to-peer communication
        self.quic_transport = QuicTransport(secondary_id, listen_port=quic_port)

        # Primary WebSocket connection
        self.primary_websocket = None

        # Sub-managers
        self.connection_manager = ConnectionManager(self)
        self.worker_mgr = WorkerManager(self)
        self.task_handler = TaskHandler(self)

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
            # Phase 1: Generate QUIC certificates (needed for welcome message)
            logger.info("Setting up QUIC certificates")
            await self.quic_transport.generate_certificates()
            await self.quic_transport.start_server()

            # Phase 2: Connect to primary
            await self.connection_manager.connect_to_primary()

            # Phase 3: Send welcome message (includes certificate info)
            await self.connection_manager.send_welcome()

            # Phase 4: Send certificate exchange
            await self.connection_manager.send_cert_exchange()

            # Phase 5: Register handlers
            self.message_router.register_handler("peer_list", self.connection_manager.handle_peer_list)
            self.message_router.register_handler("initial_assignment", self.task_handler.handle_initial_assignment)
            self.message_router.register_handler("task_assignment", self.task_handler.handle_task_assignment)
            self.message_router.register_handler("discover_sources", self.task_handler.handle_discover_sources)
            self.message_router.register_handler("transfer_complete", self.task_handler.handle_transfer_complete)
            self.message_router.register_handler("promote_primary", self.task_handler.handle_promote_primary)
            self.message_router.register_handler("full_task_list", self.task_handler.handle_full_task_list)

            # Phase 5: Connect to peers (wait for peer_list from primary)
            await self.connection_manager.connect_to_peers()

            # Phase 6: Start workers
            await self.worker_mgr.start_workers()

            # Mark setup as complete
            self.setup_complete = True
            logger.info("Setup complete, entering main processing loop")

            # Phase 7: Main processing loop
            await self._main_loop()

        except KeyboardInterrupt:
            logger.info("Received interrupt signal")
        except Exception as e:
            logger.error(f"Secondary mode error: {e}", exc_info=True)
            await self.connection_manager.send_error_to_primary(e)
        finally:
            await self._cleanup()

    async def _main_loop(self) -> None:
        """Main processing loop with keepalive"""
        logger.info("Entering main processing loop...")

        import time

        last_keepalive = time.time()
        keepalive_interval = 1.0  # 1 second

        while self.running:
            current_time = time.time()

            # Send keepalive
            if current_time - last_keepalive >= keepalive_interval:
                await self.worker_mgr.send_keepalive()
                last_keepalive = current_time

            # Check for timeouts
            self.worker_mgr.check_peer_timeouts()

            # Process worker completions
            self.worker_mgr.process_worker_updates()

            # Sleep briefly
            await asyncio.sleep(0.1)

    async def send_to_primary_ws(self, message: dict[str, Any]) -> None:
        """Send message to primary via WebSocket.

        Args:
            message: Message dict to send
        """
        if not self.primary_websocket or self.connection_closing:
            logger.warning(f"Cannot send {message.get('type')}: not connected to primary")
            return

        try:
            await self.primary_websocket.send(json.dumps(message))
            logger.debug(f"Sent {message.get('type')} to primary")
        except Exception as e:
            logger.warning(f"Failed to send {message.get('type')} to primary: {e}")

    async def _cleanup(self) -> None:
        """Clean up resources"""
        logger.info("Cleaning up secondary resources...")
        # Stop message router
        self.message_router.stop()
        # Stop QUIC transport
        if self.quic_transport:
            await self.quic_transport.stop()
