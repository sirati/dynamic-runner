"""SLURM-specific primary coordinator.

This coordinator extends the base coordinator with SLURM-specific preparation
and file transfer logic.
"""

import asyncio
import logging
import os
import subprocess
from pathlib import Path
from typing import Any

from ...binary_info import BinaryInfo
from ...gateway import create_gateway, parse_gateway_url
from ...multi_computer import ConnectionResult, FileTransferMode, PreparationResult
from ...multi_computer.primary.coordinator import BaseCoordinator
from ...runtime_env import PackagingConfig, create_packaging_method
from ...slurm import SlurmConfig, validate_slurm_config
from ...slurm.job_manager import SlurmJobManager
from ...task import TaskDefinition
from .file_transfer import SlurmFileTransfer
from .preparation import SlurmPreparation

logger = logging.getLogger(__name__)


class SlurmPrimaryCoordinator(BaseCoordinator):
    """SLURM-specific primary coordinator.

    Extends the base coordinator with SLURM-specific logic for:
    - Docker image building and transfer
    - SLURM job submission
    - SSH tunnel setup
    - File distribution via QUIC
    """

    def __init__(
        self,
        binaries: list[BinaryInfo],
        gateway_url: str,
        slurm_root_folder: str,
        packaging_method: str,
        task_definition: TaskDefinition,
        task_args: Any,
        run_id: str = "default",
        source_dir: Path | None = None,
        skip_image_build: bool = False,
        slurm_config_kwargs: dict[str, Any] | None = None,
    ):
        super().__init__(binaries, task_definition, task_args, run_id, source_dir)

        self.skip_image_build = skip_image_build

        # Parse gateway configuration and create gateway
        logger.info("Setting up gateway connection...")
        gateway_config = parse_gateway_url(gateway_url)
        self.gateway = create_gateway(gateway_config)
        
        # Store gateway config for later use
        self.gateway_config = gateway_config

        # Don't setup port forwarding yet - we need to wait for QUIC to allocate a port
        # Connect to gateway first
        self.gateway.connect()

        # Check if GatewayPorts is properly configured for SLURM
        self.use_reverse_connection = False
        if hasattr(self.gateway, "gateway_ports_enabled") and self.gateway.gateway_ports_enabled is False:
            logger.info("=" * 60)
            logger.info("USING SSH PROXYJUMP MODE")
            logger.info("=" * 60)
            logger.info("The SSH gateway does not allow public port forwarding (GatewayPorts is disabled).")
            logger.info("Using SSH ProxyJump tunnels: primary will tunnel to each secondary via gateway.")
            logger.info("Secondaries will connect to localhost:{free port} (tunneled to primary).")
            logger.info("")
            self.use_reverse_connection = True

        # Create SLURM configuration
        # Keep as string if starts with ~ for remote expansion
        root_folder = slurm_root_folder
        if not root_folder.startswith("~"):
            root_folder = Path(root_folder)

        slurm_config_kwargs = slurm_config_kwargs or {}
        self.slurm_config = SlurmConfig(
            root_folder=root_folder,
            image_subfolder=slurm_config_kwargs.get("image_subfolder", "image_bin"),
            output_subfolder=slurm_config_kwargs.get("output_subfolder", "out"),
            log_subfolder=slurm_config_kwargs.get("log_subfolder", "log"),
            notify_email=slurm_config_kwargs.get("notify_email"),
        )

        # Validate configuration (after gateway connection to check remote folder)
        try:
            validate_slurm_config(self.slurm_config, self.gateway)
        except ValueError:
            # Create directory if it doesn't exist
            logger.info(f"Creating SLURM root directory: {self.slurm_config.root_folder}")
            self.gateway.create_directory(self.slurm_config.root_folder)

        # Create packaging method
        packaging_config = PackagingConfig(method=packaging_method)
        packaging = create_packaging_method(packaging_config)

        # Create job manager
        self.job_manager = SlurmJobManager(self.gateway, self.slurm_config, packaging)

        # Clean up any existing SSH tunnels from previous runs
        logger.info("Cleaning up any existing SSH tunnels...")
        subprocess.run(["pkill", "-u", str(os.getuid()), "-f", "ssh.*-L.*localhost"], stderr=subprocess.DEVNULL)
        logger.debug("SSH tunnel cleanup complete")

        # Create SLURM-specific components
        self.slurm_preparation = SlurmPreparation(
            slurm_config=self.slurm_config,
            job_manager=self.job_manager,
            gateway=self.gateway,
            use_reverse_connection=self.use_reverse_connection,
            run_id=run_id,
        )

        self.slurm_file_transfer = SlurmFileTransfer(
            slurm_config=self.slurm_config,
            gateway=self.gateway,
            source_dir=source_dir,
        )

    async def prepare(self, num_secondaries: int, quic_port: int) -> PreparationResult:
        """Execute SLURM preparation phase.

        This includes:
        - Building and transferring Docker image
        - Submitting SLURM jobs
        - Setting up SSH tunnels (if using reverse connection)

        Args:
            num_secondaries: Number of SLURM secondaries to spawn
            quic_port: Base QUIC port

        Returns:
            PreparationResult with SLURM-specific data
        """
        prep_result = await self.slurm_preparation.prepare(
            num_secondaries=num_secondaries,
            quic_port=quic_port,
            primary_quic_port=self.primary_quic_port,
            cert_dir=self.cert_dir,
            skip_image_build=self.skip_image_build,
        )

        # Store SLURM-specific data
        self.primary_entropy = prep_result.primary_entropy

        return prep_result

    async def _setup_port_forwarding_after_quic(self) -> None:
        """Setup SSH port forwarding after QUIC server has allocated a port.
        
        This needs to happen after QUIC starts so we know which port was allocated.
        Since we already connected to gateway, we need to reconnect with forwarding.
        """
        #(self, prep_result: PreparationResult) -> ConnectionResult:
        """Wait for secondaries to connect (SLURM mode).

        Handles both normal and reverse connection modes.

        Args:
            prep_result: Result from preparation phase

        Returns:
            ConnectionResult with connection details
        """
        logger.info("Phase 2: Connecting to secondaries")

        if self.use_reverse_connection:
            # In reverse mode, we need to connect to secondaries via SSH tunnels
            await self._connect_via_ssh_tunnels(prep_result)
        else:
            # Normal mode: wait for secondaries to connect to us
            await self._wait_for_secondaries(prep_result.num_secondaries)

        # Exchange certificates for peer connections
        await self._exchange_certificates()

        # Wait for peer connections to be established
        await self._wait_for_peer_connections()

        return ConnectionResult(self.secondaries, self.peer_connections_ready)

    async def _connect_via_ssh_tunnels(self, prep_result: PreparationResult) -> None:
        """Connect to secondaries via SSH tunnels (reverse connection mode).

        Args:
            prep_result: Preparation result with SSH tunnel info
        """
        logger.info("Connecting to secondaries via SSH tunnels...")

        mode_specific = prep_result.mode_specific_data
        secondary_port_map = mode_specific.get("secondary_port_map", {})

        # Wait for all secondaries to be reachable via tunnels
        timeout = 600  # 10 minutes
        start_time = asyncio.get_event_loop().time()

        while len(self.secondaries) < prep_result.num_secondaries:
            if asyncio.get_event_loop().time() - start_time > timeout:
                logger.error(
                    f"Timeout waiting for secondaries via tunnels. "
                    f"Connected: {len(self.secondaries)}/{prep_result.num_secondaries}"
                )
                raise TimeoutError("Failed to connect to all secondaries via SSH tunnels")

            # Try to connect to each secondary via its tunnel
            for secondary_id, local_port in secondary_port_map.items():
                if secondary_id not in self.secondaries:
                    try:
                        # Connect via localhost:local_port (tunneled to secondary)
                        connection = await self.quic_transport.connect(
                            host="localhost",
                            port=local_port,
                        )

                        if connection:
                            logger.info(f"Connected to {secondary_id} via SSH tunnel (port {local_port})")
                            # Connection will be registered via welcome message
                    except Exception as e:
                        logger.debug(f"Not yet ready to connect to {secondary_id}: {e}")

            await asyncio.sleep(2)

        logger.info(f"All {prep_result.num_secondaries} secondaries connected via SSH tunnels")

    async def transfer_files(self, conn_result: ConnectionResult) -> None:
        """Transfer files to secondaries (SLURM mode).

        Uses intelligent deduplication based on discovered files from first secondary.

        Args:
            conn_result: Result from connection phase
        """
        logger.info("Phase 4: File transfer (SLURM mode)")

        await self.slurm_file_transfer.transfer_files(
            binaries=self.binaries,
            secondaries=self.secondaries,
            task_assignments=self.task_assignments,
            discovered_binaries=self.discovered_binaries,
            quic_transport=self.quic_transport,
            message_router=self.message_router,
        )

        logger.info("File transfer complete")

    def get_file_transfer_mode(self) -> FileTransferMode:
        """Get file transfer mode for SLURM (full transfer)."""
        return FileTransferMode.FULL_TRANSFER

    async def _cleanup(self) -> None:
        """Cleanup SLURM-specific resources."""
        logger.info("Cleaning up SLURM resources...")

        # Cleanup SLURM preparation (SSH tunnels, etc.)
        self.slurm_preparation.cleanup()

        # Cleanup base coordinator resources
        await super()._cleanup()

        # Clean up SSH tunnels
        logger.info("Cleaning up SSH tunnels...")
        subprocess.run(["pkill", "-u", str(os.getuid()), "-f", "ssh.*-L.*localhost"], stderr=subprocess.DEVNULL)

        # Disconnect gateway
        if self.gateway:
            self.gateway.disconnect()

        logger.info("SLURM cleanup complete")
