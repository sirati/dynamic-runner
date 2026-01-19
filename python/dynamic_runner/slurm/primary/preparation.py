"""SLURM-specific preparation phase for primary coordinator.

This module handles:
- Docker image building and transfer
- SLURM job submission
- SSH tunnel setup for reverse connections
"""

import logging
import subprocess
import time
from datetime import datetime
from pathlib import Path
from typing import Any

from ...multi_computer import PreparationResult

logger = logging.getLogger(__name__)


class SlurmPreparation:
    """Handles SLURM-specific preparation phase"""

    def __init__(
        self,
        slurm_config: Any,
        job_manager: Any,
        gateway: Any,
        use_reverse_connection: bool = False,
        run_id: str = "default",
    ):
        self.slurm_config = slurm_config
        self.job_manager = job_manager
        self.gateway = gateway
        self.use_reverse_connection = use_reverse_connection
        self.run_id = run_id

        # Create run-specific log directory
        base_log_dir = self.slurm_config.get_log_dir()
        self.run_log_dir = f"{base_log_dir}/{run_id}"

        # Track SSH tunnels and port allocations
        self.secondary_port_map: dict[str, int] = {}
        self.ssh_tunnels: list[subprocess.Popen] = []

    async def prepare(
        self,
        num_secondaries: int,
        quic_port: int,
        primary_quic_port: int,
        cert_dir: Path,
        skip_image_build: bool = False,
    ) -> PreparationResult:
        """Execute SLURM preparation phase

        Args:
            num_secondaries: Number of SLURM secondaries to spawn
            quic_port: Base QUIC port
            primary_quic_port: Primary's QUIC port
            cert_dir: Directory for certificates
            skip_image_build: Skip building and transferring Docker image

        Returns:
            PreparationResult with SLURM-specific data
        """
        logger.info("Phase 1: SLURM Preparation")

        # Prepare directories on gateway
        self.job_manager.prepare_directories()

        # Ensure run log directory exists
        self.gateway.create_directory(self.run_log_dir)

        # Build and transfer Docker image (unless skipped)
        image_path = await self._prepare_docker_image(skip_image_build)

        # Submit SLURM jobs
        self._submit_slurm_jobs(num_secondaries, primary_quic_port, image_path)

        # Setup SSH tunnels if using reverse connection
        if self.use_reverse_connection:
            await self._setup_ssh_tunnels(num_secondaries)

        mode_specific_data = {
            "image_path": image_path,
            "run_log_dir": self.run_log_dir,
            "secondary_port_map": self.secondary_port_map,
            "ssh_tunnels": self.ssh_tunnels,
        }

        # Generate primary entropy for certificate exchange
        import secrets

        primary_entropy = secrets.token_bytes(32)

        return PreparationResult(
            num_secondaries=num_secondaries,
            run_id=self.run_id,
            cert_dir=cert_dir,
            primary_entropy=primary_entropy,
            mode_specific_data=mode_specific_data,
        )

    async def _prepare_docker_image(self, skip_image_build: bool) -> str:
        """Build and transfer Docker image or verify existing image

        Args:
            skip_image_build: Skip building and transferring

        Returns:
            Path to Docker image on gateway
        """
        image_dir = self.slurm_config.get_image_dir()
        if isinstance(image_dir, str):
            image_path = f"{image_dir}/asm-tokenizer-docker.tar"
        else:
            image_path = str(image_dir / "asm-tokenizer-docker.tar")

        if skip_image_build:
            logger.info("Skipping image build and transfer (--skip-image-build)")
            logger.info(f"Assuming Docker image exists at: {image_path}")
        else:
            project_root = Path.cwd()
            image_path = self.job_manager.build_and_transfer_image(project_root)
            logger.info(f"Docker image ready at: {image_path}")

        return image_path

    def _submit_slurm_jobs(self, num_secondaries: int, primary_quic_port: int, image_path: str) -> None:
        """Submit SLURM jobs for secondaries

        Args:
            num_secondaries: Number of secondaries to spawn
            primary_quic_port: Primary's QUIC port
            image_path: Path to Docker image on gateway
        """
        logger.info("Submitting SLURM jobs...")

        # Determine gateway hostname for secondaries to connect to
        gateway_host = self._determine_gateway_host()

        for i in range(num_secondaries):
            secondary_id = f"secondary-{i}"
            job_name = f"asm-tokenizer-{secondary_id}"

            # Generate wrapper script
            wrapper = self.job_manager.generate_wrapper_script(
                image_path=image_path,
                secondary_id=secondary_id,
                gateway_host=gateway_host,
                gateway_port=primary_quic_port,
                reverse_connection=self.use_reverse_connection,
                run_log_dir=self.run_log_dir,
            )

            # Submit job
            job_id = self.job_manager.submit_job(wrapper, job_name, run_log_dir=self.run_log_dir)
            logger.info(f"Submitted job {job_id} for {secondary_id}")

        logger.info(f"All {num_secondaries} jobs submitted")

    def _determine_gateway_host(self) -> str:
        """Determine the hostname that compute nodes should use to reach the gateway

        Returns:
            Gateway hostname
        """
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

        return gateway_host

    async def _setup_ssh_tunnels(self, num_secondaries: int) -> None:
        """Setup SSH ProxyJump tunnels for reverse connections

        This is used when the gateway doesn't allow GatewayPorts (public port forwarding).
        We create SSH tunnels from primary to each secondary via the gateway.

        Args:
            num_secondaries: Number of secondaries to create tunnels for
        """
        logger.info("Setting up SSH ProxyJump tunnels for reverse connections...")

        # Wait for secondaries to write their connection info files
        connection_info_dir = f"{self.run_log_dir}/connection_info"
        self.gateway.create_directory(connection_info_dir)

        # Poll for connection info files
        connected = set()
        timeout = 600  # 10 minutes
        start_time = time.time()

        while len(connected) < num_secondaries:
            if time.time() - start_time > timeout:
                logger.error(
                    f"Timeout waiting for secondary connection info. Found: {len(connected)}/{num_secondaries}"
                )
                raise TimeoutError("Failed to get all secondary connection info")

            for i in range(num_secondaries):
                secondary_id = f"secondary-{i}"

                if secondary_id in connected:
                    continue

                # Check for connection info file
                info_file = f"{connection_info_dir}/{secondary_id}.txt"
                returncode, stdout, stderr = self.gateway.execute_command(f"cat {info_file}")

                if returncode == 0 and stdout.strip():
                    # Parse connection info
                    lines = stdout.strip().split("\n")
                    if len(lines) >= 2:
                        hostname = lines[0].split("=")[1].strip()
                        port = int(lines[1].split("=")[1].strip())

                        logger.info(f"Found connection info for {secondary_id}: {hostname}:{port}")

                        # Create SSH tunnel via gateway
                        local_port = 5001 + i  # Allocate unique port for each secondary
                        self._create_ssh_tunnel(secondary_id, hostname, port, local_port)

                        self.secondary_port_map[secondary_id] = local_port
                        connected.add(secondary_id)

            if len(connected) < num_secondaries:
                await self._async_sleep(2)

        logger.info(f"All {num_secondaries} SSH tunnels established")

    def _create_ssh_tunnel(self, secondary_id: str, remote_host: str, remote_port: int, local_port: int) -> None:
        """Create an SSH tunnel from primary to secondary via gateway

        Args:
            secondary_id: Secondary identifier
            remote_host: Hostname of the secondary (compute node)
            remote_port: Port on the secondary
            local_port: Local port to bind to
        """
        # Build SSH ProxyJump command
        gateway_host = self.gateway.host if hasattr(self.gateway, "host") else "localhost"
        gateway_user = self.gateway.user if hasattr(self.gateway, "user") else None

        jump_host = f"{gateway_user}@{gateway_host}" if gateway_user else gateway_host

        # SSH command: ssh -J gateway_host -L local_port:remote_host:remote_port remote_host -N
        ssh_cmd = [
            "ssh",
            "-J",
            jump_host,
            "-L",
            f"{local_port}:localhost:{remote_port}",
            f"{remote_user}@{remote_host}",
            "-N",  # No command execution
            "-o",
            "StrictHostKeyChecking=no",
            "-o",
            "UserKnownHostsFile=/dev/null",
        ]

        logger.info(f"Creating SSH tunnel for {secondary_id}: localhost:{local_port} -> {remote_host}:{remote_port}")
        logger.debug(f"SSH command: {' '.join(ssh_cmd)}")

        # Start SSH tunnel process
        try:
            proc = subprocess.Popen(
                ssh_cmd,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                stdin=subprocess.DEVNULL,
            )

            self.ssh_tunnels.append(proc)
            logger.info(f"SSH tunnel created for {secondary_id} (PID: {proc.pid})")

        except Exception as e:
            logger.error(f"Failed to create SSH tunnel for {secondary_id}: {e}")
            raise

    async def _async_sleep(self, seconds: float) -> None:
        """Async sleep helper"""
        import asyncio

        await asyncio.sleep(seconds)

    def cleanup(self) -> None:
        """Cleanup SLURM preparation resources"""
        logger.info("Cleaning up SLURM preparation resources...")

        # Terminate SSH tunnels
        for proc in self.ssh_tunnels:
            try:
                proc.terminate()
                proc.wait(timeout=5)
            except Exception as e:
                logger.warning(f"Failed to terminate SSH tunnel (PID: {proc.pid}): {e}")
                try:
                    proc.kill()
                except:
                    pass

        logger.info("SLURM preparation cleanup complete")
