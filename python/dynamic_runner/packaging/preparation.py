"""SLURM-specific preparation phase for primary coordinator.

Owns:
- Container image build invocation (delegates to PodmanPackaging via job_manager)
- Gateway transfer of the image artifacts
- SLURM job submission via job_manager
- SSH tunnel setup for reverse connections (when the compute nodes can't
  reach the primary directly)
"""

import logging
import subprocess
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from ..deployment_spec import TaskDeploymentSpec
from .podman import PodmanImageMetadata

logger = logging.getLogger(__name__)


@dataclass
class PreparationResult:
    """Result of preparation phase. Mirrors the legacy
    `multi_computer.PreparationResult` shape so the SLURM pipeline
    return value stays stable.
    """

    num_secondaries: int
    run_id: str
    cert_dir: Path
    primary_entropy: bytes
    mode_specific_data: dict[str, Any] = field(default_factory=dict)


class SlurmPreparation:
    """Handles SLURM-specific preparation phase."""

    def __init__(
        self,
        slurm_config: Any,
        job_manager: Any,
        gateway: Any,
        deployment: TaskDeploymentSpec,
        use_reverse_connection: bool = False,
        run_id: str = "default",
    ):
        self.slurm_config = slurm_config
        self.job_manager = job_manager
        self.gateway = gateway
        self.deployment = deployment
        self.use_reverse_connection = use_reverse_connection
        self.run_id = run_id

        base_log_dir = self.slurm_config.get_log_dir()
        self.run_log_dir = f"{base_log_dir}/{run_id}"

        self.secondary_port_map: dict[str, int] = {}
        self.ssh_tunnels: list[subprocess.Popen[Any]] = []

    async def prepare(
        self,
        num_secondaries: int,
        quic_port: int,
        primary_quic_port: int,
        cert_dir: Path,
        skip_image_build: bool = False,
    ) -> PreparationResult:
        """Execute SLURM preparation phase."""
        logger.info("Phase 1: SLURM Preparation")
        self.job_manager.prepare_directories()
        self.gateway.create_directory(self.run_log_dir)

        image_metadata = await self._prepare_docker_images(skip_image_build)
        self._submit_slurm_jobs(num_secondaries, primary_quic_port, image_metadata)

        if self.use_reverse_connection:
            await self._setup_ssh_tunnels(num_secondaries)

        mode_specific_data = {
            "image_metadata": image_metadata,
            "run_log_dir": self.run_log_dir,
            "secondary_port_map": self.secondary_port_map,
            "ssh_tunnels": self.ssh_tunnels,
        }

        import secrets

        primary_entropy = secrets.token_bytes(32)

        return PreparationResult(
            num_secondaries=num_secondaries,
            run_id=self.run_id,
            cert_dir=cert_dir,
            primary_entropy=primary_entropy,
            mode_specific_data=mode_specific_data,
        )

    async def _prepare_docker_images(self, skip_image_build: bool) -> PodmanImageMetadata:
        """Build and transfer the docker image, or verify existing path."""
        image_dir = Path(self.job_manager._expand_path(self.slurm_config.get_image_dir()))
        image_path = image_dir / self.deployment.image_tar_basename

        if skip_image_build:
            logger.info("Skipping image build and transfer (--skip-image-build)")
            logger.info("Assuming image exists at: %s", image_path)
            return PodmanImageMetadata(
                remote_path=image_path,
                image_hash="",
                uploaded=False,
            )

        project_root = Path.cwd()
        image_metadata = self.job_manager.build_and_transfer_images(project_root)

        logger.info(
            "Image %s at: %s",
            "uploaded" if image_metadata.uploaded else "reused",
            image_metadata.remote_path,
        )
        logger.info("Image hash: %s", image_metadata.image_hash)

        return image_metadata

    def _submit_slurm_jobs(
        self,
        num_secondaries: int,
        primary_quic_port: int,
        image_metadata: PodmanImageMetadata,
    ) -> None:
        """Submit SLURM jobs for secondaries."""
        logger.info("Submitting SLURM jobs...")
        gateway_host = self._determine_gateway_host()

        for i in range(num_secondaries):
            secondary_id = f"secondary-{i}"
            job_name = f"{self.deployment.effective_job_name_prefix}-{secondary_id}"

            wrapper = self.job_manager.generate_wrapper_script(
                image_metadata=image_metadata,
                secondary_id=secondary_id,
                gateway_host=gateway_host,
                gateway_port=primary_quic_port,
                reverse_connection=self.use_reverse_connection,
                run_log_dir=self.run_log_dir,
            )

            job_id = self.job_manager.submit_job(wrapper, job_name, run_log_dir=self.run_log_dir)
            logger.info("Submitted job %s for %s", job_id, secondary_id)

        logger.info("All %d jobs submitted", num_secondaries)

    def _determine_gateway_host(self) -> str:
        """Determine the hostname that compute nodes should use to reach the gateway."""
        if hasattr(self.gateway, "host") and self.gateway.host:
            logger.info("Detecting gateway hostname for compute nodes...")
            returncode, stdout, _stderr = self.gateway.execute_command("hostname -f")
            if returncode == 0 and stdout.strip():
                gateway_host = stdout.strip()
                logger.info("Using gateway FQDN: %s", gateway_host)
            else:
                gateway_host = self.gateway.host
                logger.warning("Could not detect gateway FQDN, using SSH host: %s", gateway_host)
        else:
            gateway_host = "localhost"
            logger.info("Using local gateway host: %s", gateway_host)

        return gateway_host

    async def _setup_ssh_tunnels(self, num_secondaries: int) -> None:
        """Setup SSH ProxyJump tunnels for reverse connections."""
        logger.info("Setting up SSH ProxyJump tunnels for reverse connections...")

        connection_info_dir = f"{self.run_log_dir}/connection_info"
        self.gateway.create_directory(connection_info_dir)

        connected: set[str] = set()
        timeout = 600
        start_time = time.time()

        while len(connected) < num_secondaries:
            if time.time() - start_time > timeout:
                logger.error(
                    "Timeout waiting for secondary connection info. Found: %d/%d",
                    len(connected),
                    num_secondaries,
                )
                raise TimeoutError("Failed to get all secondary connection info")

            for i in range(num_secondaries):
                secondary_id = f"secondary-{i}"
                if secondary_id in connected:
                    continue

                info_file = f"{connection_info_dir}/{secondary_id}.txt"
                returncode, stdout, _stderr = self.gateway.execute_command(f"cat {info_file}")

                if returncode == 0 and stdout.strip():
                    lines = stdout.strip().split("\n")
                    if len(lines) >= 2:
                        hostname = lines[0].split("=")[1].strip()
                        port = int(lines[1].split("=")[1].strip())

                        logger.info("Found connection info for %s: %s:%d", secondary_id, hostname, port)

                        local_port = 5001 + i
                        self._create_ssh_tunnel(secondary_id, hostname, port, local_port)

                        self.secondary_port_map[secondary_id] = local_port
                        connected.add(secondary_id)

            if len(connected) < num_secondaries:
                await self._async_sleep(2)

        logger.info("All %d SSH tunnels established", num_secondaries)

    def _create_ssh_tunnel(self, secondary_id: str, remote_host: str, remote_port: int, local_port: int) -> None:
        """Create an SSH tunnel from primary to secondary via gateway."""
        gateway_host = self.gateway.host if hasattr(self.gateway, "host") else "localhost"
        gateway_user = self.gateway.user if hasattr(self.gateway, "user") else None
        remote_user = gateway_user or "root"

        jump_host = f"{gateway_user}@{gateway_host}" if gateway_user else gateway_host

        ssh_cmd = [
            "ssh",
            "-J",
            jump_host,
            "-L",
            f"{local_port}:localhost:{remote_port}",
            f"{remote_user}@{remote_host}",
            "-N",
            "-o",
            "StrictHostKeyChecking=no",
            "-o",
            "UserKnownHostsFile=/dev/null",
        ]

        logger.info(
            "Creating SSH tunnel for %s: localhost:%d -> %s:%d",
            secondary_id,
            local_port,
            remote_host,
            remote_port,
        )
        logger.debug("SSH command: %s", " ".join(ssh_cmd))

        proc = subprocess.Popen(
            ssh_cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            stdin=subprocess.DEVNULL,
        )
        self.ssh_tunnels.append(proc)
        logger.info("SSH tunnel created for %s (PID: %s)", secondary_id, proc.pid)

    async def _async_sleep(self, seconds: float) -> None:
        import asyncio

        await asyncio.sleep(seconds)

    def cleanup(self) -> None:
        """Cleanup SLURM preparation resources."""
        logger.info("Cleaning up SLURM preparation resources...")

        for proc in self.ssh_tunnels:
            try:
                proc.terminate()
                proc.wait(timeout=5)
            except Exception:
                try:
                    proc.kill()
                except Exception:
                    pass

        logger.info("SLURM preparation cleanup complete")
