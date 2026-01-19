"""Local test primary coordinator for debugging without SLURM/Docker.

This coordinator spawns secondary processes locally for testing the multi-computer
coordination logic without the overhead of SLURM job submission and Docker containers.
"""

import asyncio
import logging
import subprocess
import sys
from pathlib import Path
from typing import Any

from ...binary_info import BinaryInfo
from ...multi_computer import ConnectionResult, FileTransferMode, PreparationResult
from ...multi_computer.primary.coordinator import BaseCoordinator
from ...multi_computer.primary.file_utils import (
    compute_file_hash,
    compute_task_hash,
    send_initial_assignment_file_ready,
)
from ...task import TaskDefinition

logger = logging.getLogger(__name__)


class LocalTestPrimaryCoordinator(BaseCoordinator):
    """Local test primary coordinator.

    Spawns secondary processes locally for testing without SLURM/Docker.
    Files are already available locally, so no transfer is needed.
    """

    def __init__(
        self,
        binaries: list[BinaryInfo],
        task_definition: TaskDefinition,
        task_args: Any,
        run_id: str = "default",
        source_dir: Path | None = None,
        num_workers_per_secondary: int = 2,
        raw_logs: bool = False,
    ):
        super().__init__(binaries, task_definition, task_args, run_id, source_dir)

        self.num_workers_per_secondary = num_workers_per_secondary
        self.secondary_processes: list[subprocess.Popen] = []
        self.raw_logs = raw_logs

    async def prepare(self, num_secondaries: int, quic_port: int) -> PreparationResult:
        """Prepare local test mode - spawn secondary processes.

        Args:
            num_secondaries: Number of secondaries to spawn
            quic_port: Base QUIC port (primary's actual port)

        Returns:
            PreparationResult with local test data
        """
        logger.info("Phase 1: Local Test Preparation")
        logger.info(f"Spawning {num_secondaries} local secondary processes")

        # Primary is already listening on self.primary_quic_port (OS-allocated)
        primary_url = f"tcp://127.0.0.1:{self.primary_quic_port}"

        for i in range(num_secondaries):
            secondary_id = f"secondary-{i}"
            logger.info(f"Starting local secondary: {secondary_id}")

            # Build command to start secondary
            cmd = [
                sys.executable,
                "-m",
                "dynamic_batch",
                "--secondary",
                primary_url,
                "--secondary-id",
                secondary_id,
                "--secondary-quic-port",
                "0",  # Let OS allocate
            ]

            # Pass --raw-logs if set on primary
            if self.raw_logs:
                cmd.append("--raw-logs")

            logger.debug(f"Command: {' '.join(cmd)}")

            # Start secondary process without capturing output (let it print directly)
            try:
                proc = subprocess.Popen(cmd)
                self.secondary_processes.append(proc)
                logger.info(f"Started {secondary_id} (PID: {proc.pid})")

            except Exception as e:
                logger.error(f"Failed to start {secondary_id}: {e}")
                raise

        # Give secondaries a moment to start
        await asyncio.sleep(2)

        mode_specific_data = {
            "secondary_processes": self.secondary_processes,
        }

        # Generate primary entropy
        import secrets

        primary_entropy = secrets.token_bytes(32)

        return PreparationResult(
            num_secondaries=num_secondaries,
            run_id=self.run_id,
            cert_dir=self.cert_dir,
            primary_entropy=primary_entropy,
            mode_specific_data=mode_specific_data,
        )

    async def transfer_files(self, conn_result: ConnectionResult) -> None:
        """Send initial assignments with file_ready mode - files already available locally.

        Args:
            conn_result: Result from connection phase
        """
        logger.info("Phase 4: File transfer (local mode - file_ready)")
        logger.info("Files are already available locally, sending initial assignments")

        if len(self.secondaries) == 0:
            logger.warning("No secondaries connected")
            return

        if len(self.binaries) == 0:
            logger.info("No binaries to process")
            return

        # For each secondary, send initial_assignment with file_ready list
        for secondary_id, info in self.secondaries.items():
            # Get assigned tasks for this secondary
            assigned_binaries = [
                binary
                for binary in self.binaries
                if self.task_assignments.get(compute_task_hash(binary)) == secondary_id
            ]

            if not assigned_binaries:
                logger.info(f"No binaries assigned to {secondary_id}")
                continue

            logger.info(f"Sending file_ready assignment to {secondary_id}: {len(assigned_binaries)} binaries")

            # Build file_ready list with local paths and hashes
            file_ready_list = []
            for binary in assigned_binaries:
                if self.source_dir is None:
                    logger.error("source_dir is not set")
                    continue

                binary_path = self.source_dir / binary.path

                if not binary_path.exists():
                    logger.warning(f"Binary not found: {binary_path}")
                    continue

                # Compute hash for verification
                file_hash = compute_file_hash(binary_path)

                # Manually create binary_info dict
                binary_info_dict = {
                    "path": str(binary.path),
                    "size": binary.size,
                    "binary_name": binary.identifier.binary_name,
                    "platform": binary.identifier.platform,
                    "compiler": binary.identifier.compiler,
                    "version": binary.identifier.version,
                    "opt_level": binary.identifier.opt_level,
                }

                file_ready_list.append(
                    {
                        "hash": file_hash,
                        "path": str(binary_path),  # Send absolute path for local mode
                        "binary_info": binary_info_dict,
                    }
                )

            if not file_ready_list:
                logger.warning(f"No valid binaries for {secondary_id}")
                continue

            # Send initial_assignment with file_ready mode
            await send_initial_assignment_file_ready(
                secondary_id=secondary_id,
                file_ready_list=file_ready_list,
                worker_assignments=[],  # Will be populated later
                secondary_info=info,
                message_router=self.message_router,
                quic_transport=self.quic_transport,
            )

        logger.info("File_ready assignments sent to all secondaries")

    def get_file_transfer_mode(self) -> FileTransferMode:
        """Get file transfer mode for local test (skip transfer)."""
        return FileTransferMode.SKIP_TRANSFER

    async def _cleanup(self) -> None:
        """Cleanup local test resources."""
        logger.info("Cleaning up local test resources...")

        # Terminate secondary processes
        for proc in self.secondary_processes:
            if proc.poll() is None:  # Process still running
                logger.info(f"Terminating secondary process (PID: {proc.pid})")
                try:
                    proc.terminate()
                    # Wait briefly for graceful shutdown
                    try:
                        proc.wait(timeout=5)
                    except subprocess.TimeoutExpired:
                        logger.warning(f"Process {proc.pid} did not terminate, killing")
                        proc.kill()
                        proc.wait()
                except Exception as e:
                    logger.warning(f"Failed to terminate process {proc.pid}: {e}")

        # Cleanup base coordinator resources
        await super()._cleanup()

        logger.info("Local test cleanup complete")
