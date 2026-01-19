"""SLURM-specific file transfer phase for primary coordinator.

This module handles intelligent file distribution with deduplication for SLURM mode.
Files are transferred from the primary to secondaries via QUIC after discovery.
"""

import logging
import secrets
import zipfile
from pathlib import Path
from typing import Any

from ...binary_info import BinaryInfo
from ...multi_computer.file_distribution import FileDistributor
from ...multi_computer.primary.file_utils import (
    compute_file_hash,
    compute_task_hash,
    send_initial_assignment_zip,
)

logger = logging.getLogger(__name__)


class SlurmFileTransfer:
    """Handles SLURM-specific file transfer phase"""

    def __init__(
        self,
        slurm_config: Any,
        gateway: Any,
        source_dir: Path,
    ):
        self.slurm_config = slurm_config
        self.gateway = gateway
        self.source_dir = source_dir

        # File distributor for deduplication
        srcbins_dir = self.slurm_config.get_srcbins_dir()
        self.file_distributor = FileDistributor(Path(srcbins_dir))

    async def transfer_files(
        self,
        binaries: list[BinaryInfo],
        secondaries: dict[str, dict[str, Any]],
        task_assignments: dict[str, str],
        discovered_binaries: dict[str, dict[str, Any]],
        quic_transport: Any,
        message_router: Any,
    ) -> None:
        """Transfer files to secondaries with intelligent deduplication

        Args:
            binaries: List of all binaries to process
            secondaries: Dict of secondary info
            task_assignments: Task hash -> secondary_id assignments
            discovered_binaries: Already discovered binaries from first secondary
            quic_transport: QUIC transport for sending messages
            message_router: Message router for creating messages
        """
        logger.info("Phase 7: File distribution")

        if len(secondaries) == 0:
            logger.warning("No secondaries connected, skipping file distribution")
            return

        if len(binaries) == 0:
            logger.info("No binaries to distribute, skipping file distribution")
            return

        # Register discovered binaries for deduplication
        for file_hash, info in discovered_binaries.items():
            self.file_distributor.register_discovered_binary(file_hash, info["zip_name"], str(info["local_path"]))

        total_size = 0
        total_files = 0

        # Ensure srcbins directory exists on gateway
        srcbins_dir = self.slurm_config.get_srcbins_dir()
        self.gateway.create_directory(str(srcbins_dir))

        for secondary_id in secondaries:
            # Get assigned tasks for this secondary
            assigned_binaries = [
                binary for binary in binaries if task_assignments.get(compute_task_hash(binary)) == secondary_id
            ]

            if not assigned_binaries:
                logger.info(f"No binaries assigned to {secondary_id}")
                continue

            logger.info(f"Distributing {len(assigned_binaries)} binaries to {secondary_id}")

            # Create batched ZIPs for efficient transfer
            zip_batches = self._create_zip_batches(assigned_binaries)

            # Upload ZIPs to gateway and send to secondary
            zip_files_info = []
            for batch_idx, (batch_binaries, batch_size) in enumerate(zip_batches):
                # Create unique ZIP name
                random_suffix = secrets.token_hex(8)
                zip_name = f"{secondary_id}_batch_{batch_idx}_{random_suffix}.zip"
                zip_path = f"{srcbins_dir}/{zip_name}"

                # Create ZIP with batch binaries
                zip_info = await self._create_and_upload_zip(zip_path, batch_binaries)
                if zip_info:
                    zip_files_info.append(zip_info)
                    total_files += len(batch_binaries)
                    total_size += batch_size

            # Send initial assignment to secondary
            await send_initial_assignment_zip(
                secondary_id=secondary_id,
                zip_files_info=zip_files_info,
                worker_assignments=[],  # Will be populated later
                secondary_info=secondaries[secondary_id],
                message_router=message_router,
                quic_transport=quic_transport,
            )

        logger.info(f"Distribution complete: {total_files} files, {total_size / (1024**3):.2f}GB")

    def _create_zip_batches(
        self, binaries: list[BinaryInfo], min_batch_size_mb: float = 20.0
    ) -> list[tuple[list[BinaryInfo], int]]:
        """Create batches of binaries for efficient ZIP transfer

        Args:
            binaries: List of binaries to batch
            min_batch_size_mb: Minimum batch size in MB

        Returns:
            List of (batch_binaries, total_size) tuples
        """
        min_batch_size_bytes = int(min_batch_size_mb * 1024 * 1024)

        # Get binary sizes and check deduplication
        binary_sizes: list[tuple[BinaryInfo, int, bool]] = []  # (binary, size, already_sent)

        for binary in binaries:
            binary_path = self.source_dir / binary.path

            if not binary_path.exists():
                logger.warning(f"Binary not found: {binary_path}")
                continue

            already_sent, _ = self.file_distributor.is_already_sent(binary_path)
            file_size = binary_path.stat().st_size if not already_sent else 0

            binary_sizes.append((binary, file_size, already_sent))

        # Sort by size (largest first) for better batching
        binary_sizes.sort(key=lambda x: x[1], reverse=True)

        batches: list[tuple[list[BinaryInfo], int]] = []
        current_batch: list[BinaryInfo] = []
        current_batch_size = 0

        for binary, file_size, already_sent in binary_sizes:
            # If binary is larger than min batch size, give it its own batch
            if file_size >= min_batch_size_bytes:
                # Flush current batch if not empty
                if current_batch:
                    batches.append((current_batch, current_batch_size))
                    current_batch = []
                    current_batch_size = 0

                # Create single-file batch
                batches.append(([binary], file_size))

            else:
                # Add to current batch
                current_batch.append(binary)
                current_batch_size += file_size

                # If batch is large enough, flush it
                if current_batch_size >= min_batch_size_bytes:
                    batches.append((current_batch, current_batch_size))
                    current_batch = []
                    current_batch_size = 0

        # Flush remaining batch
        if current_batch:
            batches.append((current_batch, current_batch_size))

        logger.debug(f"Created {len(batches)} batches for transfer")

        return batches

    async def _create_and_upload_zip(self, zip_path: str, binaries: list[BinaryInfo]) -> dict[str, Any] | None:
        """Create ZIP file locally and upload to gateway

        Args:
            zip_path: Remote path for ZIP on gateway
            binaries: Binaries to include in ZIP

        Returns:
            Dict with ZIP info or None on failure
        """
        # Create temporary local ZIP
        import tempfile

        with tempfile.NamedTemporaryFile(suffix=".zip", delete=False) as tmp_file:
            local_zip_path = Path(tmp_file.name)

        try:
            # Create ZIP with binaries (excluding duplicates)
            files_in_zip: list[str] = []
            total_size = 0

            with zipfile.ZipFile(local_zip_path, "w", compression=zipfile.ZIP_STORED) as zf:
                for binary in binaries:
                    binary_path = self.source_dir / binary.path

                    if not binary_path.exists():
                        continue

                    # Check if already sent
                    already_sent, file_hash = self.file_distributor.is_already_sent(binary_path)

                    if not already_sent:
                        arcname = str(binary.path)
                        zf.write(binary_path, arcname)
                        files_in_zip.append(arcname)
                        total_size += binary_path.stat().st_size

            if not files_in_zip:
                logger.debug(f"All binaries in batch already sent, skipping ZIP creation")
                local_zip_path.unlink()
                return None

            # Compute ZIP hash
            zip_hash = compute_file_hash(local_zip_path)

            # Upload to gateway
            logger.info(
                f"Uploading ZIP: {Path(zip_path).name} ({len(files_in_zip)} files, {total_size / (1024**2):.1f}MB)"
            )
            self.gateway.upload_file(str(local_zip_path), zip_path)

            # Clean up local ZIP
            local_zip_path.unlink()

            return {
                "zip_name": Path(zip_path).name,
                "zip_path": zip_path,
                "zip_hash": zip_hash,
                "files": files_in_zip,
                "size": total_size,
            }

        except Exception as e:
            logger.error(f"Failed to create/upload ZIP: {e}")
            if local_zip_path.exists():
                local_zip_path.unlink()
            return None
