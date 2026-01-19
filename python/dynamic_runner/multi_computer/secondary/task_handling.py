import hashlib
import logging
import zipfile
from pathlib import Path
from typing import TYPE_CHECKING, Any

from shared.binary_info import BinaryIdentifier

from ...binary_info import BinaryInfo

if TYPE_CHECKING:
    from .coordinator import SecondaryCoordinator

logger = logging.getLogger(__name__)


class TaskHandler:
    """Handles task assignment, file operations, and source discovery"""

    def __init__(self, coordinator: "SecondaryCoordinator"):
        self.coordinator = coordinator

    def compute_file_hash(self, path: Path) -> str:
        """Compute SHA256 hash of a file"""
        sha256 = hashlib.sha256()
        with open(path, "rb") as f:
            for chunk in iter(lambda: f.read(8192), b""):
                sha256.update(chunk)
        return sha256.hexdigest()

    def extract_binary_from_zip(self, zip_name: str, local_path: str, file_hash: str) -> Path | None:
        """Extract a binary from a ZIP file in src_network to src_tmp"""
        # Check if already extracted
        if file_hash in self.coordinator.extracted_binaries:
            return self.coordinator.extracted_binaries[file_hash]

        zip_path = self.coordinator.src_network / zip_name
        if not zip_path.exists():
            logger.error(f"ZIP file not found: {zip_path}")
            return None

        try:
            with zipfile.ZipFile(zip_path, "r") as zf:
                # Extract specific file
                extracted_path = self.coordinator.src_tmp / Path(local_path).name

                # Read from ZIP and write to target
                with zf.open(local_path) as source:
                    with open(extracted_path, "wb") as target:
                        target.write(source.read())

                # Verify hash
                computed_hash = self.compute_file_hash(extracted_path)
                if computed_hash != file_hash:
                    logger.error(f"Hash mismatch for {local_path}: expected {file_hash}, got {computed_hash}")
                    extracted_path.unlink()
                    return None

                # Cache the extraction
                self.coordinator.extracted_binaries[file_hash] = extracted_path
                logger.debug(f"Extracted {local_path} from {zip_name} to {extracted_path}")
                return extracted_path

        except Exception as e:
            logger.error(f"Failed to extract {local_path} from {zip_name}: {e}")
            return None

    def get_file_by_hash(self, file_hash: str, file_path: str | None = None) -> Path | None:
        """Get file by hash - either from extracted cache or from direct path"""
        # Check if already extracted
        if file_hash in self.coordinator.extracted_binaries:
            return self.coordinator.extracted_binaries[file_hash]

        # If file_path is provided (file-ready mode), use it directly
        if file_path:
            # Try as absolute path first (local mode)
            direct_path = Path(file_path)
            if direct_path.exists() and direct_path.is_absolute():
                # Verify hash
                computed_hash = self.compute_file_hash(direct_path)
                if computed_hash == file_hash:
                    self.coordinator.extracted_binaries[file_hash] = direct_path
                    return direct_path
                else:
                    logger.error(f"Hash mismatch for {file_path}: expected {file_hash}, got {computed_hash}")
                    return None

            # Fall back to relative path in src_tmp (Docker/SLURM mode)
            direct_path = self.coordinator.src_tmp / Path(file_path).name
            if direct_path.exists():
                # Verify hash
                computed_hash = self.compute_file_hash(direct_path)
                if computed_hash == file_hash:
                    self.coordinator.extracted_binaries[file_hash] = direct_path
                    return direct_path
                else:
                    logger.error(f"Hash mismatch for {file_path}: expected {file_hash}, got {computed_hash}")
                    return None

        return None

    async def handle_initial_assignment(self, message: dict[str, Any], sender_id: str | None) -> None:
        """Handle initial assignment message from primary"""
        logger.info("Received initial assignment from primary")

        zip_files = message.get("zip_files", [])
        worker_assignments = message.get("worker_assignments", [])
        file_ready_list = message.get("file_ready", [])

        # Handle file-ready mode (files already present, no extraction needed)
        if file_ready_list:
            logger.info(f"Initial assignment uses file-ready mode with {len(file_ready_list)} files")
            for file_info in file_ready_list:
                file_hash = file_info.get("hash")
                file_path = file_info.get("path")

                if not file_hash or not file_path:
                    logger.error(f"File-ready entry missing required fields: {file_info}")
                    continue

                # Try as absolute path first (local mode)
                direct_path = Path(file_path)
                if direct_path.exists() and direct_path.is_absolute():
                    # Verify hash for absolute paths
                    computed_hash = self.compute_file_hash(direct_path)
                    if computed_hash == file_hash:
                        self.coordinator.extracted_binaries[file_hash] = direct_path
                        logger.debug(f"Registered file-ready (absolute): {direct_path}")
                    else:
                        logger.error(f"Hash mismatch for {file_path}: expected {file_hash}, got {computed_hash}")
                    continue

                # Fall back to relative path in src_tmp (Docker/SLURM mode)
                direct_path = self.coordinator.src_tmp / Path(file_path).name
                if direct_path.exists():
                    self.coordinator.extracted_binaries[file_hash] = direct_path
                    logger.debug(f"Registered file-ready (relative): {direct_path.name}")
                else:
                    logger.warning(f"File-ready path does not exist: {direct_path}")

        # Handle ZIP extraction mode
        elif zip_files:
            if not zip_files:
                logger.error("Initial assignment contains no ZIP files!")
                return

            logger.info(f"Initial assignment contains {len(zip_files)} ZIP files")

            # Extract all binaries from ZIPs
            for zip_info in zip_files:
                zip_name = zip_info.get("zip_name")
                binaries = zip_info.get("binaries", [])

                if not zip_name:
                    logger.error(f"ZIP info missing zip_name: {zip_info}")
                    continue

                logger.info(f"Processing ZIP: {zip_name} with {len(binaries)} binaries")

                for binary_entry in binaries:
                    local_path = binary_entry.get("local_path")
                    file_hash = binary_entry.get("hash")

                    if not local_path or not file_hash:
                        logger.error(f"Binary entry missing required fields: {binary_entry}")
                        continue

                    # Extract the binary
                    extracted_path = self.extract_binary_from_zip(zip_name, local_path, file_hash)
                    if not extracted_path:
                        logger.error(f"Failed to extract {local_path} from {zip_name}")
                        continue

                    logger.debug(f"Extracted: {extracted_path.name}")

            logger.info(
                f"Extracted {len(self.coordinator.extracted_binaries)} binaries from {len(zip_files)} ZIP files"
            )

        if not worker_assignments:
            logger.warning("Initial assignment contains no worker assignments")
            return

        # Store worker assignments to process after transfer_complete
        self.coordinator.pending_worker_assignments = worker_assignments

        logger.info(f"Waiting for transfer_complete before assigning {len(worker_assignments)} tasks to workers")

    async def handle_task_assignment(self, message: dict[str, Any], sender_id: str | None) -> None:
        """Handle task assignment message from primary"""
        worker_id = message.get("worker_id")
        zip_file = message.get("zip_file")
        local_path = message.get("local_path")
        file_hash = message.get("file_hash")
        file_path = message.get("file_path")  # For file-ready mode
        binary_info_dict = message.get("binary_info")
        estimated_memory = message.get("estimated_memory", 0)

        if not self.coordinator.worker_manager:
            logger.error("Worker manager not initialized")
            return

        if not isinstance(worker_id, int) or worker_id >= len(self.coordinator.worker_manager.workers):
            logger.error(f"Invalid worker_id {worker_id}")
            return

        # Get the binary file
        extracted_path = None

        # Check if this is file-ready mode (file already present)
        if file_path and file_hash:
            extracted_path = self.get_file_by_hash(file_hash, file_path)
            if not extracted_path:
                logger.error(f"File-ready binary not found for worker {worker_id}: {file_path}")
                return

        # Extract binary if from ZIP
        elif zip_file and local_path and file_hash:
            extracted_path = self.extract_binary_from_zip(zip_file, local_path, file_hash)
            if not extracted_path:
                logger.error(f"Failed to extract binary for worker {worker_id}")
                return

        # Already extracted or registered
        elif file_hash:
            extracted_path = self.coordinator.extracted_binaries.get(file_hash)
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
        success = self.coordinator.worker_manager.assign_task_from_primary(worker_id, binary_info, estimated_memory)
        if success:
            logger.info(f"Assigned task to worker {worker_id}: {extracted_path.name}")
        else:
            logger.error(f"Failed to assign task to worker {worker_id}")

    async def notify_task_complete(self, worker_id: int, task_hash: str) -> None:
        """Notify all peers that a task completed"""
        msg = {
            "type": "task_complete",
            "secondary_id": self.coordinator.secondary_id,
            "worker_id": worker_id,
            "task_hash": task_hash,
            "warnings": 0,
            "filtered": 0,
        }

        # Broadcast to all peers
        try:
            if len(self.coordinator.message_router.secondary_connections) > 0:
                await self.coordinator.message_router.broadcast_to_secondaries(msg)
        except Exception as e:
            logger.warning(f"Failed to notify task complete: {e}")

        logger.info(f"Task completed: worker {worker_id}, hash {task_hash}")

    async def handle_discover_sources(self, message: dict[str, Any], sender_id: str | None) -> None:
        """Handle source discovery request from primary"""
        logger.info(f"Starting source discovery in {self.coordinator.src_network}")

        discovered_count = 0

        try:
            # Scan src_network directory for ZIP files
            for zip_path in self.coordinator.src_network.glob("*.zip"):
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
                actual_hash = self.compute_file_hash(zip_path)
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

                            await self.coordinator.message_router.send_to_primary(msg)
                            discovered_count += 1
                            logger.debug(f"Discovered: {info.filename} in {zip_path.name}")

                except Exception as e:
                    logger.error(f"Failed to process ZIP {zip_path.name}: {e}")

            logger.info(f"Source discovery complete: reported {discovered_count} binaries")

        except Exception as e:
            logger.error(f"Source discovery failed: {e}")

    async def handle_transfer_complete(self, message: dict[str, Any], sender_id: str | None) -> None:
        """Handle transfer complete notification from primary"""
        logger.info("Received transfer complete notification from primary")
        self.coordinator.transfer_complete = True

        # Now process pending worker assignments
        if self.coordinator.pending_worker_assignments:
            logger.info(f"Processing {len(self.coordinator.pending_worker_assignments)} pending worker assignments")
            await self.process_initial_worker_assignments()
        else:
            logger.warning("No pending worker assignments to process")

    async def process_initial_worker_assignments(self) -> None:
        """Process initial worker assignments after transfer is complete"""
        if not self.coordinator.worker_manager:
            logger.error("Worker manager not initialized")
            return

        for assignment in self.coordinator.pending_worker_assignments:
            worker_id = assignment.get("worker_id")
            file_hash = assignment.get("file_hash")
            estimated_memory = assignment.get("estimated_memory", 0)
            opportunistic = assignment.get("opportunistic", False)
            binary_info_dict = assignment.get("binary_info", {})

            if worker_id is None or not file_hash:
                logger.error(f"Invalid worker assignment: {assignment}")
                continue

            # Get extracted binary path
            extracted_path = self.coordinator.extracted_binaries.get(file_hash)
            if not extracted_path:
                logger.error(f"Binary not found for hash {file_hash}")
                continue

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
                continue

            # Assign to worker via SubmissiveManager
            success = self.coordinator.worker_manager.assign_task_from_primary(worker_id, binary_info, estimated_memory)

            opp_str = " (opportunistic)" if opportunistic else ""
            if success:
                logger.info(f"[Worker {worker_id}] Assigned initial task: {extracted_path.name}{opp_str}")
            else:
                logger.error(f"[Worker {worker_id}] Failed to assign initial task: {extracted_path.name}")

        logger.info("Initial worker assignments complete")

    async def handle_promote_primary(self, message: dict[str, Any], sender_id: str | None) -> None:
        """Handle primary promotion notification"""
        slurm_primary_id = message.get("slurm_primary_id")
        logger.info(f"Received primary promotion notification: {slurm_primary_id} is now SLURM-primary")
        self.coordinator.slurm_primary_id = slurm_primary_id
        self.coordinator.is_slurm_primary = slurm_primary_id == self.coordinator.secondary_id
        if self.coordinator.is_slurm_primary:
            logger.info("This secondary has been promoted to SLURM-primary")

    async def handle_full_task_list(self, message: dict[str, Any], sender_id: str | None) -> None:
        """Handle full task list from primary"""
        all_tasks = message.get("all_tasks", [])
        completed_tasks = message.get("completed_tasks", [])

        logger.info(f"Received full task list: {len(all_tasks)} total tasks, {len(completed_tasks)} completed")

        self.coordinator.all_tasks = all_tasks
        self.coordinator.completed_tasks = set(completed_tasks)
