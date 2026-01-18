import logging
import secrets
import time
from pathlib import Path
from typing import Any

from ..binary_info import BinaryInfo
from .protocol import (
    EntropyMessage,
    FullTaskListMessage,
    InitialAssignmentMessage,
    PeerInfoMessage,
    PromotePrimaryMessage,
    TaskAssignmentMessage,
    TransferCompleteMessage,
)

logger = logging.getLogger(__name__)


class PrimaryCoordinator:
    """Coordinates primary orchestration in SLURM distributed mode"""

    def __init__(self, binaries: list[BinaryInfo], slurm_config: Any, job_manager: Any):
        self.binaries = binaries
        self.slurm_config = slurm_config
        self.job_manager = job_manager

        self.secondaries: dict[str, dict[str, Any]] = {}
        self.workers: dict[str, list[dict[str, Any]]] = {}
        self.task_assignments: dict[str, str] = {}  # task_hash -> secondary_id
        self.completed_tasks: set[str] = set()
        self.failed_tasks: set[str] = set()

        self.primary_entropy = secrets.token_bytes(32)
        self.peer_info: list[dict[str, Any]] = []

        self.running = True
        self.transfer_complete = False
        self.slurm_primary_id: str | None = None

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

        try:
            # Phase 1: Submit SLURM jobs
            self._submit_slurm_jobs(num_secondaries, quic_port)

            # Phase 2: Wait for secondaries to connect
            self._wait_for_secondaries(num_secondaries)

            # Phase 3: Certificate exchange
            self._exchange_certificates()

            # Phase 4: Wait for workers ready
            self._wait_for_workers()

            # Phase 5: Preliminary assignment
            self._preliminary_assignment()

            # Phase 6: Source discovery from first secondary
            self._source_discovery()

            # Phase 7: Intelligent file distribution
            self._distribute_files()

            # Phase 8: Notify transfer complete
            self._notify_transfer_complete()

            # Phase 9: Promote SLURM-primary
            self._promote_slurm_primary()

            # Phase 10: Send full task list
            self._send_full_task_list()

            # Phase 11: Monitor until user disconnects
            self._monitor_mode()

        except KeyboardInterrupt:
            logger.info("Received interrupt signal")
        except Exception as e:
            logger.error(f"Primary coordinator error: {e}", exc_info=True)
        finally:
            self._cleanup()

    def _submit_slurm_jobs(self, num_secondaries: int, base_port: int) -> None:
        """Submit SLURM jobs for secondaries"""
        logger.info("Submitting SLURM jobs...")

        # Get primary host and port
        import socket

        primary_host = socket.gethostname()
        primary_port = base_port

        for i in range(num_secondaries):
            secondary_port = base_port + i + 1
            job_name = f"asm-tokenizer-secondary-{i}"

            # Generate wrapper script
            wrapper = self.job_manager.generate_wrapper_script(
                image_path=self.slurm_config.get_image_dir() / "asm-tokenizer-docker.tar",
                secondary_port=secondary_port,
                primary_host=primary_host,
                primary_port=primary_port,
            )

            # Submit job
            job_id = self.job_manager.submit_job(wrapper, job_name)
            logger.info(f"Submitted job {job_id} for secondary {i}")

        logger.info(f"All {num_secondaries} jobs submitted")

    def _wait_for_secondaries(self, expected_count: int) -> None:
        """Wait for secondaries to connect and send welcome"""
        logger.info(f"Waiting for {expected_count} secondaries to connect...")

        # TODO: Listen for SecondaryWelcomeMessage from each secondary
        # TODO: Store secondary info (id, ram, worker_count, hostname)

        timeout = 600  # 10 minutes
        start_time = time.time()

        while len(self.secondaries) < expected_count:
            if time.time() - start_time > timeout:
                raise TimeoutError(f"Timeout waiting for secondaries. Got {len(self.secondaries)}/{expected_count}")

            # TODO: Process incoming connections
            time.sleep(1)

        logger.info(f"All {expected_count} secondaries connected")

    def _exchange_certificates(self) -> None:
        """Exchange certificates with secondaries"""
        logger.info("Performing certificate exchange...")

        # Send entropy to all secondaries
        for secondary_id in self.secondaries:
            msg = EntropyMessage(
                sender_id="primary",
                timestamp=time.time(),
                entropy_hex=self.primary_entropy.hex(),
            )
            # TODO: Send to secondary
            logger.debug(f"Sent entropy to {secondary_id}")

        # Wait for certificate exchange responses
        # TODO: Receive CertExchangeMessage from each secondary
        # TODO: Build peer_info list

        # Broadcast peer info to all secondaries
        for secondary_id in self.secondaries:
            msg = PeerInfoMessage(
                sender_id="primary",
                timestamp=time.time(),
                peers=self.peer_info,
            )
            # TODO: Send to secondary
            logger.debug(f"Sent peer info to {secondary_id}")

        logger.info("Certificate exchange complete")

    def _wait_for_workers(self) -> None:
        """Wait for all workers to report ready"""
        logger.info("Waiting for workers to report ready...")

        # TODO: Receive ReadyResponse from each worker
        # TODO: Store worker info with memory budgets

        expected_workers = sum(s["worker_count"] for s in self.secondaries.values())
        timeout = 300  # 5 minutes
        start_time = time.time()

        total_workers = sum(len(workers) for workers in self.workers.values())
        while total_workers < expected_workers:
            if time.time() - start_time > timeout:
                raise TimeoutError(f"Timeout waiting for workers. Got {total_workers}/{expected_workers}")

            # TODO: Process worker ready messages
            time.sleep(0.5)
            total_workers = sum(len(workers) for workers in self.workers.values())

        logger.info(f"All {expected_workers} workers ready")

    def _preliminary_assignment(self) -> None:
        """Assign initial tasks to secondaries"""
        logger.info("Performing preliminary task assignment...")

        # Distribute binaries across secondaries based on memory
        total_binaries = len(self.binaries)
        binaries_per_secondary = total_binaries // len(self.secondaries)

        for idx, (secondary_id, info) in enumerate(self.secondaries.items()):
            start_idx = idx * binaries_per_secondary
            end_idx = start_idx + binaries_per_secondary if idx < len(self.secondaries) - 1 else total_binaries

            assigned = self.binaries[start_idx:end_idx]
            for binary in assigned:
                task_hash = self._compute_task_hash(binary)
                self.task_assignments[task_hash] = secondary_id

            logger.info(f"Assigned {len(assigned)} tasks to {secondary_id}")

        logger.info("Preliminary assignment complete")

    def _source_discovery(self) -> None:
        """First secondary discovers and reports source binaries"""
        logger.info("Starting source discovery phase...")

        # Get first secondary
        first_secondary = next(iter(self.secondaries.keys()))
        logger.info(f"Using {first_secondary} for source discovery")

        # TODO: First secondary scans /app/src-network
        # TODO: Opens ZIPs matching .hash files
        # TODO: Sends (zip_name, local_path, binary_info, hash) to primary

        logger.info("Source discovery complete")

    def _distribute_files(self) -> None:
        """Distribute files to secondaries with intelligent deduplication"""
        logger.info("Starting file distribution...")

        # TODO: For each secondary's assigned tasks:
        # TODO:   Check if hash matches first secondary's discovered binaries
        # TODO:   If match, mark as already_sent
        # TODO:   Create ZIP of non-duplicate files
        # TODO:   Stream ZIP to srcbins/{unique}_{random}.zip
        # TODO:   Send InitialAssignmentMessage with ZIP locations

        total_size = 0
        total_files = 0

        for secondary_id in self.secondaries:
            # TODO: Build ZIP for this secondary
            # TODO: Track size and file count
            pass

        logger.info(f"Distribution complete: {total_files} files, {total_size / (1024**3):.2f}GB")

    def _notify_transfer_complete(self) -> None:
        """Notify all secondaries that transfer is complete"""
        logger.info("Notifying secondaries: transfer complete")

        msg = TransferCompleteMessage(
            sender_id="primary",
            timestamp=time.time(),
            total_files=len(self.binaries),
            total_bytes=0,  # TODO: Track actual bytes
        )

        for secondary_id in self.secondaries:
            # TODO: Send to secondary
            logger.debug(f"Sent transfer complete to {secondary_id}")

        self.transfer_complete = True
        logger.info("Transfer complete notification sent")

    def _promote_slurm_primary(self) -> None:
        """Promote a random secondary to SLURM-primary role"""
        import random

        self.slurm_primary_id = random.choice(list(self.secondaries.keys()))
        logger.info(f"Promoting {self.slurm_primary_id} to SLURM-primary")

        msg = PromotePrimaryMessage(
            sender_id="primary",
            timestamp=time.time(),
            new_primary_id=self.slurm_primary_id,
        )

        for secondary_id in self.secondaries:
            # TODO: Send to secondary
            logger.debug(f"Sent promotion to {secondary_id}")

        logger.info("SLURM-primary promotion complete")

    def _send_full_task_list(self) -> None:
        """Send complete task list to all secondaries"""
        logger.info("Sending full task list to all secondaries...")

        all_tasks = [
            {
                "hash": self._compute_task_hash(binary),
                "binary_info": binary.__dict__,
            }
            for binary in self.binaries
        ]

        msg = FullTaskListMessage(
            sender_id="primary",
            timestamp=time.time(),
            all_tasks=all_tasks,
            completed_tasks=list(self.completed_tasks),
        )

        for secondary_id in self.secondaries:
            # TODO: Send to secondary
            logger.debug(f"Sent task list to {secondary_id}")

        logger.info("Full task list sent")

    def _monitor_mode(self) -> None:
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
            time.sleep(5)

    def _cleanup(self) -> None:
        """Clean up resources"""
        logger.info("Cleaning up primary coordinator resources")
        # TODO: Close connections
        # TODO: Flush pending data
        pass

    def _compute_task_hash(self, binary: BinaryInfo) -> str:
        """Compute unique hash for task"""
        import hashlib

        data = f"{binary.path}|{binary.platform}|{binary.compiler}"
        return hashlib.sha256(data.encode()).hexdigest()[:16]

    def _handle_task_complete(self, secondary_id: str, task_hash: str) -> None:
        """Handle task completion notification"""
        self.completed_tasks.add(task_hash)
        logger.info(f"Task complete: {task_hash} (by {secondary_id})")

    def _handle_task_failed(self, secondary_id: str, task_hash: str, error: str) -> None:
        """Handle task failure notification"""
        self.failed_tasks.add(task_hash)
        logger.warning(f"Task failed: {task_hash} (by {secondary_id}): {error}")

    def _handle_task_request(self, secondary_id: str, worker_id: int, available_memory: int) -> None:
        """Handle request for new task from secondary"""
        # TODO: Find unassigned task that fits memory budget
        # TODO: Send TaskAssignmentMessage
        logger.debug(f"Task request from {secondary_id} worker {worker_id}")
