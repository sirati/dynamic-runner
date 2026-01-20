"""Network simulation test for submissive/authoritive coordination.

This test verifies that remote submissive/authoritive managers behave identically
to local ones by simulating network communication using the existing multi-computer
protocol messages (TaskRequestMessage, TaskAssignmentMessage).

The test creates:
1. Local submissive + local authoritive (--test-master-slave baseline)
2. Simulated network with message queues between submissive and authoritive

Both should produce identical results.
"""

import asyncio
import logging
import time
from pathlib import Path
from queue import Queue
from typing import Any

from ..binary_info import BinaryInfo
from ..multi_computer.protocol import MessageType, TaskAssignmentMessage, TaskRequestMessage
from ..task import TaskDefinition
from .actual_authoritative import ActualAuthoritativeWorkerManager
from .actual_submissive import ActualSubmissiveWorkerManager

logger = logging.getLogger(__name__)


class NetworkSimulator:
    """Simulates network communication using queues and protocol messages.

    This simulator routes messages between submissive and authoritive managers
    using the existing multi-computer protocol (TaskRequestMessage, TaskAssignmentMessage).
    """

    def __init__(self):
        # Message queues (thread-safe for blocking operations)
        self.submissive_to_authoritive: Queue = Queue()
        self.authoritive_to_submissive: Queue = Queue()

        # Statistics
        self.messages_sent = 0
        self.task_requests = 0
        self.task_assignments = 0

    def send_task_request(self, worker_id: int) -> None:
        """Submissive sends task request to authoritive."""
        msg = TaskRequestMessage(
            sender_id="submissive-0",
            timestamp=time.time(),
            secondary_id="submissive-0",
            worker_id=worker_id,
            available_memory=0,  # Not used in local case
        )
        self.submissive_to_authoritive.put(msg)
        self.messages_sent += 1
        self.task_requests += 1
        logger.info(f"[NetSim] Task request: worker {worker_id}")

    def send_task_assignment(self, worker_id: int, binary: BinaryInfo, estimated_memory: int) -> None:
        """Authoritive sends task assignment to submissive."""
        msg = TaskAssignmentMessage(
            sender_id="authoritive-0",
            timestamp=time.time(),
            secondary_id="submissive-0",
            worker_id=worker_id,
            zip_file=None,
            binary_info={
                "path": str(binary.path),
                "size": binary.size,
                "binary_name": binary.binary_name,
                "platform": binary.platform,
                "compiler": binary.compiler,
                "version": binary.version,
                "opt_level": binary.opt_level,
            },
            local_path=str(binary.path),
            file_hash="",
        )

        # Attach the actual BinaryInfo object for local simulation
        msg._binary_obj = binary
        msg._estimated_memory = estimated_memory

        self.authoritive_to_submissive.put(msg)
        self.messages_sent += 1
        self.task_assignments += 1
        logger.info(f"[NetSim] Task assignment: worker {worker_id} -> {binary.path.name}")

    def get_task_request(self) -> TaskRequestMessage | None:
        """Authoritive receives task request from submissive (non-blocking)."""
        if self.submissive_to_authoritive.empty():
            return None
        return self.submissive_to_authoritive.get_nowait()

    def get_task_assignment(self) -> TaskAssignmentMessage | None:
        """Submissive receives task assignment from authoritive (non-blocking)."""
        if self.authoritive_to_submissive.empty():
            return None
        return self.authoritive_to_submissive.get_nowait()

    def print_stats(self) -> None:
        """Print simulation statistics."""
        logger.info(
            f"[NetSim] Stats: {self.messages_sent} messages, "
            f"{self.task_requests} requests, {self.task_assignments} assignments"
        )


class NetworkSimulatedSubmissiveManager(ActualSubmissiveWorkerManager):
    """Submissive manager that communicates via network simulator.

    This wraps ActualSubmissiveWorkerManager and intercepts task requests,
    sending them through the network simulator instead of direct callbacks.

    Note: This is a relay manager that only handles network communication.
    It should not log worker assignments since those are logged by the
    actual worker manager.
    """

    def __init__(
        self,
        num_workers: int,
        max_memory: int,
        source_dir: Path,
        output_dir: Path,
        task_definition: TaskDefinition,
        task_args: Any,
        skip_existing: bool,
        network_sim: NetworkSimulator,
        manual_start_worker: bool = False,
        connection_mode: str = "socketpair",
        socket_dir: Path | None = None,
    ):
        self.network_sim = network_sim

        # Create a callback that sends requests through the network simulator
        def network_request_callback(worker_id: int) -> None:
            self.network_sim.send_task_request(worker_id)

        super().__init__(
            num_workers=num_workers,
            max_memory=max_memory,
            source_dir=source_dir,
            output_dir=output_dir,
            task_definition=task_definition,
            task_args=task_args,
            skip_existing=skip_existing,
            request_task_callback=network_request_callback,
            manual_start_worker=manual_start_worker,
            connection_mode=connection_mode,
            socket_dir=socket_dir,
            enable_logging=False,  # Relay manager - don't log
        )

    def process_network_messages(self) -> None:
        """Process incoming task assignments from network simulator."""
        processed = 0
        while True:
            msg = self.network_sim.get_task_assignment()
            if msg is None:
                break

            processed += 1
            # Extract binary and estimated memory from the message
            binary = msg._binary_obj
            estimated_memory = msg._estimated_memory
            worker_id = msg.worker_id

            # Apply the assignment locally
            logger.info(f"[NetSim-Sub] Processing assignment for worker {worker_id}: {binary.path.name}")
            self.assign_task_from_authoritive(worker_id, binary, estimated_memory)

        if processed > 0:
            logger.info(f"[NetSim-Sub] Processed {processed} assignment messages")


class NetworkSimulatedAuthoritiveManager(ActualAuthoritativeWorkerManager):
    """Authoritive manager that communicates via network simulator.

    This wraps ActualAuthoritativeWorkerManager and intercepts task assignments,
    sending them through the network simulator instead of direct callbacks.

    Note: This is a relay manager that only handles network communication.
    It should not log worker assignments since those are logged by the
    actual worker manager.
    """

    def __init__(
        self,
        num_workers: int,
        max_memory: int,
        log_dir: Path,
        task_definition: TaskDefinition,
        network_sim: NetworkSimulator,
        submissive_managers: list[ActualSubmissiveWorkerManager],
    ):
        self.network_sim = network_sim
        self.network_submissive_manager: NetworkSimulatedSubmissiveManager | None = None

        super().__init__(
            num_workers=num_workers,
            max_memory=max_memory,
            log_dir=log_dir,
            task_definition=task_definition,
            submissive_managers=submissive_managers,
        )

        # Suppress logging for relay manager
        self.manager_logger.disabled = True

    def process_network_messages(self) -> None:
        """Process incoming task requests from network simulator."""
        processed = 0
        while True:
            msg = self.network_sim.get_task_request()
            if msg is None:
                break

            processed += 1
            worker_id = msg.worker_id

            # Handle the task request using the standard algorithm
            logger.info(f"[NetSim-Auth] Processing task request for worker {worker_id}")
            result = self.handle_task_request(worker_id)

            # Send assignment back through network
            if result:
                binary, estimated_memory = result
                self.network_sim.send_task_assignment(worker_id, binary, estimated_memory)
            else:
                logger.info(f"[NetSim-Auth] No task available for worker {worker_id}")

        if processed > 0:
            logger.info(f"[NetSim-Auth] Processed {processed} request messages")


def run_baseline_test(
    binaries: list[BinaryInfo],
    task_definition: TaskDefinition,
    task_args: Any,
    source_dir: Path,
    output_dir: Path,
    num_cores: int,
    max_memory: int,
) -> dict[str, Any]:
    """Run baseline test with direct local submissive + authoritive.

    This is the --test-master-slave mode.
    """
    logger.info("=" * 60)
    logger.info("BASELINE: Direct Local Submissive + Authoritive")
    logger.info("=" * 60)

    # Create submissive manager
    def request_task_callback(worker_id: int) -> None:
        result = authoritive_manager.handle_task_request(worker_id)
        if result:
            binary, estimated_memory = result
            submissive_manager.assign_task_from_authoritive(worker_id, binary, estimated_memory)

    submissive_manager = ActualSubmissiveWorkerManager(
        num_workers=num_cores,
        max_memory=max_memory,
        source_dir=source_dir,
        output_dir=output_dir,
        task_definition=task_definition,
        task_args=task_args,
        skip_existing=False,
        request_task_callback=request_task_callback,
    )

    # Create authoritive manager
    authoritive_manager = ActualAuthoritativeWorkerManager(
        num_workers=num_cores,
        max_memory=max_memory,
        log_dir=output_dir,
        task_definition=task_definition,
        submissive_managers=[submissive_manager],
    )

    # Process binaries
    submissive_manager.process_binaries(binaries)

    return {
        "completed": submissive_manager.stats["completed"],
        "errored": submissive_manager.stats["errored"],
        "total": submissive_manager.stats["total"],
    }


def run_network_sim_test(
    binaries: list[BinaryInfo],
    task_definition: TaskDefinition,
    task_args: Any,
    source_dir: Path,
    output_dir: Path,
    num_cores: int,
    max_memory: int,
) -> dict[str, Any]:
    """Run network simulation test with message queues.

    This is the --test-master-slave-netsim mode.
    """
    logger.info("=" * 60)
    logger.info("NETWORK SIM: Submissive + Authoritive via Message Queues")
    logger.info("=" * 60)

    # Create network simulator
    network_sim = NetworkSimulator()

    # Create network-simulated submissive manager
    submissive_manager = NetworkSimulatedSubmissiveManager(
        num_workers=num_cores,
        max_memory=max_memory,
        source_dir=source_dir,
        output_dir=output_dir,
        task_definition=task_definition,
        task_args=task_args,
        skip_existing=False,
        network_sim=network_sim,
    )

    # Create network-simulated authoritive manager
    authoritive_manager = NetworkSimulatedAuthoritiveManager(
        num_workers=num_cores,
        max_memory=max_memory,
        log_dir=output_dir,
        task_definition=task_definition,
        network_sim=network_sim,
        submissive_managers=[submissive_manager],
    )

    # Link them together
    authoritive_manager.network_submissive_manager = submissive_manager

    # Override the worker loop to process network messages
    original_process_loop = submissive_manager._process_worker_loop

    def network_aware_process_loop(
        active_workers: set[int],
        allow_stop: bool,
        on_failure_increment_failed: bool = True,
        is_initial_phase: bool = False,
    ) -> None:
        # Process any pending network messages before the worker loop iteration
        submissive_manager.process_network_messages()
        authoritive_manager.process_network_messages()

        # Call original worker loop
        original_process_loop(active_workers, allow_stop, on_failure_increment_failed, is_initial_phase)

        # Process network messages after the worker loop iteration
        submissive_manager.process_network_messages()
        authoritive_manager.process_network_messages()

    # Monkey-patch the process loop
    submissive_manager._process_worker_loop = network_aware_process_loop

    # Process binaries
    submissive_manager.process_binaries(binaries)

    # Print network statistics
    network_sim.print_stats()

    return {
        "completed": submissive_manager.stats["completed"],
        "errored": submissive_manager.stats["errored"],
        "total": submissive_manager.stats["total"],
    }


def compare_results(baseline: dict[str, Any], netsim: dict[str, Any]) -> bool:
    """Compare results from baseline and network simulation."""
    logger.info("=" * 60)
    logger.info("COMPARISON")
    logger.info("=" * 60)

    logger.info(f"Baseline - Completed: {baseline['completed']}/{baseline['total']}")
    logger.info(f"Baseline - Errored: {baseline['errored']}/{baseline['total']}")

    logger.info(f"NetSim   - Completed: {netsim['completed']}/{netsim['total']}")
    logger.info(f"NetSim   - Errored: {netsim['errored']}/{netsim['total']}")

    if baseline == netsim:
        logger.info("✓ PASS: Results are identical")
        return True
    else:
        logger.error("✗ FAIL: Results differ")
        return False
