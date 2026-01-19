"""Base worker manager implementation using BaseWorker abstraction.

This module provides the base worker manager that works with BaseWorker interface
instead of directly with WorkerState, allowing it to manage both local and remote workers.
"""

import logging
import random
import string
import threading
from abc import ABC, abstractmethod
from datetime import datetime
from pathlib import Path

from shared import setup_file_logger

from ..binary_info import BinaryInfo
from ..comm import ErrorType
from ..models import FailedTask, TaskResult
from ..processing_phases import process_oom_phase, process_retry_phase, process_unassigned_phase
from ..task import TaskDefinition
from ..worker.base_worker import BaseWorker


class WorkerManagerBase(ABC):
    """Base worker manager that works with BaseWorker abstraction."""

    def __init__(
        self,
        num_workers: int,
        max_memory: int,
        log_dir: Path,
        task_definition: TaskDefinition,
        always_restart_worker: bool = False,
    ):
        self.num_workers = num_workers
        self.max_memory = max_memory
        self.task_definition = task_definition
        self.reserved_memory_per_worker = task_definition.get_reserved_memory_per_worker()
        self.always_restart_worker = always_restart_worker

        # Generate random session ID
        self.session_id = "".join(random.choices(string.ascii_lowercase + string.digits, k=8))

        self.workers: list[BaseWorker] = []
        self.available_memory = max_memory
        self.total_assigned_memory = 0
        self.lock = threading.Lock()
        self.pending_binaries: list[BinaryInfo] = []
        self.failed_tasks: list[FailedTask] = []
        self.oom_tasks: list[FailedTask] = []
        self.unassigned_tasks: list[BinaryInfo] = []
        self.pending_worker_assignments: set[int] = set()
        self.stats = {"completed": 0, "total": 0, "skipped": 0, "errored": 0}
        self.idle_workers_logged: set[int] = set()
        self.in_oom_phase: bool = False

        start_time = datetime.now().strftime("%Y%m%d_%H%M%S")
        self.log_dir = log_dir / start_time
        self.log_dir.mkdir(parents=True, exist_ok=True)

        self.manager_logger = self._setup_logger()

        # Memory usage log file
        self.memuse_log_path = log_dir.parent / "memuse.log"

    def _calculate_initial_budget(self, worker_index: int) -> int:
        """Calculate initial budget for worker based on specification.

        1st worker (index 0): max_memory
        2nd worker (index 1): max_memory/2 + 150MB
        3rd worker (index 2): max_memory/4 + 150MB
        4th worker (index 3): max_memory/5 + 150MB
        5th worker onwards: follows pattern + 150MB
        """
        base_150mb = 150 * 1024 * 1024

        if worker_index == 0:
            return self.max_memory
        elif worker_index == 1:
            return self.max_memory // 2 + base_150mb
        elif worker_index == 2:
            return self.max_memory // 4 + base_150mb
        else:
            # For 4th worker (index 3): 1/5
            # For 5th worker (index 4): 1/6
            # For nth worker (index n): 1/(n+2)
            divisor = worker_index + 2
            return self.max_memory // divisor + base_150mb

    def _setup_logger(self) -> logging.Logger:
        """Setup and configure the manager logger."""
        manager_log_path = self.log_dir / "manager.log"
        logger = setup_file_logger("manager", manager_log_path, level=logging.INFO)
        return logger

    def _log_memory_usage(self, worker: BaseWorker, errored: bool = False) -> None:
        """Log memory usage to memuse.log file."""
        if not worker.current_binary:
            return

        try:
            actual_memory = worker.get_actual_memory_usage()

            with open(self.memuse_log_path, "a") as f:
                status = "ERROR" if errored else "OK"
                f.write(
                    f"{worker.current_binary.size},{worker.estimated_memory},{actual_memory},"
                    f"{worker.current_binary.path.name},{status}\n"
                )
        except Exception as e:
            self.manager_logger.warning(f"Failed to log memory usage: {e}")

    @abstractmethod
    def _create_workers(self) -> list[BaseWorker]:
        """Create worker instances. Must be implemented by subclasses."""
        pass

    def _assign_binary_to_worker_initial_phase(self, worker: BaseWorker) -> bool:
        """Assign binary during initial phase with opportunistic marking."""
        # Don't assign to workers that aren't ready
        if not worker.ready:
            return False

        # Only assign once per worker during initial phase
        if worker.has_received_initial_assignment:
            return False

        with self.lock:
            if not self.pending_binaries:
                return False

            budget = worker.reserved_budget

            # Find task that fits budget
            for i, binary in enumerate(self.pending_binaries):
                estimated = self.task_definition.estimate_memory(binary.size)

                if estimated > budget:
                    continue

                # Check if assigning would exceed memory limit (only on first assignment)
                would_exceed = (self.total_assigned_memory + estimated) > self.max_memory

                # Assign the task
                self.pending_binaries.pop(i)

                # Track all assigned memory
                self.total_assigned_memory += estimated

                # Only mark as opportunistic on first assignment
                if would_exceed:
                    worker.opportunistic = True

                success, error_msg = worker.assign_task(binary, estimated)
                if not success:
                    self.manager_logger.error(f"[Worker {worker.worker_id}] Assignment error: {error_msg}")
                    self.pending_binaries.insert(i, binary)
                    self.total_assigned_memory = max(0, self.total_assigned_memory - estimated)
                    return False

                worker.mark_busy(binary, estimated, worker.opportunistic)

                size_mb = binary.size / (1024 * 1024)
                estimated_mb = estimated / (1024 * 1024)
                budget_mb = budget / (1024 * 1024)
                opp_str = " (opportunistic)" if worker.opportunistic else ""
                self.manager_logger.info(
                    f"[Worker {worker.worker_id}] Assigned: {binary.path.name} "
                    f"(size: {size_mb:.2f}MB, est: {estimated_mb:.2f}MB, budget: {budget_mb:.2f}MB){opp_str}"
                )
                return True

            # No task fits
            worker.idle = True
            return False

    def _assign_binary_to_worker_normal(self, worker: BaseWorker, retry_attempt: bool = False) -> bool:
        """Assign a binary to a worker during normal phase."""
        # Don't assign to workers that aren't ready
        if not worker.ready:
            self.manager_logger.debug(f"[Worker {worker.worker_id}] Not ready, cannot assign")
            return False

        with self.lock:
            if not self.pending_binaries:
                return False

            # Calculate available memory using actual memory usage
            actual_total_usage = self._get_worker_actual_memory_usage()
            available = self.max_memory - actual_total_usage

            # Get idle workers ordered by budget
            idle_workers = sorted(
                [w for w in self.workers if w.idle and w.current_binary is None], key=lambda w: w.reserved_budget
            )

            if worker not in idle_workers:
                self.manager_logger.debug(
                    f"[Worker {worker.worker_id}] Not in idle_workers list "
                    f"(idle={worker.idle}, current_binary={worker.current_binary})"
                )
                return False

            worker_idle_index = idle_workers.index(worker)

            # Assign temporary budget factor: 1st=1.5, 2nd=2, 3rd=3, etc.
            if worker_idle_index == 0:
                temp_factor = 1.5
            elif worker_idle_index == 1:
                temp_factor = 2.0
            else:
                temp_factor = float(worker_idle_index + 1)

            # Determine budget to use
            if worker.opportunistic:
                temp_budget = available / temp_factor
                effective_budget = min(worker.reserved_budget, temp_budget)
            else:
                effective_budget = worker.reserved_budget

            # Find task that fits
            for i, binary in enumerate(self.pending_binaries):
                estimated = self.task_definition.estimate_memory(binary.size)

                if estimated > effective_budget:
                    continue

                # Assign the task
                self.pending_binaries.pop(i)

                # Subtract from available only for opportunistic workers
                if worker.opportunistic:
                    available -= estimated

                success, error_msg = worker.assign_task(binary, estimated)
                if not success:
                    self.manager_logger.error(f"[Worker {worker.worker_id}] Assignment error: {error_msg}")
                    self.pending_binaries.insert(i, binary)
                    worker.idle = True
                    return False

                worker.mark_busy(binary, estimated)
                worker.idle = False

                size_mb = binary.size / (1024 * 1024)
                estimated_mb = estimated / (1024 * 1024)
                budget_mb = effective_budget / (1024 * 1024)
                self.manager_logger.info(
                    f"[Worker {worker.worker_id}] Assigned: {binary.path.name} "
                    f"(size: {size_mb:.2f}MB, est: {estimated_mb:.2f}MB, budget: {budget_mb:.2f}MB)"
                )
                return True

            # No task fits - log if first time staying idle after completion
            if not retry_attempt and worker.worker_id not in self.idle_workers_logged:
                self.idle_workers_logged.add(worker.worker_id)
                self.manager_logger.info(f"[Worker {worker.worker_id}] Staying idle - no fitting task")

            return False

    def _worker_completed(self, worker: BaseWorker, result: TaskResult) -> None:
        """Mark worker as completed and release memory."""
        # Log memory usage before completing
        errored = not result.success
        self._log_memory_usage(worker, errored)

        with self.lock:
            # Decrement total_assigned_memory if this was an initial assignment
            if worker.current_binary and worker.has_received_initial_assignment and not worker.opportunistic:
                self.total_assigned_memory = max(0, self.total_assigned_memory - worker.estimated_memory)

            binary = worker.current_binary

            if result.success:
                self.stats["completed"] += 1
                binary_name = binary.path.name if binary else "unknown"
                self.manager_logger.info(f"[Worker {worker.worker_id}] Completed: {binary_name}")
            else:
                if result.error_type == ErrorType.OUT_OF_MEMORY:
                    self.oom_tasks.append(
                        FailedTask(binary=binary, error_type=result.error_type, error_message=result.error_message)
                    )
                    binary_name = binary.path.name if binary else "unknown"
                    self.manager_logger.warning(f"[Worker {worker.worker_id}] OOM: {binary_name}")
                elif result.error_type != ErrorType.NON_RECOVERABLE:
                    self.failed_tasks.append(
                        FailedTask(binary=binary, error_type=result.error_type, error_message=result.error_message)
                    )
                    binary_name = binary.path.name if binary else "unknown"
                    self.manager_logger.warning(f"[Worker {worker.worker_id}] Failed: {binary_name}")

        # Mark worker as idle and ready for reassignment
        worker.clear_task()
        worker.idle = True

    def _get_worker_actual_memory_usage(self) -> int:
        """Get actual memory usage of all workers combined."""
        total_memory = 0
        for worker in self.workers:
            total_memory += worker.get_actual_memory_usage()
        return total_memory

    def _handle_oom_killed_task(self, worker: BaseWorker, binary: BinaryInfo, reason: str) -> None:
        """Handle a task that was killed due to OOM.

        Override in subclasses to customize behavior:
        - LocalManager: requeue to pending_binaries for local retry
        - SubmissiveManager: add to oom_tasks to report to primary
        - AuthoritiveManager: N/A (no OOM checking)
        """
        pass

    def _check_memory_pressure_and_kill(self) -> None:
        """Check memory pressure and kill workers according to specification.

        Base implementation that can be overridden. By default does nothing.
        LocalManager and SubmissiveManager override to enable OOM checking.
        """
        pass

    def _handle_worker_without_task(
        self,
        worker: BaseWorker,
        worker_id: int,
        active_workers: set[int],
        allow_stop: bool,
        is_initial_phase: bool = False,
    ) -> bool:
        """Handle a worker that has no current task. Returns True if worker should continue."""
        if is_initial_phase:
            # During initial phase, only assign using initial phase logic if not yet assigned
            if not worker.has_received_initial_assignment:
                assigned = self._assign_binary_to_worker_initial_phase(worker)
            else:
                # Already received initial assignment, use normal logic
                assigned = self._assign_binary_to_worker_normal(worker, retry_attempt=False)
                if not assigned:
                    assigned = self._assign_binary_to_worker_normal(worker, retry_attempt=True)
        else:
            assigned = self._assign_binary_to_worker_normal(worker, retry_attempt=False)
            if not assigned:
                # Try once more with recalculated idle workers and budget factors
                assigned = self._assign_binary_to_worker_normal(worker, retry_attempt=True)

        if assigned:
            return True

        if not self.pending_binaries:
            if allow_stop:
                worker.terminate()
                self.manager_logger.info(f"[Worker {worker_id}] Stopping (no more tasks)")
            # Remove from active workers (but don't stop if allow_stop=False)
            active_workers.discard(worker_id)
            return False

        # Worker is idle but binaries remain - keep it in the loop
        return True

    def _handle_monitor_result(
        self,
        worker: BaseWorker,
        result: TaskResult | None,
        task_completed: bool,
        should_restart: bool,
        worker_id: int,
        active_workers: set[int],
        allow_stop: bool,
        on_failure_increment_failed: bool,
        is_initial_phase: bool = False,
    ) -> None:
        """Handle the result from monitoring a worker."""
        self.manager_logger.debug(
            f"[Worker {worker_id}] _handle_monitor_result called: "
            f"task_completed={task_completed}, should_restart={should_restart}, "
            f"result.success={result.success if result else None}"
        )
        if should_restart:
            self._worker_completed(worker, result)
            if worker.restart():
                self.pending_worker_assignments.add(worker_id)
            return

        if not task_completed:
            return

        if result.error_type == ErrorType.NON_RECOVERABLE:
            self.manager_logger.error(f"[Worker {worker_id}] Non-recoverable error, restarting")
            self._worker_completed(worker, result)
            worker.restart()
            return

        if on_failure_increment_failed and not result.success:
            with self.lock:
                self.stats["errored"] += 1
            binary_name = worker.current_binary.path.name if worker.current_binary else "unknown"
            giveup_msg = f"[GiveUp] {binary_name}"
            self.manager_logger.warning(giveup_msg)

        self._worker_completed(worker, result)

        self.manager_logger.debug(
            f"[Worker {worker_id}] After _worker_completed: "
            f"idle={worker.idle}, current_binary={worker.current_binary}, "
            f"pending_binaries={len(self.pending_binaries)}"
        )

        # Restart worker after successful completion if always_restart_worker is enabled
        # and there are still binaries to process
        if self.always_restart_worker and result.success and self.pending_binaries:
            self.manager_logger.info(f"[Worker {worker_id}] Restarting worker after successful completion")
            worker.restart()
            return

        # Assign next task
        self.manager_logger.debug(
            f"[Worker {worker_id}] Attempting to assign next task (is_initial_phase={is_initial_phase})"
        )
        if is_initial_phase:
            # During initial phase, only assign using initial phase logic if not yet assigned
            if not worker.has_received_initial_assignment:
                assigned = self._assign_binary_to_worker_initial_phase(worker)
            else:
                # Already received initial assignment, use normal logic
                assigned = self._assign_binary_to_worker_normal(worker, retry_attempt=False)
                if not assigned:
                    assigned = self._assign_binary_to_worker_normal(worker, retry_attempt=True)
        else:
            assigned = self._assign_binary_to_worker_normal(worker, retry_attempt=False)
            if not assigned:
                assigned = self._assign_binary_to_worker_normal(worker, retry_attempt=True)

        self.manager_logger.debug(
            f"[Worker {worker_id}] Assignment result: {assigned}, pending_binaries={len(self.pending_binaries)}"
        )

        if not assigned and not self.pending_binaries:
            if allow_stop:
                self.manager_logger.info(f"[Worker {worker_id}] Stopping (no more tasks)")
                worker.terminate()
            # Remove from active workers regardless of allow_stop
            active_workers.discard(worker_id)

    def _wait_for_workers_ready(self) -> None:
        """Wait for all workers to be ready."""
        import time

        self.manager_logger.info("Waiting for all workers to be ready...")

        timeout = 300  # 5 minutes
        start_time = time.time()

        while True:
            if time.time() - start_time > timeout:
                raise TimeoutError("Workers did not become ready within timeout")

            # Check if all workers are ready
            all_ready = all(w.ready for w in self.workers)

            if all_ready:
                self.manager_logger.info("All workers ready!")
                break

            # Check status on all workers to receive ready signals
            for worker in self.workers:
                if not worker.ready:
                    has_result, _ = worker.check_status()

            time.sleep(0.1)

    def _process_worker_loop(
        self,
        active_workers: set[int],
        allow_stop: bool = True,
        on_failure_increment_failed: bool = False,
        is_initial_phase: bool = False,
    ) -> None:
        """Main worker processing loop."""
        while active_workers:
            # Check for ready messages from workers
            for worker in self.workers:
                if not worker.ready:
                    has_result, _ = worker.check_status()

            for worker_id in list(active_workers):
                worker = self.workers[worker_id]

                # Skip workers that aren't ready
                if not worker.ready:
                    # If no tasks remain, remove from active workers
                    if not self.pending_binaries and allow_stop:
                        active_workers.discard(worker_id)
                    continue

                if worker.current_binary is None:
                    if not self._handle_worker_without_task(
                        worker, worker_id, active_workers, allow_stop, is_initial_phase
                    ):
                        continue
                else:
                    # Check worker status
                    has_result, result = worker.check_status()
                    self.manager_logger.debug(
                        f"[Worker {worker_id}] check_status returned: has_result={has_result}, result={result}"
                    )

                    if has_result:
                        # Determine if we should restart based on result
                        should_restart = result.error_type == ErrorType.NON_RECOVERABLE

                        self._handle_monitor_result(
                            worker,
                            result,
                            has_result,
                            should_restart,
                            worker_id,
                            active_workers,
                            allow_stop,
                            on_failure_increment_failed,
                            is_initial_phase,
                        )

            # After processing all workers, handle pending worker assignments first
            if self.pending_worker_assignments and self.pending_binaries:
                for worker_id in list(self.pending_worker_assignments):
                    worker = self.workers[worker_id]
                    if worker.current_binary is None:
                        if is_initial_phase:
                            # During initial phase, only assign using initial phase logic if not yet assigned
                            if not worker.has_received_initial_assignment:
                                self._assign_binary_to_worker_initial_phase(worker)
                            else:
                                assigned = self._assign_binary_to_worker_normal(worker, retry_attempt=False)
                                if not assigned:
                                    self._assign_binary_to_worker_normal(worker, retry_attempt=True)
                        else:
                            assigned = self._assign_binary_to_worker_normal(worker, retry_attempt=False)
                            if not assigned:
                                self._assign_binary_to_worker_normal(worker, retry_attempt=True)
                        self.pending_worker_assignments.discard(worker_id)

            # Check memory pressure and kill workers if needed (only in normal phase)
            if not is_initial_phase and self.pending_binaries:
                self._check_memory_pressure_and_kill()

            if active_workers:
                threading.Event().wait(0.1)

        # Move unassigned tasks to OOM queue at the end
        if not is_initial_phase and self.pending_binaries:
            for binary in self.pending_binaries:
                self.oom_tasks.append(
                    FailedTask(
                        binary=binary,
                        error_type=ErrorType.OUT_OF_MEMORY,
                        error_message="Could not fit in any worker budget",
                    )
                )
            self.pending_binaries.clear()

    def _initialize_workers(self) -> None:
        """Initialize all worker processes."""
        self.workers = self._create_workers()

        # Set initial budgets
        for i, worker in enumerate(self.workers):
            worker.reserved_budget = self._calculate_initial_budget(i)
            budget_mb = worker.reserved_budget / (1024 * 1024)
            self.manager_logger.info(f"[Worker {worker.worker_id}] Budget: {budget_mb:.2f}MB")

        # Start all workers
        for worker in self.workers:
            if not worker.start():
                raise RuntimeError(f"Failed to start worker {worker.worker_id}")

        # Wait for all workers to be ready before proceeding
        self._wait_for_workers_ready()

    def _run_initial_assignments(self) -> None:
        """Perform initial assignment phase - assign first task to each worker."""
        # Wait for all workers to receive their first assignment
        while not all(w.has_received_initial_assignment for w in self.workers):
            # Check for ready messages
            for worker in self.workers:
                if not worker.ready:
                    has_result, _ = worker.check_status()

            # Try to assign to workers that haven't received initial assignment
            for worker in self.workers:
                if not worker.has_received_initial_assignment and worker.ready:
                    self._assign_binary_to_worker_initial_phase(worker)

            threading.Event().wait(0.1)

        # Report assigned memory totals after initial assignments
        opportunistic_memory = sum(
            w.estimated_memory for w in self.workers if w.opportunistic and w.current_binary is not None
        )
        non_opportunistic_memory = sum(
            w.estimated_memory for w in self.workers if not w.opportunistic and w.current_binary is not None
        )
        total_mb = self.total_assigned_memory / (1024 * 1024)
        opp_mb = opportunistic_memory / (1024 * 1024)
        non_opp_mb = non_opportunistic_memory / (1024 * 1024)
        self.manager_logger.info(
            f"[Initial Phase] Total assigned: {total_mb:.2f}MB, "
            f"Non-opportunistic: {non_opp_mb:.2f}MB, "
            f"Opportunistic: {opp_mb:.2f}MB"
        )

    def _run_main_phase(self) -> None:
        """Run the main processing phase."""
        active_workers = set(range(self.num_workers))
        # Don't stop workers - keep them alive for subsequent phases
        self._process_worker_loop(
            active_workers, allow_stop=False, on_failure_increment_failed=False, is_initial_phase=False
        )

        # Report status after main phase
        errored_count = len(self.failed_tasks)
        oom_count = len(self.oom_tasks)
        self.manager_logger.info(
            f"[Main Phase Complete] Completed: {self.stats['completed']}/{self.stats['total']}, "
            f"Errored: {errored_count}/{self.stats['total']}, "
            f"OOM: {oom_count}/{self.stats['total']}"
        )

    def _run_retry_phase(self) -> None:
        """Run the retry phase for failed tasks."""
        if not self.failed_tasks:
            self.manager_logger.info("[Retry Phase] Skipped - no errored tasks to retry")
            return

        self.manager_logger.info(f"[Retry Phase] Starting retry of {len(self.failed_tasks)} failed tasks")

        # Reactivate all workers for retry phase
        process_retry_phase(
            self.failed_tasks,
            self.pending_binaries,
            self.num_workers,
            lambda aw, **kwargs: self._process_worker_loop(aw, **kwargs),
            self.manager_logger,
        )

        # Report status after retry phase
        errored_count = len(self.failed_tasks)
        oom_count = len(self.oom_tasks)
        self.manager_logger.info(
            f"[Retry Phase Complete] Completed: {self.stats['completed']}/{self.stats['total']}, "
            f"Errored: {errored_count}/{self.stats['total']}, "
            f"OOM: {oom_count}/{self.stats['total']}"
        )

    def _run_oom_phase(self) -> None:
        """Run the OOM phase with single worker."""
        if not self.oom_tasks:
            self.manager_logger.info("[OOM Phase] Skipped - no OOM tasks")
            return

        self.manager_logger.info(f"[OOM Phase] Starting processing of {len(self.oom_tasks)} OOM tasks")

        self.in_oom_phase = True
        # For OOM phase, we need to convert workers back to compatible format for processing_phases
        # This is a limitation that will need addressing based on actual needs
        self.manager_logger.warning("[OOM Phase] Using simplified OOM processing")

        # Simple approach: just add them back to pending and process with single worker
        for task in self.oom_tasks:
            self.pending_binaries.insert(0, task.binary)
        self.oom_tasks.clear()

        # Process with just worker 0
        active_workers = {0}
        self._process_worker_loop(active_workers, allow_stop=False, on_failure_increment_failed=False)

        self.in_oom_phase = False

        # Report status after OOM phase
        errored_count = len(self.failed_tasks)
        oom_count = len(self.oom_tasks)
        self.manager_logger.info(
            f"[OOM Phase Complete] Completed: {self.stats['completed']}/{self.stats['total']}, "
            f"Errored: {errored_count}/{self.stats['total']}, "
            f"OOM: {oom_count}/{self.stats['total']}"
        )

    def _run_unassigned_phase(self) -> None:
        """Run the unassigned phase with single worker and no memory limit."""
        if not self.unassigned_tasks:
            return

        self.manager_logger.info(f"[Unassigned Phase] Processing {len(self.unassigned_tasks)} unassigned tasks")

        # Add unassigned tasks to pending
        for task in self.unassigned_tasks:
            self.pending_binaries.insert(0, task)
        self.unassigned_tasks.clear()

        # Process with just worker 0
        active_workers = {0}
        self._process_worker_loop(active_workers, allow_stop=False, on_failure_increment_failed=False)

    def process_binaries(self, binaries: list[BinaryInfo]) -> None:
        """Main processing entry point."""
        self.pending_binaries = binaries.copy()
        self.stats["total"] = len(binaries)
        self.stats["completed"] = 0
        self.stats["errored"] = 0

        start_msg = f"Starting {self.num_workers} workers with {self.max_memory / (1024**3):.2f}GB memory limit"
        process_msg = f"Processing {self.stats['total']} binaries"
        self.manager_logger.info(start_msg)
        self.manager_logger.info(process_msg)

        self._initialize_workers()
        self._run_initial_assignments()
        self._run_main_phase()
        self._run_retry_phase()
        self._run_oom_phase()
        self._run_unassigned_phase()

        # Stop all workers after all phases complete
        for worker in self.workers:
            if worker.is_alive():
                try:
                    worker.terminate()
                    self.manager_logger.info(f"[Worker {worker.worker_id}] Stopping (all phases complete)")
                except Exception:
                    pass

        # Count remaining failed tasks by type
        completed = self.stats["completed"]
        errored_twice_failed = len(self.failed_tasks)
        oom_failed = len(self.oom_tasks)
        skipped = self.stats["skipped"]
        total = self.stats["total"]

        final_msg = f"[*] Completed: {completed}/{total}"

        if errored_twice_failed > 0:
            final_msg += f", Errored: {errored_twice_failed}/{total}"

        if oom_failed > 0:
            final_msg += f", OOM: {oom_failed}/{total}"

        if skipped > 0:
            final_msg += f", Skipped: {skipped}/{total}"

        self.manager_logger.info(final_msg)
