"""Decision worker manager mixin.

This module provides the decision responsibilities as a mixin:
- Initial assignment logic
- Normal assignment logic
- Opportunistic marking
- Idle worker management
- Assignment decisions (picking which task goes to which worker)

This is a mixin class that doesn't define __init__ and doesn't extend any base class.
It expects the class it's mixed with to have WorkerManagerBase attributes.
"""

from ..binary_info import BinaryInfo
from ..models import ErrorType
from ..worker.base_worker import BaseWorker


class DecisionWorkerManMixin:
    """Worker manager mixin with decision-making responsibilities.

    Provides:
    - Task assignment logic (initial and normal phases)
    - Opportunistic worker marking
    - Idle worker management
    - Assignment decisions based on memory budgets

    This is a mixin that expects WorkerManagerBase attributes to be present:
    - self.workers
    - self.pending_binaries
    - self.lock
    - self.task_definition
    - self.manager_logger
    - self.max_memory
    - self.total_assigned_memory
    - self.idle_workers_logged
    - self._check_memory_pressure_and_kill()
    - self._get_worker_actual_memory_usage()
    """

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
                    # Handle assignment failure with restart logic
                    self.pending_binaries.insert(i, binary)
                    self.total_assigned_memory = max(0, self.total_assigned_memory - estimated)
                    self.handle_assignment_failure(worker, error_msg)
                    return False

                # Reset assignment failure counter on successful assignment
                self.worker_assignment_failures[worker.worker_id] = 0
                # Remove from pending_worker_assignments on successful assignment
                self.pending_worker_assignments.discard(worker.worker_id)

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
                    # Handle assignment failure with restart logic
                    self.pending_binaries.insert(i, binary)
                    worker.idle = True
                    self.handle_assignment_failure(worker, error_msg)
                    return False

                # Reset assignment failure counter on successful assignment
                self.worker_assignment_failures[worker.worker_id] = 0
                # Remove from pending_worker_assignments on successful assignment
                self.pending_worker_assignments.discard(worker.worker_id)

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
                # Already received initial assignment during initial phase - don't reassign yet
                # This prevents completing and reassigning during initial phase
                return True
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

    def _run_initial_assignments(self) -> None:
        """Perform initial assignment phase - assign first task to each worker.

        This phase ONLY assigns the first task to each worker and does NOT
        process task completions or reassignments. That happens in the main phase.
        """
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

            import threading

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
        """Run main processing phase with normal assignments.

        This phase processes task completions and assigns new tasks to workers
        as they become available.
        """
        self.manager_logger.info("Starting main phase")

        active_workers = set(range(self.num_workers))

        while active_workers:
            for worker_id in list(active_workers):
                worker = self.workers[worker_id]

                # Check memory pressure and kill if needed (execution responsibility)
                self._check_memory_pressure_and_kill()

                # Check if worker is not ready yet
                if not worker.ready:
                    has_result, result = worker.check_status()
                    if has_result:
                        # Worker sent an error result before becoming ready
                        should_restart = result.error_type == ErrorType.NON_RECOVERABLE
                        self._handle_monitor_result(
                            worker,
                            result,
                            has_result,
                            should_restart,
                            worker_id,
                            active_workers,
                            allow_stop=True,
                            on_failure_increment_failed=True,
                            is_initial_phase=False,
                        )
                        continue
                    if worker.ready:
                        # Worker just became ready, remove from pending if present
                        self.pending_worker_assignments.discard(worker_id)
                    continue

                # Handle pending worker assignments
                # Don't discard from pending here - let assignment success or worker stop handle it
                if worker_id in self.pending_worker_assignments:
                    if worker.current_binary is None:
                        if not self._handle_worker_without_task(worker, worker_id, active_workers, False, False):
                            continue

                # Check worker status if it has a current task
                if worker.current_binary is not None:
                    has_result, result = worker.check_status()

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
                            allow_stop=False,
                            on_failure_increment_failed=True,
                            is_initial_phase=False,
                        )
                        continue
                else:
                    # Worker has no task, try to assign one
                    if not self._handle_worker_without_task(worker, worker_id, active_workers, False, False):
                        continue

            import threading

            if active_workers:
                threading.Event().wait(0.1)

        # Move unassigned tasks to unassigned queue at the end
        if self.pending_binaries:
            for binary in self.pending_binaries:
                self.unassigned_tasks.append(binary)
            self.pending_binaries.clear()

        # Report status after main phase
        errored_count = len(self.failed_tasks)
        oom_count = len(self.oom_tasks)
        self.manager_logger.info(
            f"[Main Phase] Completed: {self.stats['completed']}, "
            f"Errored: {errored_count}, "
            f"OOM: {oom_count}, "
            f"Remaining: {len(self.unassigned_tasks)}"
        )
