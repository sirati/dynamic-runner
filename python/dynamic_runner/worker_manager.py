import logging
import random
import string
import threading
from datetime import datetime
from pathlib import Path

from shared import setup_file_logger

from .binary_info import BinaryInfo
from .comm import ErrorType
from .models import TaskResult, WorkerState
from .processing_phases import process_oom_phase, process_retry_phase, process_unassigned_phase
from .task import TaskDefinition
from .task_handler import worker_completed
from .worker_communication import send_worker_command
from .worker_lifecycle import restart_worker, start_worker
from .worker_monitoring import monitor_worker_once
from .worker_utils import cleanup_workers, increment_stat, log_to_worker_file

try:
    import psutil
except ImportError:
    psutil = None


class WorkerManager:
    def __init__(
        self,
        num_workers: int,
        max_memory: int,
        source_dir: Path,
        output_dir: Path,
        task_definition: TaskDefinition,
        task_args,
        skip_existing: bool,
        print_pid: bool,
        always_restart_worker: bool = False,
        manual_start_worker: bool = False,
        connection_mode: str = "socketpair",
        socket_dir: Path | None = None,
    ):
        self.num_workers = num_workers
        self.max_memory = max_memory
        self.task_definition = task_definition
        self.task_args = task_args
        self.reserved_memory_per_worker = task_definition.get_reserved_memory_per_worker()
        self.source_dir = source_dir
        self.output_dir = output_dir
        self.skip_existing = skip_existing
        self.print_pid = print_pid
        self.always_restart_worker = always_restart_worker
        self.manual_start_worker = manual_start_worker
        self.connection_mode = connection_mode
        self.socket_dir = socket_dir

        # Validate socket_dir for named mode
        if self.connection_mode == "named":
            if self.socket_dir is None:
                raise ValueError("socket_dir is required when connection_mode is 'named'")
            self.socket_dir.mkdir(parents=True, exist_ok=True)

        # Generate random session ID for socket names
        self.session_id = "".join(random.choices(string.ascii_lowercase + string.digits, k=8))

        self.workers: list[WorkerState] = []
        self.available_memory = max_memory
        self.total_assigned_memory = 0
        self.lock = threading.Lock()
        self.pending_binaries: list[BinaryInfo] = []
        self.failed_tasks: list = []
        self.oom_tasks: list = []
        self.unassigned_tasks: list[BinaryInfo] = []
        self.pending_worker_assignments: set[int] = set()
        self.stats = {"completed": 0, "failed": 0, "total": 0, "skipped": 0}
        self.idle_workers_logged: set[int] = set()
        self.in_oom_phase: bool = False

        start_time = datetime.now().strftime("%Y%m%d_%H%M%S")
        self.log_dir = output_dir / "logs" / start_time
        self.log_dir.mkdir(parents=True, exist_ok=True)

        self.manager_logger = self._setup_logger()

        # Memory usage log file
        self.memuse_log_path = output_dir / "memuse.log"

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

    def _log_memory_usage(self, worker: WorkerState, errored: bool = False) -> None:
        """Log memory usage to memuse.log file."""
        if not worker.current_binary:
            return

        try:
            actual_memory = 0
            if psutil and worker.process and worker.process.poll() is None:
                try:
                    process = psutil.Process(worker.process.pid)
                    actual_memory = process.memory_info().rss
                except (psutil.NoSuchProcess, psutil.AccessDenied, psutil.ZombieProcess):
                    pass

            with open(self.memuse_log_path, "a") as f:
                status = "ERROR" if errored else "OK"
                f.write(
                    f"{worker.current_binary.size},{worker.estimated_memory},{actual_memory},"
                    f"{worker.current_binary.path.name},{status}\n"
                )
        except Exception as e:
            self.manager_logger.warning(f"Failed to log memory usage: {e}")

    def _get_socket_path(self, worker_id: int) -> Path | None:
        """Get socket path for a worker in named mode."""
        if self.connection_mode != "named":
            return None
        return self.socket_dir / f"worker_{worker_id}_{self.session_id}.sock"

    def _start_worker(self, worker_id: int) -> WorkerState:
        """Start a new worker process."""
        worker_log_path = self.log_dir / f"worker_{worker_id}.log"
        log_to_worker_file(self.log_dir, worker_id, f"Manager: Worker {worker_id} starting")

        socket_path = self._get_socket_path(worker_id)

        worker = start_worker(
            worker_id,
            self.source_dir,
            self.output_dir,
            worker_log_path,
            self.task_definition,
            self.task_args,
            self.skip_existing,
            self.manual_start_worker,
            self.connection_mode,
            socket_path,
        )

        # Set initial budget based on worker index
        worker.reserved_budget = self._calculate_initial_budget(worker_id)
        budget_mb = worker.reserved_budget / (1024 * 1024)

        if worker.process:
            self.manager_logger.info(
                f"[Worker {worker_id}] Started with PID {worker.process.pid}, budget: {budget_mb:.2f}MB"
            )
        else:
            self.manager_logger.info(f"[Worker {worker_id}] Waiting for manual start, budget: {budget_mb:.2f}MB")
        return worker

    def _restart_worker(self, worker_id: int) -> WorkerState:
        """Restart a worker process."""
        old_worker = self.workers[worker_id]
        worker_log_path = self.log_dir / f"worker_{worker_id}.log"
        old_pid_str = f"old PID: {old_worker.process.pid}" if old_worker.process else "waiting for manual start"
        log_to_worker_file(self.log_dir, worker_id, f"Manager: Worker {worker_id} restarting ({old_pid_str})")

        # Close old communication interface
        try:
            old_worker.comm.close()
        except Exception:
            pass

        # Generate new socket path with new random suffix for named mode
        if self.connection_mode == "named":
            # Generate new random suffix for this restart
            new_suffix = "".join(random.choices(string.ascii_lowercase + string.digits, k=8))
            socket_path = self.socket_dir / f"worker_{worker_id}_{new_suffix}.sock"
            self.manager_logger.info(f"[Worker {worker_id}] New socket path: {socket_path}")
        else:
            socket_path = None

        new_worker = restart_worker(
            old_worker,
            self.source_dir,
            self.output_dir,
            worker_log_path,
            self.task_definition,
            self.task_args,
            self.skip_existing,
            self.manual_start_worker,
            self.connection_mode,
            socket_path,
        )

        # Preserve initial budget and opportunistic status
        new_worker.reserved_budget = old_worker.reserved_budget
        new_worker.opportunistic = old_worker.opportunistic
        new_worker.idle = True
        # Reset ready flag - worker needs to send ready signal again
        new_worker.ready = False
        # In socketpair mode with automatic start, connection is established immediately
        if self.connection_mode == "socketpair" and not self.manual_start_worker:
            new_worker.connection_established = True
        else:
            new_worker.connection_established = False

        # Handle logging for manual mode where process might be None
        if new_worker.process and old_worker.process:
            self.manager_logger.info(
                f"[Worker {worker_id}] Restarted with PID {new_worker.process.pid} (old PID: {old_worker.process.pid})"
            )
        elif new_worker.process:
            self.manager_logger.info(f"[Worker {worker_id}] Restarted with PID {new_worker.process.pid}")
        elif old_worker.process:
            self.manager_logger.info(
                f"[Worker {worker_id}] Restarted (waiting for manual start, old PID: {old_worker.process.pid})"
            )
        else:
            self.manager_logger.info(f"[Worker {worker_id}] Restarted (waiting for manual start)")

        with self.lock:
            self.workers[worker_id] = new_worker

        return new_worker

    def _assign_binary_to_worker_initial_phase(self, worker: WorkerState) -> bool:
        """Assign binary during initial phase with opportunistic marking."""
        # Don't assign to workers that aren't ready
        if not worker.ready or not worker.connection_established:
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

                # Check if assigning would exceed memory limit
                would_exceed = (self.total_assigned_memory + estimated) > self.max_memory

                # Assign the task
                self.pending_binaries.pop(i)
                worker.current_binary = binary
                worker.estimated_memory = estimated
                worker.idle = False

                # Track all assigned memory
                self.total_assigned_memory += estimated

                if would_exceed:
                    worker.opportunistic = True

                try:
                    relative_path = binary.path.relative_to(self.source_dir)
                except ValueError:
                    relative_path = binary.path

                success, error_msg = send_worker_command(worker, str(relative_path))
                if not success:
                    self.manager_logger.error(f"[Worker {worker.worker_id}] Communication error: {error_msg}")
                    self.pending_binaries.insert(i, binary)
                    worker.current_binary = None
                    worker.estimated_memory = 0
                    return False

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

    def _assign_binary_to_worker_normal(self, worker: WorkerState, retry_attempt: bool = False) -> bool:
        """Assign a binary to a worker during normal phase."""
        # Don't assign to workers that aren't ready
        if not worker.ready or not worker.connection_established:
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
                worker.current_binary = binary
                worker.estimated_memory = estimated
                worker.idle = False

                # Subtract from available only for opportunistic workers
                if worker.opportunistic:
                    available -= estimated

                try:
                    relative_path = binary.path.relative_to(self.source_dir)
                except ValueError:
                    relative_path = binary.path

                success, error_msg = send_worker_command(worker, str(relative_path))
                if not success:
                    self.manager_logger.error(f"[Worker {worker.worker_id}] Communication error: {error_msg}")
                    self.pending_binaries.insert(i, binary)
                    worker.current_binary = None
                    worker.estimated_memory = 0
                    worker.idle = True
                    return False

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

    def _worker_completed(self, worker: WorkerState, result: TaskResult) -> None:
        """Mark worker as completed and release memory."""
        # Log memory usage before completing
        errored = not result.success
        self._log_memory_usage(worker, errored)

        with self.lock:
            pass

        worker_completed(
            worker,
            result,
            self.oom_tasks,
            self.failed_tasks,
            self.stats,
            self.lock,
            self.manager_logger,
        )

        # Mark worker as idle and ready for reassignment
        worker.idle = True

    def _get_worker_actual_memory_usage(self) -> int:
        """Get actual memory usage of all workers combined."""
        if psutil is None:
            return 0

        total_memory = 0
        for worker in self.workers:
            if worker.process and worker.process.poll() is None:
                if psutil and worker.process:
                    try:
                        process = psutil.Process(worker.process.pid)
                        total_memory += process.memory_info().rss
                    except (psutil.NoSuchProcess, psutil.AccessDenied, psutil.ZombieProcess):
                        pass
        return total_memory

    def _check_memory_pressure_and_kill(self) -> None:
        """Check memory pressure and kill workers according to specification."""
        actual_usage = self._get_worker_actual_memory_usage()
        threshold = min(500 * 1024 * 1024, self.max_memory // self.num_workers)

        # Check if we should kill opportunistic workers
        if actual_usage > (self.max_memory - threshold):
            opportunistic_workers = [w for w in self.workers if w.opportunistic and w.current_binary is not None]

            if opportunistic_workers:
                # Kill median opportunistic worker
                sorted_opp = sorted(opportunistic_workers, key=lambda w: w.estimated_memory)
                median_idx = len(sorted_opp) // 2
                victim = sorted_opp[median_idx]

                binary_name = victim.current_binary.path.name if victim.current_binary else "unknown"
                usage_mb = actual_usage / (1024 * 1024)
                self.manager_logger.warning(
                    f"[OOM] Killing median opportunistic worker {victim.worker_id} "
                    f"({binary_name}, usage: {usage_mb:.0f}MB)"
                )

                # Requeue task only if not in OOM phase
                if victim.current_binary and not self.in_oom_phase:
                    self.pending_binaries.insert(0, victim.current_binary)

                victim.current_binary = None
                victim.estimated_memory = 0
                self._restart_worker(victim.worker_id)
                return

        # Check if we exceeded limit (no opportunistic workers)
        if actual_usage > self.max_memory:
            active_workers = [w for w in self.workers if w.current_binary is not None]

            if not active_workers:
                return

            # Kill smallest worker
            smallest = min(active_workers, key=lambda w: w.estimated_memory)

            # Special handling for worker 0
            if smallest.worker_id == 0:
                binary_name = smallest.current_binary.path.name if smallest.current_binary else "unknown"
                self.manager_logger.error(f"[OOM] Worker 0 killed, adding task to OOM queue: {binary_name}")
                if smallest.current_binary:
                    from .models import FailedTask

                    self.oom_tasks.append(
                        FailedTask(
                            binary=smallest.current_binary,
                            error_type=ErrorType.OUT_OF_MEMORY,
                            error_message="Worker 0 exceeded memory",
                        )
                    )
                smallest.current_binary = None
                smallest.estimated_memory = 0
                self._restart_worker(smallest.worker_id)
            else:
                binary_name = smallest.current_binary.path.name if smallest.current_binary else "unknown"
                usage_mb = actual_usage / (1024 * 1024)
                self.manager_logger.warning(
                    f"[OOM] Killing smallest worker {smallest.worker_id} "
                    f"({binary_name}, usage: {usage_mb:.0f}MB), marking as opportunistic"
                )

                # Requeue task and mark worker as permanently opportunistic (only if not in OOM phase)
                if smallest.current_binary and not self.in_oom_phase:
                    self.pending_binaries.insert(0, smallest.current_binary)

                smallest.current_binary = None
                smallest.estimated_memory = 0
                smallest.opportunistic = True
                self._restart_worker(smallest.worker_id)

    def _handle_worker_without_task(
        self,
        worker: WorkerState,
        worker_id: int,
        active_workers: set[int],
        allow_stop: bool,
        is_initial_phase: bool = False,
    ) -> bool:
        """Handle a worker that has no current task. Returns True if worker should continue."""
        if is_initial_phase:
            assigned = self._assign_binary_to_worker_initial_phase(worker)
        else:
            assigned = self._assign_binary_to_worker_normal(worker, retry_attempt=False)
            if not assigned:
                # Try once more with recalculated idle workers and budget factors
                assigned = self._assign_binary_to_worker_normal(worker, retry_attempt=True)

        if assigned:
            return True

        if not self.pending_binaries and allow_stop:
            success, error = send_worker_command(worker, "stop")
            if not success:
                crash_msg = f"[Worker {worker_id}] Socket error while sending stop, worker likely crashed: {error}"
                self.manager_logger.error(crash_msg)
                self._restart_worker(worker_id)
                return True
            active_workers.discard(worker_id)
            return False

        # Worker is idle but binaries remain - keep it in the loop
        return True

    def _check_manual_worker_connections(self) -> None:
        """Check for manually started workers that haven't connected yet."""
        if not self.manual_start_worker:
            return

        import time

        from .worker_lifecycle import check_manual_worker_connection

        for worker in self.workers:
            if not worker.connection_established:
                # Try to find the process
                if worker.socket_path is None:
                    continue

                process, found = check_manual_worker_connection(worker, worker.socket_path)

                if found and process:
                    if worker.process is None:
                        # First time we found this process
                        worker.process = process
                        worker.connection_established_time = time.time()
                        self.manager_logger.info(
                            f"[Worker {worker.worker_id}] Process detected with PID {process.pid} "
                            f"(socket: {worker.socket_path})"
                        )
                    elif worker.connection_established_time:
                        # Process was already found, check if it's been stable for 5 seconds (manual mode only)
                        if self.manual_start_worker:
                            elapsed = time.time() - worker.connection_established_time
                            if elapsed >= 5.0:
                                worker.connection_established = True
                                self.manager_logger.info(
                                    f"[Worker {worker.worker_id}] Connection established (stable for 5s)"
                                )
                        else:
                            # In non-manual mode, mark as connected immediately
                            worker.connection_established = True
                            self.manager_logger.info(f"[Worker {worker.worker_id}] Connection established")
                elif not found and worker.process:
                    # Process died before becoming stable
                    self.manager_logger.warning(
                        f"[Worker {worker.worker_id}] Process died before connection established, still waiting..."
                    )
                    worker.process = None
                    worker.connection_established_time = None

    def _handle_monitor_result(
        self,
        monitor_result,
        worker: WorkerState,
        worker_id: int,
        active_workers: set[int],
        allow_stop: bool,
        on_failure_increment_failed: bool,
        is_initial_phase: bool = False,
    ) -> None:
        """Handle the result from monitoring a worker."""
        if monitor_result.should_restart:
            self._worker_completed(worker, monitor_result.result)
            self._restart_worker(worker_id)
            self.pending_worker_assignments.add(worker_id)
            return

        if not monitor_result.task_completed:
            return

        if monitor_result.result.error_type == ErrorType.NON_RECOVERABLE:
            self.manager_logger.error(f"[Worker {worker_id}] Non-recoverable error, restarting")
            self._worker_completed(worker, monitor_result.result)
            self._restart_worker(worker_id)
            return

        if on_failure_increment_failed and not monitor_result.result.success:
            increment_stat(self.lock, self.stats, "failed")
            binary_name = worker.current_binary.path.name if worker.current_binary else "unknown"
            giveup_msg = f"[GiveUp] {binary_name}"
            self.manager_logger.warning(giveup_msg)

        self._worker_completed(worker, monitor_result.result)

        # Restart worker after successful completion if always_restart_worker is enabled
        # and there are still binaries to process
        if self.always_restart_worker and monitor_result.result.success and self.pending_binaries:
            self.manager_logger.info(f"[Worker {worker_id}] Restarting worker after successful completion")
            self._restart_worker(worker_id)
            return

        # Get updated worker reference in case it was restarted elsewhere
        worker = self.workers[worker_id]
        if is_initial_phase:
            assigned = self._assign_binary_to_worker_initial_phase(worker)
        else:
            assigned = self._assign_binary_to_worker_normal(worker, retry_attempt=False)
            if not assigned:
                assigned = self._assign_binary_to_worker_normal(worker, retry_attempt=True)

        if not assigned and not self.pending_binaries and allow_stop:
            log_to_worker_file(self.log_dir, worker_id, f"Worker {worker_id} stopping (no more tasks)")
            self.manager_logger.info(f"[Worker {worker_id}] Stopping (no more tasks)")
            success, error = send_worker_command(worker, "stop")
            if not success:
                socket_error_msg = f"[Worker {worker_id}] Socket error while sending stop: {error}"
                self.manager_logger.warning(socket_error_msg)
            active_workers.discard(worker_id)

    def _wait_for_workers_ready(self) -> None:
        """Wait for all workers to be connection_established and ready."""
        import time

        self.manager_logger.info("Waiting for all workers to be ready...")

        timeout = 300  # 5 minutes
        start_time = time.time()

        while True:
            if time.time() - start_time > timeout:
                raise TimeoutError("Workers did not become ready within timeout")

            # Check manual worker connections
            self._check_manual_worker_connections()

            # Check if all workers are connected and ready
            all_connected = all(w.connection_established for w in self.workers)
            all_ready = all(w.ready for w in self.workers)

            if all_connected and all_ready:
                self.manager_logger.info("All workers ready!")
                break

            # Monitor workers to receive ready messages
            for worker in self.workers:
                if worker.connection_established and not worker.ready:
                    from .worker_communication import receive_worker_messages

                    msg = receive_worker_messages(worker)
                    if msg.success and msg.parsed_responses:
                        for response in msg.parsed_responses:
                            if response == "ready":
                                worker.ready = True
                                self.manager_logger.info(f"[Worker {worker.worker_id}] Ready signal received")

            time.sleep(0.1)

    def _process_worker_loop(
        self,
        active_workers: set[int],
        allow_stop: bool = True,
        on_failure_increment_failed: bool = False,
        is_initial_phase: bool = False,
    ) -> None:
        """Main worker processing loop."""
        while active_workers or self.pending_binaries:
            # Check for manual worker connections
            if self.manual_start_worker:
                self._check_manual_worker_connections()

            # Check for ready messages from connected workers (all modes)
            for worker in self.workers:
                if worker.connection_established and not worker.ready:
                    from .worker_communication import receive_worker_messages

                    msg = receive_worker_messages(worker)
                    if msg.success and msg.parsed_responses:
                        for response in msg.parsed_responses:
                            if response == "ready":
                                worker.ready = True
                                self.manager_logger.info(f"[Worker {worker.worker_id}] Ready signal received")

            for worker_id in list(active_workers):
                worker = self.workers[worker_id]

                # Skip workers that aren't ready
                if not worker.ready or not worker.connection_established:
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

                    def increment_failed():
                        increment_stat(self.lock, self.stats, "failed")

                    monitor_result = monitor_worker_once(
                        worker,
                        worker_id,
                        self.manager_logger,
                        self.task_definition,
                        on_failure_increment_failed,
                        increment_failed,
                    )

                    if monitor_result.should_restart or monitor_result.task_completed:
                        self._handle_monitor_result(
                            monitor_result,
                            worker,
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
                            self._assign_binary_to_worker_initial_phase(worker)
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
                from .models import FailedTask

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
        for worker_id in range(self.num_workers):
            worker = self._start_worker(worker_id)
            self.workers.append(worker)

        # Wait for all workers to be ready before proceeding
        self._wait_for_workers_ready()

    def _run_initial_phase(self) -> None:
        """Run the initial processing phase."""
        active_workers = set(range(self.num_workers))
        self._process_worker_loop(
            active_workers, allow_stop=False, on_failure_increment_failed=False, is_initial_phase=True
        )

        # Report assigned memory totals
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

    def _run_retry_phase(self) -> None:
        """Run the retry phase for failed tasks."""
        process_retry_phase(
            self.failed_tasks,
            self.pending_binaries,
            self.num_workers,
            self._process_worker_loop,
            self.manager_logger,
        )

    def _run_oom_phase(self) -> None:
        """Run the OOM phase with single worker."""
        self.in_oom_phase = True
        process_oom_phase(
            self.oom_tasks,
            self.pending_binaries,
            self.workers,
            self.log_dir,
            self._process_worker_loop,
            self.manager_logger,
        )
        self.in_oom_phase = False

    def _run_unassigned_phase(self) -> None:
        """Run the unassigned phase with single worker and no memory limit."""
        process_unassigned_phase(
            self.unassigned_tasks,
            self.workers,
            self.source_dir,
            self.log_dir,
            self.lock,
            self.stats,
            self.task_definition,
            self._restart_worker,
            self._worker_completed,
            self.manager_logger,
        )

    def process_binaries(self, binaries: list[BinaryInfo]) -> None:
        """Main processing entry point."""
        self.pending_binaries = binaries.copy()
        self.stats["total"] = len(binaries)
        self.stats["completed"] = 0
        self.stats["failed"] = 0

        start_msg = f"Starting {self.num_workers} workers with {self.max_memory / (1024**3):.2f}GB memory limit"
        process_msg = f"Processing {self.stats['total']} binaries"
        self.manager_logger.info(start_msg)
        self.manager_logger.info(process_msg)

        self._initialize_workers()
        self._run_initial_phase()
        self._run_retry_phase()
        self._run_oom_phase()
        self._run_unassigned_phase()

        cleanup_workers(self.workers)

        final_msg = (
            f"[*] Completed: {self.stats['completed']}/{self.stats['total']}, "
            f"Failed: {self.stats['failed']}/{self.stats['total']}, "
            f"Skipped: {self.stats['skipped']}/{self.stats['total']}"
        )
        self.manager_logger.info(final_msg)
