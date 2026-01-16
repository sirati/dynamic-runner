import logging
import threading
from datetime import datetime
from pathlib import Path

from .binary_info import BinaryInfo
from .models import ErrorType, TaskResult, WorkerState
from .processing_phases import process_oom_phase, process_retry_phase, process_unassigned_phase
from .task_handler import assign_binary_to_worker, worker_completed
from .worker_communication import send_worker_command
from .worker_lifecycle import restart_worker, start_worker
from .worker_monitoring import monitor_worker_once
from .worker_utils import cleanup_workers, increment_stat, log_to_worker_file


class WorkerManager:
    def __init__(
        self,
        num_workers: int,
        max_memory: int,
        source_dir: Path,
        output_dir: Path,
        platform_arg: str,
        skip_existing: bool,
        print_pid: bool,
        always_restart_worker: bool = False,
    ):
        self.num_workers = num_workers
        self.max_memory = max_memory
        self.reserved_memory_per_worker = 650 * 1024 * 1024
        self.source_dir = source_dir
        self.output_dir = output_dir
        self.platform_arg = platform_arg
        self.skip_existing = skip_existing
        self.print_pid = print_pid
        self.always_restart_worker = always_restart_worker

        self.workers: list[WorkerState] = []
        self.available_memory = max_memory
        self.lock = threading.Lock()
        self.pending_binaries: list[BinaryInfo] = []
        self.failed_tasks: list = []
        self.oom_tasks: list = []
        self.unassigned_tasks: list[BinaryInfo] = []
        self.stats = {"completed": 0, "failed": 0, "total": 0, "skipped": 0}

        start_time = datetime.now().strftime("%Y%m%d_%H%M%S")
        self.log_dir = output_dir / "logs" / start_time
        self.log_dir.mkdir(parents=True, exist_ok=True)

        self.manager_logger = self._setup_logger()

        # Memory usage log file
        self.memuse_log_path = output_dir / "memuse.log"

    def _setup_logger(self) -> logging.Logger:
        """Setup and configure the manager logger."""
        manager_log_path = self.log_dir / "manager.log"
        logger = logging.getLogger("manager")
        logger.setLevel(logging.INFO)
        logger.propagate = False

        file_handler = logging.FileHandler(manager_log_path, mode="a")
        file_handler.setLevel(logging.INFO)
        file_formatter = logging.Formatter(
            "%(levelname)s | %(asctime)s,%(msecs)03d | %(message)s", datefmt="%Y-%m-%d %H:%M:%S"
        )
        file_handler.setFormatter(file_formatter)
        logger.addHandler(file_handler)

        console_handler = logging.StreamHandler()
        console_handler.setLevel(logging.INFO)
        console_formatter = logging.Formatter("%(levelname)s %(asctime)s | %(name)s %(message)s", datefmt="%H:%M")
        console_handler.setFormatter(console_formatter)
        logger.addHandler(console_handler)

        return logger

    def _log_memory_usage(self, worker: WorkerState, errored: bool) -> None:
        """Log memory usage to memuse.log file."""
        if worker.current_binary is None:
            return

        max_memory = worker.max_memory_current_task

        # Convert to MB
        binary_size_mb = worker.current_binary.size / (1024 * 1024)
        max_memory_mb = max_memory / (1024 * 1024)

        # Format: binary_name, size_mb, max_memory_mb[+]
        binary_name = worker.current_binary.path.name
        suffix = "+" if errored else ""

        log_line = f"{binary_name}, {binary_size_mb:.2f}, {max_memory_mb:.2f}{suffix}\n"

        try:
            with open(self.memuse_log_path, "a") as f:
                f.write(log_line)
        except Exception as e:
            self.manager_logger.warning(f"Failed to write to memuse.log: {e}")

        # Reset memory tracking for this worker's next task
        worker.max_memory_current_task = 0
        worker.last_memory_check = None

    def _start_worker(self, worker_id: int) -> WorkerState:
        """Start a new worker process."""
        worker_log_path = self.log_dir / f"worker_{worker_id}.log"
        log_to_worker_file(self.log_dir, worker_id, f"Manager: Worker {worker_id} starting")

        worker = start_worker(
            worker_id,
            self.source_dir,
            self.output_dir,
            self.platform_arg,
            self.skip_existing,
            worker_log_path,
        )

        self.manager_logger.info(f"[Worker {worker_id}] Started with PID {worker.process.pid}")
        return worker

    def _restart_worker(self, worker_id: int) -> WorkerState:
        """Restart a worker process."""
        old_worker = self.workers[worker_id]
        worker_log_path = self.log_dir / f"worker_{worker_id}.log"
        log_to_worker_file(
            self.log_dir, worker_id, f"Manager: Worker {worker_id} restarting (old PID: {old_worker.process.pid})"
        )

        new_worker = restart_worker(
            old_worker,
            self.source_dir,
            self.output_dir,
            self.platform_arg,
            self.skip_existing,
            worker_log_path,
        )

        self.manager_logger.info(
            f"[Worker {worker_id}] Restarted with PID {new_worker.process.pid} (old PID: {old_worker.process.pid})"
        )

        with self.lock:
            self.workers[worker_id] = new_worker

        return new_worker

    def _assign_binary_to_worker(self, worker: WorkerState, track_unassigned: bool = False) -> bool:
        """Try to assign a binary to the worker."""
        unassigned_list = self.unassigned_tasks if track_unassigned else None

        # Calculate reserved memory based on idle workers
        # Reserve memory for idle workers (excluding the one we're trying to assign to)
        idle_workers = sum(1 for w in self.workers if w.current_binary is None)
        reserved_memory = max(0, (idle_workers - 1) * self.reserved_memory_per_worker)

        assigned, new_memory = assign_binary_to_worker(
            worker,
            self.pending_binaries,
            self.available_memory,
            reserved_memory,
            self.source_dir,
            self.lock,
            unassigned_list,
            self.manager_logger,
        )
        if assigned:
            self.available_memory = new_memory
            binary_name = worker.current_binary.path.name if worker.current_binary else "unknown"
            self.manager_logger.info(f"[Worker {worker.worker_id}] Assigned: {binary_name}")
        return assigned

    def _worker_completed(self, worker: WorkerState, result: TaskResult) -> None:
        """Mark worker as completed and release memory."""
        # Log memory usage before completing
        errored = not result.success
        self._log_memory_usage(worker, errored)

        released = worker_completed(
            worker,
            result,
            self.oom_tasks,
            self.failed_tasks,
            self.stats,
            self.lock,
            self.manager_logger,
        )
        self.available_memory += released

    def _handle_worker_without_task(
        self, worker: WorkerState, worker_id: int, active_workers: set[int], allow_stop: bool
    ) -> bool:
        """Handle a worker that has no current task. Returns True if worker should continue."""
        if self._assign_binary_to_worker(worker):
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

        return True

    def _handle_monitor_result(
        self,
        monitor_result,
        worker: WorkerState,
        worker_id: int,
        active_workers: set[int],
        allow_stop: bool,
        on_failure_increment_failed: bool,
    ) -> None:
        """Handle the result from monitoring a worker."""
        if monitor_result.should_restart:
            self._worker_completed(worker, monitor_result.result)
            self._restart_worker(worker_id)
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
            # Don't try to assign to the old worker after restart, return early
            return

        # Get updated worker reference in case it was restarted elsewhere
        worker = self.workers[worker_id]
        if not self._assign_binary_to_worker(worker) and not self.pending_binaries and allow_stop:
            log_to_worker_file(self.log_dir, worker_id, f"Worker {worker_id} stopping (no more tasks)")
            self.manager_logger.info(f"[Worker {worker_id}] Stopping (no more tasks)")
            success, error = send_worker_command(worker, "stop")
            if not success:
                socket_error_msg = f"[Worker {worker_id}] Socket error while sending stop: {error}"
                self.manager_logger.warning(socket_error_msg)
            active_workers.discard(worker_id)

    def _process_worker_loop(
        self,
        active_workers: set[int],
        allow_stop: bool = True,
        on_failure_increment_failed: bool = False,
    ) -> None:
        """Main worker processing loop."""
        while active_workers or self.pending_binaries:
            for worker_id in list(active_workers):
                worker = self.workers[worker_id]

                if worker.current_binary is None:
                    if not self._handle_worker_without_task(worker, worker_id, active_workers, allow_stop):
                        continue
                else:

                    def increment_failed():
                        increment_stat(self.lock, self.stats, "failed")

                    monitor_result = monitor_worker_once(
                        worker,
                        worker_id,
                        self.manager_logger,
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
                        )

            if active_workers:
                threading.Event().wait(0.1)

    def _initialize_workers(self) -> None:
        """Initialize all worker processes."""
        for i in range(self.num_workers):
            worker = self._start_worker(i)
            self.workers.append(worker)

    def _run_initial_phase(self) -> None:
        """Run the initial processing phase."""
        active_workers = set(range(self.num_workers))
        self._process_worker_loop(active_workers, allow_stop=False, on_failure_increment_failed=False)
        self.unassigned_tasks = list(set(self.unassigned_tasks))

    def _run_retry_phase(self) -> None:
        """Run the retry phase for failed tasks."""
        process_retry_phase(
            self.failed_tasks,
            self.pending_binaries,
            self.num_workers,
            self._process_worker_loop,
            self.manager_logger,
        )
        self.unassigned_tasks = list(set(self.unassigned_tasks))

    def _run_oom_phase(self) -> None:
        """Run the OOM phase with single worker."""
        process_oom_phase(
            self.oom_tasks,
            self.pending_binaries,
            self.workers,
            self.log_dir,
            self._process_worker_loop,
            self.manager_logger,
        )
        self.unassigned_tasks = list(set(self.unassigned_tasks))

    def _run_unassigned_phase(self) -> None:
        """Run the unassigned phase with single worker and no memory limit."""
        process_unassigned_phase(
            self.unassigned_tasks,
            self.workers,
            self.source_dir,
            self.log_dir,
            self.lock,
            self.stats,
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
