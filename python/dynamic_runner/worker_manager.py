import logging
import threading
import time
from datetime import datetime
from pathlib import Path

from .binary_info import BinaryInfo
from .models import ErrorType, ProcessingPhase, TaskResult, WorkerState
from .task_handler import assign_binary_to_worker, worker_completed
from .worker_communication import (
    WorkerCommunicationError,
    log_pickled_error,
    receive_worker_messages,
    send_worker_command,
)
from .worker_lifecycle import (
    check_worker_timeout,
    print_phase_status,
    restart_worker,
    start_worker,
)


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
    ):
        self.num_workers = num_workers
        self.max_memory = max_memory
        self.source_dir = source_dir
        self.output_dir = output_dir
        self.platform_arg = platform_arg
        self.skip_existing = skip_existing
        self.print_pid = print_pid

        self.workers: list[WorkerState] = []
        self.available_memory = max_memory
        self.lock = threading.Lock()
        self.pending_binaries: list[BinaryInfo] = []
        self.failed_tasks: list = []
        self.oom_tasks: list = []
        self.stats = {"completed": 0, "failed": 0, "total": 0}

        start_time = datetime.now().strftime("%Y%m%d_%H%M%S")
        self.log_dir = output_dir / "logs" / start_time
        self.log_dir.mkdir(parents=True, exist_ok=True)

        manager_log_path = self.log_dir / "manager.log"
        self.manager_logger = logging.getLogger("manager")
        self.manager_logger.setLevel(logging.INFO)
        self.manager_logger.propagate = False

        file_handler = logging.FileHandler(manager_log_path, mode="a")
        file_handler.setLevel(logging.INFO)
        formatter = logging.Formatter(
            "%(levelname)s | %(asctime)s,%(msecs)03d | %(message)s", datefmt="%Y-%m-%d %H:%M:%S"
        )
        file_handler.setFormatter(formatter)
        self.manager_logger.addHandler(file_handler)

        console_handler = logging.StreamHandler()
        console_handler.setLevel(logging.INFO)
        console_handler.setFormatter(formatter)
        self.manager_logger.addHandler(console_handler)

    def _start_worker(self, worker_id: int) -> WorkerState:
        worker_log_path = self.log_dir / f"worker_{worker_id}.log"

        with open(worker_log_path, "a") as f:
            f.write(f"INFO | {datetime.now().strftime('%Y-%m-%d %H:%M:%S,%f')[:-3]} | Worker {worker_id} starting\n")

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

    def _restart_worker(self, worker_id: int) -> None:
        old_worker = self.workers[worker_id]
        worker_log_path = self.log_dir / f"worker_{worker_id}.log"

        with open(worker_log_path, "a") as f:
            f.write(
                f"INFO | {datetime.now().strftime('%Y-%m-%d %H:%M:%S,%f')[:-3]} | Worker {worker_id} restarting (old PID: {old_worker.process.pid})\n"
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

    def _assign_binary_to_worker(self, worker: WorkerState) -> bool:
        assigned, new_memory = assign_binary_to_worker(
            worker,
            self.pending_binaries,
            self.available_memory,
            self.source_dir,
            self.lock,
            self.manager_logger,
        )
        if assigned:
            self.available_memory = new_memory
            self.manager_logger.info(
                f"[Worker {worker.worker_id}] Assigned: {worker.current_binary.path.name if worker.current_binary else 'unknown'}"
            )
        return assigned

    def _worker_completed(self, worker: WorkerState, result: TaskResult) -> None:
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

    def _process_worker_loop(
        self,
        active_workers: set[int],
        allow_stop: bool = True,
        on_failure_increment_failed: bool = False,
    ) -> None:
        while active_workers or self.pending_binaries:
            for worker_id in list(active_workers):
                worker = self.workers[worker_id]

                if worker.current_binary is None:
                    if not self._assign_binary_to_worker(worker):
                        if not self.pending_binaries:
                            if allow_stop:
                                success, error = send_worker_command(worker, "stop")
                                if not success:
                                    crash_msg = f"[Worker {worker_id}] Socket error while sending stop, worker likely crashed: {error}"
                                    self.manager_logger.error(crash_msg)
                                    self._restart_worker(worker_id)
                                    worker = self.workers[worker_id]
                                    active_workers.add(worker_id)
                                    continue
                            active_workers.remove(worker_id)
                else:
                    if check_worker_timeout(worker):
                        timeout_msg = f"[Timeout] Worker {worker_id} timed out - {worker.current_binary.path.name}"
                        self.manager_logger.warning(timeout_msg)
                        if on_failure_increment_failed:
                            with self.lock:
                                self.stats["failed"] += 1
                        result = TaskResult(
                            success=False, error_type=ErrorType.RECOVERABLE, error_message="Worker timeout"
                        )
                        self._worker_completed(worker, result)
                        self._restart_worker(worker_id)
                        worker = self.workers[worker_id]
                        active_workers.add(worker_id)
                        continue

                    print_phase_status(worker, self.manager_logger)

                    message = receive_worker_messages(worker)

                    if not message.success:
                        crash_msg = f"[Worker {worker_id}] {message.error_type.value}: {message.error_message}"
                        self.manager_logger.error(crash_msg)
                        if on_failure_increment_failed:
                            with self.lock:
                                self.stats["failed"] += 1
                        result = TaskResult(
                            success=False,
                            error_type=ErrorType.RECOVERABLE,
                            error_message=message.error_message or "Worker communication error",
                        )
                        self._worker_completed(worker, result)
                        self._restart_worker(worker_id)
                        worker = self.workers[worker_id]
                        active_workers.add(worker_id)
                        continue

                    if message.pickled_error_info:
                        log_pickled_error(worker_id, message.pickled_error_info, self.manager_logger)
                        if on_failure_increment_failed:
                            with self.lock:
                                self.stats["failed"] += 1
                        result = TaskResult(
                            success=False,
                            error_type=ErrorType.NON_RECOVERABLE,
                            error_message=message.pickled_error_info.get("message", "Unknown error"),
                        )
                        self._worker_completed(worker, result)
                        self._restart_worker(worker_id)
                        worker = self.workers[worker_id]
                        active_workers.add(worker_id)
                        continue

                    if message.parsed_responses:
                        for parsed in message.parsed_responses:
                            if isinstance(parsed, ProcessingPhase):
                                worker.phase = parsed
                                worker.phase_start_time = time.time()
                                worker.last_printed_minute = None
                                self.manager_logger.info(
                                    f"[Worker {worker_id}] Phase: {parsed.value} - {worker.current_binary.path.name if worker.current_binary else 'unknown'}"
                                )
                            elif isinstance(parsed, TaskResult):
                                if parsed.error_type == ErrorType.NON_RECOVERABLE:
                                    self.manager_logger.error(f"[Worker {worker_id}] Non-recoverable error, restarting")
                                    self._worker_completed(worker, parsed)
                                    self._restart_worker(worker_id)
                                    worker = self.workers[worker_id]
                                    active_workers.add(worker_id)
                                else:
                                    if on_failure_increment_failed and not parsed.success:
                                        with self.lock:
                                            self.stats["failed"] += 1
                                        giveup_msg = f"[GiveUp] {worker.current_binary.path.name if worker.current_binary else 'unknown'}"
                                        self.manager_logger.warning(giveup_msg)
                                    self._worker_completed(worker, parsed)
                                    if not self._assign_binary_to_worker(worker):
                                        if not self.pending_binaries:
                                            if allow_stop:
                                                worker_log_path = self.log_dir / f"worker_{worker_id}.log"
                                                with open(worker_log_path, "a") as f:
                                                    f.write(
                                                        f"INFO | {datetime.now().strftime('%Y-%m-%d %H:%M:%S,%f')[:-3]} | Worker {worker_id} stopping (no more tasks)\n"
                                                    )
                                                self.manager_logger.info(
                                                    f"[Worker {worker_id}] Stopping (no more tasks)"
                                                )
                                                success, error = send_worker_command(worker, "stop")
                                                if not success:
                                                    socket_error_msg = (
                                                        f"[Worker {worker_id}] Socket error while sending stop: {error}"
                                                    )
                                                    self.manager_logger.warning(socket_error_msg)
                                            active_workers.remove(worker_id)

            if active_workers:
                threading.Event().wait(0.1)

    def process_binaries(self, binaries: list[BinaryInfo]) -> None:
        """Main processing loop."""
        self.pending_binaries = binaries.copy()
        self.stats["total"] = len(binaries)
        self.stats["completed"] = 0
        self.stats["failed"] = 0

        start_msg = f"Starting {self.num_workers} workers with {self.max_memory / (1024**3):.2f}GB memory limit"
        process_msg = f"Processing {self.stats['total']} binaries"
        self.manager_logger.info(start_msg)
        self.manager_logger.info(process_msg)

        for i in range(self.num_workers):
            worker = self._start_worker(i)
            self.workers.append(worker)

        active_workers = set(range(self.num_workers))
        self._process_worker_loop(active_workers, allow_stop=False, on_failure_increment_failed=False)

        if self.failed_tasks:
            retry_msg = f"[*] Retrying {len(self.failed_tasks)} failed tasks"
            self.manager_logger.info(retry_msg)
            retry_tasks = self.failed_tasks.copy()
            self.failed_tasks = []

            for failed_task in retry_tasks:
                self.pending_binaries.append(failed_task.binary)

            active_workers = set(range(self.num_workers))
            self._process_worker_loop(active_workers, allow_stop=True, on_failure_increment_failed=True)

        if self.oom_tasks:
            oom_msg = f"[*] Processing {len(self.oom_tasks)} OOM tasks with single worker"
            self.manager_logger.info(oom_msg)

            for worker_id in range(1, self.num_workers):
                try:
                    worker_log_path = self.log_dir / f"worker_{worker_id}.log"
                    with open(worker_log_path, "a") as f:
                        f.write(
                            f"INFO | {datetime.now().strftime('%Y-%m-%d %H:%M:%S,%f')[:-3]} | Worker {worker_id} stopping for OOM processing\n"
                        )
                    self.manager_logger.info(f"[Worker {worker_id}] Stopping for OOM processing")
                    send_worker_command(self.workers[worker_id], "stop")
                    self.workers[worker_id].process.wait(timeout=5)
                    self.workers[worker_id].socket.close()
                except Exception:
                    pass

            for oom_task in self.oom_tasks:
                self.pending_binaries.append(oom_task.binary)

            active_workers = {0}
            self._process_worker_loop(active_workers, allow_stop=False, on_failure_increment_failed=True)

            worker_log_path = self.log_dir / f"worker_0.log"
            with open(worker_log_path, "a") as f:
                f.write(
                    f"INFO | {datetime.now().strftime('%Y-%m-%d %H:%M:%S,%f')[:-3]} | Worker 0 stopping after OOM tasks\n"
                )
            self.manager_logger.info("[Worker 0] Stopping after OOM tasks")
            self.workers[0].socket.sendall(b"stop\n")

        for worker in self.workers:
            try:
                worker.process.wait(timeout=5)
                worker.socket.close()
            except:
                pass

        final_msg = f"[*] Completed: {self.stats['completed']}/{self.stats['total']}, Failed: {self.stats['failed']}/{self.stats['total']}"
        self.manager_logger.info(final_msg)
