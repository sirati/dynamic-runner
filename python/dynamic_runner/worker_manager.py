import threading
import time
from pathlib import Path

from .binary_info import BinaryInfo
from .models import ErrorType, ProcessingPhase, TaskResult, WorkerState
from .task_handler import assign_binary_to_worker, parse_response, worker_completed
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

    def _start_worker(self, worker_id: int) -> WorkerState:
        worker = start_worker(
            worker_id,
            self.source_dir,
            self.output_dir,
            self.platform_arg,
            self.skip_existing,
        )
        if self.print_pid:
            print(f"[Worker {worker_id}] Started with PID {worker.process.pid}")
        return worker

    def _restart_worker(self, worker_id: int) -> None:
        old_worker = self.workers[worker_id]
        new_worker = restart_worker(
            old_worker,
            self.source_dir,
            self.output_dir,
            self.platform_arg,
            self.skip_existing,
        )
        if self.print_pid:
            print(f"[Worker {worker_id}] Restarted with PID {new_worker.process.pid}")
        with self.lock:
            self.workers[worker_id] = new_worker

    def _assign_binary_to_worker(self, worker: WorkerState) -> bool:
        assigned, new_memory = assign_binary_to_worker(
            worker,
            self.pending_binaries,
            self.available_memory,
            self.source_dir,
            self.lock,
        )
        if assigned:
            self.available_memory = new_memory
        return assigned

    def _worker_completed(self, worker: WorkerState, result: TaskResult) -> None:
        released = worker_completed(
            worker,
            result,
            self.oom_tasks,
            self.failed_tasks,
            self.stats,
            self.lock,
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
                                worker.socket.sendall(b"stop\n")
                            active_workers.remove(worker_id)
                else:
                    if check_worker_timeout(worker):
                        print(f"[Timeout] Worker {worker_id} timed out - {worker.current_binary.path.name}")
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

                    print_phase_status(worker)

                    worker.socket.setblocking(False)
                    try:
                        data = worker.socket.recv(1024)
                        if data:
                            responses = data.decode("utf-8").strip()
                            responses = responses.split("\n")
                            worker.last_keepalive = time.time()
                            for response in responses:
                                parsed = parse_response(response)

                                if isinstance(parsed, ProcessingPhase):
                                    worker.phase = parsed
                                    worker.phase_start_time = time.time()
                                    worker.last_printed_minute = None
                                elif isinstance(parsed, TaskResult):
                                    if parsed.error_type == ErrorType.NON_RECOVERABLE:
                                        # todo force kill worker after some extra time
                                        self._worker_completed(worker, parsed)
                                        self._restart_worker(worker_id)
                                        worker = self.workers[worker_id]
                                        active_workers.add(worker_id)
                                    else:
                                        if on_failure_increment_failed and not parsed.success:
                                            with self.lock:
                                                self.stats["failed"] += 1
                                            print(
                                                f"[GiveUp] {worker.current_binary.path.name if worker.current_binary else 'unknown'}"
                                            )
                                        self._worker_completed(worker, parsed)
                                        if not self._assign_binary_to_worker(worker):
                                            if not self.pending_binaries:
                                                if allow_stop:
                                                    worker.socket.sendall(b"stop\n")
                                                active_workers.remove(worker_id)
                    except BlockingIOError:
                        pass
                    finally:
                        worker.socket.setblocking(True)

            if active_workers:
                threading.Event().wait(0.1)

    def process_binaries(self, binaries: list[BinaryInfo]) -> None:
        """Main processing loop."""
        self.pending_binaries = binaries.copy()
        self.stats["total"] = len(binaries)
        self.stats["completed"] = 0
        self.stats["failed"] = 0

        print(f"Starting {self.num_workers} workers with {self.max_memory / (1024**3):.2f}GB memory limit")
        print(f"Processing {self.stats['total']} binaries")

        for i in range(self.num_workers):
            worker = self._start_worker(i)
            self.workers.append(worker)

        active_workers = set(range(self.num_workers))
        self._process_worker_loop(active_workers, allow_stop=False, on_failure_increment_failed=False)

        if self.failed_tasks:
            print(f"\n[*] Retrying {len(self.failed_tasks)} failed tasks")
            retry_tasks = self.failed_tasks.copy()
            self.failed_tasks = []

            for failed_task in retry_tasks:
                self.pending_binaries.append(failed_task.binary)

            active_workers = set(range(self.num_workers))
            self._process_worker_loop(active_workers, allow_stop=True, on_failure_increment_failed=True)

        if self.oom_tasks:
            print(f"\n[*] Processing {len(self.oom_tasks)} OOM tasks with single worker")

            for worker_id in range(1, self.num_workers):
                try:
                    self.workers[worker_id].socket.sendall(b"stop\n")
                    self.workers[worker_id].process.wait(timeout=5)
                    self.workers[worker_id].socket.close()
                except:
                    pass

            for oom_task in self.oom_tasks:
                self.pending_binaries.append(oom_task.binary)

            active_workers = {0}
            self._process_worker_loop(active_workers, allow_stop=False, on_failure_increment_failed=True)

            self.workers[0].socket.sendall(b"stop\n")

        for worker in self.workers:
            try:
                worker.process.wait(timeout=5)
                worker.socket.close()
            except:
                pass

        print(
            f"\n[*] Completed: {self.stats['completed']}/{self.stats['total']}, Failed: {self.stats['failed']}/{self.stats['total']}"
        )
