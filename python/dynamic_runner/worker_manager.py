import os
import socket
import subprocess
import sys
import threading
from dataclasses import dataclass
from enum import Enum
from pathlib import Path

from .binary_info import BinaryInfo
from .memory import estimate_memory, get_actual_memory_usage


class ErrorType(Enum):
    OUT_OF_MEMORY = "oom"
    NON_RECOVERABLE = "non_recoverable"
    RECOVERABLE = "recoverable"


@dataclass
class TaskResult:
    success: bool
    error_type: ErrorType | None = None
    error_message: str | None = None


@dataclass
class WorkerState:
    process: subprocess.Popen
    socket: socket.socket
    current_binary: BinaryInfo | None
    estimated_memory: int
    worker_id: int


@dataclass
class FailedTask:
    binary: BinaryInfo
    error_type: ErrorType
    error_message: str
    retry_count: int = 0


class WorkerManager:
    def __init__(
        self,
        num_workers: int,
        max_memory: int,
        source_dir: Path,
        output_dir: Path,
        platform_arg: str,
        skip_existing: bool,
    ):
        self.num_workers = num_workers
        self.max_memory = max_memory
        self.source_dir = source_dir
        self.output_dir = output_dir
        self.platform_arg = platform_arg
        self.skip_existing = skip_existing

        self.workers: list[WorkerState] = []
        self.available_memory = max_memory
        self.lock = threading.Lock()
        self.pending_binaries: list[BinaryInfo] = []
        self.failed_tasks: list[FailedTask] = []
        self.oom_tasks: list[FailedTask] = []
        self.completed = 0
        self.failed = 0
        self.total = 0

    def start_worker(self, worker_id: int) -> WorkerState:
        """Start a worker process with dynamic_queue mode."""
        parent_sock, child_sock = socket.socketpair()

        child_fd = child_sock.fileno()

        cmd = [
            sys.executable,
            "-m",
            "tokenizer.low_level",
            "--dynamic_queue",
            str(child_fd),
            "--source",
            str(self.source_dir),
            "--output",
            str(self.output_dir),
            "--platform",
            self.platform_arg,
        ]

        if self.skip_existing:
            cmd.append("--skip_existing")

        process = subprocess.Popen(
            cmd,
            pass_fds=[child_fd],
            stdin=subprocess.DEVNULL,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
        )

        child_sock.close()

        return WorkerState(
            process=process,
            socket=parent_sock,
            current_binary=None,
            estimated_memory=0,
            worker_id=worker_id,
        )

    def assign_binary_to_worker(self, worker: WorkerState) -> bool:
        """Try to assign a binary to the worker. Returns True if assigned, False if no suitable binary."""
        with self.lock:
            actual_usage = get_actual_memory_usage()

            for i, binary in enumerate(self.pending_binaries):
                estimated = estimate_memory(binary.size)

                if self.available_memory - estimated >= 0:
                    self.pending_binaries.pop(i)
                    worker.current_binary = binary
                    worker.estimated_memory = estimated
                    self.available_memory -= estimated

                    try:
                        relative_path = binary.path.relative_to(self.source_dir)
                    except ValueError:
                        relative_path = binary.path

                    message = f"{relative_path}\n"
                    worker.socket.sendall(message.encode("utf-8"))

                    return True

            return False

    def worker_completed(self, worker: WorkerState, result: TaskResult) -> None:
        """Mark worker as completed and release memory."""
        with self.lock:
            if worker.current_binary:
                if result.success:
                    self.completed += 1
                    print(f"[{self.completed}/{self.total}] Completed: {worker.current_binary.path.name}")
                else:
                    if result.error_type == ErrorType.OUT_OF_MEMORY:
                        print(f"[OOM] {worker.current_binary.path.name}")
                        self.oom_tasks.append(
                            FailedTask(
                                binary=worker.current_binary,
                                error_type=result.error_type,
                                error_message=result.error_message or "",
                            )
                        )
                    elif result.error_type == ErrorType.NON_RECOVERABLE:
                        print(f"[NonRecoverable] {worker.current_binary.path.name}")
                        self.failed += 1
                        return
                    else:
                        print(
                            f"[Error:{result.error_type.value if result.error_type else 'unknown'}] {worker.current_binary.path.name}"
                        )
                        self.failed_tasks.append(
                            FailedTask(
                                binary=worker.current_binary,
                                error_type=result.error_type or ErrorType.RECOVERABLE,
                                error_message=result.error_message or "",
                            )
                        )

                self.available_memory += worker.estimated_memory
                worker.current_binary = None
                worker.estimated_memory = 0

    def parse_response(self, response: str) -> TaskResult:
        """Parse worker response into TaskResult."""
        if response == "done":
            return TaskResult(success=True)
        elif response.startswith("error:"):
            parts = response.split(":", 2)
            if len(parts) >= 3:
                error_type_str = parts[1]
                error_message = parts[2]
                try:
                    error_type = ErrorType(error_type_str)
                except ValueError:
                    error_type = ErrorType.RECOVERABLE
                return TaskResult(success=False, error_type=error_type, error_message=error_message)
        return TaskResult(success=False, error_type=ErrorType.RECOVERABLE, error_message="Unknown response")

    def restart_worker(self, worker_id: int) -> None:
        """Restart a worker that encountered a non-recoverable error."""
        with self.lock:
            old_worker = self.workers[worker_id]
            try:
                old_worker.process.terminate()
                old_worker.socket.close()
            except:
                pass

            print(f"[*] Restarting worker {worker_id}")
            new_worker = self.start_worker(worker_id)
            self.workers[worker_id] = new_worker

    def process_binaries(self, binaries: list[BinaryInfo]) -> None:
        """Main processing loop."""
        self.pending_binaries = binaries.copy()
        self.total = len(binaries)
        self.completed = 0
        self.failed = 0

        print(f"Starting {self.num_workers} workers with {self.max_memory / (1024**3):.2f}GB memory limit")
        print(f"Processing {self.total} binaries")

        for i in range(self.num_workers):
            worker = self.start_worker(i)
            self.workers.append(worker)

        active_workers = set(range(self.num_workers))

        while active_workers or self.pending_binaries:
            for worker_id in list(active_workers):
                worker = self.workers[worker_id]

                if worker.current_binary is None:
                    if not self.assign_binary_to_worker(worker):
                        if not self.pending_binaries:
                            worker.socket.sendall(b"stop\n")
                            active_workers.remove(worker_id)
                else:
                    worker.socket.setblocking(False)
                    try:
                        data = worker.socket.recv(1024)
                        if data:
                            response = data.decode("utf-8").strip()
                            result = self.parse_response(response)

                            if result.error_type == ErrorType.NON_RECOVERABLE:
                                self.worker_completed(worker, result)
                                self.restart_worker(worker_id)
                                worker = self.workers[worker_id]
                                active_workers.add(worker_id)
                            else:
                                self.worker_completed(worker, result)
                                if not self.assign_binary_to_worker(worker):
                                    if not self.pending_binaries:
                                        worker.socket.sendall(b"stop\n")
                                        active_workers.remove(worker_id)
                    except BlockingIOError:
                        pass
                    finally:
                        worker.socket.setblocking(True)

            if active_workers:
                threading.Event().wait(0.1)

        if self.failed_tasks:
            print(f"\n[*] Retrying {len(self.failed_tasks)} failed tasks")
            retry_tasks = self.failed_tasks.copy()
            self.failed_tasks = []

            for failed_task in retry_tasks:
                self.pending_binaries.append(failed_task.binary)

            active_workers = set(range(self.num_workers))

            while active_workers or self.pending_binaries:
                for worker_id in list(active_workers):
                    worker = self.workers[worker_id]

                    if worker.current_binary is None:
                        if not self.assign_binary_to_worker(worker):
                            if not self.pending_binaries:
                                worker.socket.sendall(b"stop\n")
                                active_workers.remove(worker_id)
                    else:
                        worker.socket.setblocking(False)
                        try:
                            data = worker.socket.recv(1024)
                            if data:
                                response = data.decode("utf-8").strip()
                                result = self.parse_response(response)

                                if result.error_type == ErrorType.NON_RECOVERABLE:
                                    self.worker_completed(worker, result)
                                    self.restart_worker(worker_id)
                                    worker = self.workers[worker_id]
                                    active_workers.add(worker_id)
                                else:
                                    if not result.success:
                                        self.failed += 1
                                        print(
                                            f"[GiveUp] {worker.current_binary.path.name if worker.current_binary else 'unknown'}"
                                        )
                                    self.worker_completed(worker, result)
                                    if not self.assign_binary_to_worker(worker):
                                        if not self.pending_binaries:
                                            worker.socket.sendall(b"stop\n")
                                            active_workers.remove(worker_id)
                        except BlockingIOError:
                            pass
                        finally:
                            worker.socket.setblocking(True)

                if active_workers:
                    threading.Event().wait(0.1)

        if self.oom_tasks:
            print(f"\n[*] Processing {len(self.oom_tasks)} OOM tasks with single worker")

            for worker_id in range(1, self.num_workers):
                try:
                    self.workers[worker_id].socket.sendall(b"stop\n")
                    self.workers[worker_id].process.wait(timeout=5)
                    self.workers[worker_id].socket.close()
                except:
                    pass

            single_worker = self.workers[0]

            for oom_task in self.oom_tasks:
                self.pending_binaries.append(oom_task.binary)

            while self.pending_binaries:
                if single_worker.current_binary is None:
                    if not self.assign_binary_to_worker(single_worker):
                        break
                else:
                    single_worker.socket.setblocking(False)
                    try:
                        data = single_worker.socket.recv(1024)
                        if data:
                            response = data.decode("utf-8").strip()
                            result = self.parse_response(response)

                            if result.error_type == ErrorType.NON_RECOVERABLE:
                                self.worker_completed(single_worker, result)
                                self.restart_worker(0)
                                single_worker = self.workers[0]
                            else:
                                if not result.success:
                                    self.failed += 1
                                    print(
                                        f"[GiveUp] {single_worker.current_binary.path.name if single_worker.current_binary else 'unknown'}"
                                    )
                                self.worker_completed(single_worker, result)
                    except BlockingIOError:
                        pass
                    finally:
                        single_worker.socket.setblocking(True)

                threading.Event().wait(0.1)

            single_worker.socket.sendall(b"stop\n")

        for worker in self.workers:
            try:
                worker.process.wait(timeout=5)
                worker.socket.close()
            except:
                pass

        print(f"\n[*] Completed: {self.completed}/{self.total}, Failed: {self.failed}/{self.total}")
