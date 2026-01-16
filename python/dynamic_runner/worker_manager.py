import os
import socket
import subprocess
import sys
import threading
from dataclasses import dataclass
from pathlib import Path

from .binary_info import BinaryInfo
from .memory import estimate_memory, get_actual_memory_usage


@dataclass
class WorkerState:
    process: subprocess.Popen
    socket: socket.socket
    current_binary: BinaryInfo | None
    estimated_memory: int


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
        self.completed = 0
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

    def worker_completed(self, worker: WorkerState) -> None:
        """Mark worker as completed and release memory."""
        with self.lock:
            if worker.current_binary:
                self.completed += 1
                print(f"[{self.completed}/{self.total}] Completed: {worker.current_binary.path.name}")
                self.available_memory += worker.estimated_memory
                worker.current_binary = None
                worker.estimated_memory = 0

    def process_binaries(self, binaries: list[BinaryInfo]) -> None:
        """Main processing loop."""
        self.pending_binaries = binaries.copy()
        self.total = len(binaries)
        self.completed = 0

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
                            if response == "done":
                                self.worker_completed(worker)
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

        for worker in self.workers:
            worker.process.wait()
            worker.socket.close()

        print(f"\nAll {self.total} binaries processed")
