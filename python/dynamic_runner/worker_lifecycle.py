import socket
import subprocess
import sys
import time
from pathlib import Path

from .models import ProcessingPhase, WorkerState


def start_worker(
    worker_id: int,
    source_dir: Path,
    output_dir: Path,
    platform_arg: str,
    skip_existing: bool,
) -> WorkerState:
    """Start a worker process with dynamic_queue mode."""
    parent_sock, child_sock = socket.socketpair()

    child_fd = child_sock.fileno()

    cmd = [
        sys.executable,
        "-m",
        "tokenizer",
        "--dynamic_queue",
        str(child_fd),
        "--source",
        str(source_dir),
        "--output",
        str(output_dir),
        "--platform",
        platform_arg,
    ]

    if skip_existing:
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


def restart_worker(
    worker: WorkerState,
    source_dir: Path,
    output_dir: Path,
    platform_arg: str,
    skip_existing: bool,
) -> WorkerState:
    """Restart a worker that encountered a non-recoverable error."""
    try:
        worker.process.terminate()
        worker.socket.close()
    except:
        pass

    print(f"[*] Restarting worker {worker.worker_id}")
    return start_worker(
        worker.worker_id,
        source_dir,
        output_dir,
        platform_arg,
        skip_existing,
    )


def check_worker_timeout(worker: WorkerState) -> bool:
    """Check if worker has timed out based on phase."""
    if worker.phase == ProcessingPhase.PHASE_3 and worker.last_keepalive:
        if time.time() - worker.last_keepalive > 10:
            return True
    return False


def print_phase_status(worker: WorkerState) -> None:
    """Print status message for long-running phases."""
    if worker.phase in [ProcessingPhase.PHASE_1, ProcessingPhase.PHASE_2] and worker.phase_start_time:
        elapsed = time.time() - worker.phase_start_time
        if elapsed >= 60:
            minutes = int(elapsed / 60)
            if minutes in [1, 5, 10, 30, 60] or (minutes > 60 and minutes % 60 == 0):
                if worker.last_printed_minute != minutes:
                    worker.last_printed_minute = minutes
                    print(
                        f"[Worker {worker.worker_id}] Still in {worker.phase.value}, {minutes} minute(s) elapsed - {worker.current_binary.path.name if worker.current_binary else 'unknown'}"
                    )
