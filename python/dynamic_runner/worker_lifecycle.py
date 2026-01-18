import logging
import socket
import subprocess
import sys
import time
from pathlib import Path

from .models import WorkerState
from .task import TaskDefinition


def start_worker(
    worker_id: int,
    source_dir: Path,
    output_dir: Path,
    worker_log_path: Path,
    task_definition: TaskDefinition,
    task_args,
    skip_existing: bool,
) -> WorkerState:
    """Start a worker process with dynamic_queue mode."""
    parent_sock, child_sock = socket.socketpair()

    child_fd = child_sock.fileno()

    cmd = [
        sys.executable,
        "-m",
        task_definition.get_worker_module(),
        "--dynamic_queue",
        str(child_fd),
        "--source",
        str(source_dir),
        "--output",
        str(output_dir),
        "--log-file",
        str(worker_log_path),
    ]

    # Add task-specific arguments
    task_cmd_args = task_definition.build_worker_command_args(task_args, source_dir, output_dir, skip_existing)
    cmd.extend(task_cmd_args)

    if skip_existing:
        cmd.append("--skip_existing")

    # Log the full command line at debug level
    logger = logging.getLogger("manager")
    logger.info(f"[Worker {worker_id}] Starting with command: {' '.join(cmd)}")

    process = subprocess.Popen(
        cmd,
        pass_fds=[child_fd],
        stdin=subprocess.DEVNULL,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
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
    worker_log_path: Path,
    task_definition: TaskDefinition,
    task_args,
    skip_existing: bool,
) -> WorkerState:
    """Restart a worker that encountered a non-recoverable error."""
    try:
        worker.process.terminate()
        worker.socket.close()
    except Exception:
        pass

    return start_worker(
        worker.worker_id,
        source_dir,
        output_dir,
        worker_log_path,
        task_definition,
        task_args,
        skip_existing,
    )


def check_worker_timeout(worker: WorkerState, task_definition: TaskDefinition) -> bool:
    """Check if worker has timed out based on phase and task definition."""
    if worker.phase is None or worker.last_keepalive is None:
        return False

    # Find the stage definition for current phase
    stages = task_definition.get_stages()
    for stage in stages:
        if stage.name == worker.phase:
            if stage.timeout_seconds is not None:
                if time.time() - worker.last_keepalive > stage.timeout_seconds:
                    return True
            break

    return False


def print_phase_status(worker: WorkerState, logger, task_definition: TaskDefinition) -> None:
    """Log status message for long-running phases."""
    if worker.phase is None or worker.phase_start_time is None:
        return

    # Check if current phase has no timeout (long-running)
    stages = task_definition.get_stages()
    is_long_running = False
    for stage in stages:
        if stage.name == worker.phase and stage.timeout_seconds is None:
            is_long_running = True
            break

    if is_long_running and worker.phase_start_time:
        elapsed = time.time() - worker.phase_start_time
        if elapsed >= 60:
            minutes = int(elapsed / 60)
            if minutes in [1, 5, 10, 30, 60] or (minutes > 60 and minutes % 60 == 0):
                if worker.last_printed_minute != minutes:
                    worker.last_printed_minute = minutes
                    binary_name = worker.current_binary.path.name if worker.current_binary else "unknown"
                    logger.info(
                        f"[Worker {worker.worker_id}] Still in {worker.phase}, "
                        f"{minutes} minute(s) elapsed - {binary_name}"
                    )
