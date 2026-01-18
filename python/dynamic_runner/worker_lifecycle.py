import logging
import socket
import subprocess
import sys
import time
from pathlib import Path
from typing import Any

from .comm import NamedSocketInterface, UnixSocketInterface
from .models import WorkerState
from .task import TaskDefinition

try:
    import psutil
except ImportError:
    psutil = None


def start_worker(
    worker_id: int,
    source_dir: Path,
    output_dir: Path,
    worker_log_path: Path,
    task_definition: TaskDefinition,
    task_args,
    skip_existing: bool,
    manual_start: bool = False,
    connection_mode: str = "socketpair",
    socket_path: Path | None = None,
) -> WorkerState:
    """Start a worker process with dynamic_queue mode."""
    if connection_mode == "named":
        # Use named socket for named mode
        if socket_path is None:
            raise ValueError("socket_path is required for named connection mode")

        cmd = [
            sys.executable,
            "-m",
            task_definition.get_worker_module(),
            "--socket-path",
            str(socket_path),
            "--source",
            str(source_dir),
            "--output",
            str(output_dir),
            "--log-file",
            str(worker_log_path),
        ]
    elif connection_mode == "socketpair":
        # Use socketpair() for socketpair mode
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
    logger.debug(f"[Worker {worker_id}] Starting with command: {' '.join(cmd)}")

    if connection_mode == "named":
        # Create named socket interface (server side)
        comm_interface = NamedSocketInterface(socket_path, is_server=True)
        child_fd = None

        if manual_start:
            if psutil is None:
                raise RuntimeError("psutil is required for manual worker start mode")

            print(f"\n[Worker {worker_id}] Please run the following command in another terminal:")
            print(f"  {' '.join(cmd)}")
            print(f"[Worker {worker_id}] Manager will detect when worker connects via socket: {socket_path}")

            # Don't wait for process - let it be detected later
            process = None
            connection_established = False
        else:
            process = subprocess.Popen(
                cmd,
                stdin=subprocess.DEVNULL,
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
            )
            connection_established = True
    else:  # anonymous mode
        comm_interface = UnixSocketInterface(parent_sock)

        if manual_start:
            if psutil is None:
                raise RuntimeError("psutil is required for manual worker start mode")

            print(f"\n[Worker {worker_id}] Please run the following command in another terminal:")
            print(f"  {' '.join(cmd)}")
            print(f"[Worker {worker_id}] Manager will detect when worker connects")

            # Don't wait for process - let it be detected later
            process = None
            connection_established = False
        else:
            process = subprocess.Popen(
                cmd,
                pass_fds=[child_fd],
                stdin=subprocess.DEVNULL,
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
            )
            child_sock.close()
            connection_established = True

    return WorkerState(
        process=process,
        comm=comm_interface,
        current_binary=None,
        estimated_memory=0,
        worker_id=worker_id,
        child_fd=child_fd,
        socket_path=socket_path if connection_mode == "named" else None,
        connection_established=connection_established,
        ready=False,
        connection_established_time=None if not connection_established else time.time(),
    )


def restart_worker(
    worker: WorkerState,
    source_dir: Path,
    output_dir: Path,
    worker_log_path: Path,
    task_definition: TaskDefinition,
    task_args,
    skip_existing: bool,
    manual_start: bool = False,
    connection_mode: str = "socketpair",
    socket_path: Path | None = None,
) -> WorkerState:
    """Restart a worker that encountered a non-recoverable error."""
    try:
        worker.process.terminate()
        worker.comm.close()
    except Exception:
        pass

    new_worker = start_worker(
        worker.worker_id,
        source_dir,
        output_dir,
        worker_log_path,
        task_definition,
        task_args,
        skip_existing,
        manual_start,
        connection_mode,
        socket_path,
    )
    return new_worker


def check_worker_timeout(worker: WorkerState, task_definition: TaskDefinition) -> bool:
    """Check if worker has timed out based on phase and task definition."""
    if worker.phase is None or worker.last_keepalive is None:
        return False

    # Find the stage definition for current phase
    stages = task_definition.get_stages()
    for stage in stages:
        if stage.phase.value == worker.phase:
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
        if stage.phase.value == worker.phase and stage.timeout_seconds is None:
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


def check_manual_worker_connection(worker: WorkerState, socket_path: Path) -> tuple[Any, bool]:
    """Check if a manually started worker process has connected.

    Returns:
        (process, found) - process object if found and stable, or None; found=True if process detected
    """
    if psutil is None:
        return (None, False)

    socket_path_str = str(socket_path)

    for proc in psutil.process_iter(["pid", "cmdline", "create_time"]):
        try:
            cmdline = proc.info.get("cmdline")
            if cmdline and "--socket-path" in cmdline:
                try:
                    idx = cmdline.index("--socket-path")
                    if idx + 1 < len(cmdline) and cmdline[idx + 1] == socket_path_str:
                        # Found a matching process
                        # Wrap psutil.Process to look like subprocess.Popen
                        class ProcessWrapper:
                            def __init__(self, psutil_proc):
                                self._proc = psutil_proc
                                self.pid = psutil_proc.pid

                            def poll(self):
                                if self._proc.is_running():
                                    return None
                                return self._proc.status()

                            def wait(self, timeout=None):
                                self._proc.wait(timeout)

                            def terminate(self):
                                self._proc.terminate()

                        return (ProcessWrapper(proc), True)
                except (ValueError, IndexError):
                    continue
        except (psutil.NoSuchProcess, psutil.AccessDenied):
            continue

    return (None, False)
