import logging
import socket
import subprocess
import sys
import time
from pathlib import Path

from .comm import UnixSocketInterface
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

    if manual_start:
        if psutil is None:
            raise RuntimeError("psutil is required for manual worker start mode")

        print(f"\n[Worker {worker_id}] Please run the following command in another terminal:")
        print(f"  {' '.join(cmd)}")
        print(f"[Worker {worker_id}] Waiting for process with socket FD {child_fd} to start...")

        # Wait for process with matching socket FD argument to appear
        process = None
        fd_arg = str(child_fd)
        timeout = 300  # 5 minutes timeout
        start_time = time.time()

        while process is None:
            if time.time() - start_time > timeout:
                raise TimeoutError(f"Worker {worker_id} did not start within {timeout} seconds")

            for proc in psutil.process_iter(["pid", "cmdline"]):
                try:
                    cmdline = proc.info.get("cmdline")
                    if cmdline and "--dynamic_queue" in cmdline:
                        # Find the index of --dynamic_queue and check if next arg matches our FD
                        try:
                            idx = cmdline.index("--dynamic_queue")
                            if idx + 1 < len(cmdline) and cmdline[idx + 1] == fd_arg:
                                process = proc
                                logger.info(f"[Worker {worker_id}] Found process with PID {proc.pid}")
                                print(f"[Worker {worker_id}] Found process with PID {proc.pid}")

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

                                process = ProcessWrapper(proc)
                                break
                        except (ValueError, IndexError):
                            continue
                except (psutil.NoSuchProcess, psutil.AccessDenied):
                    continue

            if process is None:
                time.sleep(0.5)

        child_sock.close()
    else:
        process = subprocess.Popen(
            cmd,
            pass_fds=[child_fd],
            stdin=subprocess.DEVNULL,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )
        child_sock.close()

    comm_interface = UnixSocketInterface(parent_sock)

    return WorkerState(
        process=process,
        comm=comm_interface,
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
    manual_start: bool = False,
) -> WorkerState:
    """Restart a worker that encountered a non-recoverable error."""
    try:
        worker.process.terminate()
        worker.comm.close()
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
        manual_start,
    )


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
