import logging
import threading
from pathlib import Path

from .binary_info import BinaryInfo
from .memory import get_free_system_memory
from .models import ErrorType, FailedTask, TaskResult, WorkerState
from .task import TaskDefinition
from .worker_monitoring import WorkerMonitorResult, monitor_worker_once
from .worker_utils import increment_stat, log_to_worker_file, stop_workers_except


def process_retry_phase(
    failed_tasks: list[FailedTask],
    pending_binaries: list[BinaryInfo],
    num_workers: int,
    process_worker_loop_callback,
    logger: logging.Logger,
) -> None:
    """Process retry phase for failed tasks."""
    if not failed_tasks:
        return

    retry_msg = f"[*] Retrying {len(failed_tasks)} failed tasks"
    logger.info(retry_msg)
    retry_tasks = failed_tasks.copy()
    failed_tasks.clear()

    for failed_task in retry_tasks:
        pending_binaries.append(failed_task.binary)

    active_workers = set(range(num_workers))
    process_worker_loop_callback(active_workers, allow_stop=True, on_failure_increment_failed=True)


def process_oom_phase(
    oom_tasks: list[FailedTask],
    pending_binaries: list[BinaryInfo],
    workers: list[WorkerState],
    log_dir: Path,
    process_worker_loop_callback,
    logger: logging.Logger,
) -> None:
    """Process OOM tasks with single worker."""
    if not oom_tasks:
        return

    oom_msg = f"[*] Processing {len(oom_tasks)} OOM tasks with single worker"
    logger.info(oom_msg)

    stop_workers_except(workers, keep_worker_id=0, log_dir=log_dir, logger=logger, reason="OOM processing")

    for oom_task in oom_tasks:
        pending_binaries.append(oom_task.binary)

    active_workers = {0}
    process_worker_loop_callback(active_workers, allow_stop=False, on_failure_increment_failed=True)

    log_to_worker_file(log_dir, 0, "Worker 0 stopping after OOM tasks")
    logger.info("[Worker 0] Stopping after OOM tasks")
    workers[0].socket.sendall(b"stop\n")


def process_unassigned_phase(
    unassigned_tasks: list[BinaryInfo],
    workers: list[WorkerState],
    source_dir: Path,
    log_dir: Path,
    lock: threading.Lock,
    stats: dict,
    task_definition: TaskDefinition,
    restart_worker_callback,
    worker_completed_callback,
    logger: logging.Logger,
) -> None:
    """Process unassigned tasks with single worker and no memory limit."""
    if not unassigned_tasks:
        return

    unassigned_msg = f"[*] Processing {len(unassigned_tasks)} unassigned tasks with single worker (no memory limit)"
    logger.info(unassigned_msg)

    stop_workers_except(workers, keep_worker_id=0, log_dir=log_dir, logger=logger, reason="unassigned task processing")

    unassigned_tasks.sort(key=lambda b: b.size)

    for binary in unassigned_tasks:
        if not _process_single_unassigned_binary(
            binary,
            workers[0],
            source_dir,
            log_dir,
            lock,
            stats,
            task_definition,
            restart_worker_callback,
            worker_completed_callback,
            logger,
        ):
            workers[0] = restart_worker_callback(0)

    log_to_worker_file(log_dir, 0, "Worker 0 stopping after unassigned tasks")
    logger.info("[Worker 0] Stopping after unassigned tasks")
    workers[0].socket.sendall(b"stop\n")


def _process_single_unassigned_binary(
    binary: BinaryInfo,
    worker: WorkerState,
    source_dir: Path,
    log_dir: Path,
    lock: threading.Lock,
    stats: dict,
    task_definition: TaskDefinition,
    restart_worker_callback,
    worker_completed_callback,
    logger: logging.Logger,
) -> bool:
    """Process a single unassigned binary. Returns True if worker is still alive."""
    free_mem_mb = get_free_system_memory() / (1024 * 1024)
    if free_mem_mb < 300:
        skip_msg = f"[Skipped - Low Memory] {binary.path.name} (free: {free_mem_mb:.0f}MB)"
        logger.warning(skip_msg)
        increment_stat(lock, stats, "skipped")
        return True

    if not _assign_binary_directly(binary, worker, source_dir, logger):
        increment_stat(lock, stats, "skipped")
        return False

    return _monitor_unassigned_binary(
        binary, worker, lock, stats, task_definition, restart_worker_callback, worker_completed_callback, logger
    )


def _assign_binary_directly(binary: BinaryInfo, worker: WorkerState, source_dir: Path, logger: logging.Logger) -> bool:
    """Directly assign a binary to a worker without memory checks."""
    try:
        worker.current_binary = binary
        worker.estimated_memory = 0

        try:
            relative_path = binary.path.relative_to(source_dir)
        except ValueError:
            relative_path = binary.path

        message = f"{relative_path}\n"
        worker.socket.sendall(message.encode("utf-8"))

        size_mb = binary.size / (1024 * 1024)
        logger.info(
            f"[Worker {worker.worker_id}] Assigned (no limit): {binary.path.name}, Binary size: {size_mb:.2f}MB"
        )
        return True
    except Exception as e:
        logger.error(f"[Worker {worker.worker_id}] Failed to assign {binary.path.name}: {e}")
        return False


def _monitor_unassigned_binary(
    binary: BinaryInfo,
    worker: WorkerState,
    lock: threading.Lock,
    stats: dict,
    task_definition: TaskDefinition,
    restart_worker_callback,
    worker_completed_callback,
    logger: logging.Logger,
) -> bool:
    """Monitor worker processing an unassigned binary. Returns True if worker is still alive."""
    import time

    while True:
        free_mem_mb = get_free_system_memory() / (1024 * 1024)
        if free_mem_mb < 300:
            kill_msg = f"[Killed - Low Memory] {binary.path.name} (free: {free_mem_mb:.0f}MB)"
            logger.warning(kill_msg)
            increment_stat(lock, stats, "skipped")
            return False

        def increment_skipped():
            increment_stat(lock, stats, "skipped")

        monitor_result = monitor_worker_once(
            worker,
            worker.worker_id,
            logger,
            task_definition,
            on_failure_increment_failed=True,
            increment_failed_callback=increment_skipped,
        )

        if monitor_result.task_completed:
            if monitor_result.result and not monitor_result.result.success:
                increment_stat(lock, stats, "skipped")
                skip_final_msg = f"[Skipped] {binary.path.name}"
                logger.warning(skip_final_msg)
            worker_completed_callback(worker, monitor_result.result)
            return not monitor_result.should_restart

        time.sleep(0.1)
