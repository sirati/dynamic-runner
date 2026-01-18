import threading

from .comm import ErrorType
from .models import FailedTask, TaskResult, WorkerState


def worker_completed(
    worker: WorkerState,
    result: TaskResult,
    oom_tasks: list[FailedTask],
    failed_tasks: list[FailedTask],
    stats: dict[str, int],
    lock: threading.Lock,
    logger=None,
) -> int:
    """Mark worker as completed and release memory. Returns released memory amount."""
    with lock:
        released_memory = 0
        if worker.current_binary:
            if result.success:
                stats["completed"] += 1
                status = f"[{stats['completed']}/{stats['total']}] Completed: {worker.current_binary.path.name}"
                # if result.warnings > 0 or result.filtered > 0:
                status += f" (warnings: {result.warnings}, filtered: {result.filtered})"
                if logger:
                    logger.info(status)
            else:
                if result.error_type == ErrorType.OUT_OF_MEMORY:
                    if logger:
                        logger.warning(f"[Worker {worker.worker_id}] [OOM] {worker.current_binary.path.name}")
                        if result.error_message:
                            logger.warning(f"  Error: {result.error_message}")
                    oom_tasks.append(
                        FailedTask(
                            binary=worker.current_binary,
                            error_type=result.error_type,
                            error_message=result.error_message or "",
                        )
                    )
                elif result.error_type == ErrorType.NON_RECOVERABLE:
                    if logger:
                        logger.warning(
                            f"[Worker {worker.worker_id}] [Worker crashed] {worker.current_binary.path.name}"
                        )
                        if result.error_message:
                            logger.warning(f"  Error: {result.error_message}")
                    failed_tasks.append(
                        FailedTask(
                            binary=worker.current_binary,
                            error_type=result.error_type,
                            error_message=result.error_message or "",
                        )
                    )
                else:
                    if logger:
                        logger.warning(f"[Worker {worker.worker_id}] [Errored] {worker.current_binary.path.name}")
                        if result.error_message:
                            logger.warning(f"  Error: {result.error_message}")
                    failed_tasks.append(
                        FailedTask(
                            binary=worker.current_binary,
                            error_type=result.error_type or ErrorType.RECOVERABLE,
                            error_message=result.error_message or "",
                        )
                    )

            released_memory = worker.estimated_memory
            worker.current_binary = None
            worker.estimated_memory = 0
            worker.phase = None
            worker.phase_start_time = None
            worker.last_keepalive = None

        return released_memory
