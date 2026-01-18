import time
from dataclasses import dataclass

from .models import ErrorType, TaskResult, WorkerState
from .task import TaskDefinition
from .worker_communication import log_pickled_error, receive_worker_messages
from .worker_lifecycle import check_worker_timeout, print_phase_status

try:
    import psutil
except ImportError:
    psutil = None


@dataclass
class WorkerMonitorResult:
    should_restart: bool
    task_completed: bool
    result: TaskResult | None


def _get_process_memory(pid: int) -> int:
    """Get memory usage of a process in bytes. Returns 0 if unavailable."""
    if psutil is None:
        return 0
    try:
        process = psutil.Process(pid)
        # Get RSS (Resident Set Size) which is physical memory usage
        return process.memory_info().rss
    except (psutil.NoSuchProcess, psutil.AccessDenied, psutil.ZombieProcess):
        return 0


def monitor_worker_once(
    worker: WorkerState,
    worker_id: int,
    logger,
    task_definition: TaskDefinition,
    on_failure_increment_failed: bool = False,
    increment_failed_callback=None,
) -> WorkerMonitorResult:
    """Monitor worker once and return the result.

    Args:
        worker: Worker state
        worker_id: Worker ID for logging
        logger: Logger instance
        task_definition: Task definition for timeout checking
        on_failure_increment_failed: Whether to increment failed count on error
        increment_failed_callback: Optional callback to increment failed count

    Returns:
        WorkerMonitorResult indicating what action to take
    """
    if check_worker_timeout(worker, task_definition):
        binary_name = worker.current_binary.path.name if worker.current_binary else "unknown"
        timeout_msg = f"[Timeout] Worker {worker_id} timed out - {binary_name}"
        logger.warning(timeout_msg)
        if on_failure_increment_failed and increment_failed_callback:
            increment_failed_callback()
        result = TaskResult(success=False, error_type=ErrorType.RECOVERABLE, error_message="Worker timeout")
        return WorkerMonitorResult(should_restart=True, task_completed=True, result=result)

    print_phase_status(worker, logger, task_definition)

    message = receive_worker_messages(worker)

    if not message.success:
        crash_msg = f"[Worker {worker_id}] {message.error_type.value}: {message.error_message}"
        logger.error(crash_msg)
        if on_failure_increment_failed and increment_failed_callback:
            increment_failed_callback()
        result = TaskResult(
            success=False,
            error_type=ErrorType.RECOVERABLE,
            error_message=message.error_message or "Worker communication error",
        )
        return WorkerMonitorResult(should_restart=True, task_completed=True, result=result)

    if message.pickled_error_info:
        log_pickled_error(worker_id, message.pickled_error_info, logger)
        if on_failure_increment_failed and increment_failed_callback:
            increment_failed_callback()
        result = TaskResult(
            success=False,
            error_type=ErrorType.NON_RECOVERABLE,
            error_message=message.pickled_error_info.get("message", "Unknown error"),
        )
        return WorkerMonitorResult(should_restart=True, task_completed=True, result=result)

    if message.parsed_responses:
        for parsed in message.parsed_responses:
            if isinstance(parsed, str):
                worker.phase = parsed
                worker.phase_start_time = time.time()
                worker.last_printed_minute = None
                binary_name = worker.current_binary.path.name if worker.current_binary else "unknown"
                logger.info(f"[Worker {worker_id}] Phase: {parsed} - {binary_name}")
            elif isinstance(parsed, TaskResult):
                return WorkerMonitorResult(
                    should_restart=parsed.error_type == ErrorType.NON_RECOVERABLE,
                    task_completed=True,
                    result=parsed,
                )

    return WorkerMonitorResult(should_restart=False, task_completed=False, result=None)
