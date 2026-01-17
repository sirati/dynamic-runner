import threading
from dataclasses import dataclass
from pathlib import Path

from .binary_info import BinaryInfo
from .memory import estimate_memory, get_actual_memory_usage
from .models import ErrorType, FailedTask, ProcessingPhase, TaskResult, WorkerState


@dataclass
class AssignmentResult:
    assigned: bool
    new_available_memory: int
    socket_error: bool = False
    memory_insufficient: bool = False


def assign_binary_to_worker(
    worker: WorkerState,
    pending_binaries: list[BinaryInfo],
    available_memory: int,
    reserved_memory: int,
    source_dir: Path,
    lock: threading.Lock,
    unassigned_tasks: list[BinaryInfo] | None = None,
    logger=None,
    initial_phase_budget: int | None = None,
) -> AssignmentResult:
    """Try to assign a binary to the worker.

    Tasks that cannot be assigned due to memory constraints are added to unassigned_tasks if provided.
    Returns AssignmentResult with assignment status, new memory, and socket error flag.

    If initial_phase_budget is provided, tasks that exceed the budget are marked as opportunistic.
    If the worker has a reserved_budget from completing a non-opportunistic task, it is used instead.
    """
    with lock:
        actual_usage = get_actual_memory_usage()

        # Use worker's reserved budget if available, otherwise use initial_phase_budget
        effective_budget = worker.reserved_budget if worker.reserved_budget > 0 else initial_phase_budget

        for i, binary in enumerate(pending_binaries):
            estimated = estimate_memory(binary.size)

            # If we have a budget, skip tasks that exceed it
            if effective_budget is not None and estimated > effective_budget:
                continue

            # Check if we have enough available memory
            mark_opportunistic = False
            if available_memory - estimated < reserved_memory:
                # Not enough available memory, mark as opportunistic
                mark_opportunistic = True
                if logger:
                    available_mb = available_memory / (1024 * 1024)
                    estimated_mb = estimated / (1024 * 1024)
                    reserved_mb = reserved_memory / (1024 * 1024)
                    logger.info(
                        f"[Worker {worker.worker_id}] Insufficient memory "
                        f"(available: {available_mb:.2f}MB, estimated: {estimated_mb:.2f}MB, "
                        f"reserved: {reserved_mb:.2f}MB), marking as opportunistic"
                    )

            # Assign the task (either normally or opportunistically)
            pending_binaries.pop(i)
            worker.current_binary = binary
            worker.estimated_memory = estimated
            new_available_memory = available_memory - estimated

            try:
                relative_path = binary.path.relative_to(source_dir)
            except ValueError:
                relative_path = binary.path

            message = f"{relative_path}\n"
            try:
                worker.socket.sendall(message.encode("utf-8"))
                worker.opportunistic = mark_opportunistic
            except (BrokenPipeError, ConnectionResetError, OSError) as e:
                if logger:
                    logger.error(f"[Worker {worker.worker_id}] Socket error while assigning binary: {e}")
                pending_binaries.insert(i, binary)
                worker.current_binary = None
                worker.estimated_memory = 0
                return AssignmentResult(assigned=False, new_available_memory=available_memory, socket_error=True)

            if logger:
                size_mb = binary.size / (1024 * 1024)
                estimated_mb = estimated / (1024 * 1024)
                budget_str = ""
                if effective_budget is not None:
                    budget_mb = effective_budget / (1024 * 1024)
                    budget_str = f", budget: {budget_mb:.2f}MB"
                opportunistic_str = " (opportunistic)" if mark_opportunistic else ""
                logger.info(
                    f"[Worker {worker.worker_id}] Binary size: {size_mb:.2f}MB, "
                    f"Estimated memory: {estimated_mb:.2f}MB{budget_str}, "
                    f"Available after: {new_available_memory / (1024 * 1024):.2f}MB{opportunistic_str}"
                )

            return AssignmentResult(assigned=True, new_available_memory=new_available_memory)

        # No binary could be assigned - check if it's due to memory constraints
        memory_insufficient = False
        if pending_binaries:
            memory_insufficient = True
            if unassigned_tasks is not None:
                for binary in pending_binaries:
                    if binary not in unassigned_tasks:
                        unassigned_tasks.append(binary)

        return AssignmentResult(
            assigned=False, new_available_memory=available_memory, memory_insufficient=memory_insufficient
        )


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
                    # Reset reserved budget on OOM
                    worker.reserved_budget = 0
                    worker.has_completed_non_opportunistic = False
                elif result.error_type == ErrorType.NON_RECOVERABLE:
                    if logger:
                        logger.error(f"[Worker {worker.worker_id}] [Worker crashed] {worker.current_binary.path.name}")
                        if result.error_message:
                            logger.error(f"  Error: {result.error_message}")
                    stats["failed"] += 1
                    released_memory = worker.estimated_memory
                    worker.current_binary = None
                    worker.estimated_memory = 0
                    worker.phase = None
                    worker.phase_start_time = None
                    worker.last_keepalive = None
                    # Reset reserved budget on crash
                    worker.reserved_budget = 0
                    worker.has_completed_non_opportunistic = False
                    return released_memory
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

            # Reserve memory for non-opportunistic successful tasks
            if result.success and not worker.opportunistic:
                actual_memory = worker.max_memory_current_task
                reserved = max(actual_memory, worker.estimated_memory)
                worker.reserved_budget = reserved
                worker.has_completed_non_opportunistic = True
                if logger:
                    reserved_mb = reserved / (1024 * 1024)
                    logger.info(f"[Worker {worker.worker_id}] Reserved budget: {reserved_mb:.2f}MB for future tasks")

            released_memory = worker.estimated_memory
            worker.current_binary = None
            worker.estimated_memory = 0
            worker.phase = None
            worker.phase_start_time = None
            worker.last_keepalive = None

        return released_memory


def parse_response(response: str) -> TaskResult | ProcessingPhase | None:
    """Parse worker response into TaskResult or phase update."""
    # print(f"[Received response] {response}")
    if response == "done":
        return TaskResult(success=True)
    elif response.startswith("done:"):
        parts = response.split(":", 3)
        warnings = int(parts[1]) if len(parts) > 1 else 0
        filtered = int(parts[2]) if len(parts) > 2 else 0
        return TaskResult(success=True, warnings=warnings, filtered=filtered)
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
    elif response.startswith("phase:"):
        phase_str = response.split(":", 1)[1]
        try:
            return ProcessingPhase(phase_str)
        except ValueError:
            pass
    elif response == "keepalive":
        return None
    return TaskResult(success=False, error_type=ErrorType.RECOVERABLE, error_message="Unknown response")
