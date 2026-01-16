import threading
from pathlib import Path

from .binary_info import BinaryInfo
from .memory import estimate_memory, get_actual_memory_usage
from .models import ErrorType, FailedTask, ProcessingPhase, TaskResult, WorkerState


def assign_binary_to_worker(
    worker: WorkerState,
    pending_binaries: list[BinaryInfo],
    available_memory: int,
    source_dir: Path,
    lock: threading.Lock,
) -> tuple[bool, int]:
    """Try to assign a binary to the worker. Returns (assigned, new_available_memory)."""
    with lock:
        actual_usage = get_actual_memory_usage()

        for i, binary in enumerate(pending_binaries):
            estimated = estimate_memory(binary.size)

            if available_memory - estimated >= 0:
                pending_binaries.pop(i)
                worker.current_binary = binary
                worker.estimated_memory = estimated
                new_available_memory = available_memory - estimated

                try:
                    relative_path = binary.path.relative_to(source_dir)
                except ValueError:
                    relative_path = binary.path

                message = f"{relative_path}\n"
                worker.socket.sendall(message.encode("utf-8"))

                return True, new_available_memory

        return False, available_memory


def worker_completed(
    worker: WorkerState,
    result: TaskResult,
    oom_tasks: list[FailedTask],
    failed_tasks: list[FailedTask],
    stats: dict[str, int],
    lock: threading.Lock,
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
                print(status)
            else:
                if result.error_type == ErrorType.OUT_OF_MEMORY:
                    print(f"[OOM] {worker.current_binary.path.name}")
                    oom_tasks.append(
                        FailedTask(
                            binary=worker.current_binary,
                            error_type=result.error_type,
                            error_message=result.error_message or "",
                        )
                    )
                elif result.error_type == ErrorType.NON_RECOVERABLE:
                    print(f"[Worker crashed] {worker.current_binary.path.name}")
                    stats["failed"] += 1
                    released_memory = worker.estimated_memory
                    worker.current_binary = None
                    worker.estimated_memory = 0
                    worker.phase = None
                    worker.phase_start_time = None
                    worker.last_keepalive = None
                    return released_memory
                else:
                    print(f"[Errored] {worker.current_binary.path.name}")
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
