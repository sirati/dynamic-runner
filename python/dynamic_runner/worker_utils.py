import logging
import threading
from datetime import datetime
from pathlib import Path

from .models import WorkerState
from .worker_communication import send_worker_command


def log_to_worker_file(log_dir: Path, worker_id: int, message: str) -> None:
    """Write a log message to a worker's log file."""
    worker_log_path = log_dir / f"worker_{worker_id}.log"
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S,%f")[:-3]
    with open(worker_log_path, "a") as f:
        f.write(f"INFO | {timestamp} | {message}\n")


def stop_workers_except(
    workers: list[WorkerState],
    keep_worker_id: int,
    log_dir: Path,
    logger: logging.Logger,
    reason: str,
) -> None:
    """Stop all workers except one.

    Args:
        workers: List of all workers
        keep_worker_id: ID of worker to keep running
        log_dir: Directory for worker logs
        logger: Logger instance
        reason: Reason for stopping (for logging)
    """
    for worker_id in range(len(workers)):
        if worker_id == keep_worker_id:
            continue
        try:
            log_to_worker_file(log_dir, worker_id, f"Worker {worker_id} stopping for {reason}")
            logger.info(f"[Worker {worker_id}] Stopping for {reason}")
            send_worker_command(workers[worker_id], "stop")
            workers[worker_id].process.wait(timeout=5)
            workers[worker_id].socket.close()
        except Exception:
            pass


def stop_worker(worker: WorkerState, worker_id: int, log_dir: Path, logger: logging.Logger, reason: str) -> None:
    """Stop a single worker.

    Args:
        worker: Worker to stop
        worker_id: Worker ID
        log_dir: Directory for worker logs
        logger: Logger instance
        reason: Reason for stopping (for logging)
    """
    log_to_worker_file(log_dir, worker_id, f"Worker {worker_id} stopping {reason}")
    logger.info(f"[Worker {worker_id}] Stopping {reason}")
    worker.socket.sendall(b"stop\n")


def cleanup_workers(workers: list[WorkerState]) -> None:
    """Clean up all workers at the end of processing."""
    for worker in workers:
        try:
            worker.process.wait(timeout=5)
            worker.socket.close()
        except Exception:
            pass


def increment_stat(lock: threading.Lock, stats: dict, key: str) -> None:
    """Thread-safe increment of a stat counter."""
    with lock:
        stats[key] += 1
