import subprocess
from dataclasses import dataclass

from .binary_info import BinaryInfo
from .comm import CommunicationInterface, ErrorType


@dataclass
class TaskResult:
    success: bool
    error_type: ErrorType | None = None
    error_message: str | None = None
    warnings: int = 0
    filtered: int = 0


@dataclass
class WorkerState:
    process: subprocess.Popen
    comm: CommunicationInterface
    current_binary: BinaryInfo | None
    estimated_memory: int
    worker_id: int
    phase: str | None = None
    phase_start_time: float | None = None
    last_keepalive: float | None = None
    last_printed_minute: int | None = None
    idle: bool = False
    opportunistic: bool = False
    reserved_budget: int = 0


@dataclass
class FailedTask:
    binary: BinaryInfo
    error_type: ErrorType
    error_message: str
    retry_count: int = 0
