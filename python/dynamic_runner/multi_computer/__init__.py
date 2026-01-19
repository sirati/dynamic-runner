"""Multi-computer coordination framework for distributed batch processing.

This module provides a common framework for coordinating work across multiple
computers, with specific implementations for SLURM and local debugging.
"""

from enum import Enum, auto
from pathlib import Path
from typing import Any, Protocol


class ExecutionMode(Enum):
    """Execution mode for multi-computer coordination"""

    SLURM = "slurm"
    LOCAL = "local"


class CoordinationPhase(Enum):
    """Phases of multi-computer coordination"""

    PREPARATION = auto()  # Setup: docker, gateway, job submission
    CONNECTION = auto()  # Wait for secondaries, establish peer connections
    INITIAL_ASSIGNMENT = auto()  # Preliminary task assignment
    FILE_TRANSFER = auto()  # Distribute source files
    EXECUTION = auto()  # Main task execution
    MONITORING = auto()  # Monitor progress
    CLEANUP = auto()  # Cleanup resources


class PreparationResult:
    """Result of preparation phase"""

    def __init__(
        self,
        num_secondaries: int,
        run_id: str,
        cert_dir: Path,
        primary_entropy: bytes,
        mode_specific_data: dict[str, Any] | None = None,
    ):
        self.num_secondaries = num_secondaries
        self.run_id = run_id
        self.cert_dir = cert_dir
        self.primary_entropy = primary_entropy
        self.mode_specific_data = mode_specific_data or {}


class ConnectionResult:
    """Result of connection phase"""

    def __init__(self, secondaries: dict[str, dict[str, Any]], peer_connections_ready: set[str]):
        self.secondaries = secondaries
        self.peer_connections_ready = peer_connections_ready


class FileTransferMode(Enum):
    """Mode for file transfer"""

    FULL_TRANSFER = auto()  # Transfer all files (SLURM)
    SKIP_TRANSFER = auto()  # Files already available (local)


__all__ = [
    "ExecutionMode",
    "CoordinationPhase",
    "PreparationResult",
    "ConnectionResult",
    "FileTransferMode",
]
