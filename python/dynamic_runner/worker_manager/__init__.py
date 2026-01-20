"""Worker manager implementations for dynamic batch processing.

This package provides worker manager abstractions:
- WorkerManagerBase: Base class with common worker management logic
- DecisionWorkerManMixin: Mixin for decision-making responsibilities
- ExecutionWorkerManBaseImpl: Base for execution responsibilities (worker lifecycle, OOM)
- AuthoritativeBase: Base for authoritative managers (no OOM checking)
- SubmissiveBase: Base for submissive managers (request tasks from authoritative)
- LocalWorkerManager: Complete local worker management (decision + execution)
- ActualAuthoritativeWorkerManager: Authoritative with decision logic
- RemoteAuthoritativeWorkerManager: Authoritative relay (network)
- ActualSubmissiveWorkerManager: Submissive with execution logic
- RemoteSubmissiveWorkerManager: Submissive relay (network)
"""

from .actual_authoritative import ActualAuthoritativeWorkerManager
from .actual_submissive import ActualSubmissiveWorkerManager
from .authoritative_base import AuthoritativeBase
from .base import WorkerManagerBase
from .decision_impl import DecisionWorkerManMixin
from .execution_impl import ExecutionWorkerManBaseImpl
from .local import LocalWorkerManager
from .remote_authoritative import RemoteAuthoritativeWorkerManager
from .remote_submissive import RemoteSubmissiveWorkerManager
from .submissive_base import SubmissiveBase

__all__ = [
    # Core base classes
    "WorkerManagerBase",
    "DecisionWorkerManMixin",
    "ExecutionWorkerManBaseImpl",
    "AuthoritativeBase",
    "SubmissiveBase",
    # Concrete implementations
    "LocalWorkerManager",
    "ActualAuthoritativeWorkerManager",
    "ActualSubmissiveWorkerManager",
    "RemoteAuthoritativeWorkerManager",
    "RemoteSubmissiveWorkerManager",
]
