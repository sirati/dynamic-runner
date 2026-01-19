"""Worker manager implementations for dynamic batch processing.

This package provides worker manager abstractions:
- WorkerManagerBase: Base class with common worker management logic
- LocalManager: Complete local worker management (current behavior)
- AuthoritiveManager: Task assignment without OOM checking (for primary)
- SubmissiveManager: OOM checking without task assignment (for secondary, defers to primary)
"""

from .authoritive import AuthoritiveManager
from .base import WorkerManagerBase
from .local import LocalManager
from .submissive import SubmissiveManager

# WorkerManager is an alias for LocalManager for backward compatibility
WorkerManager = LocalManager

__all__ = [
    "WorkerManagerBase",
    "LocalManager",
    "AuthoritiveManager",
    "SubmissiveManager",
    "WorkerManager",
]
