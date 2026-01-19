"""Worker manager implementations for dynamic batch processing.

This package provides worker manager abstractions:
- WorkerManagerBase: Base class with common worker management logic
- LocalManager: Complete local worker management (current behavior)
- AuthoritiveManagerBase: Base for task assignment managers
- AuthoritiveManager: Task assignment without OOM checking (for primary, over network)
- LocalAuthoritiveManager: Local task assignment (for testing master/slave locally)
- SubmissiveManagerBase: Base for OOM checking managers
- SubmissiveManager: OOM checking without task assignment (for secondary, over network)
- LocalSubmissiveManager: Local OOM checking (for testing master/slave locally)
"""

from .authoritive import AuthoritiveManager
from .authoritive_base import AuthoritiveManagerBase
from .base import WorkerManagerBase
from .local import LocalManager
from .local_authoritive import LocalAuthoritiveManager
from .local_submissive import LocalSubmissiveManager
from .submissive import SubmissiveManager
from .submissive_base import SubmissiveManagerBase

# WorkerManager is an alias for LocalManager for backward compatibility
WorkerManager = LocalManager

__all__ = [
    "WorkerManagerBase",
    "LocalManager",
    "AuthoritiveManagerBase",
    "AuthoritiveManager",
    "LocalAuthoritiveManager",
    "SubmissiveManagerBase",
    "SubmissiveManager",
    "LocalSubmissiveManager",
    "WorkerManager",
]
