"""Worker manager compatibility wrapper.

This module provides backward compatibility by importing LocalManager
as WorkerManager. The actual implementation has been moved to the
worker_manager package with multiple specialized manager types.
"""

from .worker_manager.local import LocalManager as WorkerManager

__all__ = ["WorkerManager"]
