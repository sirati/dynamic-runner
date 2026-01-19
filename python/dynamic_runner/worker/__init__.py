"""Worker abstraction module for dynamic batch processing.

This module provides a clean abstraction for workers that can be either
local (subprocess) or remote (via network to SLURM secondary nodes).
"""

from .base_worker import BaseWorker
from .local_worker import LocalWorker
from .remote_worker import RemoteWorker

__all__ = [
    "BaseWorker",
    "LocalWorker",
    "RemoteWorker",
]
