"""Local test mode for multi-computer coordination.

This module provides local testing capabilities for the multi-computer
coordination framework without requiring SLURM or Docker.
"""

from .primary import LocalTestPrimaryCoordinator

__all__ = ["LocalTestPrimaryCoordinator"]
