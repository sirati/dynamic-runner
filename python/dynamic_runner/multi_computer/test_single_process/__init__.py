"""Single-process testing module for multi-computer mode.

This module tests the full primary-secondary coordination logic in a single process
without network overhead. The primary and secondary run in the same async context
with Python message passing for communication.

The worker manager uses direct local_submissive to local_authoritive connection
(no message passing), ensuring identical behavior to local.py.
"""

from .primary.coordinator import SingleProcessPrimaryCoordinator

__all__ = ["SingleProcessPrimaryCoordinator"]
