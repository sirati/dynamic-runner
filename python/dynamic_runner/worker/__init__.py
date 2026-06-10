"""Worker-runtime public surface.

Re-exports the names every consumer needs to write a worker module:

    from dynamic_runner.worker import (
        run, task_function, Task, WorkerOutput,
        RecoverableError, NonRecoverableError,
    )

See ``runtime.py`` for the full contract and exception → wire mapping.
"""
from .._native import PUBLISH_STRING_MAX_BYTES
from .logging_setup import setup_worker_logging
from .publish import (
    PublishError,
    publish,
    publish_all,
)
from .runtime import (
    NonRecoverableError,
    RecoverableError,
    Task,
    WorkerOutput,
    run,
    task_function,
)

__all__ = [
    "NonRecoverableError",
    "PUBLISH_STRING_MAX_BYTES",
    "PublishError",
    "RecoverableError",
    "Task",
    "WorkerOutput",
    "publish",
    "publish_all",
    "run",
    "setup_worker_logging",
    "task_function",
]
