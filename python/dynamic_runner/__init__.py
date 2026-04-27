"""Generic dynamic batch runner. Task-specific code lives in sibling
packages (e.g. `dynamic_batch_tokenizer`).

Public surface:
- `run(task, ...)` — the canonical entry point.
- `TaskDefinition`, `Phase`, `StageDefinition` — Protocol + dataclass for
  task packages.
- `make_subprocess_spawn_factory(package_name)` — convenience factory for
  the `spawn_secondary` callback.
- `factories.PodmanExecWorkerFactory`, `factories.CgroupResourceMonitor`
  — reference implementations for unusual deployments (containerised
  workers, cgroup-aware resource accounting). Lazy-imported via the
  `factories` submodule to avoid pulling podman/cgroup imports for
  the common case.
"""

from .run import run
from .spawn_secondary import make_subprocess_spawn_factory
from .task_protocol import Phase, StageDefinition, TaskDefinition

__all__ = [
    "run",
    "make_subprocess_spawn_factory",
    "TaskDefinition",
    "Phase",
    "StageDefinition",
]
