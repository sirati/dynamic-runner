"""Generic dynamic batch runner. Task-specific code lives in sibling
packages (e.g. `dynamic_runner_tokenizer`).

Public surface:
- `run(task, ...)` — the canonical Python entry point.
- `TaskDefinition`, `Phase`, `StageDefinition` — Protocol + dataclass for
  task packages.
- `make_subprocess_spawn_factory(package_name)` — convenience factory for
  the `spawn_secondary` callback.
- `factories.PodmanExecWorkerFactory`, `factories.CgroupResourceMonitor`
  — reference implementations for unusual deployments (containerised
  workers, cgroup-aware resource accounting). Lazy-imported via the
  `factories` submodule to avoid pulling podman/cgroup imports for
  the common case.
- Native runner primitives — compiled by maturin into
  `dynamic_runner._native` and re-exported here: `run_local`,
  `run_distributed`, `run_primary`, `run_secondary`, `compute_task_hash`,
  the config dataclasses, and the `Rust*Manager`/`Rust*Coordinator`
  classes. The native `Phase` and `BinaryInfo` collide with the Python
  Protocol+dataclass forms above; reach them as
  `dynamic_runner._native.Phase` / `_native.BinaryInfo` if needed.
"""

from .run import run
from .spawn_secondary import make_subprocess_spawn_factory
from .task_protocol import Phase, StageDefinition, TaskDefinition

from ._native import (
    BinaryIdentifier,
    DistributedConfig,
    FailedTask,
    LocalManagerConfig,
    LogPathConfig,
    PrimaryConfig,
    ProcessingStats,
    PyCallbackResourceMonitor,
    PyCallbackWorkerFactory,
    ResourceMap,
    RustDistributedManager,
    RustLocalManager,
    RustPrimaryCoordinator,
    RustSecondaryCoordinator,
    SchedulerConfig,
    SecondaryConfig,
    WorkerSpec,
    compute_task_hash,
    run_distributed,
    run_local,
    run_primary,
    run_secondary,
)

__all__ = [
    "run",
    "make_subprocess_spawn_factory",
    "TaskDefinition",
    "Phase",
    "StageDefinition",
    "BinaryIdentifier",
    "DistributedConfig",
    "FailedTask",
    "LocalManagerConfig",
    "LogPathConfig",
    "PrimaryConfig",
    "ProcessingStats",
    "PyCallbackResourceMonitor",
    "PyCallbackWorkerFactory",
    "ResourceMap",
    "RustDistributedManager",
    "RustLocalManager",
    "RustPrimaryCoordinator",
    "RustSecondaryCoordinator",
    "SchedulerConfig",
    "SecondaryConfig",
    "WorkerSpec",
    "compute_task_hash",
    "run_distributed",
    "run_local",
    "run_primary",
    "run_secondary",
]
