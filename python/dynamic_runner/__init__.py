"""Generic dynamic batch runner. Task-specific code lives in sibling
packages (e.g. `dynamic_runner_tokenizer`).

Public surface:
- `run(task, deployment=None, ...)` — the canonical Python entry point.
- `TaskDeploymentSpec` — task-package deployment metadata required for
  `--multi-computer slurm|local` modes (image name, secondary module,
  nix build target).
- `TaskDefinition`, `PhaseSpec`, `TaskTypeSpec` — Protocol + dataclasses for
  task packages.
- `factories.PodmanExecWorkerFactory`, `factories.CgroupResourceMonitor`
  — reference implementations for unusual deployments (containerised
  workers, cgroup-aware resource accounting). Lazy-imported via the
  `factories` submodule to avoid pulling podman/cgroup imports for
  the common case.
- Native runner primitives — compiled by maturin into
  `dynamic_runner._native` and re-exported here: `run_local`,
  `run_distributed`, `run_primary`, `run_secondary`, `compute_task_hash`,
  the config dataclasses, and the `Rust*Manager`/`Rust*Coordinator`
  classes. The native `TaskInfo` collides with the Python dataclass
  form above; reach it as `dynamic_runner._native.TaskInfo` if needed.
"""

from .deployment_spec import TaskDeploymentSpec
from .run import run
from .task_protocol import PhaseSpec, TaskDefinition, TaskTypeSpec

from ._native import (
    BinaryIdentifier,
    DistributedConfig,
    FailedTask,
    LocalManagerConfig,
    LogPathConfig,
    PrimaryConfig,
    ProcessingStats,
    PublishError,
    PyCallbackResourceMonitor,
    PyCallbackWorkerFactory,
    ResourceMap,
    RustDistributedManager,
    RustLocalManager,
    RustPrimaryCoordinator,
    RustSecondaryCoordinator,
    SchedulerConfig,
    SecondaryConfig,
    SlurmConfig,
    WorkerSpec,
    compute_task_hash,
    parse_cores,
    parse_memory,
    pick_free_port,
    run_distributed,
    run_local,
    run_primary,
    run_secondary,
)

__all__ = [
    "run",
    "TaskDeploymentSpec",
    "TaskDefinition",
    "PhaseSpec",
    "TaskTypeSpec",
    "BinaryIdentifier",
    "DistributedConfig",
    "FailedTask",
    "LocalManagerConfig",
    "LogPathConfig",
    "PrimaryConfig",
    "ProcessingStats",
    "PublishError",
    "PyCallbackResourceMonitor",
    "PyCallbackWorkerFactory",
    "ResourceMap",
    "RustDistributedManager",
    "RustLocalManager",
    "RustPrimaryCoordinator",
    "RustSecondaryCoordinator",
    "SchedulerConfig",
    "SecondaryConfig",
    "SlurmConfig",
    "WorkerSpec",
    "compute_task_hash",
    "parse_cores",
    "parse_memory",
    "pick_free_port",
    "run_distributed",
    "run_local",
    "run_primary",
    "run_secondary",
]
