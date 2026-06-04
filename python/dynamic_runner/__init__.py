"""Generic dynamic batch runner. Task-specific code lives in sibling
packages (e.g. `dynamic_runner_tokenizer`).

Public surface:
- `cli_main(task_or_factory, *, add_consumer_args=None, ...)` — the
  one-call command-line entry point. Composes the framework parser + a
  consumer's optional flags + the task's flags, parses, and runs. The
  framework's OWN entrypoint (`python -m dynamic_runner`) is this surface.
- `run(task, *, argv=None, args=None, ...)` — the programmatic entry point;
  takes an EXPLICIT argv slice OR a pre-parsed namespace, never global
  `sys.argv`.
- `add_framework_arguments(parser)` — registers ALL framework flags onto
  any parser OR subparser, so a consumer composes the framework CLI with
  its own. The framework derives its secondary-forward flag-set from this
  registration (no consumer strip-set).
- `init_logging(important_stdio_only, full_log_file, full_log_dir)` — the
  native subscriber install, called explicitly with parsed params (no env
  vars). `run`/`cli_main` call it after parse.
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

Logging is NOT installed at import: the native subscriber is chosen by the
parsed CLI flags and installed via `init_logging(...)` after parse, so
there is no import-time env-var dependency. See `logging_setup`.
"""

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
    RustLocalGateway,
    RustLocalManager,
    RustObserverLateJoiner,
    RustPrimaryCoordinator,
    RustSecondaryCoordinator,
    RustSlurmJobManager,
    SchedulerConfig,
    SecondaryConfig,
    SlurmConfig,
    WorkerSpec,
    compute_task_hash,
    init_logging,
    parse_cores,
    parse_memory,
    pick_free_port,
    run_distributed,
    run_local,
    run_observer_late_joiner,
    run_primary,
    run_secondary,
)

from ._shared import TaskDep
from .cli import add_framework_arguments
from .cli_main import cli_main
from .deployment_spec import TaskDeploymentSpec
from .run import run
from .subprocess_spec import SubprocessSpec
from .task_protocol import PhaseSpec, TaskDefinition, TaskTypeSpec

__all__ = [
    "run",
    "cli_main",
    "add_framework_arguments",
    "init_logging",
    "TaskDeploymentSpec",
    "TaskDefinition",
    "TaskDep",
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
    "RustLocalGateway",
    "RustLocalManager",
    "RustObserverLateJoiner",
    "RustPrimaryCoordinator",
    "RustSecondaryCoordinator",
    "RustSlurmJobManager",
    "SchedulerConfig",
    "SecondaryConfig",
    "SlurmConfig",
    "SubprocessSpec",
    "WorkerSpec",
    "compute_task_hash",
    "parse_cores",
    "parse_memory",
    "pick_free_port",
    "run_distributed",
    "run_local",
    "run_observer_late_joiner",
    "run_primary",
    "run_secondary",
]
