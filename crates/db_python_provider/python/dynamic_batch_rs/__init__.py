"""Public Python surface for the dynamic_batch Rust runner.

The compiled Rust extension lives at `dynamic_batch_rs._native`; this
package re-exports its symbols so existing callers continue to use the
flat `dynamic_batch_rs.X` form, and adds the pure-Python `comm`
subpackage that the worker subprocess imports.
"""

from ._native import (
    BinaryIdentifier,
    BinaryInfo,
    DistributedConfig,
    FailedTask,
    LocalManagerConfig,
    LogPathConfig,
    Phase,
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
    "BinaryIdentifier",
    "BinaryInfo",
    "DistributedConfig",
    "FailedTask",
    "LocalManagerConfig",
    "LogPathConfig",
    "Phase",
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
