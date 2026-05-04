"""Thin runner facade: parse argparse, build typed configs, dispatch to Rust.

`run(task, deployment=None, description="...")` is the canonical Python
entry point.
"""

from __future__ import annotations

import argparse
import logging
import sys
from pathlib import Path

from ._shared import (
    print_selection_summary,
    process_selection_arguments,
)

from .deployment_spec import TaskDeploymentSpec
from .logging_setup import setup_logging
from .spawn_secondary import build_subprocess_spawn
from .task_protocol import TaskDefinition

# Import the parser-builder from the slimmed cli module.
from .cli import build_arg_parser  # noqa: E402  (defined in same package)


def run(
    task: TaskDefinition,
    deployment: TaskDeploymentSpec | None = None,
    description: str = "Dynamic batch processing with memory-aware parallel execution",
) -> None:
    """Run the dynamic batch processing CLI.

    Args:
        task: Object satisfying the `TaskDefinition` protocol.
        deployment: Task-package deployment metadata (image name,
            secondary Python module, nix build target). Required when
            ``--multi-computer local|slurm`` is used; ignored in
            single-process and plain-local modes.
        description: Description for the argparse help text.
    """
    logger = setup_logging(sys.argv[1:])

    parser = build_arg_parser(description)
    task.add_task_arguments(parser)
    args = parser.parse_args()

    if args.secondary:
        _dispatch_secondary(task, args, logger)
        return

    if args.slurm and not args.multi_computer:
        args.multi_computer = "slurm"
        logger.warning("--slurm is deprecated, use --multi-computer slurm instead")

    if args.connection_mode is None:
        args.connection_mode = "named" if args.manual_start_worker else "socketpair"

    config = process_selection_arguments(args)

    if args.manual_start_worker and not args.socket_dir:
        args.socket_dir = str(config.output_dir / "sockets")

    if args.connection_mode == "named" and not args.socket_dir:
        logger.error("--socket-dir is required when --connection-mode=named")
        return

    if args.multi_computer == "slurm":
        if deployment is None:
            logger.error("--multi-computer slurm requires `deployment=TaskDeploymentSpec(...)` in run()")
            return
        _dispatch_slurm(task, args, deployment, logger)
    elif args.multi_computer == "local":
        if deployment is None:
            logger.error("--multi-computer local requires `deployment=TaskDeploymentSpec(...)` in run()")
            return
        _dispatch_multi_computer_local(task, args, deployment, logger)
    elif args.multi_computer == "single-process":
        _dispatch_single_process(task, args, config, logger)
    else:
        _dispatch_local(task, args, config, logger)


def _collect_binaries(task: TaskDefinition, args: argparse.Namespace, config) -> list:
    """Discover items via the task's `discover_items` and apply the
    framework-level overlay `--list-files`.

    Item discovery — including any corpus-shape filters and any
    `--skip-existing`-style policy — is the task's concern. The
    framework provides primitives (gateway-aware `_native.find_items`,
    deployment-correct `args.resolved_output_root`) so a task can
    compose source-walk + output-walk + filter inside its own
    `discover_items`. The framework's `--list-files` overlay just
    prints the items the task discovered; consumers wanting richer
    formatting can implement `__str__` on their TaskInfo subclass or
    print from inside `discover_items` directly.
    """
    logger = logging.getLogger()

    print_selection_summary(config)

    args.resolved_output_root = str(config.output_dir)

    logger.info("Discovering items via task.discover_items(...)")
    binaries = list(task.discover_items(config.source_dir, args))
    logger.info(f"Discovered {len(binaries)} items")

    if not binaries:
        return []

    if config.list_files:
        logger.info("\nDiscovered items:")
        for binary in binaries:
            logger.info(f"  {binary.path}")
        return []

    return binaries


def _dispatch_local(task, args, config, logger) -> None:
    """Standard in-process local manager."""
    import dynamic_runner as _rs

    num_cores = _rs.parse_cores(args.cores)
    max_memory = _rs.parse_memory(args.max_memory)
    logger.info(f"Cores: {num_cores}")
    logger.info(f"Max memory: {max_memory / (1024**3):.2f}GB")

    binaries = _collect_binaries(task, args, config)
    if not binaries:
        logger.info("No binaries to process")
        return

    cfg = _rs.LocalManagerConfig(
        num_workers=num_cores,
        max_resources=_rs.ResourceMap({"memory": max_memory}),
        always_restart_worker=args.always_restart_worker,
        print_pid=args.pid,
    )
    result = _rs.run_local(
        cfg,
        task,
        args,
        str(config.source_dir),
        str(config.output_dir),
        binaries,
        skip_existing=args.skip_existing,
        connection_mode=args.connection_mode,
        socket_dir=args.socket_dir,
        manual_start_worker=args.manual_start_worker,
    )
    _log_local_result(result, logger)


def _log_local_result(result: dict, logger) -> None:
    stats = result["stats"]
    logger.info(f"Completed: {stats.completed}/{stats.total}")
    logger.info(f"Errored: {stats.errored}")
    failed = result["failed_tasks"]
    if failed:
        logger.warning(f"Failed tasks: {len(failed)}")
        for ft in failed:
            logger.warning(f"  {ft.binary.path}: {ft.error_type}: {ft.error_message}")
    oom = result["oom_tasks"]
    if oom:
        logger.warning(f"OOM tasks: {len(oom)}")
        for ot in oom:
            logger.warning(f"  {ot.binary.path}: {ot.error_message}")


def _dispatch_secondary(task, args, logger) -> None:
    """Run as a secondary coordinator (SLURM compute node or local test).

    Every per-secondary config knob (resource budgets, src_network,
    src_tmp, output_dir) auto-resolves inside
    `SecondaryConfig.__new__` — the Python side just forwards
    user-supplied CLI overrides for `--src-network` / `--src-tmp`
    and lets Rust handle wrapper-vs-local detection,
    /proc/meminfo / available_parallelism probing, and tempdir
    creation.
    """
    import dynamic_runner as _rs

    if not args.secondary_id:
        logger.error("--secondary-id is required when running in secondary mode")
        return

    # `--disable-peer-overlay` lives on `DistributedConfig` (the
    # struct that already owns peer-related tuning). Construct an
    # explicit DistributedConfig only when the flag deviates from
    # the default; otherwise let `SecondaryConfig.__new__` install
    # the stock default.
    distributed_config = (
        _rs.DistributedConfig(disable_peer_overlay=True) if args.disable_peer_overlay else None
    )
    cfg = _rs.SecondaryConfig(
        secondary_id=args.secondary_id,
        src_network=args.src_network,
        src_tmp=args.src_tmp,
        distributed_config=distributed_config,
    )

    logger.info(f"Secondary ID: {cfg.secondary_id}")
    logger.info(f"Primary URL: {args.secondary}")
    logger.info(f"src_network={cfg.src_network}, src_tmp={cfg.src_tmp}, output_dir={cfg.output_dir}")

    # Worker's `--source` argument: prefer the bind-mount root
    # (`src_network`) when it's configured. That's where binaries
    # actually live in container deployments (the wrapper bind-mounts
    # either the primary's staged-bins dir, or — in pre-staged mode —
    # the user-named cluster path under `--source-already-staged`).
    # Workers that do `binary.path.relative_to(source_dir)` to mirror
    # the source-corpus structure under the output dir then get the
    # right relative path; passing `src_tmp` instead would make
    # `relative_to` fail and the worker fall back to a flat layout.
    # Outside container mode `src_network` is None and we fall back to
    # `src_tmp` (per-secondary scratch — the historical default).
    worker_source_dir = cfg.src_network if cfg.src_network is not None else cfg.src_tmp

    _rs.run_secondary(
        cfg,
        args.secondary,
        task,
        args,
        str(worker_source_dir),
        str(cfg.output_dir),
        skip_existing=args.skip_existing,
    )


def _dispatch_single_process(task, args, config, logger) -> None:
    """In-process distributed manager (primary + N secondaries via channels)."""
    import dynamic_runner as _rs

    binaries = _collect_binaries(task, args, config)
    if not binaries:
        logger.info("No binaries to process")
        return

    num_cores = _rs.parse_cores(args.cores)
    max_memory = _rs.parse_memory(args.max_memory)
    num_secondaries = args.jobs if args.jobs else 1
    workers_per_secondary = num_cores // num_secondaries if num_secondaries > 0 else num_cores
    ram_per_secondary = max_memory // num_secondaries if num_secondaries > 0 else max_memory

    logger.info(f"Secondaries: {num_secondaries}")
    logger.info(f"Workers per secondary: {workers_per_secondary}")
    logger.info(f"RAM per secondary: {ram_per_secondary / (1024**3):.2f}GB")

    distributed_config = None
    if getattr(args, "retry_max_passes", None) is not None:
        distributed_config = _rs.DistributedConfig(
            retry_max_passes=args.retry_max_passes,
        )
    primary_cfg = _rs.PrimaryConfig(
        num_secondaries=num_secondaries,
        distributed_config=distributed_config,
    )
    secondary_template = _rs.SecondaryConfig(
        secondary_id="<template>",
        num_workers=workers_per_secondary,
        max_resources=_rs.ResourceMap({"memory": ram_per_secondary}),
    )
    result = _rs.run_distributed(
        primary_cfg,
        secondary_template,
        task,
        args,
        str(config.source_dir),
        str(config.output_dir),
        binaries,
        skip_existing=args.skip_existing,
    )
    logger.info(f"Completed: {result['completed']}")
    logger.info(f"Failed: {result['failed']}")


def _dispatch_multi_computer_local(task, args, deployment: TaskDeploymentSpec, logger) -> None:
    """Network-based primary that spawns local secondaries via subprocess."""
    import dynamic_runner as _rs

    config = process_selection_arguments(args)
    binaries = _collect_binaries(task, args, config)
    if not binaries:
        logger.info("No binaries to process")
        return

    num_secondaries = args.jobs
    logger.info(f"Starting coordinator with {num_secondaries} local secondaries")

    spawn_secondary = build_subprocess_spawn(deployment, args)

    distributed_config = None
    if getattr(args, "retry_max_passes", None) is not None:
        distributed_config = _rs.DistributedConfig(
            retry_max_passes=args.retry_max_passes,
        )
    primary_cfg = _rs.PrimaryConfig(
        num_secondaries=num_secondaries,
        distributed_config=distributed_config,
    )
    result = _rs.run_primary(primary_cfg, task, spawn_secondary, binaries)
    logger.info(f"Completed: {result['completed']}")
    logger.info(f"Failed: {result['failed']}")


def _dispatch_slurm(task, args, deployment: TaskDeploymentSpec, logger) -> None:
    """SLURM distributed mode — image build, transfer, job submission, then Rust primary."""
    from .packaging import run_slurm_pipeline

    run_slurm_pipeline(task, args, deployment, logger)
