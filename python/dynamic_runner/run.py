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
    filter_existing_outputs,
    format_binary_info,
    normalize_opt_levels,
    print_selection_summary,
    process_selection_arguments,
)

from .deployment_spec import TaskDeploymentSpec
from .logging_setup import setup_logging
from .spawn_secondary import build_subprocess_spawn
from .system_resources import parse_cores, parse_memory
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
    framework-level overlays (`--list-files`, `--skip-existing`).

    Item discovery is the task's concern under the post-phases-redesign
    Protocol — the framework no longer scans the source directory or
    re-orders the result. The legacy `find_matching_binaries` +
    `task.organize_and_sort_items` pair is gone (see docs/PHASES.md);
    consumers fold both responsibilities into `discover_items`.
    `find_matching_binaries` remains exported from `_shared` as a
    helper a task can reach for from inside its own
    `discover_items`, but the framework does not call it directly.
    """
    logger = logging.getLogger()

    display_opt_levels = None
    if config.opt_levels:
        normalized = normalize_opt_levels(config.opt_levels, config.opt_regex)
        display_opt_levels = normalized.display_values
    print_selection_summary(config, display_opt_levels)

    logger.info("Discovering items via task.discover_items(...)")
    binaries = list(task.discover_items(config.source_dir, args))
    logger.info(f"Discovered {len(binaries)} items")

    if not binaries:
        return []

    if config.list_files:
        logger.info("\nDiscovered items:")
        for binary in binaries:
            logger.info(format_binary_info(binary, config.source_dir))
        return []

    if args.skip_existing:
        binaries, skipped = filter_existing_outputs(
            binaries,
            config.source_dir,
            config.output_dir,
            task.get_output_filename_pattern,
        )
        logger.info(f"Skipped {skipped} items with existing outputs")
        logger.info(f"Remaining items to process: {len(binaries)}")

    return binaries


def _dispatch_local(task, args, config, logger) -> None:
    """Standard in-process local manager."""
    import dynamic_runner as _rs

    num_cores = parse_cores(args.cores)
    max_memory = parse_memory(args.max_memory)
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
    """Run as a secondary coordinator (SLURM compute node or local test)."""
    import tempfile

    import dynamic_runner as _rs

    if not args.secondary_id:
        logger.error("--secondary-id is required when running in secondary mode")
        return

    # True when running inside the SLURM wrapper's podman container.
    # We detect this by the presence of the bind-mounted network drive
    # (`/app/src-network`), which the wrapper script in
    # `packaging/job_manager.py` always mounts read-only alongside
    # `/app/src-tmp` and `/app/out-tmp`. Checking the bind-mount
    # directly is more robust than peeking at container-runtime
    # sentinels (`/.dockerenv` for docker, `/run/.containerenv` for
    # podman, …) — those vary by runtime, and the question we
    # actually care about is "did the wrapper set up the
    # `/app/...` layout I'm about to consume?", not "what runtime
    # is beneath me?". The previous detection
    # (`/.dockerenv`-only) silently went False under podman and made
    # the secondary fall back to `src_network=None`, which left
    # workers exec'ing the primary's filesystem-view absolute paths
    # in an infinite recoverable-failure loop.
    in_wrapper_container = Path("/app/src-network").exists()

    # Source-binary staging directories: explicit CLI flags override the
    # container/local defaults. `src_network` is the shared-drive directory
    # the primary writes into; `src_tmp` is this secondary's per-process
    # scratch dir where StageFile copies land.
    src_network = Path(args.src_network) if args.src_network else (
        Path("/app/src-network") if in_wrapper_container else None
    )

    if args.src_tmp:
        src_tmp = Path(args.src_tmp)
    elif in_wrapper_container:
        src_tmp = Path("/app/src-tmp")
    else:
        temp_dir = Path(tempfile.mkdtemp(prefix=f"secondary-{args.secondary_id}-"))
        src_tmp = temp_dir / "src-tmp"
    out_tmp = Path("/app/out-tmp") if in_wrapper_container else (
        Path(tempfile.mkdtemp(prefix=f"secondary-{args.secondary_id}-out-"))
    )

    src_tmp.mkdir(parents=True, exist_ok=True)
    out_tmp.mkdir(parents=True, exist_ok=True)

    logger.info(f"Secondary ID: {args.secondary_id}")
    logger.info(f"Primary URL: {args.secondary}")
    logger.info(f"src_network={src_network}, src_tmp={src_tmp}")

    # `num_workers` and `max_resources` default to system-detected
    # values (logical CPUs visible to the process; RAM total from
    # /proc/meminfo) — done in Rust inside `SecondaryConfig.__new__`
    # so the Python side has no need for `psutil` just to pass two
    # integers straight back to Rust.
    cfg = _rs.SecondaryConfig(
        secondary_id=args.secondary_id,
        src_network=str(src_network) if src_network else None,
        src_tmp=str(src_tmp),
    )
    _rs.run_secondary(
        cfg,
        args.secondary,
        task,
        args,
        str(src_tmp),
        str(out_tmp),
        skip_existing=args.skip_existing,
    )


def _dispatch_single_process(task, args, config, logger) -> None:
    """In-process distributed manager (primary + N secondaries via channels)."""
    import dynamic_runner as _rs

    binaries = _collect_binaries(task, args, config)
    if not binaries:
        logger.info("No binaries to process")
        return

    num_cores = parse_cores(args.cores)
    max_memory = parse_memory(args.max_memory)
    num_secondaries = args.jobs if args.jobs else 1
    workers_per_secondary = num_cores // num_secondaries if num_secondaries > 0 else num_cores
    ram_per_secondary = max_memory // num_secondaries if num_secondaries > 0 else max_memory

    logger.info(f"Secondaries: {num_secondaries}")
    logger.info(f"Workers per secondary: {workers_per_secondary}")
    logger.info(f"RAM per secondary: {ram_per_secondary / (1024**3):.2f}GB")

    primary_cfg = _rs.PrimaryConfig(num_secondaries=num_secondaries)
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

    primary_cfg = _rs.PrimaryConfig(num_secondaries=num_secondaries)
    result = _rs.run_primary(primary_cfg, task, spawn_secondary, binaries)
    logger.info(f"Completed: {result['completed']}")
    logger.info(f"Failed: {result['failed']}")


def _dispatch_slurm(task, args, deployment: TaskDeploymentSpec, logger) -> None:
    """SLURM distributed mode — image build, transfer, job submission, then Rust primary."""
    from .packaging import run_slurm_pipeline

    run_slurm_pipeline(task, args, deployment, logger)
