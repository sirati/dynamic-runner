"""Thin runner facade: parse argparse, build typed configs, dispatch to Rust.

`run(task, spawn_secondary_factory=None, description="...")` is the new
canonical entry point. It replaces `dynamic_batch.cli.run`, which becomes a
deprecated alias for one release.
"""

from __future__ import annotations

import argparse
import logging
import sys
from collections.abc import Callable
from pathlib import Path

from shared import (
    filter_existing_outputs,
    find_matching_binaries,
    format_binary_info,
    normalize_opt_levels,
    print_selection_summary,
    process_selection_arguments,
)

from .logging_setup import setup_logging
from .system_resources import parse_cores, parse_memory
from .task_protocol import TaskDefinition

# Import the parser-builder from the slimmed cli module.
from .cli import build_arg_parser  # noqa: E402  (defined in same package)


def run(
    task: TaskDefinition,
    spawn_secondary_factory: Callable[[argparse.Namespace], Callable] | None = None,
    description: str = "Dynamic batch processing with memory-aware parallel execution",
) -> None:
    """Run the dynamic batch processing CLI.

    Args:
        task: Object satisfying the `TaskDefinition` protocol.
        spawn_secondary_factory: Optional `(args) -> spawn_secondary(...)` factory
            used by the network-based primary coordinator (multi-computer/local).
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
        _dispatch_slurm(task, args, logger)
    elif args.multi_computer == "local":
        _dispatch_multi_computer_local(task, args, logger, spawn_secondary_factory)
    elif args.multi_computer == "single-process":
        _dispatch_single_process(task, args, config, logger)
    else:
        _dispatch_local(task, args, config, logger)


def _collect_binaries(task: TaskDefinition, args: argparse.Namespace, config) -> list:
    """Scan, sort, and (optionally) skip-existing-filter binaries."""
    logger = logging.getLogger()

    display_opt_levels = None
    if config.opt_levels:
        normalized = normalize_opt_levels(config.opt_levels, config.opt_regex)
        display_opt_levels = normalized.display_values
    print_selection_summary(config, display_opt_levels)

    logger.info("Scanning for matching binaries...")
    binaries = find_matching_binaries(
        config.source_dir,
        config.platforms,
        config.compiler,
        config.compiler_versions,
        config.opt_levels,
        config.file_format,
        config.version_regex,
        config.opt_regex,
        config.name_regex,
        config.exclude_subfolders,
    )
    logger.info(f"Found {len(binaries)} matching binaries")

    if not binaries:
        return []

    if config.list_files:
        logger.info("\nMatched files:")
        for binary in binaries:
            logger.info(format_binary_info(binary, config.source_dir))
        return []

    sorted_binaries = task.organize_and_sort_items(binaries)

    if args.skip_existing:
        sorted_binaries, skipped = filter_existing_outputs(
            sorted_binaries,
            config.source_dir,
            config.output_dir,
            task.get_output_filename_pattern,
        )
        logger.info(f"Skipped {skipped} binaries with existing outputs")
        logger.info(f"Remaining binaries to process: {len(sorted_binaries)}")

    return sorted_binaries


def _dispatch_local(task, args, config, logger) -> None:
    """Standard in-process local manager."""
    import dynamic_batch_rs as _rs

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

    import psutil
    import dynamic_batch_rs as _rs

    if not args.secondary_id:
        logger.error("--secondary-id is required when running in secondary mode")
        return

    in_docker = Path("/app").exists() and Path("/.dockerenv").exists()
    if in_docker:
        src_tmp = Path("/app/src-tmp")
        out_tmp = Path("/app/out-tmp")
    else:
        temp_dir = Path(tempfile.mkdtemp(prefix=f"secondary-{args.secondary_id}-"))
        src_tmp = temp_dir / "src-tmp"
        out_tmp = temp_dir / "out-tmp"

    logger.info(f"Secondary ID: {args.secondary_id}")
    logger.info(f"Primary URL: {args.secondary}")

    ram_bytes = psutil.virtual_memory().total
    num_workers = psutil.cpu_count(logical=False) or 4

    cfg = _rs.SecondaryConfig(
        secondary_id=args.secondary_id,
        num_workers=num_workers,
        max_resources=_rs.ResourceMap({"memory": ram_bytes}),
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
    import dynamic_batch_rs as _rs

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


def _dispatch_multi_computer_local(task, args, logger, spawn_secondary_factory) -> None:
    """Network-based primary that spawns local secondaries via subprocess."""
    import dynamic_batch_rs as _rs

    config = process_selection_arguments(args)
    binaries = _collect_binaries(task, args, config)
    if not binaries:
        logger.info("No binaries to process")
        return

    num_secondaries = args.jobs
    logger.info(f"Starting coordinator with {num_secondaries} local secondaries")

    if spawn_secondary_factory is None:
        logger.error("spawn_secondary_factory is required for multi-computer/local mode")
        return
    spawn_secondary = spawn_secondary_factory(args)

    primary_cfg = _rs.PrimaryConfig(num_secondaries=num_secondaries)
    result = _rs.run_primary(primary_cfg, task, spawn_secondary, binaries)
    logger.info(f"Completed: {result['completed']}")
    logger.info(f"Failed: {result['failed']}")


def _dispatch_slurm(task, args, logger) -> None:
    """SLURM distributed mode — packaging is not yet ported to the new flow."""
    from .packaging import run_slurm_pipeline

    run_slurm_pipeline(task, args, logger)
