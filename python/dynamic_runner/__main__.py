import argparse
import logging
from pathlib import Path

from shared import (
    add_selection_arguments,
    filter_existing_outputs,
    find_matching_binaries,
    format_binary_info,
    normalize_opt_levels,
    print_selection_summary,
    process_selection_arguments,
)

from .system_resources import parse_cores, parse_memory
from .task import TokenizerTask
from .worker_manager import WorkerManager


def main():
    logger = logging.getLogger()
    logger.setLevel(logging.INFO)

    logging.basicConfig(
        level=logging.INFO,
        format="%(levelname)s | %(asctime)s,%(msecs)03d | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    parser = argparse.ArgumentParser(
        description="Dynamic batch processing for binary tokenization with memory-aware parallel execution"
    )

    add_selection_arguments(parser)

    # Create task instance to add task-specific arguments
    task = TokenizerTask()
    task.add_task_arguments(parser)

    parser.add_argument(
        "--cores",
        type=str,
        default="-0",
        help="Number of cores to use. Can be int, +int (add to available), or -int (subtract from available). "
        "Default: all available cores",
    )

    parser.add_argument(
        "--max-memory",
        type=str,
        default="-2G",
        help="Maximum memory to use (e.g., 16G, 8192M). Can use +/- prefix for relative to available memory.",
    )

    parser.add_argument("--skip-existing", action="store_true", help="Skip binaries that already have output files")

    parser.add_argument("--always-restart-worker", action="store_true", help="Restart worker after each completed task")

    parser.add_argument("--pid", action="store_true", help="Print worker PIDs when (re)started")

    parser.add_argument(
        "--manual-start-worker", action="store_true", help="Manually start worker processes (print command and wait)"
    )

    parser.add_argument(
        "--connection-mode",
        type=str,
        choices=["socketpair", "named"],
        default=None,
        help="Connection mode: 'socketpair' uses socketpair() (default), 'named' uses named Unix domain sockets",
    )

    parser.add_argument(
        "--socket-dir",
        type=str,
        help="Directory for named socket files (defaults to <output>/sockets when --manual-start-worker is used)",
    )

    args = parser.parse_args()

    # Default to named mode when manual-start-worker is used
    if args.connection_mode is None:
        args.connection_mode = "named" if args.manual_start_worker else "socketpair"

    if hasattr(args, "debugs") and args.debugs:
        args.pid = True

    config = process_selection_arguments(args)

    # Default socket-dir to output/sockets when manual-start-worker is used
    if args.manual_start_worker and not args.socket_dir:
        args.socket_dir = str(config.output_dir / "sockets")

    # Validate socket-dir is provided when using named mode
    if args.connection_mode == "named" and not args.socket_dir:
        logger.error("--socket-dir is required when --connection-mode=named")
        return

    num_cores = parse_cores(args.cores)
    max_memory = parse_memory(args.max_memory)

    display_opt_levels = None
    if config.opt_levels:
        normalized = normalize_opt_levels(config.opt_levels, config.opt_regex)
        display_opt_levels = normalized.display_values

    print_selection_summary(config, display_opt_levels)
    logger.info(f"Cores: {num_cores}")
    logger.info(f"Max memory: {max_memory / (1024**3):.2f}GB")
    logger.info("")

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
        logger.info("No binaries found matching the criteria")
        return

    if config.list_files:
        logger.info("\nMatched files:")
        for binary in binaries:
            logger.info(format_binary_info(binary, config.source_dir))
        return

    logger.info("Organizing and sorting binaries...")
    sorted_binaries = task.organize_and_sort_items(binaries)

    if args.skip_existing:
        logger.info("Filtering out binaries with existing output files...")
        sorted_binaries, skipped_count = filter_existing_outputs(
            sorted_binaries, config.source_dir, config.output_dir, task.get_output_filename_pattern
        )
        logger.info(f"Skipped {skipped_count} binaries with existing outputs")
        logger.info(f"Remaining binaries to process: {len(sorted_binaries)}")

        if not sorted_binaries:
            logger.info("No binaries to process after filtering")
            return

    manager = WorkerManager(
        num_workers=num_cores,
        max_memory=max_memory,
        source_dir=config.source_dir,
        output_dir=config.output_dir,
        task_definition=task,
        task_args=args,
        skip_existing=args.skip_existing,
        print_pid=args.pid,
        always_restart_worker=args.always_restart_worker,
        manual_start_worker=args.manual_start_worker,
        connection_mode=args.connection_mode,
        socket_dir=Path(args.socket_dir) if args.socket_dir else None,
    )

    manager.process_binaries(sorted_binaries)


if __name__ == "__main__":
    main()
