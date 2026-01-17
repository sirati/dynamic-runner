import argparse

from shared import (
    add_selection_arguments,
    find_matching_binaries,
    format_binary_info,
    normalize_opt_levels,
    print_selection_summary,
    process_selection_arguments,
)

from .binary_discovery import filter_existing_outputs, organize_and_sort_binaries
from .system_resources import parse_cores, parse_memory
from .worker_manager import WorkerManager


def main():
    parser = argparse.ArgumentParser(
        description="Dynamic batch processing for binary tokenization with memory-aware parallel execution"
    )

    add_selection_arguments(parser)

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

    args = parser.parse_args()

    if hasattr(args, "debugs") and args.debugs:
        args.pid = True

    config = process_selection_arguments(args)

    num_cores = parse_cores(args.cores)
    max_memory = parse_memory(args.max_memory)

    display_opt_levels = None
    if config.opt_levels:
        normalized = normalize_opt_levels(config.opt_levels, config.opt_regex)
        display_opt_levels = normalized.display_values

    print_selection_summary(config, display_opt_levels)
    print(f"Cores: {num_cores}")
    print(f"Max memory: {max_memory / (1024**3):.2f}GB")
    print()

    print("Scanning for matching binaries...")
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

    print(f"Found {len(binaries)} matching binaries")

    if not binaries:
        print("No binaries found matching the criteria")
        return

    if config.list_files:
        print("\nMatched files:")
        for binary in binaries:
            print(format_binary_info(binary, config.source_dir))
        return

    print("Organizing and sorting binaries...")
    sorted_binaries = organize_and_sort_binaries(binaries)

    if args.skip_existing:
        print("Filtering out binaries with existing output files...")
        sorted_binaries, skipped_count = filter_existing_outputs(sorted_binaries, config.source_dir, config.output_dir)
        print(f"Skipped {skipped_count} binaries with existing outputs")
        print(f"Remaining binaries to process: {len(sorted_binaries)}")

        if not sorted_binaries:
            print("No binaries to process after filtering")
            return

    manager = WorkerManager(
        num_workers=num_cores,
        max_memory=max_memory,
        source_dir=config.source_dir,
        output_dir=config.output_dir,
        platform_arg="file_prefix",
        skip_existing=args.skip_existing,
        print_pid=args.pid,
        always_restart_worker=args.always_restart_worker,
    )

    manager.process_binaries(sorted_binaries)


if __name__ == "__main__":
    main()
