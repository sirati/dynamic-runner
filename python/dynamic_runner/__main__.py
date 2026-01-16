import argparse
import re
import sys
from pathlib import Path

from .binary_discovery import find_matching_binaries, organize_and_sort_binaries
from .system_resources import parse_cores, parse_memory
from .worker_manager import WorkerManager


def main():
    parser = argparse.ArgumentParser(
        description="Dynamic batch processing for binary tokenization with memory-aware parallel execution"
    )

    parser.add_argument("--source", type=str, default="./src", help="Source directory containing binaries")

    parser.add_argument("--output", type=str, default="./out", help="Output directory for results")

    parser.add_argument(
        "--platform", type=str, nargs="+", default=["x86", "x64"], help="Platforms to process (default: x86 x64)"
    )

    parser.add_argument(
        "--compiler",
        type=str,
        default=None,
        help="Compiler to filter by (e.g., gcc, clang). If not specified, all compilers are included.",
    )

    parser.add_argument(
        "--compiler-versions",
        type=str,
        nargs="*",
        default=None,
        help="Compiler versions to include (e.g., 5 5.0). If not specified, all versions are included.",
    )

    parser.add_argument(
        "--opt",
        type=str,
        nargs="*",
        default=None,
        help="Optimization levels to include (e.g., O0 O1 O2 O3). If not specified, all levels are included.",
    )

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

    parser.add_argument("--pid", action="store_true", help="Print worker PIDs when (re)started")

    parser.add_argument("--list-files", action="store_true", help="List all matched files without processing")

    parser.add_argument(
        "--file-format",
        type=str,
        default="platform-compiler-version-optimisationlevel_binaryname",
        help="File format string to parse filenames. "
        "Use full names (platform, compiler, version, optimisationlevel, binaryname) "
        "or shorthand (p, c, cv, opt, name). "
        "Example: 'platform-compiler-version-optimisationlevel_binaryname' or 'p-c-cv-opt_name'. "
        "Default: platform-compiler-version-optimisationlevel_binaryname",
    )

    parser.add_argument(
        "--version-regex",
        type=str,
        default=None,
        help="Custom regex for matching version field. Cannot be used with --compiler-versions.",
    )

    parser.add_argument(
        "--opt-regex",
        type=str,
        default="[oO]?([0123s])",
        help="Custom regex for matching optimization level field. Default: [oO]([0123s])",
    )

    parser.add_argument(
        "--name-regex",
        type=str,
        default=None,
        help="Custom regex for matching binary name field.",
    )

    parser.add_argument(
        "--debugs",
        action="store_true",
        help="Debug mode: only process binaries with name 'minigzipsh' (sets --name-regex to 'minigzipsh')",
    )

    parser.add_argument(
        "--exclude-subfolder",
        type=str,
        nargs="*",
        default=None,
        help="Subfolders to exclude from source directory. Multiple values are combined with OR logic.",
    )

    args = parser.parse_args()

    if args.debugs:
        args.name_regex = "minigzipsh"
        args.pid = True

    if args.compiler_versions and args.version_regex:
        parser.error("Cannot specify both --compiler-versions and --version-regex")

    source_dir = Path(args.source).resolve()
    output_dir = Path(args.output).resolve()

    if not source_dir.exists():
        print(f"Error: Source directory does not exist: {source_dir}")
        sys.exit(1)

    output_dir.mkdir(parents=True, exist_ok=True)

    num_cores = parse_cores(args.cores)
    max_memory = parse_memory(args.max_memory)

    # Normalize opt_levels for display
    display_opt_levels = args.opt
    if args.opt:
        normalized_opt_levels = []
        opt_pattern = re.compile(args.opt_regex)
        has_subgroup = "(" in args.opt_regex and ")" in args.opt_regex
        has_invalid = False

        for opt in args.opt:
            match = opt_pattern.fullmatch(opt)
            if match:
                if has_subgroup and len(match.groups()) > 0:
                    normalized_opt_levels.append("O" + match.group(1))
                else:
                    normalized_opt_levels.append(match.group(0))
            else:
                print(f"Error: Invalid optimization level '{opt}' (did not match opt-regex)")
                has_invalid = True
        if has_invalid:
            print("Exiting due to invalid optimization levels.")
            sys.exit(1)
        display_opt_levels = normalized_opt_levels

    print(f"Source directory: {source_dir}")
    print(f"Output directory: {output_dir}")
    print(f"Platforms: {args.platform}")
    print(f"Compiler: {args.compiler if args.compiler else 'all'}")
    print(f"Compiler versions: {args.compiler_versions if args.compiler_versions else 'all'}")
    print(f"Optimization levels: {display_opt_levels if display_opt_levels else 'all'}")
    print(f"File format: {args.file_format}")
    print(f"Cores: {num_cores}")
    print(f"Max memory: {max_memory / (1024**3):.2f}GB")
    print()

    print("Scanning for matching binaries...")
    binaries = find_matching_binaries(
        source_dir,
        args.platform,
        args.compiler,
        args.compiler_versions,
        args.opt,
        args.file_format,
        args.version_regex,
        args.opt_regex,
        args.name_regex,
        args.exclude_subfolder,
    )

    print(f"Found {len(binaries)} matching binaries")

    if not binaries:
        print("No binaries found matching the criteria")
        return

    if args.list_files:
        print("\nMatched files:")
        for binary in binaries:
            rel_path = binary.path.relative_to(source_dir)
            fields = (
                f"[{binary.platform}, {binary.compiler}, {binary.version}, {binary.opt_level}, {binary.binary_name}]"
            )
            print(f"  {rel_path}  {fields}")
        return

    print("Organizing and sorting binaries...")
    sorted_binaries = organize_and_sort_binaries(binaries)

    manager = WorkerManager(
        num_workers=num_cores,
        max_memory=max_memory,
        source_dir=source_dir,
        output_dir=output_dir,
        platform_arg="file_prefix",
        skip_existing=args.skip_existing,
        print_pid=args.pid,
    )

    manager.process_binaries(sorted_binaries)


if __name__ == "__main__":
    main()
