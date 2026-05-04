import argparse
import re
import sys
from dataclasses import dataclass
from pathlib import Path


@dataclass
class SelectionConfig:
    source_dir: Path
    output_dir: Path
    platforms: list[str] | None
    compiler: str | None
    compiler_versions: list[str] | None
    opt_levels: list[str] | None
    file_format: str
    version_regex: str | None
    opt_regex: str
    name_regex: str | None
    exclude_subfolders: list[str] | None
    list_files: bool


@dataclass
class NormalizedOptLevels:
    display_values: list[str]
    filter_values: list[str]


def add_selection_arguments(parser: argparse.ArgumentParser) -> None:
    """Add all arguments related to binary selection and directory traversal."""

    parser.add_argument("--source", type=str, default="./src", help="Source directory containing binaries")

    parser.add_argument("--output", type=str, default="./out", help="Output directory for results")

    parser.add_argument(
        "--platform",
        type=str,
        nargs="+",
        default=None,
        help=(
            "Platforms to process (e.g., x86 x64 arm32 arm64). If not "
            "specified, ALL platforms matching the format string are "
            "processed — same convention as --compiler / "
            "--compiler-versions / --opt. Pre-2026 default of x86/x64 "
            "silently dropped arm/mips/etc. binaries from discovery; "
            "consumers running on multi-arch corpora MUST opt into "
            "the subset they want explicitly to avoid that footgun."
        ),
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


def normalize_opt_levels(opt_levels: list[str], opt_regex: str) -> NormalizedOptLevels:
    """Normalize optimization levels according to the opt_regex pattern.

    Returns:
        NormalizedOptLevels with display values (for printing) and filter values (for matching)
    """
    normalized = []
    opt_pattern = re.compile(opt_regex)
    has_subgroup = "(" in opt_regex and ")" in opt_regex

    for opt in opt_levels:
        match = opt_pattern.fullmatch(opt)
        if match:
            if has_subgroup and len(match.groups()) > 0:
                normalized.append("O" + match.group(1))
            else:
                normalized.append(match.group(0))
        else:
            print(f"Error: Invalid optimization level '{opt}' (did not match opt-regex)")
            sys.exit(1)

    return NormalizedOptLevels(display_values=normalized, filter_values=normalized)


def process_selection_arguments(args: argparse.Namespace) -> SelectionConfig:
    """Process and validate selection-related arguments.

    Returns:
        SelectionConfig with validated and processed values

    Exits:
        If validation fails
    """
    if args.debugs:
        args.name_regex = "minigzipsh"

    if args.compiler_versions and args.version_regex:
        print("Error: Cannot specify both --compiler-versions and --version-regex")
        sys.exit(1)

    source_dir = Path(args.source).resolve()
    output_dir = Path(args.output).resolve()

    # --source-already-staged means the data lives on the cluster
    # filesystem (gateway-side), not locally; the local --source path
    # has no local meaning. Skip the local-exists check so the consumer
    # doesn't have to pass a placeholder local dir just to clear
    # validation. The path is still passed through to discover_items —
    # consumers that want to drive find_items against the staged
    # location should resolve the gateway path themselves (typically
    # from args.source_already_staged + the SSH backend).
    if not source_dir.exists() and not getattr(args, "source_already_staged", None):
        print(f"Error: Source directory does not exist: {source_dir}")
        print(
            "Hint: if the source data lives on the gateway / cluster filesystem only, "
            "pass --source-already-staged <path> and the local --source check is skipped."
        )
        sys.exit(1)

    output_dir.mkdir(parents=True, exist_ok=True)

    return SelectionConfig(
        source_dir=source_dir,
        output_dir=output_dir,
        platforms=args.platform,
        compiler=args.compiler,
        compiler_versions=args.compiler_versions,
        opt_levels=args.opt,
        file_format=args.file_format,
        version_regex=args.version_regex,
        opt_regex=args.opt_regex,
        name_regex=args.name_regex,
        exclude_subfolders=args.exclude_subfolder,
        list_files=args.list_files,
    )


def print_selection_summary(config: SelectionConfig, display_opt_levels: list[str] | None) -> None:
    """Print a summary of the selection configuration."""
    print(f"Source directory: {config.source_dir}")
    print(f"Output directory: {config.output_dir}")
    print(f"Platforms: {config.platforms if config.platforms else 'all'}")
    print(f"Compiler: {config.compiler if config.compiler else 'all'}")
    print(f"Compiler versions: {config.compiler_versions if config.compiler_versions else 'all'}")
    print(f"Optimization levels: {display_opt_levels if display_opt_levels else 'all'}")
    print(f"File format: {config.file_format}")
    print()
