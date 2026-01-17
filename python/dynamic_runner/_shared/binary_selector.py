import os
import re
from pathlib import Path

from .binary_info import (
    BinaryIdentifier,
    BinaryInfo,
    build_binary_filename_format,
    build_field_regexes,
    parse_binary_filename,
)


def find_matching_binaries(
    source_dir: Path,
    platforms: list[str],
    compiler: str | None,
    compiler_versions: list[str] | None,
    opt_levels: list[str] | None,
    format_string: str = "platform-compiler-version-optimisationlevel_binaryname",
    version_regex: str | None = None,
    opt_regex: str | None = None,
    name_regex: str | None = None,
    exclude_subfolders: list[str] | None = None,
) -> list[BinaryInfo]:
    """Find all binaries matching the filter criteria by traversing the source directory.

    This function handles:
    - Directory traversal (walking through source_dir)
    - File filtering based on platforms, compiler, versions, optimization levels
    - Filename parsing according to the format string
    - Subfolder exclusion

    Args:
        source_dir: Root directory to search for binaries
        platforms: List of platforms to include (e.g., ['x86', 'x64'])
        compiler: Specific compiler to filter by, or None for all
        compiler_versions: List of compiler versions to include, or None for all
        opt_levels: List of optimization levels to include, or None for all
        format_string: Format template for parsing filenames
        version_regex: Custom regex for matching version field
        opt_regex: Custom regex for matching optimization level field
        name_regex: Custom regex for matching binary name field
        exclude_subfolders: List of subfolder patterns to exclude from traversal

    Returns:
        List of BinaryInfo objects for all matching binaries found
    """

    field_regexes = build_field_regexes(
        platforms=platforms,
        compilers=[compiler] if compiler else None,
        versions=compiler_versions,
        opt_levels=opt_levels,
        version_regex=version_regex,
        opt_regex=opt_regex,
        name_regex=name_regex,
    )

    binary_format = build_binary_filename_format(format_string, field_regexes)

    normalized_opt_levels = None
    if opt_levels:
        normalized_opt_levels = []
        opt_pattern = opt_regex if opt_regex else r"[oO]([0123s])"
        opt_re = re.compile(opt_pattern)
        has_subgroup = "(" in opt_pattern and ")" in opt_pattern

        for opt in opt_levels:
            match = opt_re.fullmatch(opt)
            if match:
                if has_subgroup and len(match.groups()) > 0:
                    normalized_opt_levels.append("O" + match.group(1))
                else:
                    normalized_opt_levels.append(match.group(0))
            else:
                normalized_opt_levels.append(opt)

    binaries: list[BinaryInfo] = []

    exclude_pattern = None
    if exclude_subfolders:
        pattern = "(" + "|".join(exclude_subfolders) + ")"
        exclude_pattern = re.compile(pattern)

    # Traverse the directory tree
    for root, dirs, files in os.walk(source_dir):
        root_path = Path(root)
        rel_path = root_path.relative_to(source_dir)
        rel_path_str = str(rel_path)

        # Check if this directory should be excluded
        if exclude_pattern and rel_path_str != "." and exclude_pattern.search(rel_path_str):
            dirs[:] = []  # Don't descend into subdirectories
            continue

        # Process each file in this directory
        for filename in files:
            filepath = Path(root) / filename

            # Skip non-files and hidden files
            if not filepath.is_file() or filename.startswith("."):
                continue

            # Parse the filename according to the format string
            parsed = parse_binary_filename(filename, binary_format)
            if not parsed:
                continue

            platform, comp, version, opt, binary_name = parsed

            # Apply filters
            if platform not in platforms:
                continue

            if compiler and comp != compiler:
                continue

            if compiler_versions and version not in compiler_versions:
                continue

            if normalized_opt_levels and opt not in normalized_opt_levels:
                continue

            # Get file size
            try:
                size = filepath.stat().st_size
            except Exception:
                continue

            # Add to results
            binaries.append(
                BinaryInfo(
                    path=filepath,
                    size=size,
                    identifier=BinaryIdentifier(
                        binary_name=binary_name,
                        platform=platform,
                        compiler=comp,
                        version=version,
                        opt_level=opt,
                    ),
                )
            )

    return binaries
