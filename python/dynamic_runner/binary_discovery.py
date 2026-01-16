from collections import defaultdict
from pathlib import Path

from shared import find_matching_binaries as _find_matching_binaries

from .binary_info import BinaryInfo


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

    This is a wrapper around shared.find_matching_binaries that maintains type consistency
    within the dynamic_batch module.
    """
    return _find_matching_binaries(
        source_dir,
        platforms,
        compiler,
        compiler_versions,
        opt_levels,
        format_string,
        version_regex,
        opt_regex,
        name_regex,
        exclude_subfolders,
    )


def organize_and_sort_binaries(binaries: list[BinaryInfo]) -> list[BinaryInfo]:
    """Group by binary_name, calculate average size, sort by average (largest first),
    then sort within each group by size (largest first).

    This function handles the scheduling/ordering logic for processing binaries
    to optimize resource utilization.
    """

    groups: dict[str, list[BinaryInfo]] = defaultdict(list)
    for binary in binaries:
        groups[binary.binary_name].append(binary)

    group_averages: list[tuple[str, float, list[BinaryInfo]]] = []
    for binary_name, group in groups.items():
        avg_size = sum(b.size for b in group) / len(group)
        group.sort(key=lambda b: b.size, reverse=True)
        group_averages.append((binary_name, avg_size, group))

    group_averages.sort(key=lambda x: x[1], reverse=True)

    result: list[BinaryInfo] = []
    for _, _, group in group_averages:
        result.extend(group)

    return result


def filter_existing_outputs(
    binaries: list[BinaryInfo],
    source_dir: Path,
    output_dir: Path,
) -> tuple[list[BinaryInfo], int]:
    """Filter out binaries that already have output files.

    This function is part of the processing logic to avoid reprocessing
    binaries that have already been completed.

    Returns:
        Tuple of (filtered_binaries, skipped_count)
    """
    filtered_binaries = []
    skipped_count = 0

    for binary in binaries:
        # Get relative path from source_dir
        try:
            relative_path = binary.path.relative_to(source_dir)
        except ValueError:
            # If binary is not under source_dir, include it
            filtered_binaries.append(binary)
            continue

        # Construct expected output path
        # The output structure should mirror the source structure
        output_path = output_dir / relative_path.parent / f"{binary.path.name}_output.csv"

        if output_path.exists():
            skipped_count += 1
        else:
            filtered_binaries.append(binary)

    return filtered_binaries, skipped_count
