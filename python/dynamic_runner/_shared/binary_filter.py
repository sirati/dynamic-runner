from collections.abc import Callable
from pathlib import Path

from .binary_info import BinaryInfo


def filter_existing_outputs(
    binaries: list[BinaryInfo],
    source_dir: Path,
    output_dir: Path,
    output_filename_fn: Callable[[str], str],
) -> tuple[list[BinaryInfo], int]:
    """Filter out binaries that already have output files.

    Args:
        binaries: List of BinaryInfo objects to filter
        source_dir: Source directory path
        output_dir: Output directory path
        output_filename_fn: Function that takes input filename and returns expected output filename

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

        # Construct expected output path using the provided function
        output_filename = output_filename_fn(binary.path.name)
        output_path = output_dir / relative_path.parent / output_filename

        if output_path.exists():
            skipped_count += 1
        else:
            filtered_binaries.append(binary)

    return filtered_binaries, skipped_count
