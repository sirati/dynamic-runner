from collections.abc import Callable
from pathlib import Path
from typing import Any

from .binary_info import TaskInfo


def filter_existing_outputs(
    binaries: list[TaskInfo],
    source_dir: Path,
    output_dir: Path,
    output_filename_fn: Callable[[str], str],
) -> tuple[list[TaskInfo], int]:
    """Filter out binaries that already have output files.

    Args:
        binaries: List of TaskInfo objects to filter
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


def filter_existing_outputs_remote(
    binaries: list[TaskInfo],
    source_dir: Path,
    gateway: Any,
    remote_output_dir: str,
    output_filename_fn: Callable[[str], str],
) -> tuple[list[TaskInfo], int]:
    """Filter binaries whose expected output already exists on the
    gateway-side filesystem. Used for SLURM dispatch where outputs
    land on cluster NFS rather than under the primary's local
    `--output` cache.

    One ssh listing of `remote_output_dir` (recursive, file paths
    only) builds the existence set; per-binary membership check
    after that is in-process. This avoids one round-trip per
    binary and works against the same gateway connection
    `find_items` uses for remote discovery.

    Args:
        binaries: TaskInfos to filter.
        source_dir: source directory used to derive each binary's
            relative path under the output tree (mirrors
            `filter_existing_outputs`).
        gateway: connected `Gateway`-protocol object (must expose
            `execute_command(cmd, cwd=None) -> tuple[int, str, str]`,
            i.e. `(returncode, stdout, stderr)`).
        remote_output_dir: gateway-absolute path of the output tree
            to inspect (typically `slurm_config.get_output_dir()`).
        output_filename_fn: same shape as `filter_existing_outputs`.

    Returns:
        (filtered_binaries, skipped_count)
    """
    # `find ... -type f -printf '%P\0'` emits null-delimited paths
    # relative to the listed root, surviving filenames with
    # whitespace. `2>/dev/null` swallows ENOENT if the dir
    # doesn't exist yet — first-run case where nothing is
    # skipped. Quoting the path with single quotes defends
    # against shell metacharacters; embedded single quotes get
    # escaped via the `'\''` standard.
    quoted = "'" + str(remote_output_dir).replace("'", "'\\''") + "'"
    cmd = f"find {quoted} -type f -printf '%P\\0' 2>/dev/null || true"
    _rc, stdout, _stderr = gateway.execute_command(cmd)
    existing: set[str] = {p for p in stdout.split("\0") if p}

    filtered_binaries: list[TaskInfo] = []
    skipped_count = 0
    for binary in binaries:
        try:
            relative_path = binary.path.relative_to(source_dir)
        except ValueError:
            filtered_binaries.append(binary)
            continue

        output_filename = output_filename_fn(binary.path.name)
        rel_output = str(relative_path.parent / output_filename)
        # Normalise "./<file>" → "<file>" since `find -printf %P`
        # never includes a leading "./".
        if rel_output.startswith("./"):
            rel_output = rel_output[2:]

        if rel_output in existing:
            skipped_count += 1
        else:
            filtered_binaries.append(binary)

    return filtered_binaries, skipped_count
