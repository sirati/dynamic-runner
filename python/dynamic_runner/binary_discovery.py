import os
import re
from collections import defaultdict
from pathlib import Path

from .binary_info import BinaryInfo, build_field_regexes, parse_binary_filename


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
    """Find all binaries matching the filter criteria."""

    field_regexes = build_field_regexes(
        platforms=platforms,
        compilers=[compiler] if compiler else None,
        versions=compiler_versions,
        opt_levels=opt_levels,
        version_regex=version_regex,
        opt_regex=opt_regex,
        name_regex=name_regex,
    )

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

    for root, dirs, files in os.walk(source_dir):
        root_path = Path(root)
        rel_path = root_path.relative_to(source_dir)
        rel_path_str = str(rel_path)

        if exclude_pattern and rel_path_str != "." and exclude_pattern.search(rel_path_str):
            dirs[:] = []
            continue
        for filename in files:
            filepath = Path(root) / filename

            if not filepath.is_file() or filename.startswith("."):
                continue

            parsed = parse_binary_filename(filename, format_string, field_regexes)
            if not parsed:
                continue

            platform, comp, version, opt, binary_name = parsed

            if platform not in platforms:
                continue

            if compiler and comp != compiler:
                continue

            if compiler_versions and version not in compiler_versions:
                continue

            if normalized_opt_levels and opt not in normalized_opt_levels:
                continue

            try:
                size = filepath.stat().st_size
            except Exception:
                continue

            binaries.append(
                BinaryInfo(
                    path=filepath,
                    size=size,
                    binary_name=binary_name,
                    platform=platform,
                    compiler=comp,
                    version=version,
                    opt_level=opt,
                )
            )

    return binaries


def organize_and_sort_binaries(binaries: list[BinaryInfo]) -> list[BinaryInfo]:
    """Group by binary_name, calculate average size, sort by average (largest first),
    then sort within each group by size (largest first)."""

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
