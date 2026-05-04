import os
import re
from dataclasses import dataclass
from pathlib import Path

from .binary_info import (
    BinaryFilenameFormat,
    BinaryIdentifier,
    TaskInfo,
    build_binary_filename_format,
    build_field_regexes,
    parse_binary_filename,
)
from .selection_args import SelectionConfig


@dataclass(frozen=True)
class SelectionFilters:
    """Compiled selection filters: the per-format regex plus the
    post-parse allowlists / exclude pattern. Build once with
    `compile_selection_filters`, then reuse for every filename via
    `match_filename` and every traversed subdirectory via
    `is_excluded_subfolder`.
    """

    binary_format: BinaryFilenameFormat
    platforms: list[str]
    compiler: str | None
    compiler_versions: list[str] | None
    normalized_opt_levels: list[str] | None
    exclude_pattern: re.Pattern | None


def _normalize_opt_levels_for_filter(
    opt_levels: list[str] | None, opt_regex: str | None
) -> list[str] | None:
    """Normalise user-supplied --opt values against the same opt_regex
    parse_binary_filename will produce, so the post-parse allowlist
    check matches even when the user wrote 'O0' but the filename
    encodes it as '0' (or vice versa).

    Mirrors the inline normalisation that find_matching_binaries
    used to do; lifted out so consumers driving discovery via
    find_items + a custom visitor can apply the same rule.
    """
    if not opt_levels:
        return None

    pattern = opt_regex if opt_regex else r"[oO]([0123s])"
    opt_re = re.compile(pattern)
    has_subgroup = "(" in pattern and ")" in pattern

    normalised: list[str] = []
    for opt in opt_levels:
        match = opt_re.fullmatch(opt)
        if match:
            if has_subgroup and len(match.groups()) > 0:
                normalised.append("O" + match.group(1))
            else:
                normalised.append(match.group(0))
        else:
            # Pass through unchanged when no match — preserves the
            # historical "user knows best" behaviour for non-standard
            # opt-regex shapes.
            normalised.append(opt)
    return normalised


def _build_exclude_pattern(exclude_subfolders: list[str] | None) -> re.Pattern | None:
    if not exclude_subfolders:
        return None
    return re.compile("(" + "|".join(exclude_subfolders) + ")")


def compile_selection_filters(config: SelectionConfig) -> SelectionFilters:
    """Compile a `SelectionConfig` into a reusable `SelectionFilters`.

    Centralises the filter-compilation that `find_matching_binaries`
    used to do inline so consumers driving discovery via the new
    `find_items` walker (with their own visitor body) can share the
    same rule for the standard
    'platform-compiler-version-opt-binary' filename format without
    re-implementing the regex / opt-level normalisation each time.
    """
    field_regexes = build_field_regexes(
        platforms=config.platforms,
        compilers=[config.compiler] if config.compiler else None,
        versions=config.compiler_versions,
        opt_levels=config.opt_levels,
        version_regex=config.version_regex,
        opt_regex=config.opt_regex,
        name_regex=config.name_regex,
    )
    binary_format = build_binary_filename_format(config.file_format, field_regexes)
    return SelectionFilters(
        binary_format=binary_format,
        platforms=config.platforms,
        compiler=config.compiler,
        compiler_versions=config.compiler_versions,
        normalized_opt_levels=_normalize_opt_levels_for_filter(
            config.opt_levels, config.opt_regex
        ),
        exclude_pattern=_build_exclude_pattern(config.exclude_subfolders),
    )


def match_filename(filename: str, filters: SelectionFilters) -> BinaryIdentifier | None:
    """Return a `BinaryIdentifier` for `filename` if it matches the
    compiled format AND clears every post-parse allowlist; else None.

    Hidden files (leading '.') are filtered here too so consumers
    don't need to repeat that check at every visit() call.
    """
    if filename.startswith("."):
        return None
    parsed = parse_binary_filename(filename, filters.binary_format)
    if not parsed:
        return None
    platform, comp, version, opt, binary_name = parsed
    if platform not in filters.platforms:
        return None
    if filters.compiler and comp != filters.compiler:
        return None
    if filters.compiler_versions and version not in filters.compiler_versions:
        return None
    if filters.normalized_opt_levels and opt not in filters.normalized_opt_levels:
        return None
    return BinaryIdentifier(
        binary_name=binary_name,
        platform=platform,
        compiler=comp,
        version=version,
        opt_level=opt,
    )


def is_excluded_subfolder(rel_path: str, filters: SelectionFilters) -> bool:
    """True if this relative-to-root subfolder should not be descended
    into. The root itself (rel_path == '.') is always entered; the
    exclude pattern is OR-of-substrings, matching the existing
    --exclude-subfolder semantics.
    """
    if filters.exclude_pattern is None or rel_path == ".":
        return False
    return bool(filters.exclude_pattern.search(rel_path))


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
) -> list[TaskInfo]:
    """Find all binaries matching the filter criteria by traversing the source directory.

    Composition of `compile_selection_filters` + os.walk + `match_filename`
    + `is_excluded_subfolder`. Consumers driving discovery via the
    `find_items` walker should use those building blocks directly with
    their own visitor body rather than calling this function.
    """
    config = SelectionConfig(
        source_dir=source_dir,
        output_dir=source_dir,  # unused by filter compilation
        platforms=platforms,
        compiler=compiler,
        compiler_versions=compiler_versions,
        opt_levels=opt_levels,
        file_format=format_string,
        version_regex=version_regex,
        opt_regex=opt_regex if opt_regex is not None else "[oO]?([0123s])",
        name_regex=name_regex,
        exclude_subfolders=exclude_subfolders,
        list_files=False,
    )
    filters = compile_selection_filters(config)

    binaries: list[TaskInfo] = []
    for root, dirs, files in os.walk(source_dir):
        rel_path_str = str(Path(root).relative_to(source_dir))
        if is_excluded_subfolder(rel_path_str, filters):
            dirs[:] = []
            continue

        for filename in files:
            identifier = match_filename(filename, filters)
            if identifier is None:
                continue

            filepath = Path(root) / filename
            if not filepath.is_file():
                continue
            try:
                size = filepath.stat().st_size
            except OSError:
                continue

            binaries.append(TaskInfo(path=filepath, size=size, identifier=identifier))

    return binaries
