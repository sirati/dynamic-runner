import re
from dataclasses import dataclass
from pathlib import Path


@dataclass
class BinaryInfo:
    path: Path
    size: int
    binary_name: str
    platform: str
    compiler: str
    version: str
    opt_level: str


FIELD_MAPPING = {
    "p": "platform",
    "platform": "platform",
    "c": "compiler",
    "compiler": "compiler",
    "cv": "version",
    "version": "version",
    "opt": "opt_level",
    "optimisationlevel": "opt_level",
    "name": "binary_name",
    "binaryname": "binary_name",
}

REQUIRED_FIELDS = {"platform", "compiler", "version", "opt_level", "binary_name"}


@dataclass
class FieldRegexes:
    platform: str
    compiler: str
    version: str
    opt_level: str
    opt_level_transform: str | None
    binary_name: str = r".+"


def build_field_regexes(
    platforms: list[str] | None = None,
    compilers: list[str] | None = None,
    versions: list[str] | None = None,
    opt_levels: list[str] | None = None,
    version_regex: str | None = None,
    opt_regex: str | None = None,
    name_regex: str | None = None,
) -> FieldRegexes:
    """Build regex patterns for each field based on provided constraints."""

    if versions is not None and version_regex is not None:
        raise ValueError("Cannot specify both versions list and version_regex")

    if platforms:
        platform_regex = "(?:" + "|".join(re.escape(p) for p in platforms) + ")"
    else:
        platform_regex = r"[^-_]+"

    if compilers:
        compiler_regex = "(?:" + "|".join(re.escape(c) for c in compilers) + ")"
    else:
        compiler_regex = r"[^-_]+"

    if version_regex:
        version_pattern = version_regex
    elif versions:
        version_pattern = "(?:" + "|".join(re.escape(v) for v in versions) + ")"
    else:
        version_pattern = r"[^-_]+"

    opt_level_transform = None
    if opt_regex:
        opt_level_pattern = opt_regex
        if "(" in opt_regex and ")" in opt_regex:
            opt_level_transform = "transform"
    else:
        opt_level_pattern = r"O([0123s])"
        opt_level_transform = "O"

    binary_name_pattern = name_regex if name_regex else r".+"

    return FieldRegexes(
        platform=platform_regex,
        compiler=compiler_regex,
        version=version_pattern,
        opt_level=opt_level_pattern,
        opt_level_transform=opt_level_transform,
        binary_name=binary_name_pattern,
    )


def process_escaping(
    format_string: str, field_names: list[str]
) -> tuple[str, dict[str, list[int]], dict[str, tuple[str, int]]]:
    """Process escaping and replace field names with placeholders.

    Returns:
        - Modified format string with placeholders
        - Dictionary mapping field names to list of positions where they appear
        - Dictionary mapping placeholders to (field_name, occurrence_index) tuples
    """
    field_positions = {field: [] for field in REQUIRED_FIELDS}

    sorted_field_names = sorted(field_names, key=len, reverse=True)

    result = format_string
    placeholder_counter = 0
    replacements = {}

    for field_name in sorted_field_names:
        normalized = FIELD_MAPPING.get(field_name.lower())
        if not normalized:
            continue

        new_result = []
        i = 0

        while i < len(result):
            if result[i : i + 2] == "\\\\":
                new_result.append("\\\\")
                i += 2
            elif result[i] == "\\" and result[i + 1 : i + 1 + len(field_name)] == field_name:
                new_result.append(field_name)
                i += 1 + len(field_name)
            elif result[i : i + len(field_name)] == field_name:
                placeholder = f"<<<FIELD_{placeholder_counter}>>>"
                replacements[placeholder] = (normalized, len(field_positions[normalized]))
                field_positions[normalized].append(len(field_positions[normalized]))
                new_result.append(placeholder)
                i += len(field_name)
                placeholder_counter += 1
            else:
                new_result.append(result[i])
                i += 1

        result = "".join(new_result)

    return result, field_positions, replacements


def build_regex_from_format(
    format_string: str,
    field_regexes: FieldRegexes,
) -> tuple[re.Pattern, dict[str, int], str | None]:
    """Build regex pattern from format string template.

    Args:
        format_string: Format template with field names
        field_regexes: Regex patterns for each field

    Returns:
        - Compiled regex pattern
        - Dictionary mapping field names to their capture group numbers
        - Transform string for opt_level if needed (e.g., "O" to prepend "O" to captured group)
    """
    all_field_names = list(FIELD_MAPPING.keys())

    processed_format, field_positions, replacements = process_escaping(format_string, all_field_names)

    seen_fields = {}
    field_to_group = {}
    current_group = 1

    field_regex_map = {
        "platform": field_regexes.platform,
        "compiler": field_regexes.compiler,
        "version": field_regexes.version,
        "opt_level": field_regexes.opt_level,
        "binary_name": field_regexes.binary_name,
    }

    regex_parts = []
    last_end = 0

    placeholder_pattern = re.compile(r"<<<FIELD_(\d+)>>>")

    for match in placeholder_pattern.finditer(processed_format):
        regex_parts.append(re.escape(processed_format[last_end : match.start()]))

        placeholder = match.group(0)
        field_name, occurrence_idx = replacements[placeholder]

        if field_name not in seen_fields:
            field_pattern = field_regex_map[field_name]
            regex_parts.append(f"({field_pattern})")
            seen_fields[field_name] = current_group
            field_to_group[field_name] = current_group

            if field_name == "opt_level" and "(" in field_pattern:
                current_group += 2
            else:
                current_group += 1
        else:
            group_num = seen_fields[field_name]
            regex_parts.append(f"\\{group_num}")

        last_end = match.end()

    regex_parts.append(re.escape(processed_format[last_end:]))

    final_pattern = "^" + "".join(regex_parts) + "$"

    if set(seen_fields.keys()) != REQUIRED_FIELDS:
        missing = REQUIRED_FIELDS - set(seen_fields.keys())
        raise ValueError(f"Format string must contain all required fields. Missing: {missing}")

    compiled_regex = re.compile(final_pattern)

    return compiled_regex, field_to_group, field_regexes.opt_level_transform


def parse_binary_filename(
    filename: str,
    format_string: str = "platform-compiler-version-optimisationlevel_binaryname",
    field_regexes: FieldRegexes | None = None,
) -> tuple[str, str, str, str, str] | None:
    """Parse binary filename using format string template.

    Args:
        filename: The binary filename to parse
        format_string: Format template with field names (e.g., 'p-c-cv-opt_name')
        field_regexes: Custom regex patterns for fields

    Returns:
        Tuple of (platform, compiler, version, opt_level, binary_name) or None if no match
    """
    if field_regexes is None:
        field_regexes = build_field_regexes()

    try:
        regex, field_to_group, opt_transform = build_regex_from_format(format_string, field_regexes)
    except ValueError:
        return None

    match = regex.match(filename)
    if not match:
        return None

    groups = match.groups()

    platform = groups[field_to_group["platform"] - 1]
    compiler = groups[field_to_group["compiler"] - 1]
    version = groups[field_to_group["version"] - 1]
    binary_name = groups[field_to_group["binary_name"] - 1]

    opt_group_idx = field_to_group["opt_level"] - 1
    if opt_transform == "O":
        opt_level = "O" + groups[opt_group_idx + 1]
    elif opt_transform == "transform":
        opt_level = groups[opt_group_idx]
    else:
        opt_level = groups[opt_group_idx]

    return (platform, compiler, version, opt_level, binary_name)
