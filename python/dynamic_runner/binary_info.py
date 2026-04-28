"""Re-export binary info types and functions from the _shared module.

This module maintains backward compatibility for existing imports within dynamic_runner.
"""

from ._shared.binary_info import (
    FIELD_MAPPING,
    REQUIRED_FIELDS,
    BinaryFilenameFormat,
    BinaryInfo,
    FieldRegexes,
    build_binary_filename_format,
    build_field_regexes,
    build_regex_from_format,
    format_binary_info,
    format_size,
    parse_binary_filename,
    process_escaping,
)

__all__ = [
    "BinaryInfo",
    "BinaryFilenameFormat",
    "FieldRegexes",
    "FIELD_MAPPING",
    "REQUIRED_FIELDS",
    "build_binary_filename_format",
    "build_field_regexes",
    "format_binary_info",
    "format_size",
    "parse_binary_filename",
    "build_regex_from_format",
    "process_escaping",
]
