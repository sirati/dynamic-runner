"""Re-export binary info types and functions from shared module.

This module maintains backward compatibility for existing imports within dynamic_batch.
"""

from shared.binary_info import (
    FIELD_MAPPING,
    REQUIRED_FIELDS,
    BinaryInfo,
    FieldRegexes,
    build_field_regexes,
    build_regex_from_format,
    parse_binary_filename,
    process_escaping,
)

__all__ = [
    "BinaryInfo",
    "FieldRegexes",
    "FIELD_MAPPING",
    "REQUIRED_FIELDS",
    "build_field_regexes",
    "parse_binary_filename",
    "build_regex_from_format",
    "process_escaping",
]
