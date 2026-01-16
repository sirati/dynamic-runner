"""Shared utilities for binary selection and directory traversal.

This package contains reusable code for:
- Parsing binary filenames according to format templates
- Traversing directories to find matching binaries
- Filtering binaries based on platform, compiler, version, and optimization level
"""

from .binary_info import (
    BinaryInfo,
    FieldRegexes,
    build_field_regexes,
    parse_binary_filename,
)
from .binary_selector import find_matching_binaries
from .selection_args import (
    NormalizedOptLevels,
    SelectionConfig,
    add_selection_arguments,
    normalize_opt_levels,
    print_selection_summary,
    process_selection_arguments,
)

__all__ = [
    "BinaryInfo",
    "FieldRegexes",
    "build_field_regexes",
    "parse_binary_filename",
    "find_matching_binaries",
    "SelectionConfig",
    "NormalizedOptLevels",
    "add_selection_arguments",
    "process_selection_arguments",
    "normalize_opt_levels",
    "print_selection_summary",
]
