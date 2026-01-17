"""Shared utilities for binary selection and directory traversal.

This package contains reusable code for:
- Parsing binary filenames according to format templates
- Traversing directories to find matching binaries
- Filtering binaries based on platform, compiler, version, and optimization level
"""

from .binary_info import (
    BinaryFilenameFormat,
    BinaryInfo,
    FieldRegexes,
    build_binary_filename_format,
    build_field_regexes,
    format_binary_info,
    format_size,
    parse_binary_filename,
)
from .binary_selector import find_matching_binaries
from .logging_utils import (
    WarningCounterHandler,
    remove_stream_handlers,
    setup_file_logger,
    setup_logger,
)
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
    "BinaryFilenameFormat",
    "FieldRegexes",
    "build_binary_filename_format",
    "build_field_regexes",
    "format_binary_info",
    "format_size",
    "parse_binary_filename",
    "find_matching_binaries",
    "SelectionConfig",
    "NormalizedOptLevels",
    "add_selection_arguments",
    "process_selection_arguments",
    "normalize_opt_levels",
    "WarningCounterHandler",
    "remove_stream_handlers",
    "setup_file_logger",
    "setup_logger",
]
