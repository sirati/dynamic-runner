"""Shared utilities for binary selection and directory traversal.

This package contains reusable code for:
- Parsing binary filenames according to format templates
- Traversing directories to find matching binaries
- Filtering binaries based on platform, compiler, version, and optimization level
"""

from .binary_filter import filter_existing_outputs
from .binary_info import (
    BinaryFilenameFormat,
    BinaryIdentifier,
    TaskInfo,
    FieldRegexes,
    build_binary_filename_format,
    build_field_regexes,
    format_binary_info,
    format_size,
    parse_binary_filename,
)
from .binary_selector import find_matching_binaries
from .csv_helper import increase_csv_field_size_limit
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
    "TaskInfo",
    "BinaryIdentifier",
    "BinaryFilenameFormat",
    "FieldRegexes",
    "build_binary_filename_format",
    "build_field_regexes",
    "filter_existing_outputs",
    "format_binary_info",
    "format_size",
    "parse_binary_filename",
    "find_matching_binaries",
    "increase_csv_field_size_limit",
    "SelectionConfig",
    "NormalizedOptLevels",
    "add_selection_arguments",
    "process_selection_arguments",
    "normalize_opt_levels",
    "WarningCounterHandler",
    "remove_stream_handlers",
    "setup_file_logger",
    "setup_logger",
    "print_selection_summary",
]
