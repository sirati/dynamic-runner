"""Framework-generic shared utilities.

The asm-binary specific filename parsing + selection-filter helpers
that used to live here have been moved out of the framework into
consumer packages (asm-tokenizer / asm-dataset-nix). The framework
intentionally owns no opinion on filename formats, platform/compiler
allowlists, or any other corpus-shape concern — those are task
responsibilities, surfaced through `TaskDefinition.add_task_arguments`
+ `discover_items`.

What stays here:
- `TaskInfo` (and the still-coupled `BinaryIdentifier` — decoupling
  TaskInfo's identifier into a fully generic slot is a separate
  refactor)
- `format_size` (plain byte-count formatter; generic)
- `--source`/`--output`/`--list-files` argparse + path validation
  (the framework's universal entry-point flags)
- CSV stdlib `field_size_limit` bump (`csv_helper`)
- Logging handlers (`logging_utils`)
"""

from .csv_helper import increase_csv_field_size_limit
from .logging_utils import (
    WarningCounterHandler,
    remove_stream_handlers,
    setup_file_logger,
    setup_logger,
)
from .selection_args import (
    SelectionConfig,
    add_selection_arguments,
    print_selection_summary,
    process_selection_arguments,
)
from .task_info import (
    BinaryIdentifier,
    TaskDep,
    TaskInfo,
    format_size,
)

__all__ = [
    "TaskInfo",
    "TaskDep",
    "BinaryIdentifier",
    "format_size",
    "increase_csv_field_size_limit",
    "SelectionConfig",
    "add_selection_arguments",
    "process_selection_arguments",
    "print_selection_summary",
    "WarningCounterHandler",
    "remove_stream_handlers",
    "setup_file_logger",
    "setup_logger",
]
