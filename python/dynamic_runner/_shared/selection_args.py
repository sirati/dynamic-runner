"""Framework-generic source/output argparse + path validation.

The asm-binary specific selection flags (`--platform`, `--compiler`,
`--compiler-versions`, `--opt`, `--opt-regex`, `--name-regex`,
`--exclude-subfolder`, `--file-format`, `--debugs`) used to live here
too — they've been moved out of the framework into consumer packages
(asm-tokenizer / asm-dataset-nix) because corpus-shape filters are
task concerns, not framework primitives. Consumers add their own
filter flags via `TaskDefinition.add_task_arguments(parser)` and read
them in their `discover_items` implementation.

What stays here is the framework's own contract: every dispatch
needs a `--source` (where to look for items) and an `--output`
(where to put results). Both are universally needed across tasks
and form the entry point the framework uses to set
`args.resolved_output_root` for downstream `find_items` calls.

`--list-files` is a generic introspection knob — `discover_items`
runs but the framework prints the discovered items' paths and
exits before dispatch. Consumers can hook richer formatting via
their TaskInfo subclass / __str__ if they want.
"""

import argparse
import sys
from dataclasses import dataclass
from pathlib import Path


@dataclass
class SelectionConfig:
    """Framework-generic selection state.

    Just `source_dir`, `output_dir`, and the `--list-files` toggle.
    Asm-specific filter state (platform/compiler/version/opt) lives
    on the consumer's task definition, accessed via `args.<field>`
    inside `discover_items`.
    """

    source_dir: Path
    output_dir: Path
    list_files: bool


def add_selection_arguments(parser: argparse.ArgumentParser) -> None:
    """Add the framework's source/output/list-files flags.

    Consumers who need corpus-shape filters (e.g. by platform,
    compiler, optimisation level, custom filename format) add those
    via `TaskDefinition.add_task_arguments(parser)` and consume them
    in `discover_items`.
    """
    parser.add_argument(
        "--source",
        type=str,
        default="./src",
        help="Source directory containing items to process.",
    )
    parser.add_argument(
        "--output",
        type=str,
        default="./out",
        help="Output directory for results.",
    )
    parser.add_argument(
        "--list-files",
        action="store_true",
        help=(
            "List all matched items without processing — runs "
            "`task.discover_items` then prints each item's path "
            "and exits."
        ),
    )


def process_selection_arguments(args: argparse.Namespace) -> SelectionConfig:
    """Resolve --source / --output to absolute paths and validate.

    Pre-staged-source mode (`--source-already-staged <path>`) skips
    the local-exists check on `--source` because the data lives on
    the gateway-side filesystem, not locally; the consumer's
    `discover_items` is responsible for routing through the SSH
    backend in that case.
    """
    source_dir = Path(args.source).resolve()
    output_dir = Path(args.output).resolve()

    if not source_dir.exists() and not getattr(args, "source_already_staged", None):
        print(f"Error: Source directory does not exist: {source_dir}")
        print(
            "Hint: if the source data lives on the gateway / cluster filesystem only, "
            "pass --source-already-staged <path> and the local --source check is skipped."
        )
        sys.exit(1)

    output_dir.mkdir(parents=True, exist_ok=True)

    return SelectionConfig(
        source_dir=source_dir,
        output_dir=output_dir,
        list_files=args.list_files,
    )


def print_selection_summary(config: SelectionConfig) -> None:
    """Print the framework-generic source/output summary.

    Asm-specific summary lines (Platforms / Compiler / Versions /
    Opt levels) are the consumer's concern — print from inside the
    task's `discover_items` if you want them.
    """
    print(f"Source directory: {config.source_dir}")
    print(f"Output directory: {config.output_dir}")
    print()
