"""Logging configuration for the runner.

Extracted verbatim from the previous `cli._setup_logging` so the prefix
behaviour for primary (`P|`), secondary (`S|`), and `--raw-logs` modes
is preserved.

This module also owns the `--important-stdio-only` logging mode end to
end (its single concern): both halves of the feature are "configure
logging for importance mode", so they live together.

  * The Rust side of the mode is a dual-sink tracing subscriber
    (`crates/dynrunner-pyo3/src/logging.rs`) that reads two env vars
    ONCE at `_native` import time. `apply_important_stdio_env` exports
    them from an argv lookahead and MUST run before the first `_native`
    import (driven from `dynamic_runner.__init__` top, before the eager
    `from ._native import ...`).
  * The Python side suppresses the root console handler and routes
    Python's own logs to the same full-log file, so stdio carries only
    the Rust-emitted important events while the full log keeps
    everything. This half runs inside `setup_logging`.
"""

import argparse
import logging
import os
from pathlib import Path

from ._shared.logging_utils import remove_stream_handlers

#: CLI flag that activates importance-stdio mode. Scanned from argv both
#: here (early env export) and by argparse (`cli.build_arg_parser`); the
#: literal lives in one place so the two scanners cannot drift.
IMPORTANT_STDIO_ONLY_FLAG = "--important-stdio-only"

#: Environment variables read ONCE by the Rust dual-sink subscriber at
#: `_native` import. Names mirror `crates/dynrunner-pyo3/src/logging.rs`
#: (the one cross-language contract); defined here so the Python side has
#: a single source of truth for the two strings.
IMPORTANT_STDIO_ONLY_ENV = "DYNRUNNER_IMPORTANT_STDIO_ONLY"
FULL_LOG_FILE_ENV = "DYNRUNNER_FULL_LOG_FILE"

#: Default destination for the full (unfiltered) log when the operator
#: did not pre-export `FULL_LOG_FILE_ENV`. The framework exposes no
#: dedicated full-log-file arg today (`--log-dir` / `--output` anchor
#: per-secondary worker logs, not the framework's own stdout stream), and
#: this export must resolve before the full argparse pass — so the
#: working directory is the only context available. Relative on purpose:
#: it lands wherever the run was launched, matching the historical
#: shell-redirection capture point.
DEFAULT_FULL_LOG_FILE = "dynrunner-full.log"


def important_stdio_only_requested(args_list: list[str]) -> bool:
    """Whether `--important-stdio-only` appears in the argv lookahead.

    A bare membership test (mirroring the existing `--secondary` /
    `--multi-computer` lookaheads in `setup_logging`) — the flag is a
    `store_true` with no value token, so no parser is needed.
    """
    return IMPORTANT_STDIO_ONLY_FLAG in args_list


def full_log_file_path() -> Path:
    """The full-log file the importance mode writes to.

    Honours an operator-supplied `FULL_LOG_FILE_ENV` (so a pre-export
    composes), else the cwd default. `apply_important_stdio_env` seeds
    the env var from this, so by `setup_logging` time the var is always
    set and this re-reads the same value.
    """
    configured = os.environ.get(FULL_LOG_FILE_ENV)
    if configured and configured.strip():
        return Path(configured)
    return Path(DEFAULT_FULL_LOG_FILE)


def apply_important_stdio_env(args_list: list[str]) -> None:
    """Export the dual-sink env vars when `--important-stdio-only` is in
    argv. MUST run before the first `_native` import (the Rust subscriber
    reads these ONCE at import) — hence the call sits at the top of
    `dynamic_runner.__init__`, ahead of the eager `_native` import.

    Idempotent and operator-overridable: it never overwrites a value the
    operator pre-exported (so `DYNRUNNER_FULL_LOG_FILE=/path run ...`
    composes), it only fills the gaps the flag implies.
    """
    if not important_stdio_only_requested(args_list):
        return
    os.environ.setdefault(IMPORTANT_STDIO_ONLY_ENV, "1")
    os.environ.setdefault(FULL_LOG_FILE_ENV, DEFAULT_FULL_LOG_FILE)


def _redirect_python_logs_to_full_log() -> None:
    """Importance-mode Python-side handler reconfiguration: drop the root
    console StreamHandler(s) so Python chatter does not reach stdio, and
    append a FileHandler to the full-log file so Python's full record is
    preserved alongside the Rust full sink.

    Reuses the shared `remove_stream_handlers` helper (no bespoke handler
    walk) and the same level/format `setup_logging` just installed on the
    root logger.
    """
    root = logging.getLogger()
    file_handler = logging.FileHandler(full_log_file_path(), mode="a")
    file_handler.setLevel(root.level)
    # Inherit the formatter the just-completed basicConfig installed on a
    # root stream handler so the file record reads identically to what
    # stdout would have shown.
    for handler in root.handlers:
        if isinstance(handler, logging.StreamHandler) and handler.formatter:
            file_handler.setFormatter(handler.formatter)
            break
    remove_stream_handlers(root)
    root.addHandler(file_handler)


def setup_logging(args_list: list[str]) -> logging.Logger:
    """Configure the root logger from the early-arg-parsed flags.

    Looks at `--debug`, `--raw-logs`, and the mode flags (`--secondary`,
    `--multi-computer`, `--slurm`) to choose a prefix and verbosity. The
    full argparse pass happens later; this is just a fast lookahead.

    When `--important-stdio-only` is set, the Python console handler is
    dropped and Python's logs are routed to the full-log file instead, so
    stdio carries only the Rust-emitted important events (the Rust env
    side was already exported by `apply_important_stdio_env` before the
    `_native` import). Off by default — unchanged console logging.
    """
    parser = argparse.ArgumentParser(add_help=False)
    parser.add_argument("--debug", action="store_true")
    parser.add_argument("--raw-logs", action="store_true")

    help_requested = "-h" in args_list or "--help" in args_list

    if not help_requested:
        early_args, _ = parser.parse_known_args(args_list)

        if "--secondary" in args_list:
            prefix = "S|"
        elif "--multi-computer" in args_list or "--slurm" in args_list:
            prefix = "P|"
        else:
            prefix = ""

        log_level = logging.DEBUG if early_args.debug else logging.INFO
        logger = logging.getLogger()
        logger.setLevel(log_level)

        if early_args.raw_logs:
            log_format = f"{prefix}%(message)s"
            logging.basicConfig(level=log_level, format=log_format)
        else:
            if prefix:
                log_format = f"%(levelname)s | %(asctime)s |{prefix}| %(message)s"
            else:
                log_format = "%(levelname)s | %(asctime)s | %(message)s"
            logging.basicConfig(level=log_level, format=log_format, datefmt="%H:%M:%S")
    else:
        logging.basicConfig(
            level=logging.INFO,
            format="%(levelname)s | %(asctime)s | %(message)s",
            datefmt="%H:%M:%S",
        )
        logger = logging.getLogger()

    if important_stdio_only_requested(args_list):
        _redirect_python_logs_to_full_log()

    return logger
