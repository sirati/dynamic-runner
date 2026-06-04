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

Environment-variable configuration interface (for wrapping consumers)
=====================================================================
A program that drives ``dynamic_runner`` as a subprocess (a "wrapping
consumer") configures logging through environment variables instead of
injecting flags into a CLI it does not own. The two variables below are
the supported, first-class config path; they are NOT aliases of the
``--important-stdio-only`` flag — the flag is convenience sugar that
merely SEEDS these same variables (`apply_important_stdio_env`), so the
env path and the flag path converge on one mechanism, never a parallel
surface:

  * ``DYNRUNNER_IMPORTANT_STDIO_ONLY`` — truthy (``1``/``true``/``yes``/
    ``on``, case-insensitive; mirrors the Rust `is_truthy`) arms
    importance mode: the Rust dual-sink subscriber gates stdio to the
    important target, and the Python side (here) drops its console
    handler and redirects Python logs to the full-log file. Setting this
    env var alone — with no flag — produces identical submitter behaviour
    to passing ``--important-stdio-only``.
  * ``DYNRUNNER_FULL_LOG_FILE`` — optional path for the full (unfiltered)
    log. When set, the Rust full sink writes there and the Python side
    appends its records to the same file; when unset, importance mode
    falls back to ``./dynrunner-full.log`` (`DEFAULT_FULL_LOG_FILE`). A
    pre-export is always respected — `apply_important_stdio_env` uses
    ``setdefault`` and never clobbers it.

Both variables are SUBMITTER-LOCAL: importance mode steers the
submitter's own stdout/log split. It is deliberately NOT forwarded to
secondaries (see `dynamic_runner._forwarded_argv.SUBMITTER_LOCAL_FLAGS`)
— secondaries keep their full logs for debugging, and post-relocation
the operator's narrative comes from the observer reading the CRDT.
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


#: Truthy spellings for the env-var config path. Mirrors the Rust
#: `is_truthy` (`crates/dynrunner-pyo3/src/logging.rs`) so the two sides
#: of the one cross-language env contract agree on what "on" means.
_TRUTHY_VALUES = frozenset({"1", "true", "yes", "on"})


def _env_truthy(name: str) -> bool:
    """Whether environment variable ``name`` holds a truthy value.

    Case-insensitive; trims surrounding whitespace. Unset or any other
    value is false. Shared convention with the Rust `is_truthy` so the
    env-var interface behaves identically on both sides of the FFI.
    """
    value = os.environ.get(name)
    return value is not None and value.strip().lower() in _TRUTHY_VALUES


def important_stdio_only_requested(args_list: list[str]) -> bool:
    """Whether `--important-stdio-only` appears in the argv lookahead.

    A bare membership test (mirroring the existing `--secondary` /
    `--multi-computer` lookaheads in `setup_logging`) — the flag is a
    `store_true` with no value token, so no parser is needed.

    This is the FLAG half only; `apply_important_stdio_env` keys on it to
    translate the flag into the env contract. "Is importance mode active"
    (the unified question both the flag and the env-var path answer) is
    `importance_mode_active`.
    """
    return IMPORTANT_STDIO_ONLY_FLAG in args_list


def importance_mode_active() -> bool:
    """Whether importance-stdio mode is armed for this submitter process.

    The single predicate for "should the submitter run in importance
    mode", composing the two ways it can be requested onto the ONE env
    contract the Rust subscriber also reads:

      * the ``--important-stdio-only`` flag, which `apply_important_stdio_env`
        has already translated into ``DYNRUNNER_IMPORTANT_STDIO_ONLY=1``
        by the time the submitter configures logging, and
      * a wrapping consumer exporting ``DYNRUNNER_IMPORTANT_STDIO_ONLY``
        directly (the first-class env config path).

    Both collapse to "the env var is truthy", so the Python side reads
    the same source of truth as the Rust dual-sink subscriber — the two
    can never disagree about whether the mode is on.
    """
    return _env_truthy(IMPORTANT_STDIO_ONLY_ENV)


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
    """Normalise the importance-mode env contract before the first
    `_native` import (the Rust subscriber reads these ONCE at import) —
    hence the call sits at the top of `dynamic_runner.__init__`, ahead of
    the eager `_native` import.

    Two request sources converge here onto the ONE env contract:

      * the ``--important-stdio-only`` flag in ``args_list`` — translated
        into ``DYNRUNNER_IMPORTANT_STDIO_ONLY=1``, and
      * a wrapping consumer's pre-exported truthy
        ``DYNRUNNER_IMPORTANT_STDIO_ONLY`` (the first-class env config
        path) — already on the contract, nothing to translate.

    When EITHER arms the mode, the default full-log file is seeded so the
    Rust full sink and the Python redirect agree on a destination (without
    it the Rust full sink would fall back to stdout and defeat the stdio
    gating). The flag path and the env path therefore produce identical
    behaviour — one mechanism, not a parallel surface.

    Idempotent and operator-overridable: it never overwrites a value the
    operator pre-exported (so `DYNRUNNER_FULL_LOG_FILE=/path run ...`
    composes), it only fills the gaps the armed mode implies.
    """
    if not (important_stdio_only_requested(args_list) or importance_mode_active()):
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

    When importance mode is armed (either the `--important-stdio-only`
    flag or a wrapping consumer's ``DYNRUNNER_IMPORTANT_STDIO_ONLY`` env
    export — see `importance_mode_active`), the Python console handler is
    dropped and Python's logs are routed to the full-log file instead, so
    stdio carries only the Rust-emitted important events (the Rust env
    side was already read at `_native` import). Off by default — unchanged
    console logging.
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

    if importance_mode_active():
        _redirect_python_logs_to_full_log()

    return logger
