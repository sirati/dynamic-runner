"""Logging configuration for the runner.

This module owns logging configuration end to end (its single concern),
driven by EXPLICIT PARAMETERS from the parsed framework args — never
environment variables. Both halves of the ``--important-stdio-only``
feature are "configure logging for importance mode", so they live together:

  * The Rust side is a dual-sink tracing subscriber
    (`crates/dynrunner-pyo3/src/logging.rs`) installed by the
    `init_logging(...)` pyfunction. :func:`setup_logging` calls it with the
    three parsed knobs (``important_stdio_only``, ``full_log_file``,
    ``full_log_dir``) AFTER argparse — the subscriber is no longer installed
    at `_native` import, so config is chosen by flags, not read from the
    env. Anything that logs in the (do-nothing) window before this call has
    no global subscriber and is dropped.
  * The Python side suppresses the root console handler and routes Python's
    own logs to the same full-log file, so stdio carries only the
    Rust-emitted important events while the full log keeps everything. This
    half runs inside :func:`setup_logging`.

``--important-stdio-only`` is SUBMITTER-LOCAL: it steers the submitter's own
stdout/log split and is deliberately NOT forwarded to secondaries (see
:mod:`dynamic_runner._framework_flags`) — secondaries keep their full logs
for debugging, and post-relocation the operator's narrative comes from the
observer reading the CRDT.
"""

from __future__ import annotations

import argparse
import logging
from pathlib import Path

from ._shared.logging_utils import remove_stream_handlers

#: CLI flag that activates importance-stdio mode. Owned here so the
#: classification in `_framework_flags.SUBMITTER_LOCAL_FLAGS` and any other
#: reference share one literal.
IMPORTANT_STDIO_ONLY_FLAG = "--important-stdio-only"

#: Default destination for the full (unfiltered) log when importance mode is
#: on and the operator did not pass ``--full-log-file``. Relative on
#: purpose: it lands wherever the run was launched, matching the historical
#: shell-redirection capture point.
DEFAULT_FULL_LOG_FILE = "dynrunner-full.log"


def resolve_full_log_file(
    important_stdio_only: bool, full_log_file: str | None
) -> Path | None:
    """The full-log file the importance mode writes to, or ``None``.

    Single concern: turn the two parsed knobs into the concrete path both
    the Rust full sink and the Python redirect must agree on. When
    importance mode is off, there is no submitter full-log file (``None``).
    When on, honour an explicit ``--full-log-file`` else the cwd default.

    The per-node ``--full-log-dir`` split is a separate sink (it takes
    precedence on the Rust side and needs no Python-side redirect, since a
    container's stdout is captured wholesale by the wrapper), so it is not
    considered here — this is purely the submitter's single-file path.
    """
    if not important_stdio_only:
        return None
    if full_log_file and full_log_file.strip():
        return Path(full_log_file)
    return Path(DEFAULT_FULL_LOG_FILE)


def _redirect_python_logs_to_full_log(full_log_file: Path) -> None:
    """Importance-mode Python-side handler reconfiguration: drop the root
    console StreamHandler(s) so Python chatter does not reach stdio, and
    append a FileHandler to ``full_log_file`` so Python's full record is
    preserved alongside the Rust full sink.

    Reuses the shared `remove_stream_handlers` helper (no bespoke handler
    walk) and the same level/format `setup_logging` just installed on the
    root logger.
    """
    root = logging.getLogger()
    file_handler = logging.FileHandler(full_log_file, mode="a")
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


def setup_logging(args: argparse.Namespace) -> logging.Logger:
    """Configure logging from the PARSED framework args.

    Single explicit-parameter path (no env vars, no argv lookahead):

      * Installs the native tracing subscriber via ``init_logging(...)``
        with the three parsed knobs — this is the deferred init the
        `_native` import no longer performs.
      * Configures the Python root logger's level / prefix from
        ``--debug`` / ``--raw-logs`` and the role flags (``--secondary``,
        ``--multi-computer``, ``--slurm``).
      * When importance mode is on, drops the Python console handler and
        routes Python's logs to the full-log file so stdio carries only the
        Rust-emitted important events.

    The role flags choose a prefix: ``S|`` for a secondary, ``P|`` for a
    distributed primary, none for plain local mode.
    """
    important_stdio_only = bool(getattr(args, "important_stdio_only", False))
    full_log_file = getattr(args, "full_log_file", None)
    full_log_dir = getattr(args, "full_log_dir", None)
    debug = bool(getattr(args, "debug", False))

    # Deferred native subscriber install — explicit params, after argparse.
    # Local import: the native function lives on the package's re-exported
    # surface; importing at module top would pull `_native` into modules
    # (like the test harness) that import `logging_setup` in isolation.
    from . import init_logging

    # `--debug` raises BOTH the Python root logger (below) AND the Rust
    # subscriber's verbosity ceiling: without the param the per-role/full
    # sinks stay INFO-only, so a `--debug` secondary's `secondary.log`
    # carried no DEBUG lines. Forwarded verbatim to secondaries (it is
    # neither framework-regenerated nor submitter-local), so this same path
    # raises the secondary's sink too.
    init_logging(
        important_stdio_only=important_stdio_only,
        full_log_file=full_log_file,
        full_log_dir=full_log_dir,
        debug=debug,
    )

    if getattr(args, "secondary", None):
        prefix = "S|"
    elif getattr(args, "multi_computer", None) or getattr(args, "slurm", False):
        prefix = "P|"
    else:
        prefix = ""

    log_level = logging.DEBUG if debug else logging.INFO
    logger = logging.getLogger()
    logger.setLevel(log_level)

    if getattr(args, "raw_logs", False):
        log_format = f"{prefix}%(message)s"
        logging.basicConfig(level=log_level, format=log_format)
    else:
        if prefix:
            log_format = f"%(levelname)s | %(asctime)s |{prefix}| %(message)s"
        else:
            log_format = "%(levelname)s | %(asctime)s | %(message)s"
        logging.basicConfig(level=log_level, format=log_format, datefmt="%H:%M:%S")

    resolved = resolve_full_log_file(important_stdio_only, full_log_file)
    if resolved is not None:
        _redirect_python_logs_to_full_log(resolved)

    return logger
