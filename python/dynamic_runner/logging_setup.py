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
  * The Python side BRIDGES every consumer/framework Python log record into
    Rust tracing (the :class:`_TracingBridgeHandler` installed for ALL roles),
    so a consumer hook's logging (``on_phase_end``, ``discover_items``) lands
    in the SAME per-role full-log file (``primary.log`` / ``secondary.log`` /
    ``observer.log``) as the Rust framework events — routed by the run
    future's role span, NOT by anything the Python side knows. This gives the
    relocated primary / observer a durable per-role Python sink it previously
    lacked on SLURM (a non-submitter role had no Python file sink at all).
    In importance mode the Python console handler is additionally dropped so
    stdio carries only the Rust-emitted important events.

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

#: Per-node subdirectory the SUBMITTER's full role-split logs land under,
#: anchored on ``--log-dir`` — the gateway-shared mount compute nodes use
#: for ``--full-log-dir=<log-dir>/{secondary_id}``. "setup" mirrors the
#: submitter's ``SETUP_NODE_ID`` so its bootstrap-primary log
#: (``<log-dir>/setup/primary.log``) and post-relocation observer log
#: (``<log-dir>/setup/observer.log``) sit beside the compute nodes' dirs.
SETUP_FULL_LOG_SUBDIR = "setup"


def resolve_full_log_dir(
    args: argparse.Namespace, full_log_dir: str | None
) -> str | None:
    """The per-node role-split full-log directory, or ``None``.

    Single concern: pick the directory the native PerNodeDir sink splits
    ``primary.log`` / ``secondary.log`` / ``observer.log`` under.

    An explicit ``--full-log-dir`` always wins (compute nodes are launched
    with ``--full-log-dir=<log-dir>/{secondary_id}`` by the SLURM spawn
    paths). When unset, the SUBMITTER (a distributed / SLURM primary — NOT a
    ``--secondary``) defaults to ``<log-dir>/setup`` so its bootstrap-primary
    actions land in ``setup/primary.log`` and, after it relocates its primary
    role to a compute peer, its observer actions land in
    ``setup/observer.log`` — keeping the relocated submitter debuggable. This
    full per-node record is INDEPENDENT of ``--important-stdio-only`` (that
    flag only gates the operator-facing stdio view). A plain local run and a
    ``--secondary`` leave this ``None`` (the secondary's dir is always
    forwarded explicitly; a local run keeps the single stdout stream).
    """
    if full_log_dir and full_log_dir.strip():
        return full_log_dir
    if getattr(args, "secondary", None):
        return None
    is_submitter = bool(
        getattr(args, "multi_computer", None) or getattr(args, "slurm", False)
    )
    if not is_submitter:
        return None
    log_dir = getattr(args, "log_dir", None)
    if not (log_dir and str(log_dir).strip()):
        return None
    return str(Path(log_dir) / SETUP_FULL_LOG_SUBDIR)


class _TracingBridgeHandler(logging.Handler):
    """Forward every Python log record into Rust tracing via ``py_log``.

    Single concern: be the Python end of the Python→tracing bridge. ``emit``
    hands the record's level name, logger name, and rendered message to the
    native ``py_log`` pyfunction, which emits a ``tracing::event!`` that
    inherits the run future's role span and so routes to the matching per-role
    full-log file (``primary.log`` / ``secondary.log`` / ``observer.log``).

    The handler knows NOTHING about roles, per-role files, or the importance
    gate: role attribution lives entirely in the Rust run loop's span, and the
    routing/exclusion lives in the Rust subscriber. This is the GENERAL
    per-role mechanism that replaces the submitter-only full-log FileHandler
    redirect — installed for every role, it gives a relocated primary /
    observer the durable per-role Python sink it lacked on SLURM.
    """

    def __init__(self, py_log) -> None:
        super().__init__()
        self._py_log = py_log

    def emit(self, record: logging.LogRecord) -> None:
        try:
            self._py_log(record.levelname, record.name, self.format(record))
        except Exception:
            self.handleError(record)


def _install_tracing_bridge() -> None:
    """Install the Python→tracing bridge handler on the root logger.

    Single concern: route Python's full record into the Rust per-role sinks
    for ALL roles (not just the submitter). The native ``py_log`` is imported
    lazily here (same reason as ``init_logging`` — keep ``_native`` out of
    modules that import this one in isolation). Idempotent: a re-entrant
    ``setup_logging`` (respawn / consumer calling ``cli_main`` then ``run``)
    must not stack duplicate bridges, so a pre-existing bridge is left alone.
    """
    root = logging.getLogger()
    if any(isinstance(h, _TracingBridgeHandler) for h in root.handlers):
        return
    from . import py_log

    bridge = _TracingBridgeHandler(py_log)
    bridge.setLevel(root.level)
    root.addHandler(bridge)


def setup_logging(args: argparse.Namespace) -> logging.Logger:
    """Configure logging from the PARSED framework args.

    Single explicit-parameter path (no env vars, no argv lookahead):

      * Installs the native tracing subscriber via ``init_logging(...)``
        with the three parsed knobs — this is the deferred init the
        `_native` import no longer performs.
      * Configures the Python root logger's level / prefix from
        ``--debug`` / ``--raw-logs`` and the role flags (``--secondary``,
        ``--multi-computer``, ``--slurm``).
      * Installs the Python→tracing bridge for ALL roles so consumer/framework
        Python records land in the matching per-role full-log file (routed by
        the run future's role span). The Rust stdio sink drops this bridged
        copy, so stdout is unchanged — Python chatter still reaches it via the
        console handler.
      * When importance mode is on, ADDITIONALLY drops the Python console
        handler so stdio carries only the Rust-emitted important events; the
        bridge keeps Python's full record in the per-role/full file sink.

    The role flags choose a prefix: ``S|`` for a secondary, ``P|`` for a
    distributed primary, none for plain local mode.
    """
    important_stdio_only = bool(getattr(args, "important_stdio_only", False))
    full_log_file = getattr(args, "full_log_file", None)
    # The submitter defaults to a per-setup `<log-dir>/setup` role-split dir
    # when no explicit `--full-log-dir` was passed, so its bootstrap-primary
    # (`primary.log`) and post-relocation observer (`observer.log`) actions
    # are captured for debugging — independent of `--important-stdio-only`.
    full_log_dir = resolve_full_log_dir(args, getattr(args, "full_log_dir", None))
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

    # Bridge Python's records into Rust tracing for EVERY role: the general
    # per-role mechanism that replaces the submitter-only full-log redirect, so
    # a relocated primary / observer's `on_phase_end` / `discover_items` Python
    # logging lands in `primary.log` / `observer.log` / `secondary.log`.
    _install_tracing_bridge()

    # Importance mode is operator-stdout-only: drop the Python console handler
    # so Python chatter does not reach the gated stdio. The bridge (installed
    # above) keeps Python's full record in the per-role/full file sink, so
    # nothing is lost — the Rust full sink and the per-role files carry it.
    if important_stdio_only:
        remove_stream_handlers(logger)

    return logger
