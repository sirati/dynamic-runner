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

``--important-stdio-only`` gates the OPERATOR's stdio, a classification
that follows the file descriptors, not the role. It is stripped from the
generic forward set (see :mod:`dynamic_runner._framework_flags`) because a
SLURM secondary's stdio is a per-node sbatch capture, not the operator's
terminal — there the secondary keeps its full logs for debugging, and
post-relocation the operator's narrative comes from the observer reading
the CRDT. A ``--multi-computer local`` secondary, however, spawns with
INHERITED stdio: its stdout IS the operator's terminal, so the local spawn
path re-emits the flag explicitly via :func:`stdio_mode_argv` and the
secondary installs the same gate through its own :func:`setup_logging`.
"""

from __future__ import annotations

import argparse
import contextlib
import logging
import sys
import traceback
from pathlib import Path

from ._shared.logging_utils import remove_stream_handlers

#: CLI flag that activates importance-stdio mode. Owned here so the
#: classification in `_framework_flags.SUBMITTER_LOCAL_FLAGS` and any other
#: reference share one literal.
IMPORTANT_STDIO_ONLY_FLAG = "--important-stdio-only"

#: Default destination for the full (unfiltered) log under importance mode
#: when the operator passed neither ``--full-log-file`` nor a per-node
#: ``--full-log-dir`` (and the submitter's per-setup ``<log-dir>/setup`` split
#: did not resolve). Relative on purpose: it lands wherever the run was
#: launched, matching the historical shell-redirection capture point and the
#: ``--full-log-file`` help text. Fed to the NATIVE full sink (the Rust
#: ``init_logging`` ``full_log_file`` knob) — NOT a Python-side FileHandler —
#: so BOTH framework (Rust-emitted) events and bridged Python records land in
#: it. Without this default the importance-mode full sink fell back to
#: ``FullSink::Stdout`` (gated to the important target), so a fatal dispatch
#: error logged via the Python bridge reached no durable sink at all.
DEFAULT_FULL_LOG_FILE = "dynrunner-full.log"

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


def resolve_full_log_file(
    important_stdio_only: bool,
    full_log_file: str | None,
    full_log_dir: str | None,
) -> str | None:
    """The NATIVE full-sink single-file path, or ``None``.

    Single concern: pick the path the Rust full sink's :class:`FullSink::File`
    writes to. An explicit ``--full-log-file`` always wins. Otherwise, when
    importance mode is on AND no per-node ``--full-log-dir`` sink was resolved
    (the dir sink takes precedence in Rust and is the durable per-role record),
    default to :data:`DEFAULT_FULL_LOG_FILE` so the full (unfiltered) record —
    framework events AND bridged Python records, including a FATAL dispatch
    error — is still captured to a file rather than the importance-gated
    stdout. With importance mode off, stdout already carries everything, so
    there is no implicit file (``None`` → ``FullSink::Stdout``, today's
    single-stream behaviour). The ``--full-log-dir`` path is never overridden
    here: it is its own (higher-precedence) sink.
    """
    if full_log_file and full_log_file.strip():
        return full_log_file
    if not important_stdio_only:
        return None
    if full_log_dir and full_log_dir.strip():
        return None
    return DEFAULT_FULL_LOG_FILE


def stdio_mode_argv(args: argparse.Namespace) -> list[str]:
    """Argv tokens that reproduce THIS process's operator-stdio mode in a
    child that INHERITS its stdio.

    Single concern: own the answer to "which flags must a stdio-inheriting
    child re-receive so the operator-facing stdio contract holds across the
    process boundary?". ``--important-stdio-only`` gates what reaches the
    OPERATOR's stdio — a property of the file descriptors, not of the role.
    A SLURM secondary's stdio is a per-node sbatch capture, so the flag must
    NOT reach it (it keeps its full log); a ``--multi-computer local``
    secondary spawns with inherited stdio — its stdout IS the operator's
    terminal — so the gate must ride its argv or the secondary's full INFO
    stream floods the importance-gated view (the consumer-reported local-mode
    firehose, 2026-06-10). The child then installs the gate through the SAME
    :func:`setup_logging`/``init_logging`` seam every mode uses; nothing
    mode-specific is copied.

    Callers (the local-subprocess argv assembler,
    :mod:`dynamic_runner.spawn_secondary`) append the returned tokens
    verbatim and know nothing about flag names or modes. Spawn paths whose
    children do NOT inherit the operator's stdio (the SLURM wrapper) never
    call this.
    """
    if getattr(args, "important_stdio_only", False):
        return [IMPORTANT_STDIO_ONLY_FLAG]
    return []


@contextlib.contextmanager
def surface_fatal_errors():
    """Guarantee a FATAL error reaching this boundary surfaces + is flushed.

    Single concern: a fatal/uncaught error (the process is about to exit
    non-zero) MUST always be diagnosable, REGARDLESS of
    ``--important-stdio-only``. The importance-mode stdio sink admits ONLY the
    Rust IMPORTANT target, so a fatal error logged through the ordinary
    Python→tracing bridge (``BRIDGE_TARGET``) is dropped from stdout. This
    context manager closes that gap WITHOUT weakening the normal importance
    filter for non-fatal logs: on an exception it routes the error+traceback
    through the dedicated native ``py_log_important`` primitive (which emits at
    the IMPORTANT target, so the importance gate admits it to stdio and the
    full sink keeps it as always), FLUSHES every logging handler plus
    stdout/stderr so nothing is lost on the crash-exit, then RE-RAISES so the
    caller's exit code / handling is unchanged.

    Composed once around the body of :func:`dynamic_runner.run.dispatch` — the
    single chokepoint both framework entry points (``run`` / ``cli_main``) go
    through after logging is configured — so no dispatch call site needs to
    know anything about the importance gate.
    """
    try:
        yield
    except BaseException as exc:  # noqa: BLE001 — surface ANY fatal, then re-raise
        # Local import: the native primitive lives on the package's re-exported
        # surface (same rationale as `init_logging` / `py_log`) — importing at
        # module top would pull `_native` into modules that import this one in
        # isolation (the test harness).
        from . import py_log_important

        message = "FATAL: uncaught error reached the dispatch boundary:\n" + "".join(
            traceback.format_exception(type(exc), exc, exc.__traceback__)
        )
        # Best-effort: surfacing must never mask the original error with a
        # secondary failure in the surfacing path itself.
        with contextlib.suppress(Exception):
            py_log_important(message)
        _flush_all_logging()
        raise


def _flush_all_logging() -> None:
    """Flush every root-logger handler plus stdout/stderr, and the native
    importance-stdio debounce buffer.

    Single concern: make the fatal surfacing durable before a crash-exit. The
    native full sink writes synchronously, but the Python console handler
    (importance mode off) and the process stdio streams are buffered, so flush
    them so a fatal error is on the wire regardless of how the process exits
    next. Under ``--important-stdio-only`` the native operator-stdio sink
    additionally coalesces bursts behind a debounce buffer, so flush THAT too —
    the just-emitted :func:`py_log_important` line would otherwise wait for the
    500ms-quiet / 5s-max-delay timer (or the atexit backstop). The native flush
    is a no-op off importance mode, so it is called unconditionally.
    """
    # Local import: the native primitive lives on the package's re-exported
    # surface (same rationale as `py_log_important`) — importing at module top
    # would pull `_native` into modules that import this one in isolation.
    from . import flush_important_stdio

    for handler in logging.getLogger().handlers:
        with contextlib.suppress(Exception):
            handler.flush()
    for stream in (sys.stdout, sys.stderr):
        with contextlib.suppress(Exception):
            stream.flush()
    with contextlib.suppress(Exception):
        flush_important_stdio()


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
    # The submitter defaults to a per-setup `<log-dir>/setup` role-split dir
    # when no explicit `--full-log-dir` was passed, so its bootstrap-primary
    # (`primary.log`) and post-relocation observer (`observer.log`) actions
    # are captured for debugging — independent of `--important-stdio-only`.
    full_log_dir = resolve_full_log_dir(args, getattr(args, "full_log_dir", None))
    # When importance mode is on and no `--full-log-file`/`--full-log-dir` sink
    # was chosen, default the NATIVE full sink to `./dynrunner-full.log` so the
    # full (unfiltered) record — and crucially a FATAL dispatch error — is
    # still captured to a file, never lost to the importance-gated stdout. Off
    # importance mode this stays whatever `--full-log-file` was (default None →
    # stdout single-stream).
    full_log_file = resolve_full_log_file(
        important_stdio_only, getattr(args, "full_log_file", None), full_log_dir
    )
    debug = bool(getattr(args, "debug", False))

    # Deferred native subscriber install — explicit params, after argparse.
    # Local import: the native function lives on the package's re-exported
    # surface; importing at module top would pull `_native` into modules
    # (like the test harness) that import `logging_setup` in isolation.
    from . import init_logging

    # `--debug` raises BOTH the Python root logger (below) AND the Rust
    # subscriber's STDIO verbosity ceiling. The per-role file sinks
    # (`primary.log` / `secondary.log` / `observer.log`) are forensic-
    # complete at TRACE regardless of this flag — every event a peer emits
    # is on the durable record. `--debug` only widens the operator-facing
    # stdio stream from INFO to DEBUG. Forwarded verbatim to secondaries
    # (it is neither framework-regenerated nor submitter-local), so this
    # same path raises the secondary's stdio sink too.
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
