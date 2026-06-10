"""Per-process Python crash/frame-dump diagnostics for a mesh-launched secondary.

==============================================================================
Single concern: durable process-crash diagnostics — ``faulthandler`` frame
dumps (fatal signals + on-demand ``SIGUSR1``) and the last-gasp traceback of
a clean Python exception escaping the bootstrap — written under the per-node
``--full-log-dir``.
==============================================================================

This is the Python half of the livelock instrumentation. The Rust
runtime-starvation watchdog (``dynrunner_manager_distributed::runtime_watchdog``)
runs on its OWN OS thread (it survives the runtime freeze) and, when it detects
the single ``current_thread`` tokio runtime has stopped making progress, raises
``SIGUSR1`` against this process. ``faulthandler`` — registered here at
bootstrap — catches that signal and writes every thread's Python stack to the
dump target, so the NEXT occurrence of the production livelock NAMES the wedged
loop automatically, with no operator and no ptrace.

Why ``faulthandler`` and not a logging call: ``faulthandler`` dumps via
``write(2)`` from inside the signal handler (async-signal-safe), so it works
even while the MAIN thread is pegged at 100% CPU under the GIL — exactly the
production state. A normal Python log call could not run in that state.

Why this is its OWN module (not inline in ``_secondary_bootstrap``): the
bootstrap shim's single concern is argv-orchestration + ``runpy`` ("NO RUNTIME
LOGIC HERE"). Process-diagnostics (crash/signal frame dumps) is a DISTINCT
concern, so it lives here; the shim calls :func:`enable_fault_dumps` exactly
once and otherwise knows nothing about it.

Module boundary
---------------
* IN  — the process command line (``sys.argv``) ONLY for the optional
  ``--full-log-dir`` value, read by a tiny single-purpose scan (this module
  does NOT run the framework's argparse — that stays the consumer's concern).
* OUT — a registered ``faulthandler`` (fatal-signal + ``SIGUSR1``), writing to
  ``<full-log-dir>/faulthandler.log`` when a per-node log dir is present, else
  ``sys.stderr`` (captured by conmon, so it survives either way); and, via
  :func:`write_crash_traceback`, ``<full-log-dir>/bootstrap-crash.log`` for a
  clean Python exception escaping the bootstrap shim. Idempotent and
  best-effort: any failure is swallowed so diagnostics-wiring can never break
  the secondary's cold start.
"""

from __future__ import annotations

import datetime
import faulthandler
import signal
import sys
import traceback
from pathlib import Path
from typing import IO

#: Basename of the on-demand dump file under the per-node ``--full-log-dir``.
#: Separate from the busy full.log so a frame dump is not interleaved with
#: (and lost in) the ordinary log stream.
_DUMP_BASENAME = "faulthandler.log"

#: Basename of the last-gasp crash-traceback file under the per-node
#: ``--full-log-dir``. Separate from ``faulthandler.log`` (which only ever
#: carries signal/fault frame dumps): a CLEAN Python exception escaping the
#: bootstrap is a different failure shape, and the production fire-drill
#: (run_20260610_130030) showed that when it lands only on container stderr
#: it drowns in podman debug noise while every per-node log file stays empty.
_CRASH_BASENAME = "bootstrap-crash.log"

#: Module-level reference to the open dump file. ``faulthandler.register`` /
#: ``faulthandler.enable`` retain the underlying fd, so the object MUST stay
#: alive for the process lifetime; holding it here keeps it from being closed
#: by GC. ``None`` when dumping to ``sys.stderr`` (which we never own/close).
_DUMP_FILE: IO[str] | None = None


def _full_log_dir_from_argv(argv: list[str]) -> str | None:
    """Extract ``--full-log-dir`` from ``argv`` without running argparse.

    Single purpose: find the one flag this module's dump target needs.
    Accepts both the ``--full-log-dir <v>`` and ``--full-log-dir=<v>`` forms.
    Returns ``None`` when absent or empty.
    """
    i = 0
    n = len(argv)
    while i < n:
        token = argv[i]
        if token == "--full-log-dir":
            if i + 1 < n and argv[i + 1].strip():
                return argv[i + 1]
            return None
        if token.startswith("--full-log-dir="):
            value = token[len("--full-log-dir=") :]
            return value if value.strip() else None
        i += 1
    return None


def _resolve_dump_target(argv: list[str]) -> IO[str]:
    """Pick the durable frame-dump file object.

    ``<full-log-dir>/faulthandler.log`` when a per-node log dir is present and
    creatable, else ``sys.stderr``. ``stderr`` is fd 2, captured by conmon, so
    it survives the freeze too — it is the always-available fallback.
    """
    full_log_dir = _full_log_dir_from_argv(argv)
    if full_log_dir:
        try:
            dir_path = Path(full_log_dir)
            dir_path.mkdir(parents=True, exist_ok=True)
            # Line-buffered append so concurrent dumps don't truncate prior
            # ones and each dump is flushed promptly.
            handle = (dir_path / _DUMP_BASENAME).open("a", buffering=1)
            global _DUMP_FILE
            _DUMP_FILE = handle
            return handle
        except OSError:
            # Any filesystem trouble (mount not ready, perms): fall back to
            # stderr rather than break cold start.
            pass
    return sys.stderr


def enable_fault_dumps(argv: list[str] | None = None) -> None:
    """Install ``faulthandler`` for this process. Call once, at bootstrap.

    Registers:
      * ``faulthandler.enable(file=...)`` — dumps all threads' stacks on a
        fatal signal (SIGSEGV / SIGABRT / SIGFPE / SIGBUS / SIGILL).
      * ``faulthandler.register(SIGUSR1, all_threads=True, chain=False)`` — an
        on-demand all-thread dump fired by the Rust runtime-starvation watchdog
        (or an operator ``kill -USR1``). ``chain=False`` is REQUIRED: SIGUSR1
        has no prior handler in this process, so chaining would fall through to
        the default disposition (terminate). The watchdog's contract is
        detection + dump with NO process exit, so the dump must NOT kill the
        process — ``chain=False`` returns control to the interrupted code after
        writing the traceback, leaving the process running.

    ``argv`` defaults to ``sys.argv[1:]`` (the live command line). Best-effort:
    if ``faulthandler`` cannot be wired (e.g. a redirected ``sys.stderr`` with
    no real fd, or the platform lacks ``SIGUSR1``), the exception is swallowed
    so cold start is never blocked.
    """
    raw = list(sys.argv[1:] if argv is None else argv)
    try:
        target = _resolve_dump_target(raw)
        # Fatal-signal dumps to the durable target.
        faulthandler.enable(file=target, all_threads=True)
        # On-demand all-thread dump. ``SIGUSR1`` is not used by CPython
        # internally; the wrapper/operator and the runtime watchdog both
        # deliver it. Guard on platform support (Windows lacks SIGUSR1).
        sigusr1 = getattr(signal, "SIGUSR1", None)
        if sigusr1 is not None:
            faulthandler.register(sigusr1, file=target, all_threads=True, chain=False)
    except (OSError, RuntimeError, ValueError):
        # Diagnostics wiring must never break the secondary's cold start.
        pass


def write_crash_traceback(argv: list[str] | None = None) -> None:
    """Last-gasp crash visibility: durably record the in-flight exception.

    Called from an ``except`` block (the bootstrap shim's re-raise handler).
    Appends the full traceback of the CURRENT exception to
    ``<full-log-dir>/bootstrap-crash.log`` — the same per-node directory
    (and the same argv-based resolution) the ``faulthandler`` dump target
    uses, which provably exists before any crash this guards against.

    Why this exists: a Python exception that escapes the bootstrap before
    (or outside) the Rust tracing subscriber's file sinks otherwise prints
    ONLY to container stderr, where it is buried in podman debug noise
    while ``secondary.log``/the full log stay 0 bytes — the operator sees a
    "totally silent" dead secondary (production fire-drill,
    run_20260610_130030).

    Strictly best-effort and side-effect-only:
      * never raises (any failure here would mask the original error);
      * never swallows — the caller re-raises, so the process exit code and
        the stderr traceback are unchanged;
      * a no-op when there is no current exception, when the exception is a
        clean ``SystemExit`` (code 0/None — a normal exit, not a crash), or
        when the argv carries no usable ``--full-log-dir`` (the traceback
        already reaches stderr via the caller's re-raise; duplicating it
        there would only add noise).
    """
    try:
        exc = sys.exception()
        if exc is None:
            return
        if isinstance(exc, SystemExit) and exc.code in (0, None):
            return
        raw = list(sys.argv[1:] if argv is None else argv)
        full_log_dir = _full_log_dir_from_argv(raw)
        if not full_log_dir:
            return
        dir_path = Path(full_log_dir)
        dir_path.mkdir(parents=True, exist_ok=True)
        # Append so respawn cycles accumulate instead of clobbering the
        # previous crash; one timestamped header per entry.
        with (dir_path / _CRASH_BASENAME).open("a") as handle:
            stamp = datetime.datetime.now(datetime.timezone.utc).isoformat()
            handle.write(f"==== bootstrap crash at {stamp} ====\n")
            traceback.print_exception(exc, file=handle)
            handle.flush()
    except BaseException:
        # Best-effort by contract: the handler must never mask the
        # original error with one of its own.
        pass
