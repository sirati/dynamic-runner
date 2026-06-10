"""Per-process Python frame-dump wiring for a mesh-launched secondary.

==============================================================================
Single concern: install ``faulthandler`` so EVERY thread's Python stack can be
dumped — on a fatal signal AND on demand via ``SIGUSR1`` — to a durable file.
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
  ``sys.stderr`` (captured by conmon, so it survives either way). Idempotent
  and best-effort: any failure is swallowed so diagnostics-wiring can never
  break the secondary's cold start.
"""

from __future__ import annotations

import faulthandler
import signal
import sys
from pathlib import Path
from typing import IO

#: Basename of the on-demand dump file under the per-node ``--full-log-dir``.
#: Separate from the busy full.log so a frame dump is not interleaved with
#: (and lost in) the ordinary log stream.
_DUMP_BASENAME = "faulthandler.log"

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
