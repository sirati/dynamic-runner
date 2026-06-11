"""Cold-start bootstrap shim for a mesh-launched secondary.

==============================================================================
NO RUNTIME LOGIC HERE — this file orchestrates argv + runpy, plus the one
duty only the process entry point can own: the ``__main__`` guard guarantees
the process TERMINATES with the contract exit status when an exception
crosses the boundary (flush-then-``os._exit``; see the guard's comment).
==============================================================================

==============================================================================
ENTRYPOINT CONTRACT — the image already prepends ``python -m``; DO NOT repeat
==============================================================================
The consumer container image's ENTRYPOINT is ``["python", "-m"]``. So
whatever the framework hands the container as its COMMAND is appended after
``python -m`` and run as a module. The framework therefore passes a BARE
module STRING (this shim's dotted name,
``dynamic_runner._secondary_bootstrap``) as the container command — NEVER a
``["python", "-m", ...]`` prefix of its own. Double-prepending would launch
``python -m python -m dynamic_runner._secondary_bootstrap`` and break the
cold start. The same rule holds one level down: this shim runs the consumer
module with ``runpy.run_module`` (in THIS interpreter), NOT by re-spawning
``python -m <module>`` — the ``python -m`` was already supplied by the image
entrypoint that launched US. (Wire side of the same contract:
``crates/dynrunner-slurm/.../wrapper_script/config.rs::container_command`` and
``slurm-wrapper/wrapper/src/podman_run.rs``.)

Single concern: before a freshly-spawned (or respawned) secondary's
consumer module runs, strip the shim-private ``--secondary-module`` flag
off ``sys.argv`` and run the named consumer module EXACTLY as
``python -m <module>`` does today.

Module boundary
---------------
* IN  — the process command line (``sys.argv``): the bootstrap argv the
  wrapper injected — ``--secondary-module <m>`` plus the
  framework-regenerated flags (``--secondary``/``--secondary-id``/
  ``--cores``/``--max-memory``/``--src-network``/``--log-dir``/
  ``--full-log-dir``/…), the binary-injected ``--panik-file <path>``, and an
  optional ``--mem-manager-reserved=…``. Unknown flags are NOT interpreted;
  they pass through verbatim.
* OUT — ``runpy.run_module(module, run_name="__main__", alter_sys=True)``.
* The consumer module sees a ``sys.argv`` equal to the bootstrap argv with
  ONLY the shim-private ``--secondary-module <m>`` pair removed (order
  otherwise preserved), so its own ``cli_main`` / ``run(argv=sys.argv[1:])``
  parses the boot flags directly.

The shim parses ONLY which module to run (``--secondary-module``) and uses
``parse_known_args`` so every other flag is left untouched for the
consumer's own argparse.
"""

from __future__ import annotations

import argparse
import os
import runpy
import sys
import traceback

from ._fault_dumps import enable_fault_dumps, write_crash_traceback


def _build_bootstrap_parser() -> argparse.ArgumentParser:
    """Parser for the minimal bootstrap argv the shim itself consumes.

    Deliberately tiny: the shim only needs the consumer module name
    (``--secondary-module``). Every other flag in the bootstrap argv
    (``--secondary``, ``--secondary-id``, ``--cores``, ``--panik-file``,
    ``--mem-manager-reserved=…``, …) belongs to the consumer and is left to
    its argparse via ``parse_known_args`` — this parser must NOT choke on
    them.

    ``add_help=False`` so a stray ``--help`` in the bootstrap argv is
    passed through to the consumer rather than swallowed here.

    ``allow_abbrev=False`` is REQUIRED: with only ``--secondary-module``
    defined, argparse's prefix-matching would otherwise bind the sibling
    boot flag ``--secondary`` (``--secondary <url>``) to ``--secondary-module``
    and swallow the URL as the module name. Disabling abbreviation keeps the
    shim's parse to the EXACT ``--secondary-module`` token; every other flag
    (``--secondary``/``--secondary-id``/…) falls to ``parse_known_args``'s
    passthrough untouched.
    """
    parser = argparse.ArgumentParser(add_help=False, allow_abbrev=False)
    parser.add_argument("--secondary-module", type=str, required=True)
    return parser


def _strip_secondary_module(bootstrap_argv: list[str]) -> list[str]:
    """Build the consumer's ``sys.argv[1:]`` from the bootstrap argv by
    dropping ONLY the shim-private ``--secondary-module <m>`` pair (order
    otherwise preserved).

    Every other bootstrap token (including unknown flags) passes through
    verbatim, so this never needs the framework's flag taxonomy.
    """
    kept: list[str] = []
    i = 0
    n = len(bootstrap_argv)
    while i < n:
        token = bootstrap_argv[i]
        # `--secondary-module=<m>` single-token form.
        if token.startswith("--secondary-module="):
            i += 1
            continue
        # `--secondary-module <m>` two-token form: drop the flag and its
        # value. Bounds-guarded so a trailing bare flag drops only itself.
        if token == "--secondary-module":
            i += 2 if i + 1 < n else 1
            continue
        kept.append(token)
        i += 1
    return kept


def main(bootstrap_argv: list[str] | None = None) -> None:
    """Cold-start the secondary: strip the shim-private
    ``--secondary-module`` off ``sys.argv``, then ``runpy`` the named
    consumer module as ``__main__``.

    ``bootstrap_argv`` defaults to ``sys.argv[1:]`` — reading the process
    command line is legitimate here because this IS the program's entry
    point (the wrapper launched ``python -m dynamic_runner._secondary_bootstrap``).
    A test passes an explicit slice.
    """
    raw = list(sys.argv[1:] if bootstrap_argv is None else bootstrap_argv)

    # Install per-process Python frame dumps (fatal-signal + on-demand
    # SIGUSR1) BEFORE the consumer module runs, so the runtime-starvation
    # watchdog's SIGUSR1 (or an operator `kill -USR1`) dumps every thread's
    # stack for the lifetime of this process. Self-contained, best-effort, no
    # behaviour change otherwise (see `_fault_dumps`). Uses the bootstrap argv
    # to resolve the durable dump target's `--full-log-dir`.
    enable_fault_dumps(raw)

    # Last-gasp crash visibility: any exception escaping the bootstrap (the
    # shim's own parse OR the consumer module run) is durably recorded to
    # `<full-log-dir>/bootstrap-crash.log` by the diagnostics module before
    # the bare re-raise — so the container's exit code and stderr traceback
    # are EXACTLY as without this handler, but the failure is no longer
    # visible only in container stderr (where the production fire-drill's
    # dial error drowned in podman debug noise while every per-node log
    # file stayed empty). The policy of what is crash-worthy (a deliberate
    # `SystemExit`/`KeyboardInterrupt` is not, whatever the code) lives
    # with the diagnostics concern in `_fault_dumps`, not here.
    try:
        parser = _build_bootstrap_parser()
        args, _passthrough = parser.parse_known_args(raw)

        sys.argv = ["__main__", *_strip_secondary_module(raw)]

        # Run the consumer module exactly as `python -m <module>` does today:
        # `alter_sys=True` installs the module as `__main__` and restores
        # `sys.argv[0]` to the module's file, so the consumer's
        # `if __name__ == "__main__":` / `cli_main(argv=sys.argv[1:])` path
        # parses the stripped argv identically to a full command-line launch.
        runpy.run_module(args.secondary_module, run_name="__main__", alter_sys=True)
    except BaseException:
        write_crash_traceback(raw)
        raise


def _process_exit_status(exc: BaseException) -> int:
    """Map the exception crossing the process boundary to the exit status
    the interpreter's own unhandled-exception contract would deliver.

    PURE (unit-testable): ``SystemExit`` is honoured exactly as CPython
    honours it — ``None`` → 0, an ``int`` → that code truncated to the
    low byte (C ``exit()`` semantics; ``os._exit`` requires the same
    range), any other payload (argparse's usage-message form) → 1.
    Everything else — a genuine crash, ``KeyboardInterrupt`` included —
    is CPython's unhandled-exception status 1.
    """
    if isinstance(exc, SystemExit):
        if exc.code is None:
            return 0
        if isinstance(exc.code, int):
            return exc.code & 0xFF
        return 1
    return 1


if __name__ == "__main__":
    # Process-boundary termination guarantee: this guard is the program's
    # entry point (the container runs `python -m` on this module), so it
    # OWNS the process's fate. An exception escaping `main` must reach the
    # SLURM wrapper as this process's exit status — production
    # (secondary-0/bootstrap-crash.log, 2026-06-11T11:51:23Z) showed a
    # connect-deadline RuntimeError whose crash log was written and whose
    # traceback printed, after which the process LINGERED at interpreter
    # shutdown instead of exiting non-zero (native-side teardown left by
    # the failed run; any consumer non-daemon thread wedges it the same
    # way). Normal interpreter finalization cannot be trusted to complete
    # here, so after the durable artifacts are written (`main`'s
    # `write_crash_traceback` opens/flushes/closes the crash log before
    # re-raising) we mirror the interpreter's stderr surface ourselves,
    # flush stdio, and TERMINATE via `os._exit` — the flush-then-_exit
    # pattern, uniform for EVERY `BaseException` (a deliberate
    # `SystemExit` keeps its code; nothing is special-cased on exception
    # type beyond the interpreter's own status contract). `main()` itself
    # keeps its raise contract for in-process callers (tests).
    try:
        main()
    except BaseException as exc:
        if isinstance(exc, SystemExit):
            # CPython prints a non-int SystemExit payload (argparse's
            # usage form) to stderr and nothing for int/None codes.
            if exc.code is not None and not isinstance(exc.code, int):
                print(exc.code, file=sys.stderr)
        else:
            traceback.print_exception(exc, file=sys.stderr)
        status = _process_exit_status(exc)
        try:
            sys.stdout.flush()
            sys.stderr.flush()
        except OSError:
            # A torn-down stdio pipe must not stop the exit itself.
            pass
        os._exit(status)
