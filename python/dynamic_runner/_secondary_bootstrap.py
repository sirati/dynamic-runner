"""Cold-start bootstrap shim for a mesh-launched secondary.

==============================================================================
NO RUNTIME LOGIC HERE — this file orchestrates argv + runpy, nothing more.
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
import runpy
import sys


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

    parser = _build_bootstrap_parser()
    args, _passthrough = parser.parse_known_args(raw)

    sys.argv = ["__main__", *_strip_secondary_module(raw)]

    # Run the consumer module exactly as `python -m <module>` does today:
    # `alter_sys=True` installs the module as `__main__` and restores
    # `sys.argv[0]` to the module's file, so the consumer's
    # `if __name__ == "__main__":` / `cli_main(argv=sys.argv[1:])` path
    # parses the stripped argv identically to a full command-line launch.
    runpy.run_module(args.secondary_module, run_name="__main__", alter_sys=True)


if __name__ == "__main__":
    main()
