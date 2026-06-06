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
consumer module runs, fetch the cluster-wide ``forwarded_argv`` from the
bootstrap primary over the mesh, splice it onto ``sys.argv``, and then run
the consumer module EXACTLY as ``python -m <module>`` does today.

Why this exists
---------------
The SLURM/podman wrapper used to bake the consumer's task-filter flags
into every secondary's container command (the ``forwarded_argv`` block in
``build_run_argv``). Per #238 those flags no longer ride the launch
command line — the wrapper launches the secondary as a *bare* shim
(``python -m dynamic_runner._secondary_bootstrap`` + a minimal bootstrap
argv) and the shim pulls the run-config over the mesh instead. This file
is the Python half of that cold-start fetch; the fetch RPC itself lives in
Rust (the ``_native.fetch_run_config`` driver, which dials the primary,
sends an UNWELCOMED ``RequestRunConfig``, and returns the ``forwarded_argv``).
Python stays a thin bridge: it owns ONLY the argv reconstruction and the
``runpy`` hand-off.

Module boundary
---------------
* IN  — the process command line (``sys.argv``): the bootstrap argv the
  wrapper injected — ``--secondary-module <m>`` plus the
  framework-regenerated flags (``--secondary``/``--secondary-id``/
  ``--cores``/``--max-memory``/``--src-network``/``--log-dir``/
  ``--full-log-dir``/…), the binary-injected ``--panik-file <path>``, and an
  optional ``--mem-manager-reserved=…``. Unknown flags are NOT interpreted;
  they pass through verbatim.
* OUT — ``_native.fetch_run_config(primary_url, secondary_id,
  distributed_config) -> list[str]`` (the Rust fetch driver) and
  ``runpy.run_module(module, run_name="__main__", alter_sys=True)``.
* The consumer module sees a ``sys.argv`` BYTE-IDENTICAL to a full
  command-line launch (the fetched forwarded args appended after the
  minimal-on-CLI framework flags, in their original CLI order), so its own
  ``cli_main`` / ``run(argv=sys.argv[1:])`` parses exactly as before.

The shim parses ONLY what it needs to dial the mesh
(``--secondary``/``--secondary-id`` → primary URL + return address;
``--unconfigured-deadline-secs``/``--disable-peer-overlay`` → the fetch
budget + overlay) and which module to run (``--secondary-module``). It uses
``parse_known_args`` so every other flag is left untouched for the
consumer's own argparse.
"""

from __future__ import annotations

import argparse
import runpy
import sys


def _build_bootstrap_parser() -> argparse.ArgumentParser:
    """Parser for the minimal bootstrap argv the shim itself consumes.

    Deliberately tiny: the shim only needs the mesh-connect coordinates
    (``--secondary`` URL + ``--secondary-id`` return address), the two
    knobs that shape the fetch budget/overlay
    (``--unconfigured-deadline-secs`` / ``--disable-peer-overlay``), and
    the consumer module name (``--secondary-module``). Every other flag in
    the bootstrap argv (``--cores``, ``--panik-file``,
    ``--mem-manager-reserved=…``, …) belongs to the consumer and is left to
    its argparse via ``parse_known_args`` — this parser must NOT choke on
    them.

    ``add_help=False`` so a stray ``--help`` in the bootstrap argv is
    passed through to the consumer rather than swallowed here.
    """
    parser = argparse.ArgumentParser(add_help=False)
    parser.add_argument("--secondary-module", type=str, required=True)
    parser.add_argument("--secondary", type=str, default=None)
    parser.add_argument("--secondary-id", type=str, default=None)
    parser.add_argument("--unconfigured-deadline-secs", type=float, default=None)
    parser.add_argument("--disable-peer-overlay", action="store_true")
    return parser


def _reconstruct_consumer_argv(
    bootstrap_argv: list[str], forwarded_argv: list[str]
) -> list[str]:
    """Build the consumer's ``sys.argv[1:]`` from the bootstrap argv + the
    mesh-fetched ``forwarded_argv``.

    The result is byte-identical to a full command-line launch: the
    minimal-on-CLI framework flags (the bootstrap argv with ONLY the
    shim-private ``--secondary-module <m>`` pair removed, order otherwise
    preserved) followed by the fetched ``forwarded_argv`` — mirroring the
    pre-#238 order where the wrapper emitted the framework-regenerated
    flags first and appended ``forwarded_argv`` last.

    Only the ``--secondary-module`` pair is stripped; every other bootstrap
    token (including unknown flags) passes through verbatim, so this never
    needs the framework's flag taxonomy.
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
    return kept + list(forwarded_argv)


def _build_fetch_distributed_config(args: argparse.Namespace):
    """Construct the ``DistributedConfig`` the fetch driver reads for its
    dial budget + overlay selection, or ``None`` to let the Rust default
    (600s unconfigured-deadline, peer overlay on) hold.

    Mirrors ``run._build_distributed_config`` in shape (build only when a
    knob deviates, else ``None``) but is scoped to the TWO fields the fetch
    actually consumes — the deadline (dial/fetch budget) and the overlay
    toggle. Built inline rather than reusing ``run._build_distributed_config``
    so the bootstrap path does not pull ``run.py``'s whole import graph, and
    so the shim's distributed-config concern stays self-contained.
    """
    from ._native import DistributedConfig

    kwargs: dict[str, object] = {}
    if args.unconfigured_deadline_secs is not None:
        kwargs["unconfigured_deadline_secs"] = args.unconfigured_deadline_secs
    if args.disable_peer_overlay:
        kwargs["disable_peer_overlay"] = True
    if not kwargs:
        return None
    return DistributedConfig(**kwargs)


def main(bootstrap_argv: list[str] | None = None) -> None:
    """Cold-start the secondary: fetch ``forwarded_argv`` from the mesh,
    splice ``sys.argv``, then ``runpy`` the consumer module as ``__main__``.

    ``bootstrap_argv`` defaults to ``sys.argv[1:]`` — reading the process
    command line is legitimate here because this IS the program's entry
    point (the wrapper launched ``python -m dynamic_runner._secondary_bootstrap``).
    A test passes an explicit slice.

    Failure contract: a never-joining secondary fails LOUD. If the fetch
    raises (dial exhausted / no reply within the unconfigured-deadline) the
    shim exits non-zero with a setup-deadline-style diagnostic — vs today's
    infallible baked-in argv, the mesh fetch can genuinely fail and a silent
    empty run would strand the operator.
    """
    from ._native import fetch_run_config

    raw = list(sys.argv[1:] if bootstrap_argv is None else bootstrap_argv)

    parser = _build_bootstrap_parser()
    args, _passthrough = parser.parse_known_args(raw)

    if not args.secondary:
        raise SystemExit(
            "dynamic_runner._secondary_bootstrap: missing --secondary <url> "
            "in the bootstrap argv; cannot dial the primary to fetch the "
            "run-config. The mesh-launch wrapper must inject the primary URL."
        )
    if not args.secondary_id:
        raise SystemExit(
            "dynamic_runner._secondary_bootstrap: missing --secondary-id in "
            "the bootstrap argv; the run-config fetch needs it as the unicast "
            "return address the primary's reply routes back to."
        )

    distributed_config = _build_fetch_distributed_config(args)

    try:
        forwarded_argv = fetch_run_config(
            args.secondary,
            args.secondary_id,
            distributed_config,
        )
    except Exception as exc:  # noqa: BLE001 — turn ANY fetch failure into a loud exit
        # A secondary that never reaches the primary within the
        # unconfigured-deadline gives up here rather than launching the
        # consumer with no forwarded args (which would silently mis-run).
        # Respawn-eligible: the non-zero exit lets the SLURM/podman wrapper
        # reap and the respawn pipeline bring up a replacement.
        raise SystemExit(
            "dynamic_runner._secondary_bootstrap: failed to fetch the "
            f"run-config from primary {args.secondary!r} for secondary "
            f"{args.secondary_id!r} within the setup deadline: {exc}. "
            "The secondary never joined the mesh; exiting non-zero "
            "(respawn-eligible)."
        ) from exc

    sys.argv = ["__main__", *_reconstruct_consumer_argv(raw, list(forwarded_argv))]

    # Run the consumer module exactly as `python -m <module>` does today:
    # `alter_sys=True` installs the module as `__main__` and restores
    # `sys.argv[0]` to the module's file, so the consumer's
    # `if __name__ == "__main__":` / `cli_main(argv=sys.argv[1:])` path
    # parses the spliced argv identically to a full command-line launch.
    runpy.run_module(args.secondary_module, run_name="__main__", alter_sys=True)


if __name__ == "__main__":
    main()
