"""Process-start banner for a mesh-launched secondary.

==============================================================================
Single concern: ONE stderr line announcing "this secondary process exists",
emitted at the bootstrap shim's entry — before ANY work that could hang.
==============================================================================

The owner spec (secondary startup observability): "secondaries should
immediately write a log entry upon start". At shim entry no logging
subsystem exists yet (the framework's ``setup_logging`` runs only after the
consumer module's argparse), so the banner goes to ``sys.stderr`` — conmon
captures the container's stderr, so the line survives even when the process
wedges before any per-node log file is created (the exact failure mode the
spec targets: a node that died mute between launch and its first framework
log line).

Why this is its OWN module (not inline in ``_secondary_bootstrap``): the
bootstrap shim's single concern is argv-orchestration + ``runpy`` ("NO
RUNTIME LOGIC HERE"); process-lifecycle observability is a DISTINCT concern,
so it lives here — the same split ``_fault_dumps`` (process-crash
diagnostics) already uses. The shim calls :func:`announce_secondary_start`
exactly once and otherwise knows nothing about it.

Module boundary
---------------
* IN  — the process command line (``sys.argv``) ONLY for the optional
  ``--secondary-id`` value, read by a tiny single-purpose scan (mirrors
  ``_fault_dumps._full_log_dir_from_argv``; this module never runs the
  framework's argparse).
* OUT — one flushed line on ``sys.stderr`` identifying the node: UTC
  timestamp, hostname, pid, ``SLURM_JOB_ID`` (when the environment carries
  one), the secondary id (when the boot argv carries one), and the wheel
  version (when package metadata is at hand). Best-effort and total: any
  failure is swallowed — the banner can never break the secondary's cold
  start.
"""

from __future__ import annotations

import datetime
import os
import socket
import sys


def _secondary_id_from_argv(argv: list[str]) -> str | None:
    """Extract ``--secondary-id`` from ``argv`` without running argparse.

    Single purpose: find the one identity flag the banner names. Accepts
    both the ``--secondary-id <v>`` and ``--secondary-id=<v>`` forms.
    Returns ``None`` when absent or empty.
    """
    i = 0
    n = len(argv)
    while i < n:
        token = argv[i]
        if token == "--secondary-id":
            if i + 1 < n and argv[i + 1].strip():
                return argv[i + 1]
            return None
        if token.startswith("--secondary-id="):
            value = token[len("--secondary-id=") :]
            return value if value.strip() else None
        i += 1
    return None


def _wheel_version() -> str | None:
    """The installed ``dynamic_runner`` distribution version, or ``None``
    when metadata is not at hand (source-tree runs, test stubs)."""
    try:
        from importlib import metadata

        return metadata.version("dynamic_runner")
    except Exception:
        return None


def build_banner(argv: list[str]) -> str:
    """Compose the one-line banner (pure; the emission lives in
    :func:`announce_secondary_start` so tests can pin the content without
    capturing streams)."""
    parts = [
        "dynamic_runner secondary process started",
        f"time={datetime.datetime.now(datetime.timezone.utc).isoformat()}",
        f"host={socket.gethostname()}",
        f"pid={os.getpid()}",
    ]
    slurm_job = os.environ.get("SLURM_JOB_ID")
    if slurm_job:
        parts.append(f"slurm_job_id={slurm_job}")
    secondary_id = _secondary_id_from_argv(argv)
    if secondary_id:
        parts.append(f"secondary_id={secondary_id}")
    version = _wheel_version()
    if version:
        parts.append(f"wheel_version={version}")
    return " ".join(parts)


def announce_secondary_start(argv: list[str]) -> None:
    """Write the start banner to ``sys.stderr`` and flush. Best-effort:
    any exception is swallowed so observability wiring can never break the
    cold start (the same contract as ``_fault_dumps``)."""
    try:
        print(build_banner(argv), file=sys.stderr, flush=True)
    except Exception:
        pass
