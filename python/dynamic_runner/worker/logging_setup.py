"""Compact per-worker log format, the worker-process analogue of the
manager-side per-role files.

Single concern: own the ONE line shape ``worker.log`` emits, matching the
Rust-side per-role files (``primary.log`` / ``secondary.log``):

    13:14:30 INFO W-3  <message>

i.e. ``{h:mm:ss local} {LEVEL} W-{worker_id}  {message}`` — local-timezone
``HH:MM:SS`` (no date, no microseconds), the level, the ``W-<worker_id>``
prefix, two spaces, then the message.

The worker process has no role span (that is a manager-side, Rust-tracing
construct); the only stable id the framework hands a worker is the
``worker_{id}.log`` filename it is told to write via ``--log-file`` (see
``crates/dynrunner-pyo3/src/config/log_paths.rs`` and ``factories.py``). So
the id is derived from that path — the closest stable worker identity — and
falls back to ``?`` when the path carries none (e.g. a consumer that passes a
custom ``--log-file`` name).

This is a framework primitive: the worker runtime installs it by default from
``--log-file`` ONLY when the consumer has not already configured logging
(``on_args`` runs first and wins), so a consumer keeps full control of its own
worker logging while a consumer that configures nothing still gets the compact
host-readable format the operator expects.
"""
from __future__ import annotations

import logging
import re
from pathlib import Path
from typing import Optional

#: The framework's worker-log filename pattern embeds the id as
#: ``worker_<id>.log`` (mirrors ``LogPathConfig`` / ``factories.py``); the id
#: is parsed back out for the line prefix.
_WORKER_ID_RE = re.compile(r"worker_([^/]+?)\.log$")


def worker_id_from_log_file(log_file: Optional[str]) -> str:
    """Derive the worker id for the line prefix from the ``--log-file`` path.

    Returns the id segment of a ``worker_<id>.log`` path, or ``"?"`` when the
    path is absent or does not match the framework pattern (a consumer is free
    to pass any ``--log-file`` name; the prefix degrades gracefully rather than
    guessing).
    """
    if not log_file:
        return "?"
    match = _WORKER_ID_RE.search(Path(log_file).name)
    return match.group(1) if match else "?"


class _CompactWorkerFormatter(logging.Formatter):
    """Formatter for the compact ``W-<worker_id>`` worker-log line.

    Holds the worker id so the prefix is fixed per process (one worker == one
    id), keeping the format string free of per-record id plumbing. Local time
    is inherited from ``logging.Formatter`` (``%H:%M:%S`` via the stdlib's
    ``time.localtime``), matching the manager-side files' local-timezone clock.
    """

    def __init__(self, worker_id: str) -> None:
        super().__init__(
            fmt=f"%(asctime)s %(levelname)s W-{worker_id}  %(message)s",
            datefmt="%H:%M:%S",
        )


def setup_worker_logging(log_file: Optional[str], level: int = logging.INFO) -> None:
    """Install the framework's default compact worker-log handler.

    Single concern: route the worker process's logs to ``log_file`` in the
    compact ``{h:mm:ss} {LEVEL} W-{worker_id}  {message}`` shape (local
    timezone, no date / microseconds / target). The worker id is derived from
    the ``log_file`` path (see :func:`worker_id_from_log_file`).

    Idempotent-by-precedence: this is a no-op when the root logger already
    carries handlers (a consumer configured logging itself — e.g. in the
    runtime's ``on_args`` hook, which runs first), so the framework default
    never fights a consumer's own setup. With no ``log_file`` there is nothing
    to write, so it is also a no-op.
    """
    if not log_file:
        return
    root = logging.getLogger()
    if root.handlers:
        # A consumer already configured logging; do not double-handle.
        return
    path = Path(log_file)
    path.parent.mkdir(parents=True, exist_ok=True)
    handler = logging.FileHandler(path, mode="a")
    handler.setLevel(level)
    handler.setFormatter(_CompactWorkerFormatter(worker_id_from_log_file(log_file)))
    root.setLevel(level)
    root.addHandler(handler)
