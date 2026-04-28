"""Subprocess factory used by the primary to launch secondary processes.

Ported from `dynamic_batch_tokenizer/__main__.py::_make_spawn_secondary` so
each task package can reuse the same canonical spawn shape. Task packages
that need a different spawn shape (e.g. running under SLURM srun) supply
their own factory and pass it directly to `dynamic_runner.run`.
"""

from __future__ import annotations

import argparse
import subprocess
import sys
from collections.abc import Callable


def make_subprocess_spawn_factory(
    package_name: str,
) -> Callable[[argparse.Namespace], Callable[[str, str, int], subprocess.Popen]]:
    """Return a `spawn_secondary_factory(args) -> spawn_secondary(...)` function.

    `package_name` is the importable Python module that will be invoked as
    `python -m <package_name>` to start the secondary. The returned
    factory passes through `--raw-logs` if set on the parent.
    """

    def factory(args: argparse.Namespace) -> Callable[[str, str, int], subprocess.Popen]:
        def spawn_secondary(primary_url: str, secondary_id: str, quic_port: int) -> subprocess.Popen:
            cmd = [sys.executable, "-m", package_name]
            cmd += ["--secondary", primary_url]
            cmd += ["--secondary-id", secondary_id]
            cmd += ["--secondary-quic-port", str(quic_port)]
            if getattr(args, "raw_logs", False):
                cmd.append("--raw-logs")
            return subprocess.Popen(cmd)

        return spawn_secondary

    return factory
