"""Local-subprocess spawn for the network primary coordinator.

Used by ``--multi-computer local`` (and only there): the primary
launches each secondary as a local ``python -m {secondary_module}``
subprocess. The module name comes from
:class:`dynamic_runner.deployment_spec.TaskDeploymentSpec` so the
consumer expresses it exactly once.

The SLURM path does not call this — it builds an analogous argv inside
a podman wrapper, also from the same ``TaskDeploymentSpec``. Both
paths reading the same spec is the whole point: there is one source of
truth for "what's the secondary's Python module name".
"""

from __future__ import annotations

import argparse
import subprocess
import sys
from collections.abc import Callable

from .deployment_spec import TaskDeploymentSpec


SpawnSecondary = Callable[[str, str, int], subprocess.Popen]


def build_subprocess_spawn(
    deployment: TaskDeploymentSpec,
    args: argparse.Namespace,
) -> SpawnSecondary:
    """Build the ``spawn_secondary`` callback the Rust primary calls.

    The returned callable wraps ``subprocess.Popen([python, -m,
    secondary_module, --secondary URL, --secondary-id ID,
    --secondary-quic-port PORT])``. It propagates ``--raw-logs`` from
    the primary's argv when set.
    """

    def spawn_secondary(primary_url: str, secondary_id: str, quic_port: int) -> subprocess.Popen:
        cmd = [sys.executable, "-m", deployment.secondary_module]
        cmd += ["--secondary", primary_url]
        cmd += ["--secondary-id", secondary_id]
        cmd += ["--secondary-quic-port", str(quic_port)]
        if getattr(args, "raw_logs", False):
            cmd.append("--raw-logs")
        return subprocess.Popen(cmd)

    return spawn_secondary
