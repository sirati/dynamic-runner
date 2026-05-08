"""SSH helpers for scenarios that introspect the running cluster.

Single concern: shell out to ``ssh`` against the slurm-test-env
gateway / workers without each scenario re-implementing the option
soup.

Why a helper module rather than inlining ``subprocess.run(["ssh", ...])``
in each scenario?

- The slurm-test-env exposes a single host port (``SSH_PORT``);
  workers are reachable only via ``ssh gateway -J ...`` jumps.
  Centralising the host arg shape stops it drifting.
- ``BatchMode=yes`` + ``StrictHostKeyChecking=no`` are appropriate
  for a throwaway test cluster but inappropriate for production —
  flagging them in one place documents the constraint.
"""

from __future__ import annotations

import subprocess

from ._base import DispatchEnv


_SSH_OPTIONS = (
    "-o",
    "BatchMode=yes",
    "-o",
    "StrictHostKeyChecking=no",
    "-o",
    "UserKnownHostsFile=/dev/null",
    "-o",
    "ConnectTimeout=10",
)


def gateway_ssh(
    env: DispatchEnv,
    remote_command: str,
    *,
    user: str = "testuser",
    timeout_s: int = 60,
) -> subprocess.CompletedProcess[str]:
    """Run ``remote_command`` on the slurm-test-env gateway.

    Returns the completed process. Caller checks ``returncode`` and
    parses ``stdout`` themselves — different scenarios want different
    parsers (``sacct`` is tab-separated, ``ls`` is newline, etc.) so
    this helper deliberately stays raw.
    """
    argv = [
        "ssh",
        *_SSH_OPTIONS,
        "-p",
        str(env.ssh_port),
        f"{user}@localhost",
        remote_command,
    ]
    return subprocess.run(
        argv,
        capture_output=True,
        text=True,
        timeout=timeout_s,
        check=False,
    )


def worker_ssh(
    env: DispatchEnv,
    worker_idx: int,
    remote_command: str,
    *,
    user: str = "testuser",
    timeout_s: int = 60,
) -> subprocess.CompletedProcess[str]:
    """Run ``remote_command`` on worker N via the gateway as ProxyJump.

    Worker hostnames follow ``slurm-worker{N}`` per
    ``slurm-test-env/deploy/env.sh::worker_hostname``. The gateway is
    reachable on the host port; workers are only reachable internally,
    so we route through the gateway.
    """
    argv = [
        "ssh",
        *_SSH_OPTIONS,
        "-J",
        f"{user}@localhost:{env.ssh_port}",
        f"{user}@slurm-worker{worker_idx}",
        remote_command,
    ]
    return subprocess.run(
        argv,
        capture_output=True,
        text=True,
        timeout=timeout_s,
        check=False,
    )


__all__ = ["gateway_ssh", "worker_ssh"]
