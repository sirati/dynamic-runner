"""slurm-test-env lifecycle.

Single concern: bring the cluster up if it's down. Never bring it
down (the driver leaves teardown to ``--down-on-success`` /
operator action).

This module does NOT know which scenarios will run. It is reused
across every ``run_e2e.py`` invocation regardless of scenario
selection.
"""

from __future__ import annotations

import os
import subprocess
from pathlib import Path


def gateway_container_name(instance_id: str) -> str:
    """Container name the slurm-test-env scripts produce.

    Mirrors the naming convention in ``slurm-test-env/deploy/env.sh``::

        GATEWAY_NAME="${GATEWAY_HOSTNAME}-${INSTANCE_ID}"
        GATEWAY_HOSTNAME="slurm-gateway"
    """
    return f"slurm-gateway-{instance_id}"


def is_cluster_running(instance_id: str) -> bool:
    """Probe ``podman ps`` for the gateway container.

    Returns False on any error (podman missing, daemon unreachable,
    etc.) — the caller treats False as "needs bring-up", which is
    correct for the missing-podman case anyway (up.sh will surface
    a clearer error than this probe could).
    """
    name = gateway_container_name(instance_id)
    try:
        result = subprocess.run(
            ["podman", "ps", "--format", "{{.Names}}"],
            check=False,
            capture_output=True,
            text=True,
            timeout=10,
        )
    except (FileNotFoundError, subprocess.TimeoutExpired):
        return False
    if result.returncode != 0:
        return False
    running = {line.strip() for line in result.stdout.splitlines() if line.strip()}
    return name in running


def bring_cluster_up(
    slurm_test_env_dir: Path, instance_id: str, ssh_port: int
) -> None:
    """Run ``slurm-test-env/deploy/up.sh`` with the configured instance.

    The bring-up is slow (multi-minute on first run because nix has to
    build the images). We run it inheriting stdout/stderr so the
    coordinator can see progress in real time.
    """
    up_sh = slurm_test_env_dir / "deploy" / "up.sh"
    if not up_sh.exists():
        raise RuntimeError(
            f"slurm-test-env up.sh not found at {up_sh} — is this the right repo?"
        )
    env = os.environ.copy()
    env.setdefault("INSTANCE_ID", instance_id)
    env.setdefault("SSH_PORT", str(ssh_port))
    print(
        f"[cluster] not running; bringing up via {up_sh} "
        f"(instance={instance_id}, ssh_port={ssh_port})",
        flush=True,
    )
    subprocess.run([str(up_sh)], check=True, env=env, cwd=str(slurm_test_env_dir))


def bring_cluster_down(
    slurm_test_env_dir: Path, instance_id: str
) -> None:
    """Run ``slurm-test-env/deploy/down.sh``. Caller decides whether
    to invoke (default behaviour is to leave the cluster up).
    """
    down_sh = slurm_test_env_dir / "deploy" / "down.sh"
    if not down_sh.exists():
        raise RuntimeError(
            f"slurm-test-env down.sh not found at {down_sh}"
        )
    env = os.environ.copy()
    env.setdefault("INSTANCE_ID", instance_id)
    print(
        f"[cluster] tearing down via {down_sh} (instance={instance_id})",
        flush=True,
    )
    subprocess.run([str(down_sh)], check=True, env=env, cwd=str(slurm_test_env_dir))


__all__ = [
    "bring_cluster_down",
    "bring_cluster_up",
    "gateway_container_name",
    "is_cluster_running",
]
