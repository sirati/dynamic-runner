"""slurm-test-env lifecycle.

Single concern: bring the cluster up if it's down. Never bring it
down (the driver leaves teardown to ``--down-on-success`` /
operator action).

This module does NOT know which scenarios will run. It is reused
across every ``run_e2e.py`` invocation regardless of scenario
selection.

Bring-up + tear-down go through the slurm-test-env's flake apps
(``nix run <flake>#up|#down``) — the flake wraps podman + image
pinning + env wiring. Raw bash + scavenged-store podman is
explicitly forbidden (per the slurm-test-env owner's broadcast).

Migration note (handoff/extract-dynrunner-driver)
-------------------------------------------------

``is_cluster_running`` now delegates to
:func:`dynamic_runner.driver.cluster_is_running` — the TCP probe is
identical (2s timeout, localhost only) but the implementation lives
in the public ``dynrunner-driver`` Rust crate so harnesses other
than this e2e suite can reuse it.

``bring_cluster_up`` / ``bring_cluster_down`` are NOT in the
framework crate (locked design point (l)): they shell out to the
slurm-test-env-specific flake apps, which are harness state, not
framework state.
"""

from __future__ import annotations

import os
import subprocess
from pathlib import Path

from dynamic_runner.driver import cluster_is_running as _driver_cluster_is_running


def is_cluster_running(ssh_port: int) -> bool:
    """TCP probe of the gateway sshd port (delegated to driver).

    Thin Python wrapper around
    :func:`dynamic_runner.driver.cluster_is_running`. Sufficient as
    an "is it up" gate: if SSH refuses, the cluster is either down
    or the port is wrong; either way the driver wants bring-up.
    """
    return _driver_cluster_is_running(ssh_port)


def bring_cluster_up(
    slurm_test_env_dir: Path, instance_id: str, ssh_port: int
) -> None:
    """Run ``nix run <flake>#up``.

    The flake ships podman + image-build pins + env wrapping, so we
    must NOT invoke ``deploy/up.sh`` directly (that bash falls back
    to PATH-podman, which our nix-develop environment doesn't
    provide). Bring-up is slow (multi-minute on first run because
    nix has to build the images); we inherit stdout/stderr so the
    coordinator sees progress in real time.
    """
    if not slurm_test_env_dir.exists():
        raise RuntimeError(
            f"slurm-test-env flake dir not found at {slurm_test_env_dir}"
        )
    env = os.environ.copy()
    env["INSTANCE_ID"] = instance_id
    env["SSH_PORT"] = str(ssh_port)
    print(
        f"[cluster] not running; bringing up via nix run {slurm_test_env_dir}#up "
        f"(instance={instance_id}, ssh_port={ssh_port})",
        flush=True,
    )
    subprocess.run(
        ["nix", "run", f"{slurm_test_env_dir}#up"],
        check=True,
        env=env,
    )


def bring_cluster_down(
    slurm_test_env_dir: Path, instance_id: str
) -> None:
    """Run ``nix run <flake>#down``. Caller decides whether to invoke
    (default behaviour is to leave the cluster up).
    """
    if not slurm_test_env_dir.exists():
        raise RuntimeError(
            f"slurm-test-env flake dir not found at {slurm_test_env_dir}"
        )
    env = os.environ.copy()
    env["INSTANCE_ID"] = instance_id
    print(
        f"[cluster] tearing down via nix run {slurm_test_env_dir}#down "
        f"(instance={instance_id})",
        flush=True,
    )
    subprocess.run(
        ["nix", "run", f"{slurm_test_env_dir}#down"],
        check=True,
        env=env,
    )


__all__ = [
    "bring_cluster_down",
    "bring_cluster_up",
    "is_cluster_running",
]
