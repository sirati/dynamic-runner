"""SSH helpers for scenarios that introspect the running cluster.

Single concern: shell out to ``ssh`` against the slurm-test-env
gateway / workers without each scenario re-implementing the option
soup.

Why a helper module rather than inlining ``subprocess.run(["ssh", ...])``
in each scenario?

- The slurm-test-env exposes a single host port; workers are
  reachable only via ``ssh gateway -J ...`` jumps.
  Centralising the host arg shape stops it drifting.
- The dispatcher's per-cluster SSH config (host alias,
  IdentityFile, IdentitiesOnly, no agent) lives in
  ``DispatchEnv.ssh_config_path`` — every scenario-side ssh call
  must use the same config or it fails authentication
  (the slurm-test-env contract bans agent fallthrough).
"""

from __future__ import annotations

import subprocess

from ._base import DispatchEnv


def gateway_ssh(
    env: DispatchEnv,
    remote_command: str,
    *,
    timeout_s: int = 60,
) -> subprocess.CompletedProcess[str]:
    """Run ``remote_command`` on the slurm-test-env gateway.

    Uses the dispatcher's ssh_config (Host alias, IdentityFile,
    IdentitiesOnly=yes, IdentityAgent=none, etc.) so scenario-side
    queries follow the same auth contract the framework uses for
    upload/dispatch.

    Returns the completed process. Caller checks ``returncode`` and
    parses ``stdout`` themselves — different scenarios want different
    parsers (``sacct`` is tab-separated, ``ls`` is newline, etc.) so
    this helper deliberately stays raw.
    """
    if env.ssh_config_path is None:
        raise RuntimeError(
            "gateway_ssh requires env.ssh_config_path (slurm mode); "
            "the driver populates it in main()"
        )
    argv = [
        "ssh",
        "-F", str(env.ssh_config_path),
        f"{env.ssh_user}@{env.gateway_host_alias}",
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
    timeout_s: int = 60,
) -> subprocess.CompletedProcess[str]:
    """Run ``remote_command`` on worker N via the gateway as ProxyJump.

    Worker hostnames follow ``slurm-worker{N+1}`` per
    ``slurm-test-env/deploy/env.sh::worker_hostname`` (1-indexed —
    workers are slurm-worker1..slurm-worker4 for a 4-worker cluster);
    we accept the conventional 0-indexed argument and translate.
    The gateway is reachable on the host port; workers are only
    reachable internally via the cluster's podman bridge, so we route
    through the gateway as ProxyJump.

    The same ssh_config (and therefore IdentityFile, no agent
    fallthrough) applies to both legs of the jump because the
    ``-F`` flag is honored on the proxy command.
    """
    if env.ssh_config_path is None:
        raise RuntimeError(
            "worker_ssh requires env.ssh_config_path (slurm mode); "
            "the driver populates it in main()"
        )
    # Cluster's worker hostnames are 1-indexed in slurm-test-env's
    # deploy/lib.sh; the framework conventionally indexes secondaries
    # 0-indexed. Bridge the convention here so callers can pass
    # whatever they have.
    worker_hostname = f"slurm-worker{worker_idx + 1}"
    argv = [
        "ssh",
        "-F", str(env.ssh_config_path),
        # ProxyJump through the gateway alias (which the ssh_config
        # already maps to the operator-host port forward).
        "-J", f"{env.ssh_user}@{env.gateway_host_alias}",
        # The worker's hostname only resolves on the cluster's podman
        # bridge — the ProxyJump leg's ssh client (running ON the
        # gateway) can resolve it via podman DNS. The `Host` block
        # in ssh_config only matches `slurm-gateway`, not the worker
        # hostnames, so we pin the auth options inline here for the
        # worker leg of the jump (the ssh_config still applies to the
        # gateway leg via -F).
        "-o", "StrictHostKeyChecking=no",
        "-o", "UserKnownHostsFile=/dev/null",
        "-o", "BatchMode=yes",
        "-o", "ConnectTimeout=10",
        f"{env.ssh_user}@{worker_hostname}",
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
