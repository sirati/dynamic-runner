"""Per-cluster SSH state: keypair + provisioned user + ssh_config.

Single concern: produce the three artefacts the dispatcher needs to
reach a ``slurm-test-env`` gateway, in a way that obeys the test-env
owner's hard rules (broadcast 2026-05-08):

- **No ssh-agent** — not 1Password, not gnome-keyring, not the
  operator's personal agent. Connections pass
  ``IdentitiesOnly=yes`` + ``IdentityAgent=none``.
- **No host-side identity reuse** — never read ``$HOME/.ssh/id_*``.
- **Per-cluster keypair** under the project tree (gitignored), one
  keypair per ``(project × instance_id)``.
- **Provision via the flake's CLI** (``nix run .#provision-user``),
  which is idempotent.
- **Disable host-key checking** — every cluster instance regenerates
  host keys, so a pinned ``known_hosts`` would fight us.

The dispatcher consumes the produced ``ssh_config`` via the
framework's ``--ssh-config`` flag (the escape hatch surfaced in
commit ``178a3af``).
"""

from __future__ import annotations

import os
import subprocess
from pathlib import Path


def state_dir_for_instance(state_root: Path, instance_id: str) -> Path:
    """Return ``<state_root>/<instance_id>/`` (created if absent).

    ``state_root`` is the e2e suite's per-project state root, typically
    ``tests/e2e/state``. Each cluster instance gets its own subtree so
    multiple concurrent ``run_e2e.py`` invocations against different
    instances don't clobber each other's keys.
    """
    out = state_root / instance_id
    out.mkdir(parents=True, exist_ok=True)
    (out / "keys").mkdir(parents=True, exist_ok=True)
    return out


def ensure_dispatcher_keypair(instance_state_dir: Path) -> tuple[Path, Path]:
    """Generate an ed25519 keypair if absent.

    Returns ``(private_key_path, public_key_path)``. Both live under
    ``<instance_state_dir>/keys/``; ``mode 0600`` on the private key.
    Re-runnable: on second invocation we just return the existing
    paths without regenerating (the cluster's authorized_keys already
    has the matching pubkey from the prior provision).
    """
    keys_dir = instance_state_dir / "keys"
    keys_dir.mkdir(parents=True, exist_ok=True)
    priv = keys_dir / "id_ed25519"
    pub = keys_dir / "id_ed25519.pub"
    if priv.exists() and pub.exists():
        return priv, pub
    # Wipe any half-generated state from a prior failed run.
    priv.unlink(missing_ok=True)
    pub.unlink(missing_ok=True)
    subprocess.run(
        [
            "ssh-keygen",
            "-t", "ed25519",
            "-f", str(priv),
            "-N", "",
            "-C", f"dynrunner-e2e-{instance_state_dir.name}",
        ],
        check=True,
        capture_output=True,
    )
    priv.chmod(0o600)
    return priv, pub


def provision_dispatcher_user(
    slurm_test_env_dir: Path,
    instance_id: str,
    user: str,
    pubkey_path: Path,
) -> None:
    """Re-run ``nix run .#provision-user`` (idempotent).

    Idempotency contract: the slurm-test-env owner documented the
    flow as re-runnable with the same pubkey on every invocation, so
    we don't try to detect "already provisioned" — we just re-call
    every time the driver runs. After a cluster restart the
    authorized_keys may be stale; this guarantees it's current.
    """
    if not slurm_test_env_dir.exists():
        raise RuntimeError(
            f"slurm-test-env flake dir not found at {slurm_test_env_dir}"
        )
    if not pubkey_path.exists():
        raise RuntimeError(f"pubkey not found at {pubkey_path}")
    env = os.environ.copy()
    env["INSTANCE_ID"] = instance_id
    print(
        f"[ssh] provisioning user {user!r} via nix run "
        f"{slurm_test_env_dir}#provision-user (instance={instance_id})",
        flush=True,
    )
    subprocess.run(
        [
            "nix", "run",
            f"{slurm_test_env_dir}#provision-user",
            "--", user, str(pubkey_path),
        ],
        check=True,
        env=env,
    )


def generate_ssh_config(
    instance_state_dir: Path,
    *,
    host_alias: str,
    ssh_port: int,
    user: str,
    identity_file: Path,
) -> Path:
    """Write the per-cluster ssh_config and return its absolute path.

    Pins every option the slurm-test-env contract demands:
    ``IdentitiesOnly=yes`` + ``IdentityAgent=none`` (no agent
    consultation), ``StrictHostKeyChecking=no`` +
    ``UserKnownHostsFile=/dev/null`` (no host-key reuse across
    instances), plus the explicit identity file. Threaded into the
    dispatcher via ``dynamic_runner --ssh-config <path>``.

    The ``host_alias`` is used both as the SSH ``Host`` block label
    AND as the URL host the dispatcher passes to ``--gateway
    ssh://<alias>:<port>``. The framework then propagates that
    string verbatim into the worker wrapper's ``--secondary
    tcp://<alias>:<port>`` URL (see
    ``packaging/preparation.py::_determine_gateway_host``). For the
    slurm-test-env, the alias must match the gateway's podman
    network-alias (``slurm-gateway``) so workers in the cluster's
    private podman network can DNS-resolve it. ``HostName
    localhost`` redirects the SSH client (which runs on the operator's
    host where ``localhost`` IS the cluster's port-forwarded
    SSH endpoint) without leaking a hostname workers can't reach.

    Re-running overwrites the file — cheap, captures any port/user
    drift between driver invocations.
    """
    cfg_path = instance_state_dir / "ssh_config"
    cfg_path.write_text(
        f"""# Auto-generated by tests/e2e/_ssh_state.py — do not edit.
# Instance: {instance_state_dir.name}
# Regenerated on every run_e2e.py invocation.

Host {host_alias}
    HostName localhost
    Port {ssh_port}
    User {user}
    IdentityFile {identity_file}
    IdentitiesOnly yes
    IdentityAgent none
    StrictHostKeyChecking no
    UserKnownHostsFile /dev/null
    ServerAliveInterval 30
    ServerAliveCountMax 3
    TCPKeepAlive yes
    ConnectTimeout 10
"""
    )
    cfg_path.chmod(0o600)
    return cfg_path


__all__ = [
    "ensure_dispatcher_keypair",
    "generate_ssh_config",
    "provision_dispatcher_user",
    "state_dir_for_instance",
]
