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


def spawn_ssh_master(
    instance_state_dir: Path,
    *,
    ssh_config_path: Path,
    host_alias: str,
) -> Path:
    """Pre-spawn an SSH master and return the control socket path.

    Why this exists
    ---------------

    The framework's ``ssh.rs::connect()`` spawns its own SSH master
    process via Rust's ``std::process::Command`` (with ``setsid -f``
    + ``ssh -M -N -f`` daemonisation). Empirically that master gets
    SIGTERM'd ~2 minutes after handshake when run from a tokio-driven
    Python (the dispatcher), even though identical command from a
    plain shell or Python subprocess persists indefinitely.

    Diagnosis of the Rust+tokio process supervision interaction is
    out of scope for an e2e harness. The framework exposes a
    ``DYNRUNNER_SSH_CONTROL_PATH`` env var that bypasses its own
    master spawn and reuses an externally-spawned one — that's what
    we hit here. The master is spawned via ``subprocess.Popen`` from
    Python (which doesn't have the issue), then the framework's
    ``setup_port_forwarding`` issues ``ssh -O forward`` against this
    master to add reverse forwards dynamically as the run progresses.

    Returns the control socket path the caller exports as
    ``DYNRUNNER_SSH_CONTROL_PATH`` for the dispatcher subprocess.
    """
    # Place the control socket under /tmp (not the state dir) — Unix
    # socket paths are capped at 108 bytes (sockaddr_un.sun_path), and
    # the worktree path under .claude is already 80+ chars before the
    # filename. ssh's bind silently fails on overlong paths.
    cp = Path(f"/tmp/dynrunner-ssh-master-{instance_state_dir.name}.sock")
    cp.unlink(missing_ok=True)

    cmd = [
        "setsid",
        "-f",
        "--",
        "ssh",
        "-F", str(ssh_config_path),
        "-M",
        "-N",
        "-f",
        "-o", f"ControlPath={cp}",
        "-o", "ControlMaster=auto",
        "-o", "ControlPersist=yes",
        "-o", "ServerAliveInterval=30",
        "-o", "ServerAliveCountMax=3",
        "-o", "TCPKeepAlive=yes",
        "-o", "LogLevel=ERROR",
        host_alias,
    ]
    print(f"[ssh] spawning persistent master via: {' '.join(cmd)}", flush=True)
    rc = subprocess.run(
        cmd,
        check=False,
        stdin=subprocess.DEVNULL,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.PIPE,
    )
    if rc.returncode != 0:
        raise RuntimeError(
            f"ssh master spawn failed (rc={rc.returncode}): {rc.stderr.decode()!r}"
        )

    # Wait for the control socket to appear (handshake done).
    deadline = 10.0
    waited = 0.0
    while not cp.exists():
        if waited >= deadline:
            raise RuntimeError(
                f"ssh master control socket {cp} did not appear within {deadline}s"
            )
        import time as _t
        _t.sleep(0.05)
        waited += 0.05

    print(f"[ssh] master ready: control_path={cp}", flush=True)
    return cp


def stop_ssh_master(
    *,
    ssh_config_path: Path,
    control_path: Path,
    host_alias: str,
) -> None:
    """Cleanly tear down a master spawned by :func:`spawn_ssh_master`.

    Called by the driver on exit to release the gateway-side reverse
    forwards. ``ssh -O exit`` talks to the master through the control
    socket and asks it to terminate; idempotent if the socket is gone.
    """
    if not control_path.exists():
        return
    subprocess.run(
        [
            "ssh",
            "-F", str(ssh_config_path),
            "-O", "exit",
            "-o", f"ControlPath={control_path}",
            host_alias,
        ],
        check=False,
        stdin=subprocess.DEVNULL,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )


__all__ = [
    "ensure_dispatcher_keypair",
    "generate_ssh_config",
    "provision_dispatcher_user",
    "spawn_ssh_master",
    "state_dir_for_instance",
    "stop_ssh_master",
]
