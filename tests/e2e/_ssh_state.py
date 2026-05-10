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

Migration note (handoff/extract-dynrunner-driver)
-------------------------------------------------

The keypair / ssh_config / master-spawn / master-stop primitives now
live in the public ``dynamic_runner.driver`` crate (Rust, exposed via
PyO3). This module retains the four function signatures the e2e
suite imports — ``ensure_dispatcher_keypair``, ``generate_ssh_config``,
``spawn_ssh_master``, ``stop_ssh_master`` — but delegates each to
``dynamic_runner.driver`` internally. The harness-specific
``provision_dispatcher_user`` stays here verbatim because it shells
out to the slurm-test-env flake (out of scope for the framework crate
per locked design point (l)).

The dispatcher consumes the produced ``ssh_config`` via the
framework's ``--ssh-config`` flag (the escape hatch surfaced in
commit ``178a3af``).
"""

from __future__ import annotations

import os
import subprocess
from pathlib import Path

from dynamic_runner.driver import (
    SshMaster,
    ensure_dispatcher_keypair as _driver_ensure_dispatcher_keypair,
    write_ssh_config as _driver_write_ssh_config,
)


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
    """Generate an ed25519 keypair if absent (delegated to driver).

    Thin Python wrapper around
    :func:`dynamic_runner.driver.ensure_dispatcher_keypair`. The
    behaviour matches the pre-migration helper bit-for-bit:
    ``<instance_state_dir>/keys/id_ed25519{,.pub}``, ``mode 0600`` on
    the private key, re-runnable.
    """
    return _driver_ensure_dispatcher_keypair(instance_state_dir)


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

    Out-of-scope for the framework crate per locked design point (l):
    provisioning shells out to the harness's flake app, which isn't
    a framework concern.
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
    """Write the per-cluster ssh_config and return its absolute path
    (delegated to driver).

    Thin wrapper around
    :func:`dynamic_runner.driver.write_ssh_config`. Threads
    ``host_alias`` into BOTH the ``Host`` block label AND the URL
    host the dispatcher passes downstream; pins
    ``HostName localhost`` so the SSH client (on the operator host)
    dials the cluster's port-forwarded sshd endpoint without leaking a
    hostname workers can't reach. Re-running overwrites the file —
    cheap, captures any port/user drift between driver invocations.
    """
    return _driver_write_ssh_config(
        instance_state_dir,
        host_alias,
        # host_name — see the locked design point (l) doc on why this
        # is a separate kwarg from host_alias. In the slurm-test-env
        # case the alias is the cluster-internal network alias
        # (slurm-gateway) and the host is `localhost` because the
        # cluster's sshd is reached via host-side port forward.
        "localhost",
        ssh_port,
        user,
        identity_file,
    )


# Per-process registry of live SshMaster instances keyed by control
# socket path. ``spawn_ssh_master`` returns just the path (matching
# the pre-migration contract) so the caller can stash it in scenario
# state; the underlying SshMaster object lives here, anchored against
# Python GC so its Rust Drop only fires on
# :func:`stop_ssh_master` (idempotent).
_LIVE_MASTERS: dict[Path, SshMaster] = {}


def spawn_ssh_master(
    instance_state_dir: Path,
    *,
    ssh_config_path: Path,
    host_alias: str,
) -> Path:
    """Pre-spawn an SSH master and return the control socket path
    (delegated to driver).

    Migration to ``dynamic_runner.driver.SshMaster``
    -------------------------------------------------

    The previous implementation used ``setsid -f -- ssh -M -N -f`` via
    ``subprocess.run`` to fork a detached master. The new
    implementation uses :class:`dynamic_runner.driver.SshMaster`,
    which is the Rust-side port of the same lifecycle (``ssh -M -N``
    directly, daemon-PID tracking, watcher thread, SIGTERM→SIGKILL
    ladder on drop).

    Signature is unchanged so existing callers (``run_e2e`` and the
    ``escape-hatch-external-master`` scenario) need no edits. The
    SshMaster instance is anchored in a module-level registry keyed
    by control path; :func:`stop_ssh_master` looks it up and calls
    ``disconnect()``.
    """
    # The driver's master_spawn generates its own control socket
    # path under /tmp (sun_path<108 enforced internally). The old
    # helper also placed the socket under /tmp for the same reason
    # — the worktree path under .claude is already 80+ chars before
    # the filename. So we accept the driver's path generator over
    # the prior helper's INSTANCE_ID-derived name (functionally
    # equivalent: both unique-per-spawn, both under /tmp).
    _ = instance_state_dir  # retained kwarg for caller signature compat
    master = SshMaster.spawn(
        host=host_alias,
        port=22,
        config_file=ssh_config_path,
    )
    cp = Path(master.control_path)
    _LIVE_MASTERS[cp] = master
    print(
        f"[ssh] master ready: control_path={cp} "
        f"(daemon_pid={master.master_pid})",
        flush=True,
    )
    return cp


def stop_ssh_master(
    *,
    ssh_config_path: Path,
    control_path: Path,
    host_alias: str,
) -> None:
    """Cleanly tear down a master spawned by :func:`spawn_ssh_master`
    (delegated to driver).

    Looks up the matching :class:`SshMaster` in the module registry
    and calls ``disconnect()`` (which runs the SIGTERM→SIGKILL ladder
    for spawn-master). Idempotent: missing-key paths return without
    raising, matching the prior helper's "no-op if socket is gone"
    behaviour.

    ``ssh_config_path`` / ``host_alias`` kwargs are retained for
    caller signature compat — the new disconnect path doesn't need
    them (the master holds its own target / auth state internally).
    """
    _ = ssh_config_path  # retained for caller signature compat
    _ = host_alias  # retained for caller signature compat
    master = _LIVE_MASTERS.pop(control_path, None)
    if master is None:
        return
    master.disconnect()


__all__ = [
    "ensure_dispatcher_keypair",
    "generate_ssh_config",
    "provision_dispatcher_user",
    "spawn_ssh_master",
    "state_dir_for_instance",
    "stop_ssh_master",
]
