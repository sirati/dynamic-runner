# =====================================================================
# WARNING — PYTHON BRIDGE ONLY. NO LOGIC HERE.
# =====================================================================
# This file is a thin PyO3 / CLI / config bridge. ALL business logic,
# lifecycle, state-tracking, async orchestration, and process management
# lives in Rust under `crates/dynrunner-pyo3/src/slurm/pipeline/run_remote_podman.rs`.
# If you find yourself adding logic here — STOP. Put it in Rust and call
# it from this file via PyO3.
# =====================================================================
"""Remote-podman dispatch shim.

``--multi-computer remote-podman`` runs the consumer's secondary
container on a single SSH-reachable remote host. The flow reuses the
SLURM dispatch stack — image build/transfer, wrapper-script renderer,
podman packaging, gateway tunnel primitive, ``RustPrimaryCoordinator``
— and drops the SLURM-specific pieces (sbatch, reverse-tunnel watcher,
shutdown-manager spawn). Single-secondary by construction: ``--jobs``
is rejected at CLI parse time, and the wrapper bakes ``secondary-0``
into the container-command argv.

This module exposes two callables consumed across the FFI boundary:

* :func:`run_remote_podman_pipeline` — re-exported verbatim from
  ``dynamic_runner._native`` (the Rust orchestrator), invoked by
  :func:`dynamic_runner.run._dispatch_remote_podman`.
* :func:`build_remote_podman_spawn` — builds the
  ``spawn_secondary`` callback the Rust ``RustPrimaryCoordinator``
  calls once per secondary. The Rust orchestrator imports this from
  here so the SSH argv assembly stays Python-side (where the
  ``SSHGateway`` Python facade lives), mirroring how the
  ``--multi-computer local`` path keeps ``build_subprocess_spawn``
  in :mod:`dynamic_runner.spawn_secondary`.
"""

from __future__ import annotations

from collections.abc import Callable

from .._native import run_remote_podman_pipeline as run_remote_podman_pipeline  # noqa: F401
from ..subprocess_spec import SubprocessSpec


def build_remote_podman_spawn(
    gateway,
    wrapper_remote_path: str,
) -> Callable[..., SubprocessSpec]:
    """Build the ``spawn_secondary`` callback for remote-podman.

    The returned callable assembles
    ``["ssh", *auth_options, "-p", port, target, "bash",
    wrapper_remote_path]`` and returns it inside a
    :class:`SubprocessSpec`. The Rust primary's ``Command::spawn``
    owns the lifetime of the resulting ssh process; on coordinator
    shutdown the ssh process is killed, the session ends, the
    container's secondary process gets SIGHUP, and the wrapper's
    trap cleans up the container.

    The wrapper script bakes the listen URL
    (``--secondary tcp://localhost:<primary_quic_port>``) and the
    secondary id (``--secondary-id secondary-0``) at render time —
    every value the Rust spawn-caller would otherwise pass is
    deterministic for this dispatch mode. The callback therefore
    ignores its dynamic ``(primary_url, secondary_id, quic_port)``
    positional arguments and returns the same argv each call.
    Respawn is rejected at CLI parse time (the wrapper's baked
    ``secondary-0`` would collide with a fresh respawn id), so
    "ignore the dynamic args" is safe by construction.

    The reverse tunnel (``-R 0.0.0.0:port:localhost:port``) is held
    by the gateway ControlMaster (registered via
    ``gateway.setup_port_forwarding`` BEFORE ``gateway.connect()``),
    not by this per-secondary ssh subprocess. The spawned ssh
    therefore needs no ``-R`` of its own — it just runs the wrapper.
    """
    gw_host = gateway.host
    gw_user = gateway.user
    gw_port = gateway.port
    auth_options = list(gateway.auth_options())
    target = f"{gw_user}@{gw_host}" if gw_user else gw_host

    ssh_argv: list[str] = ["ssh"]
    # Port flag must precede the target. Argparse default for
    # ``SSHGateway.port`` is 22; we forward it verbatim so the spawn
    # uses the same port the gateway master connected to.
    ssh_argv += ["-p", str(gw_port)]
    ssh_argv += auth_options
    # Keep-alives so a network blip on the dispatcher kills the ssh
    # session (and triggers the wrapper trap) rather than hanging the
    # secondary indefinitely.
    ssh_argv += ["-o", "ServerAliveInterval=30", "-o", "ServerAliveCountMax=3"]
    ssh_argv += [target, "bash", wrapper_remote_path]

    def spawn(
        primary_url: str,
        secondary_id: str,
        quic_port: int,
        **_kwargs: object,
    ) -> SubprocessSpec:
        # All three positional arguments are baked into the wrapper at
        # render time; see the module-level docstring. ``**_kwargs``
        # absorbs additive arguments the Rust caller may supply (e.g.
        # ``primary_pubkey_pem`` from the respawn pipeline) for
        # forward-compatibility.
        del primary_url, secondary_id, quic_port
        return SubprocessSpec(argv=list(ssh_argv))

    return spawn
