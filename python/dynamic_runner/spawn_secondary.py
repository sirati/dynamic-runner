"""Local-subprocess spawn-spec builder for the network primary coordinator.

==============================================================================
NO LOGIC HERE — this file is a thin CLI bridge.
==============================================================================

Single concern: assemble the argv (and forwarded CLI flags) for one
``--multi-computer local`` secondary subprocess, and return a data-only
:class:`dynamic_runner.subprocess_spec.SubprocessSpec` describing what
to spawn. Nothing here owns a live process: no ``subprocess.Popen`` is
constructed, no ``wait`` / ``kill`` happens, and there is no callback
state across calls.

Process lifecycle (spawn / wait / kill) lives in Rust —
``crates/dynrunner-pyo3/src/managers/primary.rs`` reads the
:class:`SubprocessSpec` and owns the resulting ``std::process::Child``
through ``std::process::Command::spawn``. This split exists because
the framework's invariant (per
``feedback_features_in_rust_python_is_bridge``) is "runtime lifecycle
lives in Rust crates; Python is CLI/config/PyO3 only". Assembling the
argv string is legitimate Python concern (the spawned process IS
Python; ``deployment.secondary_module`` is the consumer's
``TaskDeploymentSpec`` field). Owning the resulting OS process across
calls is not.

Used by ``--multi-computer local`` (and only there): the primary
launches each secondary as a local ``python -m {secondary_module}``
subprocess. The module name comes from
:class:`dynamic_runner.deployment_spec.TaskDeploymentSpec` so the
consumer expresses it exactly once.

The SLURM path does not call this — it builds an analogous argv inside
a podman wrapper, also from the same ``TaskDeploymentSpec``. Both
paths reading the same spec is the whole point: there is one source of
truth for "what's the secondary's Python module name". The SLURM
callback returns ``None`` from
``packaging.pipeline._slurm_already_spawned`` because sbatch handled
the spawning out of band.
"""

from __future__ import annotations

import argparse
import sys
from collections.abc import Callable

from .deployment_spec import TaskDeploymentSpec
from .subprocess_spec import SubprocessSpec


SpawnSecondary = Callable[..., "SubprocessSpec | None"]


def build_subprocess_spawn(
    deployment: TaskDeploymentSpec,
    args: argparse.Namespace,
) -> SpawnSecondary:
    """Build the ``spawn_secondary`` callback the Rust primary calls.

    The returned callable assembles
    ``[python, -m, secondary_module, --secondary URL, --secondary-id
    ID, --secondary-quic-port PORT]`` and returns it inside a
    :class:`SubprocessSpec`. It propagates ``--raw-logs`` from the
    primary's argv when set.

    ``--src-network`` is auto-threaded to the dispatcher's ``--source``
    so the spawned secondary can resolve the framework's auto-stage
    relative paths against the primary's source tree. Without this,
    every relative ``stage_file`` errors with "src_path is relative
    but no src_network is configured" because the secondary subprocess
    defaults ``src_network=None`` outside the SLURM wrapper container.
    Single-host invariant: in ``--multi-computer local`` mode primary
    and secondary share the local filesystem, so the primary's
    ``source_dir`` IS the secondary's ``src_network``.

    ``--cores`` is forwarded as the verbatim spec string (e.g.
    ``"2"``, ``"-2"``, ``"-0"``) so each secondary resolves it
    against its own host via :func:`parse_cores`. This preserves the
    per-machine semantic in heterogeneous deployments — a
    ``--cores -2`` request yields 30 on a 32-core node and 6 on an
    8-core node simultaneously. Without this thread-through the
    secondary subprocess used its argparse default (``"-0"`` = all
    cores), which on the consumer's 32-core host meant 32 workers
    per secondary instead of the intended 2.

    ``--max-memory`` is NOT forwarded: the subprocess secondary's
    default ``"-2G"`` (host-minus-2G headroom) is the right
    behaviour for the single-host ``--multi-computer local`` case.
    Forwarding the primary's verbatim spec would double-count when
    multiple secondaries share one host's RAM, so this is left
    unchanged from the pre-fix behaviour pending an explicit memory
    policy decision.
    """

    def spawn_secondary(
        primary_url: str,
        secondary_id: str,
        quic_port: int,
        **_kwargs: object,
    ) -> SubprocessSpec:
        # ``**_kwargs`` absorbs additive arguments the Rust caller may
        # supply (e.g. ``primary_pubkey_pem`` from the multi-process
        # respawner). The local-subprocess argv builder does not need
        # them today, but accepting them keeps the callback signature
        # forward-compatible with the in-progress respawn pipeline
        # without forcing every callsite to thread the kwargs in.
        cmd = [sys.executable, "-m", deployment.secondary_module]
        cmd += ["--secondary", primary_url]
        cmd += ["--secondary-id", secondary_id]
        cmd += ["--secondary-quic-port", str(quic_port)]
        source = getattr(args, "source", None)
        if source:
            cmd += ["--src-network", str(source)]
        cores = getattr(args, "cores", None)
        if cores is not None:
            cmd += ["--cores", str(cores)]
        if getattr(args, "raw_logs", False):
            cmd.append("--raw-logs")
        if getattr(args, "log_oom_watcher", False):
            cmd.append("--log-oom-watcher")
        return SubprocessSpec(argv=cmd)

    return spawn_secondary
