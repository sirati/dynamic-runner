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
from .logging_setup import stdio_mode_argv
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

    ``--output-dir`` is auto-threaded from the dispatcher's resolved
    ``--output`` (``args.resolved_output_root``) so every spawned
    secondary's ``SecondaryConfig.output_dir`` — the publish target its
    workers receive as ``--output`` — IS the operator's output
    directory. This mirrors the SLURM-production semantic, where every
    secondary publishes into the one user-visible directory via the
    wrapper's ``/app/out-network`` bind-mount; on the shared local
    filesystem the same-host path needs no mount, just this
    thread-through. Without it the secondary auto-resolves to a
    ``<TMPDIR>/secondary-<id>-<pid>-out`` tempdir and every artifact
    dies with it (consumer-validated at 2212c136).

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

    **Task-specific flags** (``--memprofile``, ``--unified-vocab``,
    anything else the consumer's argparse declared) are propagated
    verbatim via :data:`args.forwarded_argv`, which is the operator's
    ``sys.argv[1:]`` filtered through
    :func:`dynamic_runner._forwarded_argv.filter_framework_argv` —
    framework-regenerated flags (``--secondary``, ``--secondary-id``,
    ``--secondary-quic-port``, ``--src-network``, ``--cores``,
    ``--max-memory``, ``--log-dir``) are stripped there so they don't
    duplicate the explicit emissions above, and operator-stdio flags
    (``--important-stdio-only``) are stripped from the GENERIC forward
    set (on SLURM a secondary's stdio is a per-node sbatch capture, so
    it keeps its FULL logs). Boolean store_true flags that ARE manually
    re-emitted (``--raw-logs``, ``--log-oom-watcher``) may appear twice
    if the operator passed them; argparse's store_true tolerates that.

    **Operator-stdio mode** is re-emitted here regardless: a local
    subprocess secondary INHERITS the primary's stdio (Rust spawns it
    with ``Command::spawn`` defaults), so its stdout IS the operator's
    terminal and the operator's stdio gate must hold across the process
    boundary. :func:`dynamic_runner.logging_setup.stdio_mode_argv` owns
    which tokens that takes; this builder appends them verbatim. The
    spawned secondary then installs the gate through the same
    ``setup_logging``/``init_logging`` seam every mode uses.
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
        output_root = getattr(args, "resolved_output_root", None)
        if output_root:
            cmd += ["--output-dir", str(output_root)]
        cores = getattr(args, "cores", None)
        if cores is not None:
            cmd += ["--cores", str(cores)]
        if getattr(args, "raw_logs", False):
            cmd.append("--raw-logs")
        if getattr(args, "log_oom_watcher", False):
            cmd.append("--log-oom-watcher")
        # The subprocess inherits this process's stdio, so the operator's
        # stdio-mode flags must ride along (owned by logging_setup; see
        # the docstring above).
        cmd += stdio_mode_argv(args)
        # Task-specific + memprofile + any other operator flags the
        # consumer's argparse declared. Pre-filtered by
        # ``filter_framework_argv`` so the framework-regenerated flags
        # emitted above don't duplicate here.
        forwarded = getattr(args, "forwarded_argv", None)
        if forwarded:
            cmd += list(forwarded)
        return SubprocessSpec(argv=cmd)

    return spawn_secondary
