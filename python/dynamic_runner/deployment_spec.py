"""Task-package deployment metadata for the SLURM packaging path.

Owned by the consumer (the task package), passed to
:func:`dynamic_runner.run` as the ``deployment`` argument. The framework
treats every field as opaque — the consumer's flake / image / module
layout is not the framework's concern. This is the *only* place
task-package identity crosses the framework boundary; nothing in
``dynamic_runner.packaging`` should reach for a task name another way.

When ``--multi-computer slurm`` is requested, ``deployment`` is required
and supplies four facts the framework cannot derive on its own:

1. **secondary_module** — the Python module the secondary process
   invokes (``python -m {secondary_module} --secondary ...``). Same
   string is used for the local-subprocess spawn path so consumers
   express the module name once.
2. **image_name / image_tag** — the docker tag the consumer's flake
   produces. Image artifact tar / sha256 marker basenames are derived
   from ``image_name``; the SLURM job name prefix defaults to it too.
3. **nix_build_target** — the flake target ``nix build`` runs to
   produce the docker-archive. Resolved against the cwd at
   pipeline-start time (the consumer repo's flake must be the cwd's
   flake).

The single-process and plain-local execution modes ignore
``deployment`` entirely; consumers that never use SLURM may pass
``None`` (the default).
"""

from __future__ import annotations

from dataclasses import dataclass, field


@dataclass(frozen=True, slots=True)
class TaskDeploymentSpec:
    """Consumer-supplied deployment identity for SLURM packaging."""

    # Python module name invoked as ``python -m {secondary_module}``
    # for both the local-subprocess spawn (``--multi-computer local``)
    # and the SLURM secondary wrapper.
    secondary_module: str

    # Container image identity. ``image_name`` is the docker repo
    # name (e.g. ``"asm-tokenizer"``); ``image_tag`` is the tag
    # (e.g. ``"latest"``). The framework derives:
    #   - image tar basename:    ``{image_name}.tar``
    #   - sha256 marker basename: ``{image_name}.sha256``
    #   - SLURM job name prefix:  ``{image_name}`` (override below).
    image_name: str
    image_tag: str = "latest"

    # Nix flake target that produces the docker-archive. Built via
    # ``nix build {nix_build_target} --out-link <link>`` from the
    # consumer's cwd.
    nix_build_target: str = ".#dockerImage"

    # Override the SLURM job name prefix when it should differ from
    # ``image_name`` (rare). Final job name is
    # ``{prefix}-{secondary_id}``.
    slurm_job_name_prefix: str | None = None

    # Additional `(local_port, gateway_port)` pairs to register on
    # the framework's connected SSH gateway, on top of the
    # primary's QUIC port (which the framework forwards
    # unconditionally). Each entry becomes another
    # `ssh -R 0.0.0.0:gateway_port:localhost:local_port` flag on
    # the same ControlMaster, so a service the consumer runs
    # locally on `localhost:local_port` is reachable from compute
    # nodes at `<gateway-host>:gateway_port` without a parallel
    # SSH connection. Use cases: peer-to-peer binary-cache
    # federation, debug-side ssh-into-container endpoints, etc.
    # The framework treats the values as opaque port numbers and
    # makes no other guarantees about what's listening; consumers
    # are responsible for actually starting the local listener
    # before secondaries try to connect.
    extra_port_forwards: tuple[tuple[int, int], ...] = field(default_factory=tuple)

    # Additional flags interpolated into the SLURM wrapper's
    # ``podman run`` invocation, BEFORE the ``{image_name}:{image_tag}``
    # argument and AFTER the framework's own flags (``--pull=never``,
    # ``--network=host``, the auto-derived ``--memory`` cap, the
    # ``-e PRIMARY_NODE_IPV{4,6}`` env hints, the standard ``-v``
    # volume mounts). Intended as a consumer-controlled escape hatch
    # for *workload-dependent* podman flags the framework can't
    # auto-derive from system state — e.g. ``--pids-limit=16384`` for
    # a fanout-heavy autotools build that hits Krater rootless
    # podman's 2048 PID default, ``--ulimit=nofile=...``, extra
    # ``--cap-add``/``--cap-drop``, etc. Each entry is bash-quoted
    # via :func:`shlex.quote` when interpolated, so values containing
    # spaces or shell-metacharacters survive intact.
    #
    # Do NOT set ``--memory`` / ``--memory-swap`` here: the wrapper
    # auto-caps container memory at ``NodeRAM - 2GiB`` (probed at
    # wrapper-execution time on the compute node), and a duplicate
    # flag from this hook would conflict / silently override that
    # cap. Do NOT set ``--network`` here either: the wrapper sets
    # ``--network=host`` unconditionally for the SLURM case.
    #
    # The framework treats every entry as opaque and makes no
    # validation; misconfigured flags surface as a ``podman run``
    # error inside the SLURM job, not at submit time.
    extra_run_args: tuple[str, ...] = field(default_factory=tuple)

    @property
    def effective_job_name_prefix(self) -> str:
        """SLURM job name prefix, defaulting to ``image_name``."""
        return self.slurm_job_name_prefix or self.image_name

    @property
    def image_tar_basename(self) -> str:
        """Filename of the docker-archive on disk and on the gateway."""
        return f"{self.image_name}.tar"

    @property
    def image_marker_basename(self) -> str:
        """Filename of the per-image sha256 marker on the gateway."""
        return f"{self.image_name}.sha256"
