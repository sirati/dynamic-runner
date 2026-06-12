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

    # Host path for the framework's filesystem-based control-plane
    # mount. When set, the SLURM wrapper bind-mounts this directory
    # into the container at ``/app/dynrunner-network`` and exports
    # ``DYNRUNNER_NETWORK=/app/dynrunner-network`` to the worker
    # process. Use this for any file the framework or task code
    # needs to share between primary and secondaries that is
    # *neither* a build output (``out-network``) *nor* a log file
    # (``log-network``) — typical examples are nix manifests and
    # peer-substituters lists. The framework writes nothing here on
    # its own; sub-layout is the consumer's concern.
    #
    # When ``None`` (default) no third bind is added and
    # ``DYNRUNNER_NETWORK`` is unset. Consumers that need it must
    # detect the unset env var and fail fast — silently writing to
    # ``log-network`` or ``out-network`` is what produced the
    # mount-conflation bug this field exists to fix.
    dynrunner_network_dir: str | None = None

    # Additional `(local_port, gateway_port)` pairs the framework
    # forwards so a service the consumer runs locally on the
    # submitter at `localhost:local_port` is reachable from every
    # SLURM secondary at `localhost:gateway_port`. Use cases:
    # peer-to-peer binary-cache federation (harmonia), debug-side
    # ssh-into-container endpoints, etc.
    #
    # Topology contract (ProxyJump-into-secondaries, the only
    # SLURM auto-default since dynamic_runner commit 244ade5): each
    # entry becomes a `-R gateway_port:localhost:local_port` flag
    # on the per-secondary `ssh -J gateway secondary` session that
    # the framework opens to bridge primary → secondary. The
    # forward terminates on the secondary's loopback (sshd's
    # default `-R` bind address). Inside the SLURM container —
    # which runs `--network host` — `localhost:gateway_port`
    # therefore reaches `submitter:localhost:local_port` directly.
    #
    # Consumers MUST advertise the service URL as
    # `<scheme>://localhost:<gateway_port>` (not
    # `<scheme>://<gateway_host>:<gateway_port>`). The gateway
    # host's external interface does NOT receive a public
    # `-R 0.0.0.0:<gateway_port>` bind in this topology — the
    # gateway is only an SSH ProxyJump hop, not a data-plane
    # endpoint. Using the gateway hostname would fail at
    # GatewayPorts=no sshds (e.g. LMU `remote.cip.ifi.lmu.de`,
    # which silently downgrades `-R *:port` to a 127.0.0.1 bind)
    # AND at segmented-network clusters where the gateway's
    # external IP is not routable from compute nodes.
    #
    # Gateway-direct topology fallback (legacy, currently
    # unreachable for SLURM but preserved for non-SLURM dispatch
    # callers): if `use_reverse_connection=False` is ever wired
    # back in, the same entries are registered on the
    # submitter→gateway ControlMaster as
    # `-R 0.0.0.0:gateway_port:localhost:local_port`, making the
    # service reachable at `<gateway_host>:gateway_port`. The two
    # topologies share `(local_port, gateway_port)` pair shape so
    # consumer config is portable between them.
    #
    # The framework treats the values as opaque port numbers and
    # makes no other guarantees about what's listening; consumers
    # are responsible for actually starting the local listener
    # before secondaries try to connect.
    extra_port_forwards: tuple[tuple[int, int], ...] = field(default_factory=tuple)

    # Additional flags interpolated into the SLURM wrapper's
    # ``podman run`` invocation, BEFORE the ``{image_name}:{image_tag}``
    # argument and AFTER the framework's own flags (``--pull=never``,
    # ``--network=host``, ``--pids-limit=0``,
    # ``--ulimit nproc=32768:32768``, the auto-derived ``--memory``
    # cap, the ``-e PRIMARY_NODE_IPV{4,6}`` env hints, the standard
    # ``-v`` volume mounts). Intended as a consumer-controlled escape
    # hatch for *workload-dependent* podman flags the framework can't
    # auto-derive from system state — e.g. ``--ulimit=nofile=...``,
    # extra ``--cap-add``/``--cap-drop``, ``--shm-size=...``, etc.
    # Each entry is bash-quoted via :func:`shlex.quote` when
    # interpolated, so values containing spaces or
    # shell-metacharacters survive intact.
    #
    # The framework passes ``--pids-limit=0`` (unlimited) explicitly to
    # suppress podman rootless's silent 2048 builtin default
    # (containers.conf ``pids_limit``). The 2048 cap is too tight for
    # compile-heavy workloads (autotools + parallel gcc/clang fans out
    # hundreds of transient PIDs per build) and for JVM-heavy ones
    # (Ghidra spawns native threads that count against ``pids.max``),
    # causing ``clone() EAGAIN``. The host protections are the RAM cap
    # and nice level. To impose a limit, pass ``--pids-limit=<N>`` here;
    # podman takes the LAST value when the flag appears twice, so the
    # consumer-supplied entry wins.
    #
    # The framework's ``--ulimit nproc=32768:32768`` default overrides
    # the host-side RLIMIT_NPROC podman would otherwise propagate from
    # the SLURM job's inherited per-user cap (or podman's
    # ``containers.conf`` default). Without it, fork-heavy in-container
    # workloads (autotools ``./configure``, parallel gcc/clang, JVM
    # thread spawn) hit ``EAGAIN: Resource temporarily unavailable``
    # whenever the inherited cap lands below the workload's peak fork
    # count — independently of ``--pids-limit`` (which caps cgroup
    # pids.max, a different counter). To override, pass
    # ``--ulimit=nproc=<N>:<N>`` here; same last-wins semantic.
    # NOTE: this default cannot raise the SLURM cgroup's pids.max —
    # that's operator policy. If your workload needs more than 32768
    # forks per user across all concurrent containers on a node, the
    # cluster operator must raise pam_limits' nproc and/or the SLURM
    # cgroup's pids.max. See ``docs/MIGRATION_2026_05_PYTHON_TO_RUST.md``.
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
