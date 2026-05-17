"""Thin runner facade: parse argparse, build typed configs, dispatch to Rust.

`run(task, deployment=None, description="...")` is the canonical Python
entry point.
"""

from __future__ import annotations

import argparse
import logging
import sys
from pathlib import Path

from ._forwarded_argv import filter_framework_argv
from ._shared import (
    print_selection_summary,
    process_selection_arguments,
)

from .deployment_spec import TaskDeploymentSpec
from .logging_setup import setup_logging
from .spawn_secondary import build_subprocess_spawn
from .task_protocol import TaskDefinition

# Import the parser-builder from the slimmed cli module.
from .cli import build_arg_parser, validate_parsed_args  # noqa: E402  (defined in same package)


def run(
    task: TaskDefinition,
    deployment: TaskDeploymentSpec | None = None,
    description: str = "Dynamic batch processing with memory-aware parallel execution",
) -> None:
    """Run the dynamic batch processing CLI.

    Args:
        task: Object satisfying the `TaskDefinition` protocol.
        deployment: Task-package deployment metadata (image name,
            secondary Python module, nix build target). Required when
            ``--multi-computer local|slurm`` is used; ignored in
            single-process and plain-local modes.
        description: Description for the argparse help text.
    """
    logger = setup_logging(sys.argv[1:])

    parser = build_arg_parser(description)
    task.add_task_arguments(parser)
    args = parser.parse_args()
    validate_parsed_args(args, parser)

    # Stash the dispatcher's argv-minus-framework-regenerated-flags so
    # the SLURM wrapper can forward task-specific filter args
    # (e.g. `--platform`, `--compiler`, `--name-regex`) verbatim to
    # the setup-promoted secondary. Filtering is centralised in
    # `_forwarded_argv.filter_framework_argv`; every layer below this
    # is a dumb data carrier. Computed unconditionally — `args` already
    # carries other dispatch-time-only attributes (`resolved_output_root`,
    # `_setup_deferred_to_secondary`) and the non-SLURM dispatch paths
    # simply ignore the field.
    args.forwarded_argv = filter_framework_argv(sys.argv[1:])

    if args.secondary:
        _dispatch_secondary(task, args, logger)
        return

    # Late-joiner observer (transport-unification Step 9). The flag
    # is mutually exclusive with `--secondary` (validated in
    # `validate_parsed_args`); routed before the multi-computer
    # branch so the observer never touches the SLURM / local /
    # single-process dispatch helpers — those construct a primary
    # AND spawn secondaries, neither of which the observer wants
    # (it joins an EXISTING cluster).
    if args.observer_join_from_peer_info_dir:
        _dispatch_late_joiner(task, args, logger)
        return

    if args.slurm and not args.multi_computer:
        args.multi_computer = "slurm"
        logger.warning("--slurm is deprecated, use --multi-computer slurm instead")

    if args.connection_mode is None:
        args.connection_mode = "named" if args.manual_start_worker else "socketpair"

    config = process_selection_arguments(args)

    if args.manual_start_worker and not args.socket_dir:
        args.socket_dir = str(config.output_dir / "sockets")

    if args.connection_mode == "named" and not args.socket_dir:
        logger.error("--socket-dir is required when --connection-mode=named")
        return

    if args.multi_computer == "slurm":
        if deployment is None:
            logger.error("--multi-computer slurm requires `deployment=TaskDeploymentSpec(...)` in run()")
            return
        _dispatch_slurm(task, args, deployment, logger)
    elif args.multi_computer == "local":
        if deployment is None:
            logger.error("--multi-computer local requires `deployment=TaskDeploymentSpec(...)` in run()")
            return
        _dispatch_multi_computer_local(task, args, deployment, logger)
    elif args.multi_computer == "single-process":
        _dispatch_single_process(task, args, config, logger)
    else:
        _dispatch_local(task, args, config, logger)


def _collect_binaries(task: TaskDefinition, args: argparse.Namespace, config) -> list:
    """Discover items via the task's `discover_items` and apply the
    framework-level overlay `--list-files`.

    Item discovery — including any corpus-shape filters and any
    `--skip-existing`-style policy — is the task's concern. The
    framework provides primitives (gateway-aware `_native.find_items`,
    deployment-correct `args.resolved_output_root`) so a task can
    compose source-walk + output-walk + filter inside its own
    `discover_items`. The framework's `--list-files` overlay just
    prints the items the task discovered; consumers wanting richer
    formatting can implement `__str__` on their TaskInfo subclass or
    print from inside `discover_items` directly.

    Pre-staged-source mode (`--source-already-staged <path>`) defers
    discovery to the cluster secondary that has the staged path
    bind-mounted as `src_network`. The submitter has no local view
    of those files, so we return an empty list and let the
    coordinator's setup-promote handshake drive `discover_items` on
    the chosen secondary. `args._setup_deferred_to_secondary` is set
    so dispatch helpers can distinguish "intentionally empty" from
    "task discovered nothing" — only the latter is a useful no-op.
    The empty-list signal is also what the Step 6 PyO3 wrapper reads
    (together with `source_pre_staged_root.is_some()`) to flip
    `PrimaryConfig.required_setup_on_promote` to true.
    """
    logger = logging.getLogger()

    print_selection_summary(config)

    args.resolved_output_root = str(config.output_dir)

    if getattr(args, "source_already_staged", None):
        logger.info(
            "Pre-staged source mode: deferring task discovery to the "
            "setup-promoted secondary."
        )
        args._setup_deferred_to_secondary = True
        return []

    logger.info("Discovering items via task.discover_items(...)")
    binaries = list(task.discover_items(config.source_dir, args))
    logger.info(f"Discovered {len(binaries)} items")

    if not binaries:
        return []

    if config.list_files:
        logger.info("\nDiscovered items:")
        for binary in binaries:
            logger.info(f"  {binary.path}")
        return []

    return binaries


def _build_respawn_args(args: argparse.Namespace, spawn_secondary) -> tuple:
    """Translate the four CLI respawn knobs into the
    ``(respawn_policy, respawn_spawner)`` pair the Rust
    ``run_primary`` / ``RustPrimaryCoordinator`` constructor expects.

    Single concern: own the policy + spawner construction so every
    dispatch path (in-process local, SLURM, future remote) consumes
    one helper. When ``--respawn-policy=disabled`` (the default), the
    helper returns ``(None, None)`` and the coordinator's respawn
    pipeline stays unwired (CCD-5).
    """
    import dynamic_runner as _rs

    policy_name = getattr(args, "respawn_policy", "disabled")
    if policy_name == "disabled":
        return None, None
    if policy_name == "on-secondary-death":
        max_per = int(getattr(args, "respawn_max_per_secondary", 3))
        max_total = int(getattr(args, "respawn_max_total", 10))
        # `parse_duration_secs` is the cli-side suffix parser; we
        # re-import here so this helper stays portable across
        # callsites (the run.py module-level import in 3.x lazy-binds
        # cli.parse_duration_secs only at first call).
        from .cli import parse_duration_secs

        cooldown_secs = parse_duration_secs(
            getattr(args, "respawn_cooldown", "30s")
        )
        policy = _rs.RespawnPolicy.on_secondary_death(
            max_per, max_total, cooldown_secs
        )
        # Today's spawner adapter is the multi-process one. The SLURM
        # equivalent lives in `dynrunner-slurm` and is wired by the
        # SLURM pipeline (see `_dispatch_slurm` / the Rust
        # `_run_slurm_pipeline`), not here. The Python
        # `spawn_secondary` callable is the same one the initial-
        # cohort loop uses; reusing it keeps the wire-flow shape
        # identical for the operator ("respawn = re-run the same
        # callback with a fresh id").
        #
        # The primary's listen endpoint + pubkey PEM no longer travel
        # through this constructor — they are bound inside the Rust
        # primary's detached tokio runtime (after `NetworkServer::bind`
        # returns its cert) and threaded into `enable_respawn`
        # directly. Each per-spawn `SecondarySpawnSpec` carries them
        # to the adapter, which relays them into the Python callback
        # as the existing `primary_url` positional + `primary_pubkey_pem`
        # kwarg.
        spawner = _rs.PyMultiProcessSpawner(spawn_secondary)
        return policy, spawner
    raise ValueError(
        f"unknown --respawn-policy={policy_name!r}; expected one of "
        "'disabled' / 'on-secondary-death'"
    )


def _build_scheduler_config(args: argparse.Namespace):
    """Construct a ``SchedulerConfig`` from the OOM-preempt CLI knobs.

    Single concern: turn the ``--oom-cgroup-safety-margin`` /
    ``--oom-pressure-threshold`` argparse strings (M/G-suffixed) into a
    typed PyO3 ``SchedulerConfig`` every Rust manager-hosting pyclass
    accepts. Pulled out of every dispatcher so all six modes
    (``--multi-computer local`` / ``slurm`` / ``single-process`` /
    plain in-process, plus the secondary and observer late-joiner) feed
    the same operator-supplied margins into their inner scheduler.
    The argparse defaults (``"1G"`` / ``"500M"``) guarantee these
    attributes always exist; callers don't need a fallback.
    """
    import dynamic_runner as _rs

    return _rs.SchedulerConfig(
        cgroup_safety_margin=_rs.parse_memory(args.oom_cgroup_safety_margin),
        pressure_threshold=_rs.parse_memory(args.oom_pressure_threshold),
    )


def _panik_kwargs(args: argparse.Namespace) -> dict:
    """Pull the operator-supplied panik-watcher CLI flags into the
    kwargs shape every Rust manager-hosting pyclass / pyfunction
    accepts (``panik_watcher_paths`` and
    ``panik_watcher_poll_interval_secs``).

    Single concern: read the argparse Namespace once, return the
    kwarg dict the dispatcher splats into the ``_rs.run_*`` call.
    Each dispatcher pairs this with ``_build_scheduler_config(args)``
    — both share the same "translate CLI flags into per-call
    kwargs" pattern, both default to empty / default values when
    unset, neither dispatcher special-cases panik-disabled mode.

    Returns an empty dict when no `--panik-file` flags were supplied
    AND the poll interval is at its default — both sides treat that
    as "watcher off" by passing an empty paths list. Returning the
    dict (rather than calling ``set_item`` style on a passed-in
    dict) keeps every dispatch call site readable as
    ``**_panik_kwargs(args)``.
    """
    out: dict = {}
    paths = getattr(args, "panik_file_paths", None) or []
    if paths:
        out["panik_watcher_paths"] = list(paths)
    poll = getattr(args, "panik_poll_interval_secs", None)
    if poll is not None:
        out["panik_watcher_poll_interval_secs"] = float(poll)
    return out


def _dispatch_local(task, args, config, logger) -> None:
    """Standard in-process local manager."""
    import dynamic_runner as _rs

    num_cores = _rs.parse_cores(args.cores)
    max_memory = _rs.parse_memory(args.max_memory)
    logger.info(f"Cores: {num_cores}")
    logger.info(f"Max memory: {max_memory / (1024**3):.2f}GB")

    binaries = _collect_binaries(task, args, config)
    if not binaries:
        logger.info("No binaries to process")
        return

    # SchedulerConfig surfaces the OOM headroom knobs operators need
    # to keep the framework's userland preempt ahead of the kernel's
    # cgroup-OOM. Built once per dispatch via the shared helper so
    # every mode (local / slurm / distributed / observer) consumes the
    # same CLI surface.
    scheduler_config = _build_scheduler_config(args)
    cfg = _rs.LocalManagerConfig(
        num_workers=num_cores,
        max_resources=_rs.ResourceMap({"memory": max_memory}),
        always_restart_worker=args.always_restart_worker,
        print_pid=args.pid,
        log_oom_watcher=getattr(args, "log_oom_watcher", False),
        scheduler_config=scheduler_config,
    )
    result = _rs.run_local(
        cfg,
        task,
        args,
        str(config.source_dir),
        str(config.output_dir),
        binaries,
        skip_existing=args.skip_existing,
        connection_mode=args.connection_mode,
        socket_dir=args.socket_dir,
        manual_start_worker=args.manual_start_worker,
        log_dir=getattr(args, "log_dir", None),
        **_panik_kwargs(args),
    )
    _log_local_result(result, logger)


def _log_local_result(result: dict, logger) -> None:
    stats = result["stats"]
    logger.info(f"Completed: {stats.completed}/{stats.total}")
    logger.info(f"Errored: {stats.errored}")
    failed = result["failed_tasks"]
    if failed:
        logger.warning(f"Failed tasks: {len(failed)}")
        for ft in failed:
            logger.warning(f"  {ft.binary.path}: {ft.error_type}: {ft.error_message}")
    oom = result["oom_tasks"]
    if oom:
        logger.warning(f"OOM tasks: {len(oom)}")
        for ot in oom:
            logger.warning(f"  {ot.binary.path}: {ot.error_message}")


def _dispatch_secondary(task, args, logger) -> None:
    """Run as a secondary coordinator (SLURM compute node or local test).

    Every per-secondary config knob (resource budgets, src_network,
    src_tmp, output_dir) auto-resolves inside
    `SecondaryConfig.__new__` — the Python side just forwards
    user-supplied CLI overrides for `--src-network` / `--src-tmp`
    and lets Rust handle wrapper-vs-local detection,
    /proc/meminfo / available_parallelism probing, and tempdir
    creation.
    """
    import dynamic_runner as _rs

    if not args.secondary_id:
        logger.error("--secondary-id is required when running in secondary mode")
        return

    # `--secondary-primary-pubkey-pem` is supplied by the respawn
    # pipeline so a respawned secondary can pin the primary's trust
    # anchor at QUIC handshake time. The structural plumbing
    # (Rust primary `NetworkServer::cert_pem()` → coordinator
    # `enable_respawn` → `SecondarySpawnSpec` → SLURM wrapper-script
    # `forwarded_argv` → secondary argparse) is in place; the
    # handshake-time verification is a follow-up. Log the receipt
    # explicitly so an operator inspecting a respawned secondary's
    # log can verify the value reached this side end-to-end.
    pubkey_pem = getattr(args, "secondary_primary_pubkey_pem", None)
    if pubkey_pem:
        # Truncate for the log line (PEMs are several lines long).
        fingerprint_snippet = pubkey_pem.replace("\n", "")[:48]
        logger.info(
            "Received primary pubkey PEM via respawn pipeline "
            f"(prefix: {fingerprint_snippet}...); QUIC handshake-time "
            "verification against this anchor is a follow-up — value "
            "stored but not yet enforced."
        )

    # `--disable-peer-overlay` and `--slurm-setup-deadline-secs` both
    # live on `DistributedConfig` (the struct that already owns the
    # peer-related + setup-phase tuning knobs). Construct an explicit
    # DistributedConfig only when at least one flag deviates from the
    # default; otherwise let `SecondaryConfig.__new__` install the
    # stock default.
    #
    # `--slurm-setup-deadline-secs` reaches the secondary by one of
    # two paths, both terminating at this argparse attribute:
    #
    # * Operator-supplied: the flag travels verbatim through
    #   `filter_framework_argv` (it's not in
    #   `FRAMEWORK_REGENERATED_FLAGS`) and the SLURM wrapper's
    #   forwarded_argv block re-emits it on the secondary command line.
    # * Pipeline-derived: when the operator left it unset, the Rust
    #   SLURM pipeline computes `max(60, num_secondaries * 15)` and
    #   appends `--slurm-setup-deadline-secs=N` to forwarded_argv so
    #   every secondary's argparse re-derives the same effective
    #   deadline as the dispatcher (single source of truth: the
    #   `dynrunner_slurm::pipeline::compute_setup_deadline_secs`
    #   function).
    #
    # In both cases the secondary's argparse stores the int on
    # `args.slurm_setup_deadline_secs`; here we hand it to
    # `DistributedConfig.setup_deadline_secs`. Outside SLURM mode
    # (no pipeline injection, no operator override) the attribute is
    # `None` and the stock 60s default takes over — preserving the
    # historical small-scale behaviour.
    setup_deadline_override = getattr(args, "slurm_setup_deadline_secs", None)
    dc_kwargs: dict[str, object] = {}
    # `--log-oom-watcher` rides through to the secondary via the
    # SLURM wrapper's `forwarded_argv` block (it's NOT a
    # framework-regenerated flag, so it passes through verbatim);
    # here we just wire it onto the secondary's DistributedConfig
    # so the Rust-side `OomWatcher` picks it up.
    if getattr(args, "log_oom_watcher", False):
        dc_kwargs["log_oom_watcher"] = True
    if args.disable_peer_overlay:
        dc_kwargs["disable_peer_overlay"] = True
    if setup_deadline_override is not None:
        dc_kwargs["setup_deadline_secs"] = float(setup_deadline_override)
    distributed_config = _rs.DistributedConfig(**dc_kwargs) if dc_kwargs else None
    # Per-machine cores AND memory: resolve both specs against
    # THIS host's detected resources, not the primary's. The
    # spawn argv carries `--cores=<spec>` always; `--max-memory`
    # is plumbed by the SLURM wrapper (each secondary on its own
    # host with its own cgroup) but intentionally NOT by
    # `--multi-computer local`'s `build_subprocess_spawn`
    # (multiple subprocesses share the operator host's RAM, so
    # the secondary's argparse default `"-2G"` is sufficient and
    # avoids the double-counting trap of forwarding an explicit
    # absolute spec verbatim N times to N subprocesses on one
    # host).
    #
    # Plumbing args.max_memory into SecondaryConfig.max_resources
    # is load-bearing for SLURM: pre-fix the SLURM wrapper
    # correctly emitted `--max-memory=2G` in the secondary's
    # argv, the secondary's argparse stored `args.max_memory =
    # "2G"`, but THIS function (`_dispatch_secondary`) never
    # read it — SecondaryConfig fell through to its auto-detect
    # path (`detect_total_memory_bytes`) which read the cgroup
    # memory.max (the FULL secondary container's cap, e.g. 4
    # GiB). The descending ResourceStealingScheduler budgets
    # then gave `worker_0 = 4 GiB`, `worker_1 = 2.2 GiB`, sum
    # over the cap, concurrent peak OOM-killed both workers
    # (asm-dataset-nix T3 at afd1654). With explicit
    # `--max-memory 2G` plumbed here, SecondaryConfig gets
    # `max_resources = 2 GiB`, and the descending formula's
    # `worker_0 = 2 GiB` + `worker_1 = 1.15 GiB` = 3.15 GiB
    # fits comfortably under the 4 GiB cgroup cap.
    #
    # For `--multi-computer local`: spawn_secondary.py omits
    # `--max-memory` so args.max_memory stays at the argparse
    # default `"-2G"`. parse_memory("-2G") on the operator host
    # gives `host_mem - 2 GiB`. With N>1 subprocess secondaries
    # on the same host this nominally over-allocates (N*(host-
    # 2G)), but the descending-budget formula's intent is exactly
    # that workers don't all peak simultaneously — for workloads
    # like tokenizer this is fine. For workloads that DO peak
    # concurrently (nix-build), the operator passes an explicit
    # absolute spec on the primary's argv; for SLURM that
    # propagates; for local mode the user accepts the over-
    # commit trade-off or switches to `--multi-computer
    # single-process` (which divides cluster-wide).
    num_workers = _rs.parse_cores(args.cores)
    max_memory_bytes = _rs.parse_memory(args.max_memory)
    # `--mem-manager-reserved` opts the secondary into the nested
    # workers cgroup: workers run in `<container-cgroup>/workers/`
    # with `memory.max = container_max - reserved_bytes` so a kernel
    # cgroup-OOM in the workers subgroup spares the secondary. The
    # argparse default ("500M") parses to a concrete u64; an empty
    # string (operator opting out) collapses to None so
    # `SecondaryConfig.mem_manager_reserved_bytes` stays unset and
    # the legacy flat layout applies.
    mem_manager_reserved_spec = getattr(args, "mem_manager_reserved", "")
    mem_manager_reserved_bytes = (
        _rs.parse_memory(mem_manager_reserved_spec)
        if mem_manager_reserved_spec
        else None
    )
    logger.info(
        f"resolved per-machine resources: args.cores={args.cores!r} → "
        f"num_workers={num_workers}, args.max_memory={args.max_memory!r} → "
        f"max_memory_bytes={max_memory_bytes}, "
        f"args.mem_manager_reserved={mem_manager_reserved_spec!r} → "
        f"mem_manager_reserved_bytes={mem_manager_reserved_bytes}"
    )
    cfg = _rs.SecondaryConfig(
        secondary_id=args.secondary_id,
        num_workers=num_workers,
        max_resources=_rs.ResourceMap({"memory": max_memory_bytes}),
        src_network=args.src_network,
        src_tmp=args.src_tmp,
        distributed_config=distributed_config,
        mem_manager_reserved_bytes=mem_manager_reserved_bytes,
    )

    logger.info(f"Secondary ID: {cfg.secondary_id}")
    logger.info(f"Primary URL: {args.secondary}")
    logger.info(f"src_network={cfg.src_network}, src_tmp={cfg.src_tmp}, output_dir={cfg.output_dir}")

    # Worker's `--source` argument: prefer the bind-mount root
    # (`src_network`) when it's configured. That's where binaries
    # actually live in container deployments (the wrapper bind-mounts
    # either the primary's staged-bins dir, or — in pre-staged mode —
    # the user-named cluster path under `--source-already-staged`).
    # Workers that do `binary.path.relative_to(source_dir)` to mirror
    # the source-corpus structure under the output dir then get the
    # right relative path; passing `src_tmp` instead would make
    # `relative_to` fail and the worker fall back to a flat layout.
    # Outside container mode `src_network` is None and we fall back to
    # `src_tmp` (per-secondary scratch — the historical default).
    worker_source_dir = cfg.src_network if cfg.src_network is not None else cfg.src_tmp

    _rs.run_secondary(
        cfg,
        args.secondary,
        task,
        args,
        str(worker_source_dir),
        str(cfg.output_dir),
        skip_existing=args.skip_existing,
        log_dir=getattr(args, "log_dir", None),
        scheduler_config=_build_scheduler_config(args),
        **_panik_kwargs(args),
    )


def _dispatch_late_joiner(task, args, logger) -> None:
    """Late-joiner observer dispatcher (transport-unification Step 9).

    Reads peer-info files from `args.observer_join_from_peer_info_dir`,
    starts a real `PeerNetwork` on this host, dials in via
    `peer_transport.join_running_cluster`, restores the snapshot, and
    drives the secondary run loop with `is_observer=true` and
    `num_workers=0`. The Rust-side coordinator handles every detail
    of the bootstrap; the Python dispatcher's only job is to surface
    the configured peer-info-dir and forward the task_definition (so
    the Rust side can pull the resource estimator off it).

    No primary URL is required: a late-joiner is a peer-mesh-only
    participant. It reaches the primary (a TunneledPeerTransport mesh
    member since Step 5b) via `Address::Role(Role::Primary)`
    dispatch over the peer mesh once the snapshot's `current_primary`
    warms the role-cache during `cluster_state.restore`. The
    coordinator's `primary_transport` slot is filled with a
    `NoPrimaryTransport` stub (see Rust `no_primary.rs`) so the
    setup-skip latch is the single source of truth for "this node
    doesn't speak primary protocol".
    """
    import dynamic_runner as _rs

    # `--disable-peer-overlay` would tell the secondary to install
    # `NoPeerTransport`; a late-joiner relies on the peer mesh as its
    # ONLY transport (no primary URL is dialed — see Rust
    # `no_primary.rs` for the design contract). The validator
    # short-circuited the obvious-error case (`--secondary` +
    # `--observer-join-from-peer-info-dir`); here we tolerate
    # `--disable-peer-overlay` but skip applying it so the observer
    # actually gets a working peer transport. The warning makes the
    # silent override visible to the operator.
    distributed_config = None
    if args.disable_peer_overlay:
        logger.warning(
            "--disable-peer-overlay is ignored under "
            "--observer-join-from-peer-info-dir: the observer relies on "
            "the peer mesh as its ONLY transport. Constructing the "
            "observer with the real PeerNetwork; rerun without "
            "--disable-peer-overlay to silence this warning."
        )

    logger.info(
        f"Late-joiner observer: peer-info-dir={args.observer_join_from_peer_info_dir}"
    )
    result = _rs.run_observer_late_joiner(
        args.observer_join_from_peer_info_dir,
        task,
        distributed_config=distributed_config,
        scheduler_config=_build_scheduler_config(args),
        **_panik_kwargs(args),
    )
    logger.info(f"Observer Completed (observed): {result['completed']}")


def _dispatch_single_process(task, args, config, logger) -> None:
    """In-process distributed manager (primary + N secondaries via channels)."""
    import dynamic_runner as _rs

    binaries = _collect_binaries(task, args, config)
    # Pre-staged-source mode hands the manager an empty list on
    # purpose; the setup-promoted secondary will run discovery and
    # seed the cluster ledger. The "no binaries to process" no-op is
    # only correct when discovery actually ran and found nothing.
    if not binaries and not getattr(args, "_setup_deferred_to_secondary", False):
        logger.info("No binaries to process")
        return

    # Per-machine semantic: each secondary uses the cores/memory the
    # user asked for ON ITS HOST. In-process mode runs all secondaries
    # in the same process (one machine), so workers_per_secondary
    # equals parse_cores(args.cores) directly without dividing by
    # num_secondaries. Previously this divided cluster-wide which
    # silently halved the per-secondary worker count whenever
    # --jobs > 1; that conflicted with the documented spec
    # (`--cores 2` = 2 workers per machine, regardless of --jobs).
    # Memory keeps its cluster-wide divide because in-process mode
    # actually shares one host's RAM across all secondaries — giving
    # each secondary the full budget would double-count.
    num_workers = _rs.parse_cores(args.cores)
    max_memory = _rs.parse_memory(args.max_memory)
    num_secondaries = args.jobs if args.jobs else 1
    workers_per_secondary = num_workers
    ram_per_secondary = max_memory // num_secondaries if num_secondaries > 0 else max_memory

    logger.info(f"Secondaries: {num_secondaries}")
    logger.info(f"Workers per secondary: {workers_per_secondary}")
    logger.info(f"RAM per secondary: {ram_per_secondary / (1024**3):.2f}GB")

    distributed_config = None
    if getattr(args, "retry_max_passes", None) is not None:
        distributed_config = _rs.DistributedConfig(
            retry_max_passes=args.retry_max_passes,
        )
    primary_cfg = _rs.PrimaryConfig(
        num_secondaries=num_secondaries,
        distributed_config=distributed_config,
    )
    secondary_template = _rs.SecondaryConfig(
        secondary_id="<template>",
        num_workers=workers_per_secondary,
        max_resources=_rs.ResourceMap({"memory": ram_per_secondary}),
    )
    # Pre-staged-source plumbing: `_collect_binaries` already returned
    # `[]` and set `args._setup_deferred_to_secondary` when
    # `args.source_already_staged` is set. The string path goes
    # through to the Rust pyfunction's `Option<PathBuf>` kwarg
    # uniformly with the SLURM and local-multi-computer paths so the
    # in-process manager's `PrimaryConfig.required_setup_on_promote`
    # flips to `True` and the chosen secondary owns discovery +
    # ledger-seed.
    # `unfulfillable_reinject_max_per_task` is the CLI knob plumbed
    # uniformly through every primary path; the in-process distributed
    # manager mints its `PrimaryHandle` from a shared
    # `ReinjectCapCell` seeded with this value, so the
    # `PrimaryHandle::set_unfulfillable_reinject_max_per_task` setter
    # and the `run()`-time PrimaryConfig snapshot stay in lockstep
    # with the network-primary and SLURM paths.
    unfulfillable_cap = getattr(args, "unfulfillable_reinject_max_per_task", None)
    result = _rs.run_distributed(
        primary_cfg,
        secondary_template,
        task,
        args,
        str(config.source_dir),
        str(config.output_dir),
        binaries,
        skip_existing=args.skip_existing,
        source_pre_staged_root=args.source_already_staged,
        fulfillability_matcher=getattr(task, "fulfillability_matcher", None),
        peer_lifecycle_listener=getattr(task, "peer_lifecycle_listener", None),
        task_completed_listener=getattr(task, "task_completed_listener", None),
        unfulfillable_reinject_max_per_task=unfulfillable_cap,
        log_dir=getattr(args, "log_dir", None),
        scheduler_config=_build_scheduler_config(args),
        **_panik_kwargs(args),
    )
    logger.info(f"Completed: {result['completed']}")
    logger.info(f"Failed: {result['failed']}")
    # Stranded = tasks the run loop never accounted for (cluster routing
    # collapsed before dispatch / mid-run). Always logged so ops scripts
    # see a deterministic three-line shape; >0 implies the inner run()
    # raised RuntimeError and this branch is unreachable for collapses.
    # Kept here for the no-op zero on every healthy run, paired with the
    # underlying coordinator's `stranded_count` getter that the
    # cluster-collapse tracing::error already surfaces.
    logger.info(f"Stranded: {result['stranded']}")


def _dispatch_multi_computer_local(task, args, deployment: TaskDeploymentSpec, logger) -> None:
    """Network-based primary that spawns local secondaries via subprocess."""
    import dynamic_runner as _rs

    config = process_selection_arguments(args)
    binaries = _collect_binaries(task, args, config)
    # See `_dispatch_single_process` for the pre-staged-mode rationale:
    # an empty list in pre-staged mode is the intended setup-promote
    # signal, not a "nothing to process" no-op.
    if not binaries and not getattr(args, "_setup_deferred_to_secondary", False):
        logger.info("No binaries to process")
        return

    num_secondaries = args.jobs
    logger.info(f"Starting coordinator with {num_secondaries} local secondaries")

    spawn_secondary = build_subprocess_spawn(deployment, args)

    distributed_config = None
    if getattr(args, "retry_max_passes", None) is not None:
        distributed_config = _rs.DistributedConfig(
            retry_max_passes=args.retry_max_passes,
        )
    primary_cfg = _rs.PrimaryConfig(
        num_secondaries=num_secondaries,
        distributed_config=distributed_config,
    )
    # Pre-staged-source plumbing — see `_dispatch_single_process` for
    # the rationale; `run_primary` forwards the kwarg into the inner
    # `RustPrimaryCoordinator(source_pre_staged_root=...)` whose
    # `run()` derives `required_setup_on_promote` from
    # `source_pre_staged_root.is_some() && binaries.is_empty()` (the
    # `_collect_binaries` helper guarantees the empty list in pre-
    # staged mode, so both halves of the gate agree).
    unfulfillable_cap = getattr(args, "unfulfillable_reinject_max_per_task", None)
    # Build the respawn-pipeline wiring from the CLI knobs. The
    # `_build_respawn_args` helper centralises the policy +
    # PyMultiProcessSpawner construction so every dispatch path
    # (in-process local, future SLURM) consumes the same code.
    # When --respawn-policy=disabled (the default), the helper
    # returns `(None, None)` and `run_primary` ignores both kwargs.
    respawn_policy, respawn_spawner = _build_respawn_args(args, spawn_secondary)
    result = _rs.run_primary(
        primary_cfg,
        task,
        spawn_secondary,
        binaries,
        scheduler_config=_build_scheduler_config(args),
        source_dir=str(config.source_dir),
        # `output_dir` + `task_args` together unlock `on_run_start` on
        # the primary side: the Rust shim fires the hook with a freshly-
        # minted `PrimaryHandle` so consumers can drive
        # `primary_handle.spawn_tasks(...)` from inside their lifecycle.
        # Legacy task signatures without the kwarg fall back to the
        # positional-only shape.
        output_dir=str(config.output_dir),
        task_args=args,
        source_pre_staged_root=args.source_already_staged,
        unfulfillable_reinject_max_per_task=unfulfillable_cap,
        respawn_policy=respawn_policy,
        respawn_spawner=respawn_spawner,
        fulfillability_matcher=getattr(task, "fulfillability_matcher", None),
        peer_lifecycle_listener=getattr(task, "peer_lifecycle_listener", None),
        task_completed_listener=getattr(task, "task_completed_listener", None),
        **_panik_kwargs(args),
    )
    logger.info(f"Completed: {result['completed']}")
    logger.info(f"Failed: {result['failed']}")
    # See the run_distributed branch above for stranded semantics.
    logger.info(f"Stranded: {result['stranded']}")


def _dispatch_slurm(task, args, deployment: TaskDeploymentSpec, logger) -> None:
    """SLURM distributed mode — image build, transfer, job submission, then Rust primary."""
    from .packaging import run_slurm_pipeline

    run_slurm_pipeline(task, args, deployment, logger)
