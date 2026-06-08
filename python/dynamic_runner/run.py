"""Thin runner facade: parse argparse, build typed configs, dispatch to Rust.

`run(task, ..., *, argv=None, args=None)` is the canonical Python entry
point. It NEVER reads global ``sys.argv``: callers pass an explicit ``argv``
slice OR a pre-parsed ``args`` namespace, so the framework's CLI is an API
consumers compose against (see :func:`dynamic_runner.cli_main` and
:func:`dynamic_runner.cli.add_framework_arguments`).
"""

from __future__ import annotations

import argparse
import logging
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
    *,
    argv: list[str] | None = None,
    args: argparse.Namespace | None = None,
) -> None:
    """Run the dynamic batch processing CLI.

    Args:
        task: Object satisfying the `TaskDefinition` protocol.
        deployment: Task-package deployment metadata (image name,
            secondary Python module, nix build target). Required when
            ``--multi-computer local|slurm`` is used; ignored in
            single-process and plain-local modes.
        description: Description for the argparse help text.
        argv: Explicit framework+task argv slice to parse (the tokens a
            framework parser, plus ``task.add_task_arguments``, accept).
            Mutually exclusive with ``args``. Defaults to ``[]`` — the
            framework NEVER reads global ``sys.argv``; a programmatic
            caller that wants the historical "parse my command line"
            behaviour passes ``argv=sys.argv[1:]`` explicitly.
        args: A pre-parsed framework namespace (e.g. a consumer that ran
            its own combined parser with
            :func:`dynamic_runner.cli.add_framework_arguments` attached).
            Mutually exclusive with ``argv``. When given, parsing is
            skipped and the namespace is used directly; the forwarded
            secondary argv is empty (the consumer carries no task-filter
            flags to relay — those, if any, ride the ``argv`` path).

    The forwarded secondary argv (task-filter flags the setup-promoted
    secondary must re-parse, minus the framework-regenerated/submitter-local
    flags) is derived from ``argv`` via
    :func:`dynamic_runner._forwarded_argv.filter_framework_argv` — the
    framework's own flag knowledge, not a consumer strip-set.
    """
    if argv is not None and args is not None:
        raise ValueError("run() accepts argv OR args, not both")

    if args is None:
        parse_argv = argv if argv is not None else []
        # Detect secondary-ness with a FRAMEWORK-ONLY parser first: on a SLURM
        # secondary the boot argv carries only the framework-regenerated flags;
        # the task-specific run-config (`forwarded_argv`) arrives over the mesh
        # AFTER connect. Parsing the full task parser eagerly+strictly here would
        # make a `required=True` task arg trip `parser.error()` → SystemExit(2)
        # BEFORE dispatch could reach the deferred-finalize seam — so a required-
        # arg task could never boot as a secondary. `parse_known_args` on a
        # framework-only parser sees `--secondary` without enforcing task args.
        boot_ns, _ = build_arg_parser(description).parse_known_args(parse_argv)
        if boot_ns.secondary:
            # SECONDARY boot: relaxed parse (task-arg `required` not enforced).
            # The real task-arg values ride the mesh post-connect; the STRICT
            # full parse + `validate_parsed_args` happens later in the finalize
            # over `[*boot_argv, *forwarded_argv]` (see `make_reparse_finalizer`).
            args = _parse_secondary_boot_args(task, description, parse_argv)
        else:
            # SUBMITTER / local boot: the submitter has ALL args, so a strict
            # full parse + validation is correct (unchanged behaviour).
            parser = build_arg_parser(description)
            task.add_task_arguments(parser)
            args = parser.parse_args(parse_argv)
            validate_parsed_args(args, parser)
        forward_source = parse_argv
    else:
        # Pre-parsed namespace path: the consumer owns parse + validation
        # (it attached `add_framework_arguments` to its own parser). No
        # task-filter argv to forward — the secondary re-discovers from the
        # framework-regenerated invocation.
        parse_argv = []
        forward_source = []

    # Stash the boot argv (the tokens THIS process parsed, minus any
    # forwarded run-config the boot CLI omits) as a dispatch-time-only
    # attribute, consistent with the `args.forwarded_argv` /
    # `args._discovery_deferred_to_primary` pattern. The joining node's
    # deferred run-config finalize re-parses `[*boot_argv,
    # *delivered_forwarded_argv]` once the primary's post-welcome push delivers
    # the forwarded slice, so `dispatch`'s signature stays unchanged. On the
    # `args=` path the boot argv is empty (the consumer's namespace is already
    # complete).
    args._boot_argv = list(parse_argv)

    # Stash the dispatcher's argv-minus-framework-regenerated-flags so
    # the SLURM wrapper can forward task-specific filter args
    # (e.g. `--platform`, `--compiler`, `--name-regex`) verbatim to
    # the joining node. Filtering is centralised in
    # `_forwarded_argv.filter_framework_argv`; every layer below this
    # is a dumb data carrier. Computed unconditionally — `args` already
    # carries other dispatch-time-only attributes (`resolved_output_root`,
    # `_discovery_deferred_to_primary`) and the non-SLURM dispatch paths
    # simply ignore the field.
    args.forwarded_argv = filter_framework_argv(forward_source)

    # Build the run-config finalize closure the secondary's deferred
    # cmd_args rebuild fires once the primary's post-welcome push delivers
    # `forwarded_argv`. On the internal-parse branch the framework owns the
    # parse, so it can faithfully re-parse `[*boot_argv, *delivered]`; on the
    # `args=` branch the consumer owns the parse (and forwards nothing), so
    # the finalizer is the identity. Stashed as a dispatch-time-only attr —
    # `dispatch`'s signature stays unchanged.
    if argv is not None or args is None:
        args._finalize_run_config = make_reparse_finalizer(task, description, args)
    else:
        args._finalize_run_config = make_identity_finalizer(args)

    # Configure logging from the PARSED args — explicit params, after parse
    # (installs the native subscriber via `init_logging`, no env vars).
    setup_logging(args)

    dispatch(task, args, deployment)


# Dispatch-time attributes the framework sets on `args` AFTER parse (not
# argparse-owned). The reparse finalizer copies these forward onto its
# freshly-parsed namespace so the secondary's deferred-finalize namespace
# carries the same contract the boot-time `args` did. Listed once here so
# the copy-forward set is a single source of truth.
_DISPATCH_TIME_ATTRS = (
    "forwarded_argv",
    "resolved_output_root",
    "_discovery_deferred_to_primary",
    "_boot_argv",
)


def _parse_secondary_boot_args(
    task: TaskDefinition,
    description: str,
    parse_argv: list[str],
) -> argparse.Namespace:
    """Lenient boot parse for the SECONDARY path — task-arg ``required`` relaxed.

    Single concern: produce a COMPLETE boot-time namespace for a secondary
    whose argv carries only the framework-regenerated flags. A SLURM secondary
    boots BEFORE the primary's post-welcome push delivers the task-specific
    ``forwarded_argv``, so its boot argv legitimately lacks every required task
    arg (e.g. asm-tokenizer ``build_memmap --unified-vocab``). A strict full
    parse would call ``parser.error()`` → ``SystemExit(2)`` and the deferred-
    finalize seam (where the strict parse actually belongs, over
    ``[*boot_argv, *forwarded_argv]``) would be unreachable.

    The fix builds a DEDICATED boot parser (framework + task args), relaxes
    every action's ``required`` so the missing task args don't abort, and parses
    ``parse_argv``. The result is a complete namespace carrying task-arg
    DEFAULTS — enough for the placeholder ``build_worker_command_args`` and any
    ``discover_items`` to read their attributes without crashing on missing
    fields; the real values land later in the finalize. ``validate_parsed_args``
    is DEFERRED to the finalize (it runs the strict parser there) so genuinely
    missing required args in the delivered config are still caught.
    """
    parser = build_arg_parser(description)
    task.add_task_arguments(parser)
    relax_required(parser)
    return parser.parse_args(parse_argv)


def relax_required(parser: argparse.ArgumentParser) -> None:
    """Clear every action's ``required`` flag on a boot parser.

    Single concern: the secondary boot-parse relaxation, factored out so both
    framework entry points (``run`` and ``cli_main``) share ONE relaxation
    rule rather than each spelling out the ``_actions`` walk. The strict parser
    that ``make_reparse_finalizer`` rebuilds keeps ``required`` intact, so the
    post-push parse over ``[*boot_argv, *forwarded_argv]`` still rejects a
    delivered config that omits a required arg.
    """
    for action in parser._actions:
        action.required = False


def make_reparse_finalizer(task, description, args):
    """Build the deferred run-config finalize closure for the framework-owned
    parse path (``run(argv=...)`` and the internal default-parse).

    The returned callable takes the DELIVERED ``forwarded_argv`` (the
    post-welcome push payload) and re-parses
    ``[*boot_argv, *delivered_forwarded_argv]`` with the SAME parser ``run``
    built (``build_arg_parser(description)`` + ``task.add_task_arguments``),
    validates it, copies the framework's dispatch-time attributes forward, and
    returns the complete namespace. Rust then re-runs
    ``build_worker_command_args`` against it.

    Parse LOGIC is unchanged — only the TIMING moves to post-push: the boot
    CLI omits the run-config-bearing task-filter flags on a cold-start
    secondary, so the per-type worker ``cmd_args`` cannot be derived until the
    push delivers them.
    """
    boot_argv = list(getattr(args, "_boot_argv", []))

    def finalize_run_config(delivered_forwarded_argv: list[str]) -> argparse.Namespace:
        parser = build_arg_parser(description)
        task.add_task_arguments(parser)
        reparsed = parser.parse_args([*boot_argv, *delivered_forwarded_argv])
        validate_parsed_args(reparsed, parser)
        # Copy the framework's dispatch-time attributes forward — they were
        # set on the boot-time `args` after parse and are not argparse-owned,
        # so a fresh `parse_args` namespace lacks them.
        for attr in _DISPATCH_TIME_ATTRS:
            if hasattr(args, attr):
                setattr(reparsed, attr, getattr(args, attr))
        # The reparsed namespace is the run-config the worker command is built
        # from; the delivered argv it incorporated is now the authoritative
        # forwarded set for this node.
        reparsed.forwarded_argv = list(delivered_forwarded_argv)
        return reparsed

    return finalize_run_config


def make_identity_finalizer(args):
    """Build the no-op finalize closure for the consumer-owned-parse
    (``args=``) path.

    The consumer parsed + validated its own namespace and forwards no
    task-filter argv (the delivered slice is empty), so re-parsing with the
    framework parser would DROP the consumer's pre-parsed values. The
    finalizer therefore returns the consumer's ``args`` unchanged — no flag
    loss. The Rust side still rebuilds ``cmd_args`` from this namespace, which
    for the ``args=`` consumer (compiler_suit) is byte-identical to the boot
    build (its worker command ignores the forwarded run-config).
    """

    def finalize_run_config(_delivered_forwarded_argv: list[str]) -> argparse.Namespace:
        return args

    return finalize_run_config


def dispatch(
    task: TaskDefinition,
    args: argparse.Namespace,
    deployment: TaskDeploymentSpec | None,
) -> None:
    """Route a parsed-and-logging-configured ``args`` to the right mode.

    The single dispatch implementation shared by :func:`run` and
    :func:`dynamic_runner.cli_main.cli_main` — there is no duplicated
    argv→mode logic (CLAUDE.md no-two-implementations). Callers must have
    already parsed ``args``, set ``args.forwarded_argv``, and configured
    logging; this function owns only the secondary / observer /
    multi-computer / local mode selection.
    """
    logger = logging.getLogger()

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
    elif args.multi_computer == "remote-podman":
        if deployment is None:
            logger.error(
                "--multi-computer remote-podman requires "
                "`deployment=TaskDeploymentSpec(...)` in run()"
            )
            return
        _dispatch_remote_podman(task, args, deployment, logger)
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
    discovery to the PRIMARY that owns the run's CRDT — a SLURM-relocated
    compute-peer primary, or the in-process local primary — which has the
    staged corpus on its filesystem. The submitter has no local view of
    those files, so we return an empty list; the relocated/local primary
    seeds `DiscoveryDebt=Owed` and runs `discover_items` itself
    (`discover_on_promotion`) to seed the cluster ledger.
    `args._discovery_deferred_to_primary` is set so dispatch helpers can
    distinguish "intentionally empty" from "task discovered nothing" —
    only the latter is a useful no-op.
    """
    logger = logging.getLogger()

    print_selection_summary(config)

    args.resolved_output_root = str(config.output_dir)

    if getattr(args, "source_already_staged", None):
        logger.info(
            "Pre-staged source mode: deferring task discovery to the "
            "run's primary (relocated compute peer or in-process local)."
        )
        args._discovery_deferred_to_primary = True
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


def _build_distributed_config(args: argparse.Namespace):
    """Return a `DistributedConfig` carrying any operator-supplied retry
    knobs (`--retry-max-passes`, `--oom-retry-max-passes`) or the setup
    deadline override (`--unconfigured-deadline-secs`), or `None` when
    all are unset so the Rust default applies. Both retry buckets run at
    each phase drain edge — Recoverable first, then OOM — and have
    independent per-(phase, kind) counters so neither budget bleeds into
    the other. `--unconfigured-deadline-secs` rides on the same
    `DistributedConfig` and propagates to each spawned secondary's
    `SecondaryConfig.unconfigured_deadline` (the pre-Operational wait).
    """
    import dynamic_runner as _rs

    kwargs = {}
    if getattr(args, "retry_max_passes", None) is not None:
        kwargs["retry_max_passes"] = args.retry_max_passes
    if getattr(args, "oom_retry_max_passes", None) is not None:
        kwargs["oom_retry_max_passes"] = args.oom_retry_max_passes
    if getattr(args, "unconfigured_deadline_secs", None) is not None:
        kwargs["unconfigured_deadline_secs"] = args.unconfigured_deadline_secs
    if not kwargs:
        return None
    return _rs.DistributedConfig(**kwargs)


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
        reuse_workers=args.reuse_workers,
        print_pid=args.pid,
        log_oom_watcher=getattr(args, "log_oom_watcher", False),
        scheduler_config=scheduler_config,
        # `--memprofile` opt-in. The run-level output directory
        # (operator's `--output`, already resolved to an absolute
        # path by `process_selection_arguments`) is the anchor; the
        # Rust-side PyO3 boundary composes `<output>/memprofile/`
        # and constructs the sampler. Passing `output_dir`
        # unconditionally is harmless — the Rust resolver skips
        # composition when `memprofile_enabled = False`.
        output_dir=str(config.output_dir),
        memprofile_enabled=getattr(args, "memprofile", False),
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

    # `--log-oom-watcher` lives on `DistributedConfig` (the struct that
    # already owns the peer-related + setup-phase tuning knobs).
    # Construct an explicit DistributedConfig only when at least one
    # flag deviates from the default; otherwise let
    # `SecondaryConfig.__new__` install the stock default.
    dc_kwargs: dict[str, object] = {}
    # `--log-oom-watcher` rides through to the secondary via the
    # SLURM wrapper's `forwarded_argv` block (it's NOT a
    # framework-regenerated flag, so it passes through verbatim);
    # here we just wire it onto the secondary's DistributedConfig
    # so the Rust-side `OomWatcher` picks it up.
    if getattr(args, "log_oom_watcher", False):
        dc_kwargs["log_oom_watcher"] = True
    # `--unconfigured-deadline-secs` rides through to the secondary the
    # same way (NOT a framework-regenerated flag, so it passes verbatim
    # in `forwarded_argv`); wire it onto THIS secondary's
    # DistributedConfig so the pre-Operational wait the coordinator
    # reads from `SecondaryConfig.unconfigured_deadline` honours the
    # operator override on large/slow clusters.
    if getattr(args, "unconfigured_deadline_secs", None) is not None:
        dc_kwargs["unconfigured_deadline_secs"] = args.unconfigured_deadline_secs
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
    # `--mem-manager-reserved` arrives in two shapes: a human-readable
    # spec ("500M" / "1G") from the operator-facing CLI, OR the
    # already-parsed decimal byte count (`524288000`) rendered by the
    # SLURM wrapper-script generator that consumed the operator's
    # spec at dispatch time. The bytes form is the round-trip:
    # dispatcher parses → wraps into argv → secondary reads it back.
    # `parse_memory` only accepts the suffix form, so route raw
    # decimals straight through `int()` and only fall back to the
    # framework parser when there's an M/G suffix actually present.
    mem_manager_reserved_spec = getattr(args, "mem_manager_reserved", "")
    if not mem_manager_reserved_spec:
        mem_manager_reserved_bytes = None
    elif mem_manager_reserved_spec.isdigit():
        mem_manager_reserved_bytes = int(mem_manager_reserved_spec)
    else:
        mem_manager_reserved_bytes = _rs.parse_memory(mem_manager_reserved_spec)
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
        # `--memprofile` opt-in. The Rust-side
        # `PySecondaryCoordinator::run` resolves the actual output
        # path against the SLURM wrapper's `/app/out-network`
        # bind-mount, so the Python side just forwards the bool.
        memprofile_enabled=getattr(args, "memprofile", False),
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
        # The deferred run-config finalize closure (built at the entry point
        # where parse-ownership is known). Fired by the coordinator after the
        # primary's post-welcome push delivers `forwarded_argv`, BEFORE workers
        # spawn, to re-derive the per-type worker `cmd_args`. Absent (an
        # out-of-tree caller driving `_dispatch_secondary` directly) → None →
        # the Rust side keeps the boot-CLI cmd_args.
        finalize_run_config=getattr(args, "_finalize_run_config", None),
        **_panik_kwargs(args),
    )


def _dispatch_late_joiner(task, args, logger) -> None:
    """Late-joiner observer dispatcher (transport-unification Step 9).

    Reads peer-info files from `args.observer_join_from_peer_info_dir`,
    starts a real `PeerNetwork` on this host, dials in via
    `peer_transport.join_running_cluster`, restores the snapshot(s), and
    cold-joins the standalone `ObserverCoordinator` — a zero-authority
    node that holds the replicated CRDT and narrates the run from it. The
    Rust-side coordinator handles every detail of the bootstrap; the
    Python dispatcher's only job is to surface the configured
    peer-info-dir. The observer needs no task_definition / scheduler /
    estimator (it runs no workers), so none are forwarded — `task` stays
    in the signature only for dispatcher-table uniformity.

    No primary URL is required: a late-joiner is a peer-mesh-only
    participant. It reaches the primary via the peer mesh once the
    snapshot's `current_primary` warms the role-cache during
    `cluster_state.restore`.
    """
    import dynamic_runner as _rs

    # A late-joiner relies on the peer mesh as its ONLY transport (no
    # primary URL is dialed — see Rust `no_primary.rs` for the design
    # contract), so it always uses the real `PeerNetwork` with the stock
    # DistributedConfig default.
    distributed_config = None

    logger.info(
        f"Late-joiner observer: peer-info-dir={args.observer_join_from_peer_info_dir}"
    )
    result = _rs.run_observer_late_joiner(
        args.observer_join_from_peer_info_dir,
        distributed_config=distributed_config,
        **_panik_kwargs(args),
    )
    logger.info(f"Observer Completed (observed): {result['completed']}")


def _dispatch_single_process(task, args, config, logger) -> None:
    """In-process distributed manager (primary + N secondaries via channels)."""
    import dynamic_runner as _rs

    binaries = _collect_binaries(task, args, config)
    # Pre-staged-source mode hands the manager an empty list on
    # purpose; the in-process local primary owes discovery
    # (`DiscoveryDebt=Owed`) and runs it itself to seed the cluster
    # ledger. The "no binaries to process" no-op is only correct when
    # discovery actually ran and found nothing.
    if not binaries and not getattr(args, "_discovery_deferred_to_primary", False):
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

    distributed_config = _build_distributed_config(args)
    primary_cfg = _rs.PrimaryConfig(
        num_secondaries=num_secondaries,
        distributed_config=distributed_config,
    )
    secondary_template = _rs.SecondaryConfig(
        secondary_id="<template>",
        num_workers=workers_per_secondary,
        max_resources=_rs.ResourceMap({"memory": ram_per_secondary}),
        # Pin the template's `output_dir` to the run-level output
        # path so per-template-field documentation matches the
        # actual data the in-process `PyDistributedManager.run`
        # threads into each spawned secondary's `SecondaryConfig`.
        # The manager derives its own per-secondary
        # `SecondaryConfig.output_dir` from
        # `self.output_dir` + the `memprofile_enabled` bool below,
        # so this template value is informational; setting it
        # keeps Python and Rust agreeing on the same path.
        output_dir=str(config.output_dir),
        # `--memprofile` opt-in forwarded uniformly with the slurm and
        # local-multi-computer dispatch paths (Rust resolves the actual
        # output path; Python just forwards the bool).
        memprofile_enabled=getattr(args, "memprofile", False),
    )
    # Pre-staged-source plumbing: `_collect_binaries` already returned
    # `[]` and set `args._discovery_deferred_to_primary` when
    # `args.source_already_staged` is set. The string path goes
    # through to the Rust pyfunction's `Option<PathBuf>` kwarg
    # uniformly with the SLURM and local-multi-computer paths; the
    # in-process run constructs `SeedSource::RelocatedSeed`
    # (`DiscoveryDebt=Owed`) and the local primary's
    # `discover_on_promotion` walks the staged root on the host fs.
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
    # an empty list in pre-staged mode is the intended discovery-deferred
    # signal (the relocated compute-peer primary discovers the corpus),
    # not a "nothing to process" no-op.
    if not binaries and not getattr(args, "_discovery_deferred_to_primary", False):
        logger.info("No binaries to process")
        return

    num_secondaries = args.jobs
    logger.info(f"Starting coordinator with {num_secondaries} local secondaries")

    spawn_secondary = build_subprocess_spawn(deployment, args)

    distributed_config = _build_distributed_config(args)
    primary_cfg = _rs.PrimaryConfig(
        num_secondaries=num_secondaries,
        distributed_config=distributed_config,
    )
    # Pre-staged-source plumbing — see `_dispatch_single_process` for
    # the rationale; `run_primary` forwards the kwarg into the inner
    # `RustPrimaryCoordinator(source_pre_staged_root=...)`, which makes the
    # submitter originate `SeedSource::RelocatedSeed` (`DiscoveryDebt=Owed`)
    # and relocate; the promoted compute-peer primary's
    # `discover_on_promotion` discovers the staged corpus and seeds it.
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


def _dispatch_remote_podman(task, args, deployment: TaskDeploymentSpec, logger) -> None:
    """Single-remote podman mode — image build, transfer, ssh+podman exec, then Rust primary.

    Same shape as ``_dispatch_slurm``: the orchestration body is in
    Rust (``crates/dynrunner-pyo3/src/slurm/pipeline/run_remote_podman.rs``);
    this dispatcher is a one-line delegation so the run.py branch
    surface stays uniform across all four ``--multi-computer`` modes.
    """
    from .packaging import run_remote_podman_pipeline

    run_remote_podman_pipeline(task, args, deployment, logger)
