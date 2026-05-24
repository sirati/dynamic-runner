"""argparse setup for the dynamic_runner.

The public surface of this module is `build_arg_parser(description)`,
called by `dynamic_runner.run.run`.
"""

from __future__ import annotations

import argparse

from ._shared import add_selection_arguments


def parse_duration_secs(value: str) -> float:
    """Parse a duration string like '30s', '2m', '1h' into seconds.

    Accepted suffixes: ``s`` (seconds, default if no suffix),
    ``m`` (minutes), ``h`` (hours). Sub-second precision is preserved
    when the numeric part is a float. The single concern is wire-shape
    parsing — every CLI knob that surfaces a duration consumes this
    helper rather than re-implementing the suffix table.
    """
    s = value.strip()
    if not s:
        raise ValueError("empty duration")
    suffix_table = {"s": 1.0, "m": 60.0, "h": 3600.0}
    suffix = s[-1].lower()
    if suffix in suffix_table:
        body = s[:-1]
        multiplier = suffix_table[suffix]
    else:
        body = s
        multiplier = 1.0
    return float(body) * multiplier


def build_arg_parser(description: str) -> argparse.ArgumentParser:
    """Construct the runner argparse with all generic dynamic_runner flags.

    Task-specific arguments are added by the caller via
    `task.add_task_arguments(parser)` after this function returns.
    """
    parser = argparse.ArgumentParser(description=description)
    parser.add_argument("--debug", action="store_true", help="Enable debug logging for detailed output")
    parser.add_argument(
        "--raw-logs",
        action="store_true",
        help="Use raw log formatting (no level, timestamp - only prefix and message)",
    )

    add_selection_arguments(parser)

    parser.add_argument(
        "--cores",
        type=str,
        default="0",
        help=(
            "Cores per secondary (per-machine semantic). Accepted forms: "
            "0 = all detected cores (default), N = exactly N workers, "
            "+N = detected+N (clamped to detected), -N = detected-N "
            "(floored at 1). Each secondary resolves this against its "
            "own host's detected CPU count, so heterogeneous clusters "
            "get sized per-node."
        ),
    )
    parser.add_argument(
        "--max-memory",
        type=str,
        default="-2G",
        help="Maximum memory to use (e.g., 16G, 8192M). Can use +/- prefix for relative to available memory.",
    )
    parser.add_argument(
        "--skip-existing", action="store_true", help="Skip binaries that already have output files"
    )
    parser.add_argument(
        "--always-restart-worker",
        action="store_true",
        help="Restart worker after each completed task",
    )
    parser.add_argument("--pid", action="store_true", help="Print worker PIDs when (re)started")
    parser.add_argument(
        "--log-oom-watcher",
        action="store_true",
        help=(
            "Enable structured OOM-watcher JSON logging at "
            "`target=oom_watcher`. The watcher emits a 10s heartbeat "
            "plus delta-under-pressure (any tracked field +1GiB while "
            "host RAM > 80%% full) and immediate kill events. Useful "
            "for forensic diagnosis of OOM-killer activity; off by "
            "default."
        ),
    )
    parser.add_argument(
        "--oom-cgroup-safety-margin",
        type=str,
        default="1G",
        metavar="SIZE",
        help=(
            "Headroom below the container's cgroup cap at which the "
            "framework preempts (kills the smallest active worker) — "
            "gives userland a chance to act before the kernel's "
            "cgroup-OOM fires. Accepts the same M/G suffixes as "
            "--max-memory. Default 1G. Set to 0M to restore the "
            "pre-fix behaviour (preempt only AT the cgroup cap, races "
            "kernel-OOM)."
        ),
    )
    parser.add_argument(
        "--oom-pressure-threshold",
        type=str,
        default="500M",
        metavar="SIZE",
        help=(
            "Extra headroom above the safety margin at which the "
            "framework kills a median opportunistic worker. Total "
            "opportunistic-kill threshold is (cgroup_cap − "
            "safety_margin − pressure_threshold). Accepts the same "
            "M/G suffixes as --max-memory. Default 500M."
        ),
    )
    parser.add_argument(
        "--mem-manager-reserved",
        type=str,
        default="500M",
        metavar="SIZE",
        help=(
            "Bytes withheld from the workers' nested cgroup so the "
            "secondary process itself has a protected slice of the "
            "container's cgroup-v2 memory.max. When set (the default "
            "500M), the secondary nests a `workers/` subgroup under "
            "its own cgroup and sets `workers/memory.max = "
            "container_max - SIZE`; a runaway worker then trips "
            "kernel-OOM on that subgroup, leaving the secondary "
            "alive so the framework can observe the kill, requeue "
            "the displaced task, and report cleanly. Empty / unset "
            "skips the nesting (legacy flat layout — a worker OOM "
            "reaps the secondary too). Accepts the same M/G suffixes "
            "as --max-memory."
        ),
    )
    parser.add_argument(
        "--panik-file",
        action="append",
        dest="panik_file_paths",
        default=[],
        metavar="PATH",
        help=(
            "Operator-initiated emergency stop sentinel path. Every "
            "node polls each --panik-file at --panik-poll-interval-secs "
            "cadence; the first existing path triggers a "
            "ClusterMutation::PanikRequested broadcast, kills all "
            "workers AND their child trees (SIGTERM → 5s grace → "
            "SIGKILL on each worker's process group), and exits the "
            "container with code 137 so the SLURM/podman wrapper reaps "
            "cleanly. May be passed multiple times — first match wins. "
            "Per the 2026-05-17 design, operators typically pair a "
            "per-host path (e.g. /tmp/<consumer>.panik) with a "
            "shared-network path (e.g. /app/log-network/<consumer>.panik) "
            "so they can trip a single node or the entire cluster "
            "without re-deploying. Off by default (no sentinels)."
        ),
    )
    parser.add_argument(
        "--panik-poll-interval-secs",
        type=float,
        default=10.0,
        metavar="SECONDS",
        help=(
            "Poll cadence for the --panik-file watcher. Default 10s per "
            "the 2026-05-17 design thread; lower values give faster "
            "operator response at the cost of more fs.stat traffic. "
            "Ignored when --panik-file is not set."
        ),
    )
    parser.add_argument(
        "--manual-start-worker",
        action="store_true",
        help="Manually start worker processes (print command and wait)",
    )
    parser.add_argument(
        "--connection-mode",
        type=str,
        choices=["socketpair", "named"],
        default=None,
        help=(
            "Connection mode: 'socketpair' uses socketpair() (default), "
            "'named' uses named Unix domain sockets"
        ),
    )
    parser.add_argument(
        "--socket-dir",
        type=str,
        help="Directory for named socket files (defaults to <output>/sockets when --manual-start-worker is used)",
    )
    parser.add_argument(
        "--debug-simulate-errors",
        type=float,
        metavar="PERCENTAGE",
        dest="simulate_errors",
        help="Simulate random worker error on a task with given percentage chance (0-100)",
    )

    parser.add_argument(
        "--secondary",
        type=str,
        help="Run in secondary mode, connecting to primary at specified URL (e.g., tcp://host:port)",
    )
    parser.add_argument(
        "--observer-join-from-peer-info-dir",
        type=str,
        default=None,
        metavar="DIR",
        help=(
            "Late-joiner OBSERVER mode (transport-unification Step 9): join "
            "an already-running cluster as a peer-mesh-only observer that "
            "performs no work (num_workers=0) and is excluded from "
            "primary-election candidates. DIR is the SLURM wrapper's "
            "<run_log_dir>/connection_info directory — every v2 *.info file "
            "in it becomes a seed entry for the bootstrap-snapshot RPC "
            "(`peer_transport.join_running_cluster`). On success the "
            "observer restores the snapshot, joins steady-state broadcasts, "
            "and exits when the cluster broadcasts RunComplete. Mutually "
            "exclusive with --secondary (a secondary is not a late-joiner)."
        ),
    )
    parser.add_argument(
        "--secondary-id",
        type=str,
        help="Unique identifier for this secondary (required with --secondary)",
    )
    parser.add_argument(
        "--secondary-quic-port",
        type=int,
        default=0,
        help="Port for QUIC server to listen on (0 = let OS pick, default: 0)",
    )
    parser.add_argument(
        "--secondary-primary-pubkey-pem",
        type=str,
        default=None,
        help=(
            "PEM-encoded primary QUIC server certificate, supplied by the "
            "respawn pipeline so the respawned secondary can pin the primary's "
            "trust anchor at QUIC handshake time. Carried verbatim from the "
            "primary's `NetworkServer::cert_pem()`. Today's secondary stores "
            "the value but does not yet act on it (the handshake-time "
            "verification is a follow-up to the respawn-fix bundle); when the "
            "verification path lands, this flag is its source of truth."
        ),
    )
    parser.add_argument(
        "--disable-peer-overlay",
        action="store_true",
        help=(
            "Disable secondary<->secondary peer mesh. Use on clusters that "
            "firewall inter-compute-node networking (LMU SLURM, etc.). "
            "Incompatible with the failover/promote-primary path: "
            "with peer overlay off, primary loss = job loss."
        ),
    )
    parser.add_argument(
        "--src-network",
        type=str,
        default=None,
        help="Shared-drive directory where the primary stages source binaries. "
        "Secondaries copy from here on StageFile notifications. Defaults to "
        "/app/src-network in container deployments.",
    )
    parser.add_argument(
        "--log-dir",
        type=str,
        default=None,
        help=(
            "Per-run log-mount root. The framework anchors per-secondary "
            "log directories (`{timestamp}/{secondary_id}/worker_*.log` by "
            "default) under this path; falls back to the output dir when "
            "unset. The SLURM wrapper passes `--log-dir=/app/log-network` "
            "so worker logs land under the dedicated log-mount tree "
            "instead of the output-mount tree."
        ),
    )
    parser.add_argument(
        "--src-tmp",
        type=str,
        default=None,
        help="Per-secondary scratch directory where staged files land. "
        "Defaults to /app/src-tmp in container deployments or a system tempdir "
        "otherwise.",
    )
    parser.add_argument(
        "--gateway",
        type=str,
        help="Gateway for SLURM controller. Use 'local' or 'ssh://user@host[:port]'",
    )
    parser.add_argument(
        "--ssh-identity-file",
        type=str,
        default=None,
        help=(
            "Path to a private key file used for every framework-issued "
            "ssh/scp invocation in the gateway path (master, exec, scp, "
            "reverse tunnel). Adds '-i <path> -o IdentitiesOnly=yes' so "
            "the agent's other keys are NOT offered. Use this when "
            "~/.ssh/config-driven IdentityAgent over-offers keys and "
            "trips the gateway sshd's MaxAuthTries."
        ),
    )
    parser.add_argument(
        "--ssh-config",
        type=str,
        default=None,
        help=(
            "Path to an alternate ssh_config (passed as '-F <path>'). "
            "Escape hatch for any auth/option setup the framework "
            "doesn't expose directly. Composes with --ssh-identity-file."
        ),
    )
    parser.add_argument(
        "--multi-computer",
        type=str,
        choices=["slurm", "local", "single-process", "remote-podman"],
        help=(
            "Enable multi-computer distributed mode (slurm, local, "
            "single-process, or remote-podman). remote-podman SSHes to one "
            "remote host and runs the consumer's podman image there; "
            "single-secondary by design (no --jobs, no respawn)."
        ),
    )
    parser.add_argument(
        "--slurm",
        action="store_true",
        help="(Deprecated) Enable SLURM distributed mode. Use --multi-computer slurm instead.",
    )
    parser.add_argument(
        "--packaging",
        type=str,
        choices=["docker", "podman"],
        help=(
            "Packaging method for SLURM deployment (required with --multi-computer slurm). "
            "Use 'podman' for SLURM clusters."
        ),
    )
    parser.add_argument(
        "--slurm-root-folder",
        type=str,
        help="Root folder for SLURM operations on gateway (required with --multi-computer slurm)",
    )
    parser.add_argument("--slurm-notify-email", type=str, help="Email address for SLURM job notifications")
    parser.add_argument(
        "--slurm-time-limit",
        type=str,
        default=None,
        help=(
            "Per-secondary SLURM job wallclock limit, in any format sbatch's "
            "--time accepts (e.g. '1:00:00', '02-12', '120'). Defaults to the "
            "SlurmConfig.time_limit value (48:00:00) when unset."
        ),
    )
    parser.add_argument(
        "--slurm-partition",
        type=str,
        default=None,
        help=(
            "SLURM partition to submit jobs against (sbatch --partition). "
            "Defaults to the SlurmConfig.partition value ('All') when unset."
        ),
    )
    parser.add_argument(
        "--slurm-mem",
        type=str,
        default=None,
        help=(
            "Per-secondary SLURM memory request (sbatch --mem, e.g. '60G'). "
            "When unset, no --mem flag is emitted and SLURM falls back to "
            "DefMemPerNode — which on some clusters (notably LMU Krater) is "
            "1 MB and instantly OOMs every worker."
        ),
    )
    parser.add_argument(
        "--slurm-cpus-per-task",
        type=int,
        default=None,
        help=(
            "Per-secondary SLURM cpus-per-task (sbatch --cpus-per-task). "
            "Defaults to the SlurmConfig.cpus_per_task value (14) when unset."
        ),
    )
    parser.add_argument(
        "--slurm-setup-deadline-secs",
        type=int,
        default=None,
        help=(
            "Per-secondary wall-clock budget (in seconds) for the setup "
            "phase (welcome + cert exchange + wait-for-setup) before the "
            "secondary cold-exits. Slow clusters can take well over 60s "
            "to schedule + boot every secondary; at scale this default "
            "helps avoid early-arriving secondaries cold-exiting before "
            "the late ones have connected (which would otherwise cause "
            "the primary's setup-bootstrap broadcast to fail with "
            "'channel closed' on the gone secondaries). When unset, the "
            "SLURM pipeline derives a scale-aware default of "
            "max(60, num_secondaries * 15) so --jobs 1..4 keeps the "
            "60s minimum and --jobs 15 ramps to 225s, --jobs 32 to 480s. "
            "An explicit value here always wins regardless of --jobs."
        ),
    )
    parser.add_argument(
        "--retry-max-passes",
        type=int,
        default=None,
        help=(
            "Number of Recoverable-retry passes at each phase drain edge. "
            "Default 1: tasks that fail Recoverable get one more attempt in the "
            "Recoverable retry bucket before the phase advances; tasks that "
            "fail twice are permanent. Set 0 to disable retry (Recoverable "
            "failures become terminal immediately)."
        ),
    )
    parser.add_argument(
        "--oom-retry-max-passes",
        type=int,
        default=None,
        help=(
            "Number of OOM-retry passes at each phase drain edge for tasks "
            "failed with ErrorType::ResourceExhausted(memory). Runs AFTER the "
            "Recoverable bucket exhausts for that phase. Default 1. Set 0 to "
            "disable (OOM failures become terminal after the first attempt). "
            "Separate from --retry-max-passes so Recoverable retries don't "
            "consume the OOM-retry budget and vice versa."
        ),
    )
    parser.add_argument(
        "--unfulfillable-reinject-max-per-task",
        type=int,
        default=None,
        help=(
            "Per-task budget for the PrimaryHandle.reinject_task control-plane "
            "operation (operator-resolvable failures, e.g. unfulfillable "
            "resource requests). Default None: unbounded — a control plane "
            "can re-inject the same task as often as it likes. An integer N "
            "permits at most N successful reinjects per task; subsequent "
            "reinject calls receive a typed error from PrimaryHandle and the "
            "structured log event 'unfulfillable_reinject_budget_exhausted' "
            "fires. Separate from --retry-max-passes — that knob is the "
            "framework's auto-retry for Recoverable failures; this one is "
            "external-control-only."
        ),
    )
    parser.add_argument(
        "--respawn-policy",
        type=str,
        choices=["disabled", "on-secondary-death"],
        default="disabled",
        help=(
            "Secondary-respawn policy. 'disabled' (default) leaves a dead "
            "secondary dead — pending tasks land on the remaining secondaries "
            "via the normal requeue path. 'on-secondary-death' enables the "
            "respawn pipeline: the primary observes PeerRemoved lifecycle "
            "events and asks the configured spawner (multi-process or SLURM) "
            "to bring up a replacement, subject to the per-secondary/total "
            "budgets and the per-family cooldown below."
        ),
    )
    parser.add_argument(
        "--respawn-max-per-secondary",
        type=int,
        default=3,
        metavar="N",
        help=(
            "Per-family respawn cap (default 3). A 'family' is the chain "
            "rooted at the operator-provisioned secondary id: when N deaths "
            "in the same chain have already been respawned, the next death "
            "in that family is rejected with the structured log event "
            "'respawn_budget_exhausted'."
        ),
    )
    parser.add_argument(
        "--respawn-max-total",
        type=int,
        default=10,
        metavar="N",
        help=(
            "Global respawn cap (default 10). When N respawns have happened "
            "across the lifetime of the coordinator (any family), subsequent "
            "respawn requests are rejected regardless of per-family budget."
        ),
    )
    parser.add_argument(
        "--respawn-cooldown",
        type=str,
        default="30s",
        metavar="DUR",
        help=(
            "Minimum gap between consecutive respawns in the same family "
            "(default 30s). Accepts 'Ns' / 'Nm' / 'Nh' suffixes. The "
            "cooldown is per-family so a well-behaved cluster losing one "
            "peer per minute never trips it; a flapping family does."
        ),
    )
    parser.add_argument(
        "--slurm-image-subfolder",
        type=str,
        default="image_bin",
        help="Subdirectory for Docker images (default: image_bin)",
    )
    parser.add_argument(
        "--slurm-output-subfolder",
        type=str,
        default="out",
        help="Subdirectory for output files (default: out)",
    )
    parser.add_argument(
        "--slurm-log-subfolder",
        type=str,
        default="log",
        help="Subdirectory for log files (default: log)",
    )
    parser.add_argument(
        "--slurm-test-job",
        action="store_true",
        help="Submit a test SLURM job to validate Docker image loading (requires --multi-computer slurm)",
    )
    parser.add_argument(
        "--source-already-staged",
        type=str,
        default=None,
        metavar="PATH",
        help=(
            "Skip the primary's source-staging pass and bind-mount the named "
            "directory into each secondary container at /app/src-network. "
            "PATH is on the cluster filesystem the secondaries see (typically "
            "the gateway's NFS); absolute paths are used as-is, relative paths "
            "resolve against --slurm-root-folder. Useful when the source data "
            "is already laid out on cluster NFS (or on a per-job snapshot a "
            "sibling tool produced) and re-staging would be wasted bytes."
        ),
    )
    parser.add_argument(
        "--jobs",
        type=int,
        default=1,
        help="Number of SLURM secondary nodes to spawn (default: 1)",
    )
    parser.add_argument(
        "--skip-image-build",
        action="store_true",
        help="Skip building and transferring Docker image (assumes image already exists on gateway)",
    )
    return parser


def validate_parsed_args(args: argparse.Namespace, parser: argparse.ArgumentParser) -> None:
    """Cross-flag validation that argparse's per-argument hooks can't express.

    Today's rules — all centred on the load-bearing
    ``--source-already-staged`` flag, which moves discovery + ledger
    seed from the submitter to a chosen secondary:

    * ``--list-files`` is a submitter-side introspection knob that
      runs ``task.discover_items`` and prints what it found.
      ``--source-already-staged`` deliberately defers that walk to
      the setup-promoted secondary — the submitter has no local view
      of the staged corpus, so it cannot list what it never
      discovers. The two flags cannot meaningfully combine.
    * ``--source-already-staged`` only makes sense when a secondary
      exists to take the setup hand-off. Plain local mode
      (``--multi-computer`` absent) runs everything in-process with
      no peer to delegate to, so the combination is rejected here.
      The four ``--multi-computer`` modes (``slurm``, ``local``,
      ``single-process``, ``remote-podman``) all participate in the
      setup-promote handshake; the deprecated ``--slurm`` shorthand
      is treated as equivalent to ``--multi-computer slurm`` for this
      check.

    Called from ``dynamic_runner.run.run`` immediately after
    ``parser.parse_args``. Failures route through ``parser.error``,
    which prints usage + the message to stderr and exits 2 —
    argparse's standard error path, so the conflicting flag names
    surface to the operator in the same shape as a malformed CLI
    invocation.
    """
    if getattr(args, "source_already_staged", None):
        if getattr(args, "list_files", False):
            parser.error(
                "--list-files is incompatible with --source-already-staged: "
                "the submitter does not discover items in pre-staged mode "
                "(discovery runs on the setup-promoted secondary)."
            )
        # `--slurm` is the deprecated alias for `--multi-computer slurm`
        # (see `dynamic_runner.run.run` which performs the equivalence
        # rewrite); accept it here so the validation doesn't reject a
        # combination `run()` would otherwise quietly promote.
        has_distributed_mode = (
            args.multi_computer is not None or getattr(args, "slurm", False)
        )
        if not has_distributed_mode:
            parser.error(
                "--source-already-staged requires a distributed mode "
                "(--multi-computer slurm|local|single-process|remote-podman); "
                "plain local mode has no secondary to delegate setup to."
            )

    # remote-podman is a single-secondary mode by construction: the
    # spawn callback bakes a static argv (secondary_id + listen port +
    # wrapper path), so any second invocation would collide on
    # container name and port-probe. The wrapper renderer also bakes
    # the gateway-host/port into the container-command argv, so a
    # respawn with a fresh secondary-id would still re-register as
    # secondary-0. Reject both --jobs (other than the argparse default
    # of 1) and --respawn-policy=on-secondary-death up-front so the
    # operator learns at parse time rather than at the first peer
    # collision.
    if getattr(args, "multi_computer", None) == "remote-podman":
        if getattr(args, "jobs", 1) != 1:
            parser.error(
                "--jobs is incompatible with --multi-computer remote-podman "
                "(single-secondary by design; the wrapper bakes secondary-0 "
                "and a fixed listen port, so a second concurrent secondary "
                "on the same remote would collide). Rerun without --jobs."
            )
        if getattr(args, "respawn_policy", "disabled") == "on-secondary-death":
            parser.error(
                "--respawn-policy=on-secondary-death is incompatible with "
                "--multi-computer remote-podman: the wrapper bakes "
                "secondary-0 at render time, so a respawn would re-register "
                "under the same id. For a single-machine deployment, "
                "re-dispatch the whole run instead."
            )

    # --observer-join-from-peer-info-dir is a late-joiner-OBSERVER role
    # (transport-unification Step 9). It joins an already-running
    # cluster via the peer mesh and runs zero workers. Combining it
    # with --secondary is a category error: a `--secondary` invocation
    # IS the welcome / cert-exchange / wait-for-setup handshake the
    # late-joiner intentionally skips (via the Rust-side
    # `restore_from_snapshot_and_skip_setup` latch on the secondary
    # coordinator). Reject up-front rather than silently letting one
    # flag take precedence — the operator's intent is ambiguous.
    if getattr(args, "observer_join_from_peer_info_dir", None):
        if getattr(args, "secondary", None):
            parser.error(
                "--observer-join-from-peer-info-dir is incompatible with "
                "--secondary: a late-joiner observer is a peer-mesh-only "
                "participant that does NOT speak the primary-secondary "
                "handshake. Pick one: --observer-join-from-peer-info-dir "
                "to attach a passive observer to an already-running "
                "cluster, OR --secondary to be a regular worker-bearing "
                "secondary that connects to the primary URL."
            )


