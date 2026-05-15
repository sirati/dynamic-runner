"""argparse setup for the dynamic_runner.

The public surface of this module is `build_arg_parser(description)`,
called by `dynamic_runner.run.run`.
"""

from __future__ import annotations

import argparse

from ._shared import add_selection_arguments


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
        choices=["slurm", "local", "single-process"],
        help="Enable multi-computer distributed mode (slurm, local, or single-process)",
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
            "Number of retry passes after the main operational loop drains. "
            "Default 1: tasks that fail Recoverable in the main pass get one "
            "more attempt in a retry pass; tasks that fail twice are permanent. "
            "Set 0 to disable retry (Recoverable failures become terminal "
            "immediately)."
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
      The three ``--multi-computer`` modes (``slurm``, ``local``,
      ``single-process``) all participate in the setup-promote
      handshake; the deprecated ``--slurm`` shorthand is treated as
      equivalent to ``--multi-computer slurm`` for this check.

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
                "(--multi-computer slurm|local|single-process); plain "
                "local mode has no secondary to delegate setup to."
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


