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
        default="-0",
        help=(
            "Number of cores to use. Can be int, +int (add to available), or -int "
            "(subtract from available). Default: all available cores"
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


