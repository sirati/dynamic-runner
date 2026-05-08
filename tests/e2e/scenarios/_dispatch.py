"""Shared dispatch-argv builder.

Single concern: encode the framework CLI shape so every scenario
constructs argvs the same way. New CLI flags only need adding here,
not in N copies under each scenario module.

Why a builder rather than per-scenario raw lists?
-------------------------------------------------

The dispatch CLI evolves with the framework (e.g. ``--multi-computer``
gained the ``slurm`` value during the migration). Centralising the
argv shape gives one place to update when the framework adds /
renames / deprecates flags. Scenarios still tweak their plan's
``argv`` post-hoc (insert / append / prepend) for scenario-specific
arguments — they just don't re-implement the base shape.
"""

from __future__ import annotations

import sys
from pathlib import Path

from ._base import DispatchEnv


# Default consumer module the test_consumer was written against. A
# scenario whose topology requires a different consumer (e.g. ≥3
# phases instead of 2) overrides ``consumer_module`` in the call.
DEFAULT_CONSUMER_MODULE = "tests.e2e.test_consumer"


def build_dispatch_argv(
    *,
    env: DispatchEnv,
    source: Path,
    output: Path,
    num_tasks: int,
    consumer_module: str = DEFAULT_CONSUMER_MODULE,
    jobs: int | None = None,
    extra_args: tuple[str, ...] = (),
) -> list[str]:
    """Build a ``[python, -m <consumer_module>, ...]`` argv.

    Parameters
    ----------
    env
        Cluster-wide configuration. Decides whether SLURM-mode flags
        get appended (``--multi-computer slurm``, ``--gateway``,
        ``--slurm-root-folder``).
    source, output
        Per-scenario staging dirs.
    num_tasks
        Forwarded to the consumer's ``--num-tasks``. The consumer's
        ``discover_items`` reads it.
    consumer_module
        ``-m <module>`` target. Defaults to the canonical consumer.
    jobs
        ``--jobs`` value (SLURM secondary count). When None, defaults
        to ``env.workers`` in slurm mode and is omitted otherwise.
    extra_args
        Scenario-specific flags appended at the end. Use this for
        ``--skip-existing``, ``--connection-mode``, etc.
    """
    argv: list[str] = [
        sys.executable,
        "-m",
        consumer_module,
        "--source",
        str(source),
        "--output",
        str(output),
        "--num-tasks",
        str(num_tasks),
        "--raw-logs",
    ]
    if env.mode == "slurm":
        argv += [
            "--multi-computer",
            "slurm",
            "--packaging",
            "podman",
            "--gateway",
            f"ssh://{env.ssh_user}@{env.gateway_host_alias}:{env.ssh_port}",
            "--slurm-root-folder",
            env.slurm_root_folder,
            "--slurm-partition",
            env.slurm_partition,
            "--slurm-cpus-per-task",
            str(env.slurm_cpus_per_task),
            "--jobs",
            str(jobs if jobs is not None else env.workers),
        ]
        if env.ssh_config_path is not None:
            # Pin all the slurm-test-env contract options
            # (IdentitiesOnly=yes, IdentityAgent=none,
            # StrictHostKeyChecking=no, UserKnownHostsFile=/dev/null,
            # explicit IdentityFile) via the framework's --ssh-config
            # escape hatch (commit 178a3af).
            argv += ["--ssh-config", str(env.ssh_config_path)]
    elif env.mode == "single-process":
        argv += ["--multi-computer", "single-process"]
    # mode == "in-process" → no --multi-computer flag, framework picks
    # the in-process run_local path.
    argv += list(extra_args)
    return argv


__all__ = ["DEFAULT_CONSUMER_MODULE", "build_dispatch_argv"]
