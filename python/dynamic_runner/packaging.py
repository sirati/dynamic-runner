"""SLURM packaging: build container image, transfer to gateway, submit jobs.

For Phase 4 this is a thin delegate to the still-existing
`slurm.primary.SlurmPrimaryCoordinator`; a fuller port that drops the
legacy coordinator (and tilts towards the Rust runner once SLURM has a
typed-config entry point) is tracked as a follow-up. The existing
coordinator is the single source of truth for the docker/podman build,
the gateway transfer, and the slurm job submission.
"""

from __future__ import annotations

import argparse
import logging
from datetime import datetime
from pathlib import Path

from shared import process_selection_arguments

from .task_protocol import TaskDefinition


def _make_run_id() -> str:
    return f"run_{datetime.now().strftime('%Y%m%d_%H%M%S')}"


def run_slurm_pipeline(
    task: TaskDefinition,
    args: argparse.Namespace,
    logger: logging.Logger,
) -> None:
    """Build the image, transfer it, submit slurm jobs, then run the primary
    coordinator. Validates the required `--multi-computer slurm` flags before
    delegating.
    """
    if not args.gateway:
        logger.error("--gateway is required when --multi-computer slurm is enabled")
        return
    if not args.packaging:
        logger.error("--packaging is required when --multi-computer slurm is enabled")
        return
    if not args.slurm_root_folder:
        home = Path.home()
        suggestions = [home / "slurm", home / "BIG" / "slurm"]
        logger.error("--slurm-root-folder is required when --multi-computer slurm is enabled")
        logger.error(f"Suggested locations: {', '.join(str(s) for s in suggestions)}")
        return

    # Lazy import — avoids paying the cost when slurm mode is not used and
    # keeps the legacy coordinator quarantined behind this single call site.
    from .slurm.primary import SlurmPrimaryCoordinator
    from shared import find_matching_binaries

    sel_result = process_selection_arguments(args)
    binaries = find_matching_binaries(
        sel_result.source_dir,
        sel_result.platforms,
        sel_result.compiler,
        sel_result.compiler_versions,
        sel_result.opt_levels,
    )
    if not binaries:
        logger.warning("No binaries found to process. Coordinator will run in test mode.")

    num_secondaries = args.jobs
    run_id = _make_run_id()
    logger.info(f"Run ID: {run_id}")

    coordinator = SlurmPrimaryCoordinator(
        binaries=binaries,
        gateway_url=args.gateway,
        slurm_root_folder=args.slurm_root_folder,
        packaging_method=args.packaging,
        task_definition=task,
        task_args=args,
        run_id=run_id,
        source_dir=sel_result.source_dir,
        skip_image_build=args.skip_image_build,
        slurm_config_kwargs={
            "image_subfolder": args.slurm_image_subfolder,
            "output_subfolder": args.slurm_output_subfolder,
            "log_subfolder": args.slurm_log_subfolder,
            "notify_email": args.slurm_notify_email,
        },
    )
    coordinator.run(num_secondaries=num_secondaries)
