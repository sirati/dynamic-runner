"""Top-level SLURM pipeline driver.

Replaces the legacy `slurm.primary.SlurmPrimaryCoordinator` for the new
runner architecture: build the container image (`podman.PodmanPackaging`),
transfer it to the gateway, submit the SLURM jobs (`job_manager`), then
hand control to `dynamic_batch_rs.run_primary` to coordinate the work.

The legacy coordinator inherited a lot of QUIC-handshake / file-transfer
plumbing from `multi_computer.primary.coordinator.BaseCoordinator` —
that whole stack is now Rust-side. Python only owns the
build/transfer/submit pre-amble plus optional SSH-tunnel setup for the
reverse-connection mode (compute node → gateway → primary).
"""

from __future__ import annotations

import argparse
import asyncio
import logging
import os
import subprocess
from datetime import datetime
from pathlib import Path

from shared import find_matching_binaries, process_selection_arguments

from .gateway import create_gateway, parse_gateway_url
from .job_manager import SlurmJobManager
from .podman import PodmanPackaging
from .preparation import SlurmPreparation
from .slurm_config import SlurmConfig, validate_slurm_config
from ..task_protocol import TaskDefinition

logger = logging.getLogger(__name__)


def _make_run_id() -> str:
    return f"run_{datetime.now().strftime('%Y%m%d_%H%M%S')}"


def _validate_slurm_args(args: argparse.Namespace, log: logging.Logger) -> bool:
    """Cheap pre-flight check before we touch the gateway."""
    if not args.gateway:
        log.error("--gateway is required when --multi-computer slurm is enabled")
        return False
    if not args.packaging:
        log.error("--packaging is required when --multi-computer slurm is enabled")
        return False
    if args.packaging != "podman":
        log.error(
            f"--packaging={args.packaging!r} is not supported. "
            "Only 'podman' works in SLURM batch jobs (docker requires user-session systemd)."
        )
        return False
    if not args.slurm_root_folder:
        home = Path.home()
        suggestions = [home / "slurm", home / "BIG" / "slurm"]
        log.error("--slurm-root-folder is required when --multi-computer slurm is enabled")
        log.error(f"Suggested locations: {', '.join(str(s) for s in suggestions)}")
        return False
    return True


def _make_slurm_config(args: argparse.Namespace) -> SlurmConfig:
    root_folder: str | Path = args.slurm_root_folder
    if not isinstance(root_folder, str) or not root_folder.startswith("~"):
        root_folder = Path(args.slurm_root_folder)
    return SlurmConfig(
        root_folder=root_folder,
        image_subfolder=args.slurm_image_subfolder,
        output_subfolder=args.slurm_output_subfolder,
        log_subfolder=args.slurm_log_subfolder,
        notify_email=args.slurm_notify_email,
    )


def run_slurm_pipeline(
    task: TaskDefinition,
    args: argparse.Namespace,
    log: logging.Logger,
) -> None:
    """Build the image, transfer it, submit slurm jobs, then run the
    primary coordinator (Rust-side via `dynamic_batch_rs.run_primary`).

    Compatibility fallback: if `dynamic_batch_rs` is unavailable (e.g.
    development checkout without maturin build), the function logs an
    actionable error and exits cleanly instead of crashing inside the
    coordinator. The build/transfer/submit half still runs so the user
    can verify their gateway + image build setup.
    """
    if not _validate_slurm_args(args, log):
        return

    sel_result = process_selection_arguments(args)
    binaries = find_matching_binaries(
        sel_result.source_dir,
        sel_result.platforms,
        sel_result.compiler,
        sel_result.compiler_versions,
        sel_result.opt_levels,
    )
    if not binaries:
        log.warning("No binaries found to process. Pipeline will run in test/job-submission mode.")

    num_secondaries = args.jobs
    run_id = _make_run_id()
    log.info(f"Run ID: {run_id}")

    # Set up gateway + slurm config
    log.info("Connecting to gateway...")
    gateway_config = parse_gateway_url(args.gateway)
    gateway = create_gateway(gateway_config)
    gateway.connect()

    slurm_config = _make_slurm_config(args)
    try:
        validate_slurm_config(slurm_config, gateway)
    except ValueError:
        log.info(f"Creating SLURM root directory: {slurm_config.root_folder}")
        gateway.create_directory(slurm_config.root_folder)

    # Reverse-connection mode: when the gateway forbids public port
    # forwarding (GatewayPorts off), we tunnel from primary → each
    # secondary via the gateway instead.
    use_reverse_connection = (
        hasattr(gateway, "gateway_ports_enabled") and gateway.gateway_ports_enabled is False
    )
    if use_reverse_connection:
        log.info("Gateway disallows public port forwarding; switching to SSH ProxyJump tunnel mode.")

    # Clean up any leftover SSH tunnels from previous runs.
    subprocess.run(
        ["pkill", "-u", str(os.getuid()), "-f", "ssh.*-L.*localhost"],
        stderr=subprocess.DEVNULL,
    )

    # Build + transfer images, then submit slurm jobs, then (if reverse
    # mode) wait for tunnels to establish.
    packaging = PodmanPackaging()
    job_manager = SlurmJobManager(gateway, slurm_config, packaging)

    primary_quic_port = _pick_free_local_port()
    cert_dir = Path("/tmp") / f"db-runner-cert-{run_id}"
    cert_dir.mkdir(parents=True, exist_ok=True)

    preparation = SlurmPreparation(
        slurm_config=slurm_config,
        job_manager=job_manager,
        gateway=gateway,
        use_reverse_connection=use_reverse_connection,
        run_id=run_id,
    )

    try:
        prep_result = asyncio.run(
            preparation.prepare(
                num_secondaries=num_secondaries,
                quic_port=primary_quic_port,
                primary_quic_port=primary_quic_port,
                cert_dir=cert_dir,
                skip_image_build=args.skip_image_build,
            )
        )
        log.info(f"SLURM jobs submitted; run_id={prep_result.run_id}")

        _drive_rust_primary(task, args, prep_result, primary_quic_port, log)
    finally:
        preparation.cleanup()
        subprocess.run(
            ["pkill", "-u", str(os.getuid()), "-f", "ssh.*-L.*localhost"],
            stderr=subprocess.DEVNULL,
        )
        gateway.disconnect()


def _pick_free_local_port() -> int:
    """Bind to port 0 to let the OS pick a free port, then close it. The
    SLURM jobs will dial back into this number once the Rust runner
    re-binds it."""
    import socket

    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.bind(("0.0.0.0", 0))
        return sock.getsockname()[1]


def _drive_rust_primary(
    task: TaskDefinition,
    args: argparse.Namespace,
    prep_result,
    primary_quic_port: int,
    log: logging.Logger,
) -> None:
    """Hand the run over to the Rust primary coordinator.

    The SLURM jobs already spawned the secondaries, so the
    `spawn_secondary` callback is a no-op (returns None: Python doesn't
    own the secondary processes; SLURM does).
    """
    try:
        import dynamic_batch_rs as _rs
    except ImportError:
        log.error(
            "dynamic_batch_rs is not installed; cannot run the primary coordinator. "
            "Install it via: cd rust/dynamic_batch/crates/db_python_provider && maturin develop --release"
        )
        log.warning(
            "Build/transfer/submit completed successfully — your SLURM jobs are running. "
            "Re-invoke once dynamic_batch_rs is available, or use the legacy --use-python-backend "
            "flag with a previous release for end-to-end coordination."
        )
        return

    sel_result = process_selection_arguments(args)
    binaries = find_matching_binaries(
        sel_result.source_dir,
        sel_result.platforms,
        sel_result.compiler,
        sel_result.compiler_versions,
        sel_result.opt_levels,
    )

    def _slurm_already_spawned(_primary_url: str, _secondary_id: str, _quic_port: int):
        # SLURM did the actual spawning; the Rust runner's spawn_secondary
        # callback isn't responsible for any subprocess. Returning None tells
        # the Rust side it doesn't own a process to clean up at the end.
        return None

    # Construct the coordinator pyclass directly (rather than the
    # `run_primary` free function) so we can pre-stage every binary on
    # every secondary before the coordinator's run-loop starts assigning
    # work. The coordinator flushes these notifications once secondary
    # connections are established and before TaskAssignment dispatch.
    coord = _rs.RustPrimaryCoordinator(
        prep_result.num_secondaries,
        task,
        _slurm_already_spawned,
        distributed_config=None,
    )

    source_root = Path(sel_result.source_dir)
    for binary in binaries:
        try:
            rel = str(Path(binary.path).relative_to(source_root))
        except ValueError:
            # Binary lives outside source_root (e.g. absolute path scan).
            # Fall back to the full path; the secondary's StageFile
            # handler treats absolute src_path as out-of-band staged.
            rel = str(binary.path)
        file_hash = _rs.compute_task_hash(binary)
        for i in range(prep_result.num_secondaries):
            sec_id = f"secondary-{i}"
            coord.notify_stage_file(sec_id, file_hash, rel, rel)

    log.info(
        "Queued %d StageFile notifications across %d secondaries; starting coordinator",
        len(binaries),
        prep_result.num_secondaries,
    )
    coord.run(binaries)
    log.info(f"Completed: {coord.completed}")
    log.info(f"Failed: {coord.failed}")
