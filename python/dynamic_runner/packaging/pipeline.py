"""Top-level SLURM pipeline driver.

Replaces the legacy `slurm.primary.SlurmPrimaryCoordinator` for the new
runner architecture: build the container image (`podman.PodmanPackaging`),
transfer it to the gateway, submit the SLURM jobs (`job_manager`), then
hand control to `dynamic_runner.run_primary` to coordinate the work.

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

from .._shared import process_selection_arguments

from ..deployment_spec import TaskDeploymentSpec
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


def _make_slurm_config(args: argparse.Namespace, gateway: object) -> SlurmConfig:
    """Build the SlurmConfig with `~` expanded against the gateway's remote home.

    Expanding once at this entry point means every downstream path
    constructor (`get_image_dir`, `get_log_dir`, …) and every shell
    command emitted from `job_manager` / `layered_transfer` sees an
    absolute path. Without this, `shlex.quote("~/...")` single-quotes
    the path so bash never tilde-expands it, and `mkdir -p` creates a
    literal `~` directory under `$HOME` while `scp` (which expands `~`
    server-side) targets the absolute path — the two paths diverge
    and uploads land in the wrong place.
    """
    root = str(args.slurm_root_folder)
    remote_home = getattr(gateway, "remote_home", None)
    if root.startswith("~") and remote_home:
        root = root.replace("~", str(remote_home), 1)
    overrides: dict[str, object] = {}
    if getattr(args, "slurm_time_limit", None):
        overrides["time_limit"] = args.slurm_time_limit
    return SlurmConfig(
        root_folder=Path(root),
        image_subfolder=args.slurm_image_subfolder,
        output_subfolder=args.slurm_output_subfolder,
        log_subfolder=args.slurm_log_subfolder,
        notify_email=args.slurm_notify_email,
        **overrides,
    )


def run_slurm_pipeline(
    task: TaskDefinition,
    args: argparse.Namespace,
    deployment: TaskDeploymentSpec,
    log: logging.Logger,
) -> None:
    """Build the image, transfer it, submit slurm jobs, then run the
    primary coordinator (Rust-side via `dynamic_runner.run_primary`).

    Compatibility fallback: if `dynamic_runner` is unavailable (e.g.
    development checkout without maturin build), the function logs an
    actionable error and exits cleanly instead of crashing inside the
    coordinator. The build/transfer/submit half still runs so the user
    can verify their gateway + image build setup.
    """
    if not _validate_slurm_args(args, log):
        return

    sel_result = process_selection_arguments(args)
    # Item discovery is the task's concern under the post-phases-
    # redesign Protocol; framework no longer scans. We discover ONCE
    # here and pass the same list down into `_drive_rust_primary` —
    # avoids the double-scan that previously could disagree if the
    # underlying source directory changed mid-run.
    binaries = list(task.discover_items(sel_result.source_dir, args))
    if not binaries:
        log.warning("No items discovered. Pipeline will run in test/job-submission mode.")

    num_secondaries = args.jobs
    run_id = _make_run_id()
    log.info(f"Run ID: {run_id}")

    # Set up gateway + slurm config.
    #
    # The QUIC port and the SSH -R forward have to be configured BEFORE
    # `gateway.connect()`: SSHGateway.connect() reads `forwarded_ports`
    # to build its `-R 0.0.0.0:remote:localhost:local` flags, and
    # `_check_gateway_ports()` (which decides whether to fall back to
    # reverse-connection mode) short-circuits when `forwarded_ports`
    # is empty. So a port-pick + setup_port_forwarding has to happen
    # before connect — otherwise no listener exists on the gateway,
    # secondaries get "Connection refused" dialing the gateway FQDN,
    # AND the reverse-connection fallback never fires either.
    log.info("Connecting to gateway...")
    gateway_config = parse_gateway_url(args.gateway)
    gateway = create_gateway(gateway_config)

    primary_quic_port = _pick_free_local_port()
    gateway.setup_port_forwarding(primary_quic_port, primary_quic_port)

    # Consumer-supplied extra `-R local:gateway` forwards. Same
    # ControlMaster, same `gateway.connect()` — the framework only
    # needs to know the (local, gateway) port pairs; what's actually
    # listening on `localhost:local` and who connects to
    # `<gateway-host>:gateway` are the consumer's concerns. Avoids
    # spawning a parallel SSHGateway from consumer code (which
    # would duplicate auth and fight SIGHUP semantics on shutdown).
    for local_port, gateway_port in deployment.extra_port_forwards:
        gateway.setup_port_forwarding(local_port, gateway_port)

    gateway.connect()

    slurm_config = _make_slurm_config(args, gateway)
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
        ["pkill", "-u", str(os.getuid()), "-f", "ssh.*-R.*localhost"],
        stderr=subprocess.DEVNULL,
    )

    # Build + transfer images, then submit slurm jobs, then (if reverse
    # mode) wait for tunnels to establish.
    packaging = PodmanPackaging(deployment=deployment)
    job_manager = SlurmJobManager(gateway, slurm_config, packaging, deployment)

    cert_dir = Path("/tmp") / f"db-runner-cert-{run_id}"
    cert_dir.mkdir(parents=True, exist_ok=True)

    preparation = SlurmPreparation(
        slurm_config=slurm_config,
        job_manager=job_manager,
        gateway=gateway,
        deployment=deployment,
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

        _drive_rust_primary(task, args, prep_result, primary_quic_port, binaries, log)
    finally:
        preparation.cleanup()
        subprocess.run(
            ["pkill", "-u", str(os.getuid()), "-f", "ssh.*-R.*localhost"],
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
    binaries: list,
    log: logging.Logger,
) -> None:
    """Hand the run over to the Rust primary coordinator.

    `binaries` is the already-discovered item list from
    `run_slurm_pipeline` — passed through rather than re-discovered
    so both halves see the exact same set (avoids divergence between
    the count we logged earlier and the count the coordinator
    actually queues StageFile notifications for).

    The SLURM jobs already spawned the secondaries, so the
    `spawn_secondary` callback is a no-op (returns None: Python doesn't
    own the secondary processes; SLURM does).
    """
    try:
        import dynamic_runner as _rs
    except ImportError:
        log.error(
            "dynamic_runner is not installed; cannot run the primary coordinator. "
            "Install it via: cd rust/dynamic_batch/crates/db_python_provider && maturin develop --release"
        )
        log.warning(
            "Build/transfer/submit completed successfully — your SLURM jobs are running. "
            "Re-invoke once dynamic_runner is available, or use the legacy --use-python-backend "
            "flag with a previous release for end-to-end coordination."
        )
        return

    sel_result = process_selection_arguments(args)

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
        listen_port=primary_quic_port,
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
        # Content hash: SHA256 of the file the secondary will land at
        # `<src_tmp>/<rel>` after copying from `<src_network>/<rel>`.
        # `compute_task_hash` is path/identifier-derived (cache key);
        # `compute_file_content_hash` reads the file once on the
        # primary side. Computing here means we hit the local source
        # tree once per binary — pre-staging step on the user side
        # has already deposited identical content on the gateway, so
        # the secondary's verification matches.
        content_hash = _rs.compute_file_content_hash(str(binary.path))
        for i in range(prep_result.num_secondaries):
            sec_id = f"secondary-{i}"
            coord.notify_stage_file(sec_id, file_hash, content_hash, rel, rel)

    log.info(
        "Queued %d StageFile notifications across %d secondaries; starting coordinator",
        len(binaries),
        prep_result.num_secondaries,
    )
    coord.run(binaries)
    log.info(f"Completed: {coord.completed}")
    log.info(f"Failed: {coord.failed}")
