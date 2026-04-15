"""Generic CLI entry point for dynamic batch processing.

Task-specific packages call `run()` with their TaskDefinition instance
and an optional spawn_secondary factory.
"""

import argparse
import logging
from collections.abc import Callable
from datetime import datetime
from pathlib import Path

from shared import (
    add_selection_arguments,
    filter_existing_outputs,
    find_matching_binaries,
    format_binary_info,
    normalize_opt_levels,
    print_selection_summary,
    process_selection_arguments,
)

from .system_resources import parse_cores, parse_memory
from .task import TaskDefinition


def _try_import_rust(class_name: str):
    """Try to import a class from dynamic_batch_rs. Returns (cls, True) or (None, False)."""
    try:
        import dynamic_batch_rs

        return getattr(dynamic_batch_rs, class_name), True
    except (ImportError, AttributeError):
        return None, False


def _check_rust_backend(class_name: str, args) -> tuple:
    """Check Rust backend availability with proper error handling.

    Returns (cls_or_None, is_available).
    """
    if args.use_python_backend:
        return None, False

    cls, available = _try_import_rust(class_name)
    if not available and args.use_rust_backend:
        logging.getLogger().error(
            "Rust backend not available. Install it with: "
            "cd rust/dynamic_batch/crates/db_python_provider && maturin develop --release"
        )
        return None, None  # sentinel: hard error
    return cls, available


def _collect_binaries(args, task: TaskDefinition, full_args: bool = True):
    """Scan and filter binaries based on selection arguments.

    Returns (binaries, sel_result) or None on empty.
    """
    logger = logging.getLogger()
    logger.info("Collecting binaries from source directory...")
    sel_result = process_selection_arguments(args)

    if full_args:
        binaries = find_matching_binaries(
            sel_result.source_dir,
            sel_result.platforms,
            sel_result.compiler,
            sel_result.compiler_versions,
            sel_result.opt_levels,
            sel_result.file_format,
            sel_result.version_regex,
            sel_result.opt_regex,
            sel_result.name_regex,
            sel_result.exclude_subfolders,
        )
    else:
        binaries = find_matching_binaries(
            sel_result.source_dir,
            sel_result.platforms,
            sel_result.compiler,
            sel_result.compiler_versions,
            sel_result.opt_levels,
        )

    if args.skip_existing:
        binaries, _ = filter_existing_outputs(
            binaries, sel_result.source_dir, sel_result.output_dir, task.get_output_filename_pattern
        )

    logger.info(f"Found {len(binaries)} binaries to process")
    return binaries, sel_result


def _make_run_id() -> str:
    return f"run_{datetime.now().strftime('%Y%m%d_%H%M%S')}"


def _setup_logging(args_list: list[str]) -> logging.Logger:
    """Set up logging based on early argument parsing."""
    import sys

    parser = argparse.ArgumentParser(add_help=False)
    parser.add_argument("--debug", action="store_true")
    parser.add_argument("--raw-logs", action="store_true")

    help_requested = "-h" in args_list or "--help" in args_list

    if not help_requested:
        early_args, _ = parser.parse_known_args(args_list)

        if "--secondary" in args_list:
            prefix = "S|"
        elif "--multi-computer" in args_list or "--slurm" in args_list:
            prefix = "P|"
        else:
            prefix = ""

        log_level = logging.DEBUG if early_args.debug else logging.INFO
        logger = logging.getLogger()
        logger.setLevel(log_level)

        if early_args.raw_logs:
            log_format = f"{prefix}%(message)s"
            logging.basicConfig(level=log_level, format=log_format)
        else:
            if prefix:
                log_format = f"%(levelname)s | %(asctime)s |{prefix}| %(message)s"
            else:
                log_format = "%(levelname)s | %(asctime)s | %(message)s"
            logging.basicConfig(level=log_level, format=log_format, datefmt="%H:%M:%S")
    else:
        logging.basicConfig(
            level=logging.INFO,
            format="%(levelname)s | %(asctime)s | %(message)s",
            datefmt="%H:%M:%S",
        )
        logger = logging.getLogger()

    return logger


def _add_arguments(parser: argparse.ArgumentParser) -> None:
    """Add all generic dynamic_batch arguments to the parser."""
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
        help="Number of cores to use. Can be int, +int (add to available), or -int (subtract from available). "
        "Default: all available cores",
    )
    parser.add_argument(
        "--max-memory",
        type=str,
        default="-2G",
        help="Maximum memory to use (e.g., 16G, 8192M). Can use +/- prefix for relative to available memory.",
    )
    parser.add_argument("--skip-existing", action="store_true", help="Skip binaries that already have output files")
    parser.add_argument("--always-restart-worker", action="store_true", help="Restart worker after each completed task")
    parser.add_argument("--pid", action="store_true", help="Print worker PIDs when (re)started")
    parser.add_argument(
        "--manual-start-worker", action="store_true", help="Manually start worker processes (print command and wait)"
    )
    parser.add_argument(
        "--connection-mode",
        type=str,
        choices=["socketpair", "named"],
        default=None,
        help="Connection mode: 'socketpair' uses socketpair() (default), 'named' uses named Unix domain sockets",
    )
    parser.add_argument(
        "--socket-dir",
        type=str,
        help="Directory for named socket files (defaults to <output>/sockets when --manual-start-worker is used)",
    )
    parser.add_argument(
        "--simulate-errors",
        type=float,
        metavar="PERCENTAGE",
        help="Simulate random worker error on a task with given percentage chance (0-100)",
    )

    # SLURM / distributed arguments
    parser.add_argument(
        "--secondary",
        type=str,
        help="Run in secondary mode, connecting to primary at specified URL (e.g., tcp://host:port)",
    )
    parser.add_argument(
        "--secondary-id", type=str, help="Unique identifier for this secondary (required with --secondary)"
    )
    parser.add_argument(
        "--secondary-quic-port",
        type=int,
        default=0,
        help="Port for QUIC server to listen on (0 = let OS pick, default: 0)",
    )
    parser.add_argument(
        "--gateway", type=str, help="Gateway for SLURM controller. Use 'local' or 'ssh://user@host[:port]'"
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
        help="Packaging method for SLURM deployment (required with --multi-computer slurm). Use 'podman' for SLURM clusters.",
    )
    parser.add_argument(
        "--slurm-root-folder",
        type=str,
        help="Root folder for SLURM operations on gateway (required with --multi-computer slurm)",
    )
    parser.add_argument("--slurm-notify-email", type=str, help="Email address for SLURM job notifications")
    parser.add_argument(
        "--slurm-image-subfolder", type=str, default="image_bin", help="Subdirectory for Docker images (default: image_bin)"
    )
    parser.add_argument(
        "--slurm-output-subfolder", type=str, default="out", help="Subdirectory for output files (default: out)"
    )
    parser.add_argument(
        "--slurm-log-subfolder", type=str, default="log", help="Subdirectory for log files (default: log)"
    )
    parser.add_argument(
        "--slurm-test-job",
        action="store_true",
        help="Submit a test SLURM job to validate Docker image loading (requires --multi-computer slurm)",
    )
    parser.add_argument("--jobs", type=int, default=1, help="Number of SLURM secondary nodes to spawn (default: 1)")
    parser.add_argument(
        "--skip-image-build",
        action="store_true",
        help="Skip building and transferring Docker image (assumes image already exists on gateway)",
    )
    parser.add_argument(
        "--test-master-slave",
        action="store_true",
        help="Test master/slave architecture locally without networking (uses local_submissive and local_authoritative)",
    )
    parser.add_argument(
        "--test-master-slave-netsim",
        action="store_true",
        help="Test master/slave with network simulation (uses message queues to verify network protocol compatibility)",
    )
    parser.add_argument(
        "--use-rust-backend",
        action="store_true",
        help="Use the Rust-based local manager (default when dynamic_batch_rs is installed; kept for compatibility)",
    )
    parser.add_argument(
        "--use-python-backend",
        action="store_true",
        help="Force the Python-based local manager instead of the Rust backend",
    )
    parser.add_argument(
        "--use-rust-distributed-backend",
        action="store_true",
        help="(Deprecated) Use --multi-computer single-process --use-rust-backend instead",
    )


def _run_secondary(task: TaskDefinition, args, logger) -> None:
    """Handle secondary mode (SLURM compute nodes or local test)."""
    import tempfile

    import psutil

    if not args.secondary_id:
        logger.error("--secondary-id is required when running in secondary mode")
        return

    in_docker = Path("/app").exists() and Path("/.dockerenv").exists()

    if in_docker:
        logger.info("=" * 60)
        logger.info("SECONDARY MODE (SLURM Compute Node)")
        logger.info("=" * 60)
        src_tmp = Path("/app/src-tmp")
        out_tmp = Path("/app/out-tmp")
        log_tmp = Path("/app/log-tmp")
        src_network = Path("/app/src-network")
        out_network = Path("/app/out-network")
        log_network = Path("/app/log-network")
        socket_dir = Path("/app/sockets")
    else:
        logger.info("=" * 60)
        logger.info("SECONDARY MODE (Local Test)")
        logger.info("=" * 60)
        temp_dir = Path(tempfile.mkdtemp(prefix=f"secondary-{args.secondary_id}-"))
        src_tmp = temp_dir / "src-tmp"
        out_tmp = temp_dir / "out-tmp"
        log_tmp = temp_dir / "log-tmp"
        src_network = temp_dir / "src-network"
        out_network = temp_dir / "out-network"
        log_network = temp_dir / "log-network"
        socket_dir = temp_dir / "sockets"

    logger.info(f"Secondary ID: {args.secondary_id}")
    logger.info(f"Primary URL: {args.secondary}")
    logger.info(f"QUIC Port: {args.secondary_quic_port if args.secondary_quic_port else 'auto'}")

    ram_bytes = psutil.virtual_memory().total
    num_workers = psutil.cpu_count(logical=False) or 4

    RustSecondaryCoordinator, rust_available = _check_rust_backend("RustSecondaryCoordinator", args)
    if rust_available is None:
        return

    if rust_available:
        logger.info("Using Rust secondary coordinator backend")
        secondary = RustSecondaryCoordinator(
            primary_url=args.secondary,
            secondary_id=args.secondary_id,
            num_workers=num_workers,
            ram_bytes=ram_bytes,
            source_dir=str(src_tmp),
            output_dir=str(out_tmp),
            task_definition=task,
            task_args=args,
            skip_existing=args.skip_existing,
        )
    else:
        from .multi_computer.secondary import SecondaryCoordinator

        logger.info("Using Python secondary coordinator backend")
        secondary = SecondaryCoordinator(
            primary_url=args.secondary,
            secondary_id=args.secondary_id,
            num_workers=num_workers,
            ram_bytes=ram_bytes,
            src_tmp=src_tmp,
            out_tmp=out_tmp,
            log_tmp=log_tmp,
            src_network=src_network,
            out_network=out_network,
            log_network=log_network,
            socket_dir=socket_dir,
            task_definition=task,
            task_args=args,
            skip_existing=args.skip_existing,
            quic_port=args.secondary_quic_port,
        )

    secondary.run()


def _run_slurm(task: TaskDefinition, args, logger) -> None:
    """Handle SLURM distributed mode."""
    from .slurm.primary import SlurmPrimaryCoordinator

    logger.info("=" * 60)
    logger.info("SLURM DISTRIBUTED MODE")
    logger.info("=" * 60)

    binaries, sel_result = _collect_binaries(args, task, full_args=False)

    if len(binaries) == 0:
        logger.warning("No binaries found to process. Coordinator will run in test mode.")

    num_secondaries = args.jobs
    logger.info(f"Starting coordinator with {num_secondaries} secondaries")

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

    try:
        coordinator.run(num_secondaries=num_secondaries)
    finally:
        pass


def _run_multi_computer_local(
    task: TaskDefinition,
    args,
    logger,
    spawn_secondary_factory: Callable | None,
) -> None:
    """Handle multi-computer local mode."""
    from .multi_computer.test_network.primary import LocalTestPrimaryCoordinator

    logger.info("=" * 60)
    logger.info("LOCAL MULTI-COMPUTER MODE (Testing)")
    logger.info("=" * 60)

    binaries, sel_result = _collect_binaries(args, task)

    if len(binaries) == 0:
        logger.warning("No binaries found to process.")
        return

    num_secondaries = args.jobs
    logger.info(f"Starting coordinator with {num_secondaries} local secondaries")

    RustPrimaryCoordinator, rust_available = _check_rust_backend("RustPrimaryCoordinator", args)
    if rust_available is None:
        return

    if rust_available:
        logger.info("Using Rust primary coordinator backend")

        spawn_secondary = spawn_secondary_factory(args) if spawn_secondary_factory else None
        if spawn_secondary is None:
            logger.error("spawn_secondary_factory is required for Rust primary coordinator")
            return

        coordinator = RustPrimaryCoordinator(
            num_secondaries=num_secondaries,
            task_definition=task,
            spawn_secondary=spawn_secondary,
        )

        coordinator.run(binaries)

        logger.info(f"Completed: {coordinator.completed}")
        logger.info(f"Failed: {coordinator.failed}")
    else:
        logger.info("Using Python primary coordinator backend")

        run_id = _make_run_id()
        logger.info(f"Run ID: {run_id}")

        coordinator = LocalTestPrimaryCoordinator(
            binaries=binaries,
            task_definition=task,
            task_args=args,
            run_id=run_id,
            source_dir=sel_result.source_dir,
            raw_logs=args.raw_logs,
        )

        try:
            coordinator.run(num_secondaries=num_secondaries)
        finally:
            pass


def _run_single_process(task: TaskDefinition, args, logger) -> None:
    """Handle single-process multi-computer mode."""
    binaries, sel_result = _collect_binaries(args, task)

    if len(binaries) == 0:
        logger.warning("No binaries found to process.")
        return

    num_secondaries = args.jobs if args.jobs else 1
    num_cores = parse_cores(args.cores)
    max_memory = parse_memory(args.max_memory)
    workers_per_secondary = num_cores // num_secondaries if num_secondaries > 0 else num_cores
    ram_per_secondary = max_memory // num_secondaries if num_secondaries > 0 else max_memory

    RustDistributedManager, rust_available = _check_rust_backend("RustDistributedManager", args)
    if rust_available is None:
        return

    if rust_available:
        logger.info("=" * 60)
        logger.info("SINGLE-PROCESS MULTI-COMPUTER MODE (Rust)")
        logger.info("=" * 60)

        logger.info(f"Secondaries: {num_secondaries}")
        logger.info(f"Workers per secondary: {workers_per_secondary}")
        logger.info(f"RAM per secondary: {ram_per_secondary / (1024**3):.2f}GB")

        rust_dm = RustDistributedManager(
            num_secondaries=num_secondaries,
            num_workers_per_secondary=workers_per_secondary,
            ram_per_secondary=ram_per_secondary,
            source_dir=str(sel_result.source_dir),
            output_dir=str(sel_result.output_dir),
            task_definition=task,
            task_args=args,
            skip_existing=args.skip_existing,
        )

        rust_dm.run(binaries)

        logger.info(f"Completed: {rust_dm.completed}")
        logger.info(f"Failed: {rust_dm.failed}")
    else:
        if not args.use_python_backend:
            logger.info("Backend: Python (Rust not available, falling back)")
        else:
            logger.info("Backend: Python (explicitly selected)")

        logger.info("=" * 60)
        logger.info("SINGLE-PROCESS MULTI-COMPUTER MODE (Testing)")
        logger.info("=" * 60)

        logger.info(f"Starting coordinator with {num_secondaries} in-process secondaries")

        run_id = _make_run_id()
        logger.info(f"Run ID: {run_id}")

        from .multi_computer.test_single_process import SingleProcessPrimaryCoordinator

        coordinator = SingleProcessPrimaryCoordinator(
            binaries=binaries,
            task_definition=task,
            task_args=args,
            run_id=run_id,
            source_dir=sel_result.source_dir,
            output_dir=sel_result.output_dir,
            num_workers_per_secondary=workers_per_secondary,
        )

        try:
            coordinator.run(num_secondaries=num_secondaries)
        finally:
            pass


def _run_distributed_rust(task: TaskDefinition, args, config, sorted_binaries, logger, num_cores, max_memory) -> None:
    """Run RustDistributedManager (shared by deprecated --use-rust-distributed-backend and test modes)."""
    RustDistributedManager, rust_available = _check_rust_backend("RustDistributedManager", args)
    if rust_available is None:
        return None, None

    if not rust_available:
        return None, False

    num_secondaries = args.jobs if args.jobs else 1
    workers_per_secondary = num_cores // num_secondaries if num_secondaries > 0 else num_cores
    ram_per_secondary = max_memory // num_secondaries if num_secondaries > 0 else max_memory

    logger.info(f"Secondaries: {num_secondaries}")
    logger.info(f"Workers per secondary: {workers_per_secondary}")
    logger.info(f"RAM per secondary: {ram_per_secondary / (1024**3):.2f}GB")

    rust_dm = RustDistributedManager(
        num_secondaries=num_secondaries,
        num_workers_per_secondary=workers_per_secondary,
        ram_per_secondary=ram_per_secondary,
        source_dir=str(config.source_dir),
        output_dir=str(config.output_dir),
        task_definition=task,
        task_args=args,
        skip_existing=args.skip_existing,
    )

    rust_dm.run(sorted_binaries)

    logger.info(f"Completed: {rust_dm.completed}")
    logger.info(f"Failed: {rust_dm.failed}")
    return rust_dm, True


def _run_local(
    task: TaskDefinition,
    args,
    config,
    sorted_binaries,
    logger,
    num_cores,
    max_memory,
    spawn_secondary_factory: Callable | None,
) -> None:
    """Handle standard local processing and test modes."""
    if args.test_master_slave_netsim:
        RustDistributedManager, rust_available = _check_rust_backend("RustDistributedManager", args)
        if rust_available is None:
            return

        if rust_available:
            logger.info("=" * 60)
            logger.info("TEST MASTER-SLAVE NETWORK SIMULATION (Rust)")
            logger.info("=" * 60)

            _run_distributed_rust(task, args, config, sorted_binaries, logger, num_cores, max_memory)
        else:
            from .worker_manager.test_network_sim import run_network_sim_test

            logger.info("=" * 60)
            logger.info("TEST MASTER-SLAVE NETWORK SIMULATION")
            logger.info("=" * 60)
            logger.info("Testing submissive/authoritative coordination via network message queues")
            logger.info("")

            run_network_sim_test(
                binaries=sorted_binaries,
                task_definition=task,
                task_args=args,
                source_dir=config.source_dir,
                output_dir=config.output_dir,
                num_cores=num_cores,
                max_memory=max_memory,
            )

    elif args.test_master_slave:
        RustDistributedManager, rust_available = _check_rust_backend("RustDistributedManager", args)
        if rust_available is None:
            return

        if rust_available:
            logger.info("=" * 60)
            logger.info("TEST MASTER-SLAVE MODE (Rust)")
            logger.info("=" * 60)

            _run_distributed_rust(task, args, config, sorted_binaries, logger, num_cores, max_memory)
        else:
            from .worker_manager import ActualAuthoritativeWorkerManager, ActualSubmissiveWorkerManager

            logger.info("=" * 60)
            logger.info("TEST MASTER-SLAVE MODE (Local)")
            logger.info("=" * 60)
            logger.info("Using local_submissive + local_authoritative architecture")
            logger.info("")

            def request_task_callback(worker_id: int) -> None:
                result = authoritative_manager.handle_task_request(worker_id)
                if result:
                    binary, estimated_memory = result
                    submissive_manager.assign_task_from_authoritative(worker_id, binary, estimated_memory)

            submissive_manager = ActualSubmissiveWorkerManager(
                num_workers=num_cores,
                max_memory=max_memory,
                source_dir=config.source_dir,
                output_dir=config.output_dir,
                task_definition=task,
                task_args=args,
                skip_existing=args.skip_existing,
                request_task_callback=request_task_callback,
                manual_start_worker=args.manual_start_worker,
                connection_mode=args.connection_mode,
                socket_dir=Path(args.socket_dir) if args.socket_dir else None,
            )

            submissive_manager.initialize_workers_only()

            authoritative_manager = ActualAuthoritativeWorkerManager(
                num_workers=num_cores,
                max_memory=max_memory,
                log_dir=config.output_dir,
                task_definition=task,
                submissive_managers=[submissive_manager],
            )

            authoritative_manager.pending_binaries = sorted_binaries.copy()
            authoritative_manager.stats["total"] = len(sorted_binaries)
            authoritative_manager.stats["completed"] = 0
            authoritative_manager.stats["errored"] = 0

            logger.info(f"Starting {num_cores} workers with {max_memory / (1024**3):.2f}GB memory limit")
            logger.info(f"Processing {len(sorted_binaries)} binaries")

            authoritative_manager._initialize_workers()
            authoritative_manager._run_initial_assignments()
            authoritative_manager._run_main_phase()
            authoritative_manager._run_retry_phase()
            authoritative_manager._run_oom_phase()
            authoritative_manager._run_unassigned_phase()

            for worker in authoritative_manager.workers:
                if worker.is_alive():
                    try:
                        worker.terminate()
                        logger.info(f"[Worker {worker.worker_id}] Stopping (all phases complete)")
                    except Exception:
                        pass
    else:
        # Standard local processing
        RustLocalManager, rust_available = _check_rust_backend("RustLocalManager", args)
        if rust_available is None:
            return

        if rust_available:
            logger.info("Backend: Rust (dynamic_batch_rs)")

            rust_manager = RustLocalManager(
                num_workers=num_cores,
                max_memory=max_memory,
                source_dir=str(config.source_dir),
                output_dir=str(config.output_dir),
                task_definition=task,
                task_args=args,
                skip_existing=args.skip_existing,
                always_restart_worker=args.always_restart_worker,
                print_pid=args.pid,
                connection_mode=args.connection_mode,
                socket_dir=args.socket_dir,
                manual_start_worker=args.manual_start_worker,
            )

            rust_manager.process_binaries(sorted_binaries)

            stats = rust_manager.stats
            logger.info(f"Completed: {stats.completed}/{stats.total}")
            logger.info(f"Errored: {stats.errored}")
            if rust_manager.failed_tasks:
                logger.warning(f"Failed tasks: {len(rust_manager.failed_tasks)}")
                for ft in rust_manager.failed_tasks:
                    logger.warning(f"  {ft.binary.path}: {ft.error_type}: {ft.error_message}")
            if rust_manager.oom_tasks:
                logger.warning(f"OOM tasks: {len(rust_manager.oom_tasks)}")
                for ot in rust_manager.oom_tasks:
                    logger.warning(f"  {ot.binary.path}: {ot.error_message}")
        else:
            from .worker_manager import LocalWorkerManager

            if not args.use_python_backend:
                logger.info("Backend: Python (Rust not available, falling back)")
            else:
                logger.info("Backend: Python (explicitly selected)")

            manager = LocalWorkerManager(
                num_workers=num_cores,
                max_memory=max_memory,
                source_dir=config.source_dir,
                output_dir=config.output_dir,
                task_definition=task,
                task_args=args,
                skip_existing=args.skip_existing,
                print_pid=args.pid,
                always_restart_worker=args.always_restart_worker,
                manual_start_worker=args.manual_start_worker,
                connection_mode=args.connection_mode,
                socket_dir=Path(args.socket_dir) if args.socket_dir else None,
            )

            manager.process_binaries(sorted_binaries)


def run(
    task: TaskDefinition,
    spawn_secondary_factory: Callable[[argparse.Namespace], Callable] | None = None,
    description: str = "Dynamic batch processing with memory-aware parallel execution",
) -> None:
    """Run the dynamic batch processing CLI.

    Args:
        task: TaskDefinition instance for the specific task.
        spawn_secondary_factory: Optional factory that receives parsed args and returns
            a callable ``spawn_secondary(primary_url, secondary_id, quic_port) -> Popen``.
            Required for Rust primary coordinator in multi-computer local mode.
        description: Description for the argparse help text.
    """
    import sys

    logger = _setup_logging(sys.argv[1:])

    parser = argparse.ArgumentParser(description=description)
    _add_arguments(parser)
    task.add_task_arguments(parser)

    args = parser.parse_args()

    # Handle secondary mode early
    if args.secondary:
        _run_secondary(task, args, logger)
        return

    # Handle backward compatibility: --slurm maps to --multi-computer slurm
    if args.slurm and not args.multi_computer:
        args.multi_computer = "slurm"
        logger.warning("--slurm is deprecated, use --multi-computer slurm instead")

    # Validate multi-computer arguments
    if args.multi_computer == "slurm":
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

    # Default to named mode when manual-start-worker is used
    if args.connection_mode is None:
        args.connection_mode = "named" if args.manual_start_worker else "socketpair"

    if hasattr(args, "debugs") and args.debugs:
        args.pid = True

    config = process_selection_arguments(args)

    if args.manual_start_worker and not args.socket_dir:
        args.socket_dir = str(config.output_dir / "sockets")

    if args.connection_mode == "named" and not args.socket_dir:
        logger.error("--socket-dir is required when --connection-mode=named")
        return

    # Dispatch to the appropriate mode
    if args.multi_computer == "slurm":
        _run_slurm(task, args, logger)
    elif args.multi_computer == "local":
        _run_multi_computer_local(task, args, logger, spawn_secondary_factory)
    elif args.multi_computer == "single-process":
        _run_single_process(task, args, logger)
    elif args.use_rust_distributed_backend:
        logger.warning(
            "--use-rust-distributed-backend is deprecated. "
            "Use --multi-computer single-process --use-rust-backend instead."
        )
        # Re-use single-process logic
        args.multi_computer = "single-process"
        _run_single_process(task, args, logger)
    else:
        # Standard local processing — needs binary scanning
        num_cores = parse_cores(args.cores)
        max_memory = parse_memory(args.max_memory)

        display_opt_levels = None
        if config.opt_levels:
            normalized = normalize_opt_levels(config.opt_levels, config.opt_regex)
            display_opt_levels = normalized.display_values

        print_selection_summary(config, display_opt_levels)
        logger.info(f"Cores: {num_cores}")
        logger.info(f"Max memory: {max_memory / (1024**3):.2f}GB")
        logger.info("")

        logger.info("Scanning for matching binaries...")
        binaries = find_matching_binaries(
            config.source_dir,
            config.platforms,
            config.compiler,
            config.compiler_versions,
            config.opt_levels,
            config.file_format,
            config.version_regex,
            config.opt_regex,
            config.name_regex,
            config.exclude_subfolders,
        )

        logger.info(f"Found {len(binaries)} matching binaries")

        if not binaries:
            logger.info("No binaries found matching the criteria")
            return

        if config.list_files:
            logger.info("\nMatched files:")
            for binary in binaries:
                logger.info(format_binary_info(binary, config.source_dir))
            return

        logger.info("Organizing and sorting binaries...")
        sorted_binaries = task.organize_and_sort_items(binaries)

        if args.skip_existing:
            logger.info("Filtering out binaries with existing output files...")
            sorted_binaries, skipped_count = filter_existing_outputs(
                sorted_binaries, config.source_dir, config.output_dir, task.get_output_filename_pattern
            )
            logger.info(f"Skipped {skipped_count} binaries with existing outputs")
            logger.info(f"Remaining binaries to process: {len(sorted_binaries)}")

            if not sorted_binaries:
                logger.info("No binaries to process after filtering")
                return

        _run_local(task, args, config, sorted_binaries, logger, num_cores, max_memory, spawn_secondary_factory)
