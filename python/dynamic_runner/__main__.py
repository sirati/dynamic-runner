import argparse
import logging
import os
import secrets
import subprocess
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

from .gateway import GatewayConfig, create_gateway, parse_gateway_url
from .multi_computer import ExecutionMode
from .multi_computer.test_network.primary import LocalTestPrimaryCoordinator
from .runtime_env import PackagingConfig, create_packaging_method
from .slurm import SlurmConfig, validate_slurm_config
from .slurm.job_manager import SlurmJobManager
from .slurm.primary import SlurmPrimaryCoordinator
from .system_resources import parse_cores, parse_memory
from .task import TokenizerTask
from .worker_manager import WorkerManager


def main():
    # Parse args early to check for debug flag and mode
    parser = argparse.ArgumentParser(
        description="Dynamic batch processing for binary tokenization with memory-aware parallel execution"
    )

    parser.add_argument(
        "--debug",
        action="store_true",
        help="Enable debug logging for detailed output",
    )

    parser.add_argument(
        "--raw-logs",
        action="store_true",
        help="Use raw log formatting (no level, timestamp - only prefix and message)",
    )

    # Check if help is requested - if so, we need to add all arguments first
    import sys

    help_requested = "-h" in sys.argv or "--help" in sys.argv

    if not help_requested:
        # Parse known args first to determine mode and logging flags
        early_args, _ = parser.parse_known_args()

        # Determine mode and prefix
        if "--secondary" in sys.argv:
            prefix = "S|"
        else:
            # Check if we're in primary multi-computer mode or normal mode
            if "--multi-computer" in sys.argv or "--slurm" in sys.argv:
                prefix = "P|"
            else:
                prefix = ""  # Normal local mode, no prefix

        # Set up logging based on flags
        log_level = logging.DEBUG if early_args.debug else logging.INFO
        logger = logging.getLogger()
        logger.setLevel(log_level)

        if early_args.raw_logs:
            log_format = f"{prefix}%(message)s"
            logging.basicConfig(
                level=log_level,
                format=log_format,
            )
        else:
            if prefix:
                log_format = f"%(levelname)s | %(asctime)s |{prefix}| %(message)s"
            else:
                log_format = "%(levelname)s | %(asctime)s | %(message)s"
            logging.basicConfig(
                level=log_level,
                format=log_format,
                datefmt="%H:%M:%S",
            )
    else:
        # Set up default logging for help display
        logging.basicConfig(
            level=logging.INFO,
            format="%(levelname)s | %(asctime)s | %(message)s",
            datefmt="%H:%M:%S",
        )
        logger = logging.getLogger()

    add_selection_arguments(parser)

    # Create task instance to add task-specific arguments
    task = TokenizerTask()
    task.add_task_arguments(parser)

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
        "--simulate-crash",
        type=float,
        metavar="PERCENTAGE",
        help="Simulate random worker crashes with given percentage chance (0-100)",
    )

    # SLURM distributed processing arguments
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
        help="Packaging method for SLURM deployment (required with --multi-computer slurm). Use 'podman' for SLURM clusters.",
    )

    parser.add_argument(
        "--slurm-root-folder",
        type=str,
        help="Root folder for SLURM operations on gateway (required with --multi-computer slurm)",
    )

    parser.add_argument(
        "--slurm-notify-email",
        type=str,
        help="Email address for SLURM job notifications",
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

    parser.add_argument(
        "--test-master-slave",
        action="store_true",
        help="Test master/slave architecture locally without networking (uses local_submissive and local_authoritive)",
    )

    args = parser.parse_args()

    # Handle secondary mode early - this is for SLURM compute nodes or local test
    if args.secondary:
        import socket
        import tempfile

        import psutil

        from .multi_computer.secondary import SecondaryCoordinator

        if not args.secondary_id:
            logger.error("--secondary-id is required when running in secondary mode")
            return

        # Detect if running in Docker container or locally
        in_docker = Path("/app").exists() and Path("/.dockerenv").exists()

        if in_docker:
            logger.info("=" * 60)
            logger.info("SECONDARY MODE (SLURM Compute Node)")
            logger.info("=" * 60)
            # Paths inside container
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
            # Use temporary directory for local testing
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

        # Get system resources
        ram_bytes = psutil.virtual_memory().total
        num_workers = psutil.cpu_count(logical=False) or 4

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
        return

    # Handle backward compatibility: --slurm maps to --multi-computer slurm
    if args.slurm and not args.multi_computer:
        args.multi_computer = "slurm"
        logger.warning("--slurm is deprecated, use --multi-computer slurm instead")

    # Validate multi-computer arguments
    if args.multi_computer:
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
                logger.error(f"--slurm-root-folder is required when --multi-computer slurm is enabled")
                logger.error(f"Suggested locations: {', '.join(str(s) for s in suggestions)}")
                return
        elif args.multi_computer == "local":
            # Local mode validation
            pass
        elif args.multi_computer == "single-process":
            # Single-process mode validation
            pass

    # Default to named mode when manual-start-worker is used
    if args.connection_mode is None:
        args.connection_mode = "named" if args.manual_start_worker else "socketpair"

    if hasattr(args, "debugs") and args.debugs:
        args.pid = True

    config = process_selection_arguments(args)

    # Default socket-dir to output/sockets when manual-start-worker is used
    if args.manual_start_worker and not args.socket_dir:
        args.socket_dir = str(config.output_dir / "sockets")

    # Validate socket-dir is provided when using named mode
    if args.connection_mode == "named" and not args.socket_dir:
        logger.error("--socket-dir is required when --connection-mode=named")
        return

    # Handle multi-computer mode early - before scanning binaries
    if args.multi_computer == "slurm":
        logger.info("=" * 60)
        logger.info("SLURM DISTRIBUTED MODE")
        logger.info("=" * 60)

        # Collect binaries to process
        logger.info("Collecting binaries from source directory...")
        sel_result = process_selection_arguments(args)
        binaries_info = find_matching_binaries(
            sel_result.source_dir,
            sel_result.platforms,
            sel_result.compiler,
            sel_result.compiler_versions,
            sel_result.opt_levels,
        )

        if args.skip_existing:
            binaries_info, _ = filter_existing_outputs(
                binaries_info, sel_result.source_dir, sel_result.output_dir, task.get_output_filename_pattern
            )

        logger.info(f"Found {len(binaries_info)} binaries to process")

        if len(binaries_info) == 0:
            logger.warning("No binaries found to process. Coordinator will run in test mode.")

        num_secondaries = args.jobs
        logger.info(f"Starting coordinator with {num_secondaries} secondaries")

        # Create unique run directory with timestamp
        run_timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        run_id = f"run_{run_timestamp}"
        logger.info(f"Run ID: {run_id}")

        # Create coordinator - it will handle all gateway setup, validation, etc.
        coordinator = SlurmPrimaryCoordinator(
            binaries=binaries_info,
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
            # Run coordinator
            coordinator.run(num_secondaries=num_secondaries)
        finally:
            # Coordinator handles its own cleanup
            pass

        return

    elif args.multi_computer == "local":
        logger.info("=" * 60)
        logger.info("LOCAL MULTI-COMPUTER MODE (Testing)")
        logger.info("=" * 60)

        # Collect binaries to process
        logger.info("Collecting binaries from source directory...")
        sel_result = process_selection_arguments(args)
        binaries_info = find_matching_binaries(
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

        if args.skip_existing:
            binaries_info, _ = filter_existing_outputs(
                binaries_info, sel_result.source_dir, sel_result.output_dir, task.get_output_filename_pattern
            )

        logger.info(f"Found {len(binaries_info)} binaries to process")

        if len(binaries_info) == 0:
            logger.warning("No binaries found to process.")
            return

        num_secondaries = args.jobs
        logger.info(f"Starting coordinator with {num_secondaries} local secondaries")

        # Create unique run directory with timestamp
        run_timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        run_id = f"run_{run_timestamp}"
        logger.info(f"Run ID: {run_id}")

        # Create local test coordinator - no gateway, no Docker, no SSH
        coordinator = LocalTestPrimaryCoordinator(
            binaries=binaries_info,
            task_definition=task,
            task_args=args,
            run_id=run_id,
            source_dir=sel_result.source_dir,
            raw_logs=args.raw_logs,
        )

        try:
            # Run coordinator
            coordinator.run(num_secondaries=num_secondaries)
        finally:
            # Coordinator handles its own cleanup
            pass

        return

    elif args.multi_computer == "single-process":
        logger.info("=" * 60)
        logger.info("SINGLE-PROCESS MULTI-COMPUTER MODE (Testing)")
        logger.info("=" * 60)

        # Collect binaries to process
        logger.info("Collecting binaries from source directory...")
        sel_result = process_selection_arguments(args)
        binaries_info = find_matching_binaries(
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

        if args.skip_existing:
            binaries_info, _ = filter_existing_outputs(
                binaries_info, sel_result.source_dir, sel_result.output_dir, task.get_output_filename_pattern
            )

        logger.info(f"Found {len(binaries_info)} binaries to process")

        if len(binaries_info) == 0:
            logger.warning("No binaries found to process.")
            return

        num_secondaries = args.jobs
        logger.info(f"Starting coordinator with {num_secondaries} in-process secondaries")

        # Create unique run directory with timestamp
        run_timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        run_id = f"run_{run_timestamp}"
        logger.info(f"Run ID: {run_id}")

        # Import here to avoid circular dependency
        from .multi_computer.test_single_process import SingleProcessPrimaryCoordinator

        # Create single-process coordinator
        coordinator = SingleProcessPrimaryCoordinator(
            binaries=binaries_info,
            task_definition=task,
            task_args=args,
            run_id=run_id,
            source_dir=sel_result.source_dir,
            output_dir=sel_result.output_dir,
            num_workers_per_secondary=parse_cores(args.cores) // num_secondaries if num_secondaries > 0 else 1,
        )

        try:
            # Run coordinator
            coordinator.run(num_secondaries=num_secondaries)
        finally:
            # Coordinator handles its own cleanup
            pass

        return

    # Standard local processing mode
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

    # Check if test-master-slave mode is enabled
    if args.test_master_slave:
        from .worker_manager import LocalAuthoritiveManager, LocalSubmissiveManager

        logger.info("=" * 60)
        logger.info("TEST MASTER-SLAVE MODE (Local)")
        logger.info("=" * 60)
        logger.info("Using local_submissive + local_authoritive architecture")
        logger.info("")

        # Create submissive manager
        def request_task_callback(worker_id: int) -> None:
            """Callback for submissive to request tasks from authoritive."""
            result = authoritive_manager.handle_task_request(worker_id)
            if result:
                binary, estimated_memory = result
                submissive_manager.assign_task_from_authoritive(worker_id, binary, estimated_memory)

        submissive_manager = LocalSubmissiveManager(
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

        # Create authoritive manager with the submissive's workers
        authoritive_manager = LocalAuthoritiveManager(
            num_workers=num_cores,
            max_memory=max_memory,
            log_dir=config.output_dir,
            task_definition=task,
            submissive_managers=[submissive_manager],
        )

        # Process binaries through the submissive (which will coordinate with authoritive)
        submissive_manager.process_binaries(sorted_binaries)
    else:
        # Use standard local manager
        manager = WorkerManager(
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


if __name__ == "__main__":
    main()
