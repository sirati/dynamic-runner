import argparse
import logging
import secrets
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
from .runtime_env import PackagingConfig, create_packaging_method
from .slurm import SlurmConfig, validate_slurm_config
from .slurm.job_manager import SlurmJobManager
from .system_resources import parse_cores, parse_memory
from .task import TokenizerTask
from .worker_manager import WorkerManager


def main():
    # Parse args early to check for debug flag
    parser = argparse.ArgumentParser(
        description="Dynamic batch processing for binary tokenization with memory-aware parallel execution"
    )

    parser.add_argument(
        "--debug",
        action="store_true",
        help="Enable debug logging for detailed output",
    )

    # Parse known args first to get debug flag
    early_args, _ = parser.parse_known_args()

    # Set up logging based on debug flag
    log_level = logging.DEBUG if early_args.debug else logging.INFO
    logger = logging.getLogger()
    logger.setLevel(log_level)

    logging.basicConfig(
        level=log_level,
        format="%(levelname)s | %(asctime)s,%(msecs)03d | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

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
        help="Run in secondary mode, connecting to primary at specified URL (e.g., quic://host:port)",
    )

    parser.add_argument(
        "--gateway",
        type=str,
        help="Gateway for SLURM controller. Use 'local' or 'ssh://user@host[:port]'",
    )

    parser.add_argument(
        "--slurm",
        action="store_true",
        help="Enable SLURM distributed mode",
    )

    parser.add_argument(
        "--packaging",
        type=str,
        choices=["docker", "podman"],
        help="Packaging method for SLURM deployment (required with --slurm). Use 'podman' for SLURM clusters.",
    )

    parser.add_argument(
        "--slurm-root-folder",
        type=str,
        help="Root folder for SLURM operations on gateway (required with --slurm)",
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
        help="Submit a test SLURM job to validate Docker image loading (requires --slurm)",
    )

    args = parser.parse_args()

    # Handle secondary mode early - this is for SLURM compute nodes
    if args.secondary:
        import socket

        import psutil

        from .slurm.secondary_mode import SecondaryMode

        logger.info("=" * 60)
        logger.info("SECONDARY MODE (SLURM Compute Node)")
        logger.info("=" * 60)

        # Get system resources
        ram_bytes = psutil.virtual_memory().total
        num_workers = psutil.cpu_count(logical=False) or 4

        # Paths inside container
        src_tmp = Path("/app/src-tmp")
        out_tmp = Path("/app/out-tmp")
        log_tmp = Path("/app/log-tmp")
        src_network = Path("/app/src-network")
        out_network = Path("/app/out-network")
        log_network = Path("/app/log-network")
        socket_dir = Path("/app/sockets")

        # Generate secondary ID
        secondary_id = f"secondary_{socket.gethostname()}_{secrets.token_hex(4)}"

        secondary = SecondaryMode(
            primary_url=args.secondary,
            secondary_id=secondary_id,
            num_workers=num_workers,
            ram_bytes=ram_bytes,
            src_tmp=src_tmp,
            out_tmp=out_tmp,
            log_tmp=log_tmp,
            src_network=src_network,
            out_network=out_network,
            log_network=log_network,
            socket_dir=socket_dir,
        )

        secondary.run()
        return

    # Validate SLURM arguments
    if args.slurm:
        if not args.gateway:
            logger.error("--gateway is required when --slurm is enabled")
            return
        if not args.packaging:
            logger.error("--packaging is required when --slurm is enabled")
            return
        if not args.slurm_root_folder:
            home = Path.home()
            suggestions = [home / "slurm", home / "BIG" / "slurm"]
            logger.error(f"--slurm-root-folder is required when --slurm is enabled")
            logger.error(f"Suggested locations: {', '.join(str(s) for s in suggestions)}")
            return

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

    # Handle SLURM mode early - before scanning binaries
    if args.slurm:
        logger.info("=" * 60)
        logger.info("SLURM DISTRIBUTED MODE")
        logger.info("=" * 60)

        # Parse gateway configuration
        gateway_config = parse_gateway_url(args.gateway)
        gateway = create_gateway(gateway_config)

        # Create SLURM configuration
        # Keep as string if starts with ~ for remote expansion
        root_folder = args.slurm_root_folder
        if not root_folder.startswith("~"):
            root_folder = Path(root_folder)

        slurm_config = SlurmConfig(
            root_folder=root_folder,
            image_subfolder=args.slurm_image_subfolder,
            output_subfolder=args.slurm_output_subfolder,
            log_subfolder=args.slurm_log_subfolder,
            notify_email=args.slurm_notify_email,
        )

        # Create packaging method
        packaging_config = PackagingConfig(method=args.packaging)
        packaging = create_packaging_method(packaging_config)

        # Connect to gateway
        gateway.connect()

        # Validate configuration (after gateway connection to check remote folder)
        try:
            validate_slurm_config(slurm_config, gateway)
        except ValueError:
            # Create directory if it doesn't exist
            logger.info(f"Creating SLURM root directory: {slurm_config.root_folder}")
            gateway.create_directory(slurm_config.root_folder)

        try:
            # Create job manager
            job_manager = SlurmJobManager(gateway, slurm_config, packaging)

            # Prepare directories on gateway
            job_manager.prepare_directories()

            # Build and transfer Docker image
            project_root = Path.cwd()
            image_path = job_manager.build_and_transfer_image(project_root)

            logger.info(f"Docker image ready at: {image_path}")
            logger.info("")

            # If test mode, submit a simple test job
            if args.slurm_test_job:
                logger.info("Submitting test SLURM job...")

                # Generate test wrapper script
                test_script = job_manager.generate_test_wrapper_script(image_path)

                # Submit test job
                test_job_id = job_manager.submit_job(
                    wrapper_script=test_script,
                    job_name="asm-tokenizer-test",
                    nodes=1,
                )

                logger.info(f"Test job submitted: {test_job_id}")
                logger.info("")
                logger.info("Monitor job status with:")
                logger.info(f"  ssh {gateway_config.ssh_host} 'squeue -j {test_job_id}'")
                logger.info("")
                logger.info("Check job output at:")
                logger.info(f"  {slurm_config.get_log_dir()}/slurm_{test_job_id}.out")
                logger.info("")
                logger.info("To view logs:")
                logger.info(
                    f"  ssh {gateway_config.ssh_host} 'tail -f {slurm_config.get_log_dir()}/slurm_{test_job_id}.out'"
                )
            else:
                logger.info("Next steps:")
                logger.info("1. Use --slurm-test-job to validate Docker image loading")
                logger.info("2. Primary will coordinate initial distribution")
                logger.info("3. Secondaries will be submitted as SLURM jobs")
                logger.info("4. After all files transferred, primary can disconnect")
                logger.info("")
                logger.info("SLURM mode setup complete!")
                logger.info("Note: Full SLURM orchestration not yet implemented")

        finally:
            gateway.disconnect()

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
