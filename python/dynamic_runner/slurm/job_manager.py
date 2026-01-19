import logging
import secrets
from pathlib import Path
from typing import Any

logger = logging.getLogger(__name__)


class SlurmJobManager:
    """Manages SLURM job submission and lifecycle"""

    def __init__(self, gateway: Any, slurm_config: Any, packaging_method: Any):
        self.gateway = gateway
        self.slurm_config = slurm_config
        self.packaging = packaging_method
        self.job_ids: list[str] = []

    def _expand_path(self, path: str | Path) -> str:
        """Expand tilde paths for remote execution

        Args:
            path: Path that may contain ~

        Returns:
            Path with ~ expanded to remote home directory
        """
        path_str = str(path)
        if path_str.startswith("~") and hasattr(self.gateway, "remote_home") and self.gateway.remote_home:
            return path_str.replace("~", self.gateway.remote_home, 1)
        return path_str

    def prepare_directories(self) -> None:
        """Create necessary directories on gateway"""
        logger.info("Creating SLURM directories on gateway...")

        self.gateway.create_directory(self.slurm_config.get_image_dir())
        self.gateway.create_directory(self.slurm_config.get_srcbins_dir())
        self.gateway.create_directory(self.slurm_config.get_output_dir())
        self.gateway.create_directory(self.slurm_config.get_log_dir())

        logger.info("Directories created successfully")

    def build_and_transfer_image(self, local_project_root: Path) -> Path:
        """Build Docker image locally and transfer to gateway

        Args:
            local_project_root: Root directory of project locally

        Returns:
            Path to image on gateway
        """
        logger.info("Building and transferring Docker image...")

        image_path = f"{self.slurm_config.get_image_dir()}/asm-tokenizer-docker.tar"

        # Build image locally using Nix, then transfer to gateway
        built_image = self.packaging.build_image(self.gateway, local_project_root, image_path)

        logger.info(f"Image available at: {built_image}")
        # Return expanded path for use in scripts
        return self._expand_path(built_image)

    def generate_wrapper_script(
        self,
        image_path: Path,
        secondary_id: str,
        gateway_host: str | None,
        gateway_port: int | None,
        reverse_connection: bool = False,
        run_log_dir: str | None = None,
    ) -> str:
        """Generate bash wrapper script for SLURM job

        Args:
            image_path: Path to Docker image on compute node
            secondary_id: Unique identifier for this secondary
            gateway_host: Gateway hostname to connect to (None for reverse mode)
            gateway_port: Gateway port (forwarded to primary) (None for reverse mode)
            reverse_connection: If True, secondary listens and writes connection info
            run_log_dir: Run-specific log directory (if None, uses base log dir)

        Returns:
            Bash script content
        """
        rnd_suffix = secrets.token_hex(4)
        rndtmp = f"/tmp/asm-{rnd_suffix}"

        # Directory paths
        src_tmp = f"{rndtmp}/src"
        out_tmp = f"{rndtmp}/out"
        log_tmp = f"{rndtmp}/log"

        # Podman storage paths for SLURM environment (keep short - runroot limit is 50 chars)
        podman_storage = f"{rndtmp}/storage"
        podman_run = f"{rndtmp}/run"

        # Network paths (expand ~ for remote execution)
        srcbins_network = self._expand_path(self.slurm_config.get_srcbins_dir())
        output_network = self._expand_path(self.slurm_config.get_output_dir())

        # Use run-specific log directory if provided, otherwise use base log dir
        if run_log_dir:
            log_network = self._expand_path(run_log_dir)
        else:
            log_network = self._expand_path(self.slurm_config.get_log_dir())

        # Expand image path
        image_path_expanded = self._expand_path(image_path)

        # Socket paths
        socket_dir = f"{rndtmp}/sockets"
        cmd_socket = f"{socket_dir}/cmd.sock"

        image_name = self.packaging.get_image_name()
        image_tag = self.packaging.get_image_tag()

        script = f"""#!/bin/bash
set -e

echo "=================================================="
echo "SLURM Secondary Job Starting"
echo "Node: $(hostname)"
echo "Job ID: $SLURM_JOB_ID"
echo "Time: $(date)"
echo "=================================================="

# Create temporary directories
RNDTMP="{rndtmp}"
echo "Creating temporary directory: $RNDTMP"
mkdir -p "$RNDTMP"
mkdir -p "{src_tmp}"
mkdir -p "{out_tmp}"
mkdir -p "{log_tmp}"
mkdir -p "{socket_dir}"

# Cleanup on exit
cleanup() {{
    echo "Cleaning up temporary directory: $RNDTMP"
    # Force remove with sudo for podman overlay permission issues
    rm -rf "$RNDTMP" 2>/dev/null || sudo rm -rf "$RNDTMP" 2>/dev/null || true
}}
trap cleanup EXIT

# Setup Podman environment for SLURM
PODMAN_STORAGE="{podman_storage}"
PODMAN_RUN="{podman_run}"
mkdir -p "$PODMAN_STORAGE" "$PODMAN_RUN"
chmod 700 "$PODMAN_STORAGE" "$PODMAN_RUN"
export XDG_RUNTIME_DIR="$PODMAN_RUN"
echo "Podman storage: $PODMAN_STORAGE"
echo "Podman run root: $PODMAN_RUN"
echo "XDG_RUNTIME_DIR: $XDG_RUNTIME_DIR"
echo ""

# Copy Docker image to local /tmp for faster loading
echo "Copying Docker image to local temp directory..."
LOCAL_IMAGE="$RNDTMP/asm-tokenizer-docker.tar"
cp "{image_path_expanded}" "$LOCAL_IMAGE"
echo "Docker image copied to: $LOCAL_IMAGE"

# Load Docker image with Podman
echo "Loading Docker image into container runtime..."
{self.packaging.get_load_command("$LOCAL_IMAGE", "$PODMAN_STORAGE", "$PODMAN_RUN")}
echo "Docker image loaded successfully"

# Start command relay service in background
echo "Starting command relay service..."
{{
    rm -f "{cmd_socket}"
    while true; do
        if [ -e "{cmd_socket}" ]; then
            # Read command from socket
            CMD=$(cat "{cmd_socket}")
            if [ -n "$CMD" ]; then
                # Execute command and write result back
                eval "$CMD" > "{cmd_socket}.out" 2>&1
            fi
        fi
        sleep 0.1
    done
}} &
CMD_RELAY_PID=$!

# Run Docker container
echo "Starting Docker container..."
echo "  Volumes:"
echo "    {src_tmp} -> /app/src-tmp"
echo "    {out_tmp} -> /app/out-tmp"
echo "    {log_tmp} -> /app/log-tmp"
echo "    {srcbins_network} -> /app/src-network (ro)"
echo "    {output_network} -> /app/out-network"
echo "    {log_network} -> /app/log-network"
echo "    {socket_dir} -> /app/sockets"
echo "  Secondary ID: {secondary_id}"
"""

        # Add connection mode specific parts
        if reverse_connection:
            # SSH ProxyJump mode: write connection info, then connect to localhost
            if run_log_dir:
                connection_info_dir = self._expand_path(f"{run_log_dir}/connection_info")
            else:
                connection_info_dir = self._expand_path(f"{self.slurm_config.get_log_dir()}/connection_info")
            script += f"""echo "  Mode: SSH ProxyJump (primary tunnels to secondary via gateway)"
echo ""

# Find a free port on this compute node
echo "Finding free port on compute node..."
FREE_PORT=$(python3 -c "import socket; s=socket.socket(); s.bind(('', 0)); print(s.getsockname()[1]); s.close()")
echo "Using port: $FREE_PORT"

# Write connection info for primary to find and create tunnel
HOSTNAME=$(hostname -f)
mkdir -p "{connection_info_dir}"
echo "{secondary_id},$HOSTNAME,$FREE_PORT" > "{connection_info_dir}/{secondary_id}.info"
echo "Connection info written to: {connection_info_dir}/{secondary_id}.info"
echo "  Hostname: $HOSTNAME"
echo "  Port: $FREE_PORT"

# Run container with Podman using host networking - secondary connects to localhost:FREE_PORT (primary will tunnel to it)
podman --root "$PODMAN_STORAGE" --runroot "$PODMAN_RUN" --runtime /usr/bin/crun run --rm \
    --network host \
    -v "{src_tmp}:/app/src-tmp" \
    -v "{out_tmp}:/app/out-tmp" \
    -v "{log_tmp}:/app/log-tmp" \
    -v "{srcbins_network}:/app/src-network:ro" \
    -v "{output_network}:/app/out-network" \
    -v "{log_network}:/app/log-network" \
    -v "{socket_dir}:/app/sockets" \
    {image_name}:{image_tag} \
    dynamic_batch --secondary tcp://localhost:$FREE_PORT --secondary-id {secondary_id}"""
        else:
            # Standard mode: secondary connects to primary via gateway
            script += f"""echo "  Gateway: {gateway_host}:{gateway_port}"
echo "  Mode: Standard (secondary connects to primary via gateway)"
echo ""

# Run container with Podman
podman --root "$PODMAN_STORAGE" --runroot "$PODMAN_RUN" --runtime /usr/bin/crun run --rm \
    -v "{src_tmp}:/app/src-tmp" \
    -v "{out_tmp}:/app/out-tmp" \
    -v "{log_tmp}:/app/log-tmp" \
    -v "{srcbins_network}:/app/src-network:ro" \
    -v "{output_network}:/app/out-network" \
    -v "{log_network}:/app/log-network" \
    -v "{socket_dir}:/app/sockets" \
    {image_name}:{image_tag} \
    dynamic_batch --secondary tcp://{gateway_host}:{gateway_port} --secondary-id {secondary_id}"""

        # Continue with common cleanup
        script += f"""

CONTAINER_EXIT_CODE=$?
echo "Container exited with code: $CONTAINER_EXIT_CODE"

# Kill command relay service
kill $CMD_RELAY_PID 2>/dev/null || true

echo "=================================================="
echo "Job completed"
echo "Time: $(date)"
echo "=================================================="

exit $CONTAINER_EXIT_CODE
"""
        return script

    def generate_test_wrapper_script(self, image_path: Path) -> str:
        """Generate test bash wrapper script that validates Docker image loading

        Args:
            image_path: Path to Docker image on compute node

        Returns:
            Bash script content for test job
        """
        rnd_suffix = secrets.token_hex(4)
        rndtmp = f"/tmp/asm-test-{rnd_suffix}"

        image_name = self.packaging.get_image_name()
        image_tag = self.packaging.get_image_tag()

        # Expand image path for remote execution
        image_path_expanded = self._expand_path(image_path)

        # Podman storage paths (keep short - runroot limit is 50 chars)
        podman_storage = f"{rndtmp}/storage"
        podman_run = f"{rndtmp}/run"

        script = f"""#!/bin/bash
set -e

echo "=================================================="
echo "SLURM Test Job - Docker Image Validation"
echo "Node: $(hostname)"
echo "Job ID: $SLURM_JOB_ID"
echo "Time: $(date)"
echo "=================================================="
echo ""

# Create temporary directory
RNDTMP="{rndtmp}"
echo "Creating temporary directory: $RNDTMP"
mkdir -p "$RNDTMP"

# Cleanup on exit
cleanup() {{
    echo ""
    echo "Cleaning up temporary directory: $RNDTMP"
    rm -rf "$RNDTMP" 2>/dev/null || sudo rm -rf "$RNDTMP" 2>/dev/null || true
}}
trap cleanup EXIT

# Setup Podman environment for SLURM
PODMAN_STORAGE="{podman_storage}"
PODMAN_RUN="{podman_run}"
mkdir -p "$PODMAN_STORAGE" "$PODMAN_RUN"
chmod 700 "$PODMAN_STORAGE" "$PODMAN_RUN"
export XDG_RUNTIME_DIR="$PODMAN_RUN"
echo "Podman storage: $PODMAN_STORAGE"
echo "Podman run root: $PODMAN_RUN"
echo "XDG_RUNTIME_DIR: $XDG_RUNTIME_DIR"
echo ""

# Copy Docker image to local /tmp
echo "Copying Docker image to local temp directory..."
LOCAL_IMAGE="$RNDTMP/asm-tokenizer-docker.tar"
echo "  Source: {image_path_expanded}"
echo "  Destination: $LOCAL_IMAGE"
cp "{image_path_expanded}" "$LOCAL_IMAGE"
echo "  Size: $(du -h "$LOCAL_IMAGE" | cut -f1)"
echo "Docker image copied successfully"
echo ""

# Load Docker image with Podman
echo "Loading Docker image into container runtime..."
{self.packaging.get_load_command("$LOCAL_IMAGE", "$PODMAN_STORAGE", "$PODMAN_RUN")}
echo ""

# List loaded images
echo "Verifying image is loaded..."
podman --root "$PODMAN_STORAGE" --runroot "$PODMAN_RUN" images | grep {image_name} || echo "WARNING: Image not found in listing"
echo ""

# Test run container with simple command
echo "Testing container execution..."
podman --root "$PODMAN_STORAGE" --runroot "$PODMAN_RUN" --runtime /usr/bin/crun run --rm {image_name}:{image_tag} python --version
echo ""

echo "Testing dynamic_batch module..."
podman --root "$PODMAN_STORAGE" --runroot "$PODMAN_RUN" --runtime /usr/bin/crun run --rm {image_name}:{image_tag} dynamic_batch --help | head -5
echo ""

echo "=================================================="
echo "Test Job Completed Successfully"
echo "Time: $(date)"
echo "=================================================="
"""
        return script

    def submit_job(
        self,
        wrapper_script: str,
        job_name: str,
        nodes: int = 1,
        run_log_dir: str | None = None,
    ) -> str:
        """Submit SLURM job

        Args:
            wrapper_script: Bash script content
            job_name: Name for SLURM job
            nodes: Number of nodes to request
            run_log_dir: Run-specific log directory (if None, uses base log dir)

        Returns:
            Job ID
        """
        logger.info(f"Submitting SLURM job: {job_name}")

        # Create wrapper script on gateway
        script_path = f"{self.slurm_config.root_folder}/job_{job_name}.sh"

        # Write script via gateway
        write_cmd = f"cat > {script_path} << 'EOFSCRIPT'\n{wrapper_script}\nEOFSCRIPT"
        returncode, stdout, stderr = self.gateway.execute_command(write_cmd)

        if returncode != 0:
            raise RuntimeError(f"Failed to write job script: {stderr}")

        # Make executable
        self.gateway.execute_command(f"chmod +x {script_path}")

        # Determine log directory
        if run_log_dir:
            log_dir = self._expand_path(run_log_dir)
        else:
            log_dir = self._expand_path(self.slurm_config.get_log_dir())

        # Build sbatch command
        sbatch_cmd_parts = [
            "sbatch",
            "--parsable",
            f"--job-name={job_name}",
            f"--nodes={nodes}",
            f"--ntasks=1",
            f"--cpus-per-task={self.slurm_config.cpus_per_task}",
            f"--partition={self.slurm_config.partition}",
            f"--time={self.slurm_config.time_limit}",
            f"--output={log_dir}/slurm_%j.out",
            f"--error={log_dir}/slurm_%j.err",
        ]

        if self.slurm_config.notify_email:
            sbatch_cmd_parts.extend(
                [
                    "--mail-type=ALL",
                    f"--mail-user={self.slurm_config.notify_email}",
                ]
            )

        sbatch_cmd_parts.append(str(script_path))
        sbatch_cmd = " ".join(sbatch_cmd_parts)

        # Submit job
        returncode, stdout, stderr = self.gateway.execute_command(sbatch_cmd)

        if returncode != 0:
            raise RuntimeError(f"Job submission failed: {stderr}")

        job_id = stdout.strip()
        self.job_ids.append(job_id)

        logger.info(f"Job submitted successfully: {job_id}")
        return job_id

    def cancel_job(self, job_id: str) -> None:
        """Cancel SLURM job

        Args:
            job_id: Job ID to cancel
        """
        logger.info(f"Cancelling job: {job_id}")

        returncode, stdout, stderr = self.gateway.execute_command(f"scancel {job_id}")

        if returncode != 0:
            logger.warning(f"Failed to cancel job {job_id}: {stderr}")
        else:
            logger.info(f"Job {job_id} cancelled")

    def cancel_all_jobs(self) -> None:
        """Cancel all submitted jobs"""
        for job_id in self.job_ids:
            self.cancel_job(job_id)
        self.job_ids.clear()

    def get_job_status(self, job_id: str) -> dict[str, str]:
        """Get status of SLURM job

        Args:
            job_id: Job ID to query

        Returns:
            Dictionary with job status information
        """
        cmd = f"squeue -j {job_id} -o '%T|%N|%r' --noheader"
        returncode, stdout, stderr = self.gateway.execute_command(cmd)

        if returncode != 0 or not stdout.strip():
            return {"state": "UNKNOWN", "nodes": "", "reason": ""}

        parts = stdout.strip().split("|")
        return {
            "state": parts[0] if len(parts) > 0 else "UNKNOWN",
            "nodes": parts[1] if len(parts) > 1 else "",
            "reason": parts[2] if len(parts) > 2 else "",
        }
