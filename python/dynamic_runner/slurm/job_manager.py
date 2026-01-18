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

    def prepare_directories(self) -> None:
        """Create necessary directories on gateway"""
        logger.info("Creating SLURM directories on gateway...")

        self.gateway.create_directory(self.slurm_config.get_image_dir())
        self.gateway.create_directory(self.slurm_config.get_srcbins_dir())
        self.gateway.create_directory(self.slurm_config.get_output_dir())
        self.gateway.create_directory(self.slurm_config.get_log_dir())

        logger.info("Directories created successfully")

    def build_and_transfer_image(self, project_root: Path) -> Path:
        """Build Docker image and transfer to gateway

        Args:
            project_root: Root directory of project on gateway

        Returns:
            Path to image on gateway
        """
        logger.info("Building and transferring Docker image...")

        image_path = f"{self.slurm_config.get_image_dir()}/asm-tokenizer-docker.tar"

        # Build image on gateway
        built_image = self.packaging.build_image(self.gateway, project_root, image_path)

        logger.info(f"Image available at: {built_image}")
        return built_image

    def generate_wrapper_script(
        self,
        image_path: Path,
        secondary_port: int,
        primary_host: str,
        primary_port: int,
    ) -> str:
        """Generate bash wrapper script for SLURM job

        Args:
            image_path: Path to Docker image on compute node
            secondary_port: Port for secondary QUIC connections
            primary_host: Primary host address
            primary_port: Primary port

        Returns:
            Bash script content
        """
        rnd_suffix = secrets.token_hex(8)
        rndtmp = f"/tmp/asm-tokenizer-{rnd_suffix}"

        # Directory paths
        src_tmp = f"{rndtmp}/src"
        out_tmp = f"{rndtmp}/out"
        log_tmp = f"{rndtmp}/log"

        # Network paths
        srcbins_network = self.slurm_config.get_srcbins_dir()
        output_network = self.slurm_config.get_output_dir()
        log_network = self.slurm_config.get_log_dir()

        # Socket paths
        socket_dir = f"{rndtmp}/sockets"
        cmd_socket = f"{socket_dir}/cmd.sock"

        image_name = self.packaging.get_image_name()
        image_tag = self.packaging.get_image_tag()

        script = f"""#!/bin/bash
set -e

# Create temporary directories
RNDTMP="{rndtmp}"
mkdir -p "$RNDTMP"
mkdir -p "{src_tmp}"
mkdir -p "{out_tmp}"
mkdir -p "{log_tmp}"
mkdir -p "{socket_dir}"

# Cleanup on exit
cleanup() {{
    echo "Cleaning up temporary directory: $RNDTMP"
    rm -rf "$RNDTMP"
}}
trap cleanup EXIT

# Load Docker image
echo "Loading Docker image..."
{self.packaging.get_load_command(image_path)}

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
docker run --rm \\
    -v "{src_tmp}:/app/src-tmp" \\
    -v "{out_tmp}:/app/out-tmp" \\
    -v "{log_tmp}:/app/log-tmp" \\
    -v "{srcbins_network}:/app/src-network:ro" \\
    -v "{output_network}:/app/out-network" \\
    -v "{log_network}:/app/log-network" \\
    -v "{socket_dir}:/app/sockets" \\
    -p {secondary_port}:{secondary_port} \\
    {image_name}:{image_tag} \\
    dynamic_batch --secondary quic://{primary_host}:{primary_port}

# Kill command relay service
kill $CMD_RELAY_PID 2>/dev/null || true

echo "Job completed successfully"
"""
        return script

    def submit_job(
        self,
        wrapper_script: str,
        job_name: str,
        nodes: int = 1,
    ) -> str:
        """Submit SLURM job

        Args:
            wrapper_script: Bash script content
            job_name: Name for SLURM job
            nodes: Number of nodes to request

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
            f"--output={self.slurm_config.get_log_dir()}/slurm_%j.out",
            f"--error={self.slurm_config.get_log_dir()}/slurm_%j.err",
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
