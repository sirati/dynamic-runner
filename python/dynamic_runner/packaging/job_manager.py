import logging
import secrets
from pathlib import Path
from typing import Any

from .podman import PodmanImageMetadata

logger = logging.getLogger(__name__)


class SlurmJobManager:
    """Manages SLURM job submission and lifecycle."""

    def __init__(self, gateway: Any, slurm_config: Any, packaging_method: Any):
        self.gateway = gateway
        self.slurm_config = slurm_config
        self.packaging = packaging_method
        self.job_ids: list[str] = []

    def _normalize_path(self, path: str | Path) -> Path:
        if isinstance(path, Path):
            return path
        return Path(path)

    def _expand_path(self, path: str | Path) -> str:
        """Expand tilde paths for remote execution."""
        path_str = str(path)
        if path_str.startswith("~") and hasattr(self.gateway, "remote_home") and self.gateway.remote_home:
            return path_str.replace("~", self.gateway.remote_home, 1)
        return path_str

    def _expanded_remote_path(self, path: str | Path) -> Path:
        return Path(self._expand_path(path))

    def prepare_directories(self) -> None:
        """Create necessary directories on gateway."""
        logger.info("Creating SLURM directories on gateway...")

        self.gateway.create_directory(str(self.slurm_config.get_image_dir()))
        self.gateway.create_directory(str(self.slurm_config.get_srcbins_dir()))
        self.gateway.create_directory(str(self.slurm_config.get_output_dir()))
        self.gateway.create_directory(str(self.slurm_config.get_log_dir()))

        logger.info("Directories created successfully")

    def build_and_transfer_images(self, local_project_root: Path) -> PodmanImageMetadata:
        """Build dual images locally and transfer to gateway."""
        logger.info("Building and transferring Docker images...")

        metadata = self.packaging.build_images(
            gateway=self.gateway,
            local_project_root=local_project_root,
            output_dir=self.slurm_config.get_image_dir(),
        )

        normalized = PodmanImageMetadata(
            base_remote_path=self._expanded_remote_path(metadata.base_remote_path),
            app_remote_path=self._expanded_remote_path(metadata.app_remote_path),
            base_hash=metadata.base_hash,
            base_uploaded=metadata.base_uploaded,
        )

        logger.info("Base image path: %s", normalized.base_remote_path)
        logger.info("App image path: %s", normalized.app_remote_path)
        return normalized

    def generate_wrapper_script(
        self,
        image_metadata: PodmanImageMetadata,
        secondary_id: str,
        gateway_host: str | None,
        gateway_port: int | None,
        reverse_connection: bool = False,
        run_log_dir: str | None = None,
    ) -> str:
        """Generate bash wrapper script for SLURM job."""
        rnd_suffix = secrets.token_hex(4)
        rndtmp = f"/tmp/asm-{rnd_suffix}"

        src_tmp = f"{rndtmp}/src"
        out_tmp = f"{rndtmp}/out"
        log_tmp = f"{rndtmp}/log"

        podman_storage = f"{rndtmp}/storage"
        podman_run = f"{rndtmp}/run"

        srcbins_network = self._expand_path(self.slurm_config.get_srcbins_dir())
        output_network = self._expand_path(self.slurm_config.get_output_dir())
        log_network = self._expand_path(run_log_dir or self.slurm_config.get_log_dir())

        base_image_path = self._expand_path(image_metadata.base_remote_path)
        app_image_path = self._expand_path(image_metadata.app_remote_path)

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

RNDTMP="{rndtmp}"
echo "Creating temporary directory: $RNDTMP"
mkdir -p "$RNDTMP"
mkdir -p "{src_tmp}" "{out_tmp}" "{log_tmp}" "{socket_dir}"

cleanup() {{
    echo "Cleaning up temporary directory: $RNDTMP"
    rm -rf "$RNDTMP" 2>/dev/null || sudo rm -rf "$RNDTMP" 2>/dev/null || true
}}
trap cleanup EXIT

PODMAN_STORAGE="{podman_storage}"
PODMAN_RUN="{podman_run}"
mkdir -p "$PODMAN_STORAGE" "$PODMAN_RUN"
chmod 700 "$PODMAN_STORAGE" "$PODMAN_RUN"
export XDG_RUNTIME_DIR="$PODMAN_RUN"
echo "Podman storage: $PODMAN_STORAGE"
echo "Podman run root: $PODMAN_RUN"
echo "XDG_RUNTIME_DIR: $XDG_RUNTIME_DIR"
echo ""
"""

        if reverse_connection:
            connection_info_dir = self._expand_path(f"{run_log_dir or self.slurm_config.get_log_dir()}/connection_info")
            script += f"""
echo "Finding free ports on compute node..."
TUNNEL_PORT=$(python3 -c "import socket; s=socket.socket(); s.bind(('', 0)); print(s.getsockname()[1]); s.close()")
QUIC_PORT=$(python3 -c "import socket; s=socket.socket(); s.bind(('', 0)); print(s.getsockname()[1]); s.close()")
echo "Using tunnel port: $TUNNEL_PORT"
echo "Using QUIC port: $QUIC_PORT"

HOSTNAME=$(hostname -f)
mkdir -p "{connection_info_dir}"
echo "{secondary_id},$HOSTNAME,$TUNNEL_PORT" > "{connection_info_dir}/{secondary_id}.info"
echo "Connection info written to: {connection_info_dir}/{secondary_id}.info"
"""
        else:
            script += """
echo "Finding free port for QUIC server..."
QUIC_PORT=$(python3 -c "import socket; s=socket.socket(); s.bind(('', 0)); print(s.getsockname()[1]); s.close()")
echo "Using QUIC port: $QUIC_PORT"
"""

        script += f"""
echo "Starting command relay service..."
SOCKET_COUNTER=0
{{
    rm -f "{cmd_socket}" "{cmd_socket}.response"
    mkfifo "{cmd_socket}"
    mkfifo "{cmd_socket}.response"
    while true; do
        if read -r CMD < "{cmd_socket}"; then
            if [ -n "$CMD" ]; then
                SOCKET_COUNTER=$((SOCKET_COUNTER + 1))
                OUTPUT_SOCK="{socket_dir}/output_${{SOCKET_COUNTER}}.sock"
                EXIT_SOCK="{socket_dir}/exit_${{SOCKET_COUNTER}}.sock"
                SIGNAL_SOCK="{socket_dir}/signal_${{SOCKET_COUNTER}}.sock"
                mkfifo "$OUTPUT_SOCK" "$EXIT_SOCK" "$SIGNAL_SOCK"
                {{
                    eval "$CMD" > "$OUTPUT_SOCK" 2>&1
                    EXIT_CODE=$?
                    rm -f "$OUTPUT_SOCK"
                    echo "$EXIT_CODE" > "$EXIT_SOCK"
                    rm -f "$EXIT_SOCK"
                }} &
                CMD_PID=$!
                {{
                    if read -r SIGNAL < "$SIGNAL_SOCK"; then
                        if [ -n "$SIGNAL" ]; then
                            kill -$SIGNAL $CMD_PID 2>/dev/null || true
                        fi
                    fi
                    rm -f "$SIGNAL_SOCK"
                }} &
                echo "output_${{SOCKET_COUNTER}}.sock,exit_${{SOCKET_COUNTER}}.sock,signal_${{SOCKET_COUNTER}}.sock,$CMD_PID" > "{cmd_socket}.response"
            fi
        fi
    done
}} &
CMD_RELAY_PID=$!

echo "Copying base and app images to local temp directory..."
LOCAL_BASE_IMAGE="$RNDTMP/asm-tokenizer-base.tar"
LOCAL_APP_IMAGE="$RNDTMP/asm-tokenizer-app.tar"
cp "{base_image_path}" "$LOCAL_BASE_IMAGE"
cp "{app_image_path}" "$LOCAL_APP_IMAGE"
echo "Base image copied to: $LOCAL_BASE_IMAGE"
echo "App image copied to: $LOCAL_APP_IMAGE"

echo "Loading base image into container runtime..."
{self.packaging.get_load_command("$LOCAL_BASE_IMAGE", "$PODMAN_STORAGE", "$PODMAN_RUN")}
echo "Base image loaded successfully"

echo "Loading app image into container runtime..."
{self.packaging.get_load_command("$LOCAL_APP_IMAGE", "$PODMAN_STORAGE", "$PODMAN_RUN")}
echo "App image loaded successfully"

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

        if reverse_connection:
            script += f"""echo "  Mode: SSH ProxyJump (primary tunnels to secondary via gateway)"
echo ""

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
    dynamic_batch --secondary tcp://localhost:$TUNNEL_PORT --secondary-id {secondary_id} --secondary-quic-port $QUIC_PORT"""
        else:
            script += f"""echo "  Gateway: {gateway_host}:{gateway_port}"
echo "  Mode: Standard (secondary connects to primary via gateway)"
echo ""

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
    dynamic_batch --secondary tcp://{gateway_host}:{gateway_port} --secondary-id {secondary_id} --secondary-quic-port $QUIC_PORT"""

        script += """
CONTAINER_EXIT_CODE=$?
echo "Container exited with code: $CONTAINER_EXIT_CODE"
kill $CMD_RELAY_PID 2>/dev/null || true

echo "=================================================="
echo "Job completed"
echo "Time: $(date)"
echo "=================================================="

exit $CONTAINER_EXIT_CODE
"""
        return script

    def generate_test_wrapper_script(self, image_metadata: PodmanImageMetadata) -> str:
        """Generate test wrapper script that validates dual image loading."""
        rnd_suffix = secrets.token_hex(4)
        rndtmp = f"/tmp/asm-test-{rnd_suffix}"

        image_name = self.packaging.get_image_name()
        image_tag = self.packaging.get_image_tag()

        base_image_path = self._expand_path(image_metadata.base_remote_path)
        app_image_path = self._expand_path(image_metadata.app_remote_path)

        podman_storage = f"{rndtmp}/storage"
        podman_run = f"{rndtmp}/run"

        return f"""#!/bin/bash
set -e

echo "=================================================="
echo "SLURM Test Job - Docker Image Validation"
echo "Node: $(hostname)"
echo "Job ID: $SLURM_JOB_ID"
echo "Time: $(date)"
echo "=================================================="
echo ""

RNDTMP="{rndtmp}"
echo "Creating temporary directory: $RNDTMP"
mkdir -p "$RNDTMP"

cleanup() {{
    echo ""
    echo "Cleaning up temporary directory: $RNDTMP"
    rm -rf "$RNDTMP" 2>/dev/null || sudo rm -rf "$RNDTMP" 2>/dev/null || true
}}
trap cleanup EXIT

PODMAN_STORAGE="{podman_storage}"
PODMAN_RUN="{podman_run}"
mkdir -p "$PODMAN_STORAGE" "$PODMAN_RUN"
chmod 700 "$PODMAN_STORAGE" "$PODMAN_RUN"
export XDG_RUNTIME_DIR="$PODMAN_RUN"
echo "Podman storage: $PODMAN_STORAGE"
echo "Podman run root: $PODMAN_RUN"
echo "XDG_RUNTIME_DIR: $XDG_RUNTIME_DIR"
echo ""

echo "Copying base and app images to local /tmp..."
LOCAL_BASE_IMAGE="$RNDTMP/asm-tokenizer-base.tar"
LOCAL_APP_IMAGE="$RNDTMP/asm-tokenizer-app.tar"
echo "  Base source: {base_image_path}"
echo "  App source: {app_image_path}"
cp "{base_image_path}" "$LOCAL_BASE_IMAGE"
cp "{app_image_path}" "$LOCAL_APP_IMAGE"
echo "  Base size: $(du -h "$LOCAL_BASE_IMAGE" | cut -f1)"
echo "  App size: $(du -h "$LOCAL_APP_IMAGE" | cut -f1)"
echo ""

echo "Loading base image..."
{self.packaging.get_load_command("$LOCAL_BASE_IMAGE", "$PODMAN_STORAGE", "$PODMAN_RUN")}
echo "Loading app image..."
{self.packaging.get_load_command("$LOCAL_APP_IMAGE", "$PODMAN_STORAGE", "$PODMAN_RUN")}
echo ""

echo "Verifying image is loaded..."
podman --root "$PODMAN_STORAGE" --runroot "$PODMAN_RUN" images | grep {image_name} || echo "WARNING: Image not found in listing"
echo ""

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

    def submit_job(
        self,
        wrapper_script: str,
        job_name: str,
        nodes: int = 1,
        run_log_dir: str | None = None,
    ) -> str:
        """Submit SLURM job."""
        logger.info("Submitting SLURM job: %s", job_name)

        script_path = f"{self.slurm_config.root_folder}/job_{job_name}.sh"
        write_cmd = f"cat > {script_path} << 'EOFSCRIPT'\n{wrapper_script}\nEOFSCRIPT"
        returncode, _, stderr = self.gateway.execute_command(write_cmd)
        if returncode != 0:
            raise RuntimeError(f"Failed to write job script: {stderr}")

        self.gateway.execute_command(f"chmod +x {script_path}")

        log_dir = self._expand_path(run_log_dir or self.slurm_config.get_log_dir())
        sbatch_cmd_parts = [
            "sbatch",
            "--parsable",
            f"--job-name={job_name}",
            f"--nodes={nodes}",
            "--ntasks=1",
            f"--cpus-per-task={self.slurm_config.cpus_per_task}",
            f"--partition={self.slurm_config.partition}",
            f"--time={self.slurm_config.time_limit}",
            f"--output={log_dir}/slurm_%j.out",
            f"--error={log_dir}/slurm_%j.err",
        ]

        if self.slurm_config.notify_email:
            sbatch_cmd_parts.extend(["--mail-type=ALL", f"--mail-user={self.slurm_config.notify_email}"])

        sbatch_cmd_parts.append(str(script_path))
        sbatch_cmd = " ".join(sbatch_cmd_parts)

        returncode, stdout, stderr = self.gateway.execute_command(sbatch_cmd)
        if returncode != 0:
            raise RuntimeError(f"Job submission failed: {stderr}")

        job_id = stdout.strip()
        self.job_ids.append(job_id)
        logger.info("Job submitted successfully: %s", job_id)
        return job_id

    def cancel_job(self, job_id: str) -> None:
        """Cancel SLURM job."""
        logger.info("Cancelling job: %s", job_id)
        returncode, _, stderr = self.gateway.execute_command(f"scancel {job_id}")
        if returncode != 0:
            logger.warning("Failed to cancel job %s: %s", job_id, stderr)
        else:
            logger.info("Job %s cancelled", job_id)

    def cancel_all_jobs(self) -> None:
        """Cancel all submitted jobs."""
        for job_id in self.job_ids:
            self.cancel_job(job_id)
        self.job_ids.clear()

    def get_job_status(self, job_id: str) -> dict[str, str]:
        """Get status of SLURM job."""
        cmd = f"squeue -j {job_id} -o '%T|%N|%r' --noheader"
        returncode, stdout, _ = self.gateway.execute_command(cmd)

        if returncode != 0 or not stdout.strip():
            return {"state": "UNKNOWN", "node": "", "reason": ""}

        parts = stdout.strip().split("|")
        if len(parts) != 3:
            return {"state": "UNKNOWN", "node": "", "reason": ""}

        return {"state": parts[0], "node": parts[1], "reason": parts[2]}
