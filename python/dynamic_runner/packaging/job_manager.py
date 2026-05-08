"""Python-facing SLURM job manager.

Thin shim over `dynamic_runner._native.RustSlurmJobManager` for the
SLURM lifecycle primitives the Rust `dynrunner_slurm::SlurmJobManager`
already owns (directory prep, single-job cancel, status query). The
remaining methods — submit, source-binary upload, image build/transfer,
bash wrapper-script generation — keep their Python implementations
until their dedicated migration units (L1.7 / L1.8 / L1.9 / L2.E)
land and reconcile the Python ↔ Rust semantic gaps. The public class
name and method signatures are preserved across the cutover so
callers don't see the move.
"""

from __future__ import annotations

import logging
import secrets
import shlex
from pathlib import Path
from typing import Any

from .._native import RustSlurmJobManager
from ..deployment_spec import TaskDeploymentSpec
from .podman import PodmanImageMetadata

logger = logging.getLogger(__name__)


class SlurmJobManager:
    """Manages SLURM job submission and lifecycle."""

    def __init__(
        self,
        gateway: Any,
        slurm_config: Any,
        packaging_method: Any,
        deployment: TaskDeploymentSpec,
    ):
        self.gateway = gateway
        self.slurm_config = slurm_config
        self.packaging = packaging_method
        self.deployment = deployment
        self.job_ids: list[str] = []
        # Rust-side delegate for the lifecycle primitives that have
        # already migrated. The remaining Python methods on this
        # class don't need it; they're still using ``gateway`` /
        # ``slurm_config`` / ``packaging`` directly.
        self._rust = RustSlurmJobManager(
            gateway,
            slurm_config,
            packaging_method,
            deployment,
        )

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
        self._rust.prepare_directories()
        logger.info("Directories created successfully")

    def upload_source_binaries(
        self,
        binaries: list[Any],
        source_root: Path,
    ) -> None:
        """Upload each binary's underlying file to ``<srcbins_dir>/<rel>``
        on the gateway so the wrapper's read-only bind-mount of srcbins
        into ``/app/src-network`` actually has the staged source.

        Without this the StageFile pipeline (which tells the secondary
        "the file is now at src_network/<rel_path>") points at an empty
        directory and every TaskAssignment surfaces as ``not pre-staged``
        — the framework had no primitive that turned the consumer's
        local ``--source`` tree into a populated ``src_network`` view
        on the cluster.

        Caller-side gating decides WHEN to call this (file-based task,
        not ``--source-already-staged``); this method assumes the
        caller already wants the upload.

        ``binary.path`` may be:

        * absolute under ``source_root`` — uploaded to ``<srcbins>/<rel>``
          where ``<rel>`` is the strip-prefixed tail (legacy shape);
        * absolute out-of-tree — skipped; the StageFile record ships
          the absolute path which the secondary's ``stage_file``
          handler treats as out-of-band-staged (must already exist on
          the secondary by some other means);
        * relative — resolved against ``source_root`` for the on-disk
          read; uploaded to ``<srcbins>/<binary.path>`` verbatim. This
          is the wire-identifier shape consumers should prefer post-
          Bug B (mirrors the Rust ``queue_initial_staging`` fix in
          primary.rs).
        """
        srcbins_dir = self._expanded_remote_path(self.slurm_config.get_srcbins_dir())
        src_root = Path(source_root).resolve()
        logger.info(
            "Uploading %d source files to %s on gateway",
            len(binaries),
            srcbins_dir,
        )

        created_dirs: set[str] = {str(srcbins_dir)}
        uploaded = 0
        for binary in binaries:
            raw = Path(binary.path)
            # Resolve the on-disk read location: relative paths join
            # against source_root (post-Bug-B wire-id shape — mirrors
            # the Rust queue_initial_staging fix); absolute paths use
            # binary.path verbatim.
            local = raw if raw.is_absolute() else src_root / raw
            try:
                rel = local.resolve().relative_to(src_root)
            except ValueError:
                logger.warning(
                    "Binary %s (resolved %s) is not under --source root %s; "
                    "skipping upload (absolute path will ship as out-of-band; "
                    "secondary must already see it).",
                    raw,
                    local.resolve(),
                    src_root,
                )
                continue
            remote = srcbins_dir / rel
            parent = str(remote.parent)
            if parent not in created_dirs:
                self.gateway.create_directory(parent)
                created_dirs.add(parent)
            self.gateway.transfer_file(local, str(remote))
            uploaded += 1
        logger.info("Source-binary upload complete (%d/%d files)", uploaded, len(binaries))

    def build_and_transfer_images(self, local_project_root: Path) -> PodmanImageMetadata:
        """Build the single docker image locally and transfer to gateway."""
        logger.info("Building and transferring Docker image...")

        metadata = self.packaging.build_images(
            gateway=self.gateway,
            local_project_root=local_project_root,
            output_dir=self.slurm_config.get_image_dir(),
        )

        normalized = PodmanImageMetadata(
            remote_path=self._expanded_remote_path(metadata.remote_path),
            image_hash=metadata.image_hash,
            uploaded=metadata.uploaded,
        )

        logger.info("Image path: %s", normalized.remote_path)
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
        container_name = f"asm-{rnd_suffix}-{secondary_id}"

        src_tmp = f"{rndtmp}/src"
        out_tmp = f"{rndtmp}/out"
        log_tmp = f"{rndtmp}/log"

        podman_storage = f"{rndtmp}/storage"
        podman_run = f"{rndtmp}/run"

        srcbins_network = self._expand_path(self.slurm_config.get_srcbins_mount_source())
        output_network = self._expand_path(self.slurm_config.get_output_dir())
        log_network = self._expand_path(run_log_dir or self.slurm_config.get_log_dir())

        # Optional third mount: framework-provided control-plane
        # filesystem (manifests, peer substituters, etc). Bound only
        # when the consumer set ``dynrunner_network_dir`` on their
        # ``TaskDeploymentSpec`` — see the field doc for why we don't
        # silently fall back to ``log-network``.
        dynrunner_network_host = (
            self._expand_path(self.deployment.dynrunner_network_dir)
            if self.deployment.dynrunner_network_dir
            else None
        )
        if dynrunner_network_host:
            dynrunner_volume_block = f'    -v "{dynrunner_network_host}:/app/dynrunner-network" \\\n'
            dynrunner_env_block = '    -e DYNRUNNER_NETWORK="/app/dynrunner-network" \\\n'
        else:
            dynrunner_volume_block = ""
            dynrunner_env_block = ""

        image_path = self._expand_path(image_metadata.remote_path)

        socket_dir = f"{rndtmp}/sockets"
        cmd_socket = f"{socket_dir}/cmd.sock"

        image_name = self.packaging.get_image_name()
        image_tag = self.packaging.get_image_tag()

        # Bash-quote each consumer-supplied flag so values containing
        # spaces or shell-metacharacters survive intact, then render as
        # one continuation line per arg so the resulting `podman run`
        # block keeps the same readable shape regardless of how many
        # flags the consumer passes. Empty tuple → empty string, which
        # collapses cleanly between the env+volume block and the
        # image-tag line.
        extra_run_args_block = "".join(
            f"    {shlex.quote(arg)} \\\n" for arg in self.deployment.extra_run_args
        )

        script = f"""#!/usr/bin/env bash
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
    # Terminate the command-relay subshell and WAIT for it to exit
    # before removing its FIFO. Without `wait`, kill is racy with
    # the rm-rf below — and the relay loop is designed to exit 1
    # with a loud diagnostic if its FIFO disappears unexpectedly
    # (so a careless ops mistake gets noticed instead of silently
    # neutering the secondary). During intentional cleanup we don't
    # want that diagnostic; we want the subshell killed cleanly via
    # SIGTERM before the FIFO vanishes.
    # `${{CMD_RELAY_PID:-}}` guard handles early-failure paths where
    # the relay was never started.
    if [ -n "${{CMD_RELAY_PID:-}}" ]; then
        kill -TERM "$CMD_RELAY_PID" 2>/dev/null || true
        wait "$CMD_RELAY_PID" 2>/dev/null || true
    fi
    echo "Cleaning up temporary directory: $RNDTMP"
    rm -rf "$RNDTMP" 2>/dev/null || sudo rm -rf "$RNDTMP" 2>/dev/null || true
}}
# Also cleanup on SLURM-induced signals: SIGTERM is sent by sbatch
# at time-limit / scancel, SIGHUP by an ssh disconnect, SIGINT by
# Ctrl+C from interactive jobs. Without these, the trap fires only
# on graceful exit and SLURM-killed jobs leak /tmp/asm-XXXX dirs
# until the node's /tmp fills (observed in the field on multi-day
# clusters). EXIT alone misses every non-graceful termination.
trap cleanup EXIT TERM HUP INT

PODMAN_STORAGE="{podman_storage}"
PODMAN_RUN="{podman_run}"
mkdir -p "$PODMAN_STORAGE" "$PODMAN_RUN"
chmod 700 "$PODMAN_STORAGE" "$PODMAN_RUN"
export XDG_RUNTIME_DIR="$PODMAN_RUN"
echo "Podman storage: $PODMAN_STORAGE"
echo "Podman run root: $PODMAN_RUN"
echo "XDG_RUNTIME_DIR: $XDG_RUNTIME_DIR"
echo ""

# Cap container memory at NodeRAM - 2GiB so a runaway worker hits a
# graceful container-OOM (just kills the worker process) instead of a
# host kernel-OOM that wedges the cgroup and leaves zombie SLURM jobs
# stuck COMPLETING. Probed at wrapper-execution time on the compute
# node — node RAM is not known at submit time and may differ from the
# primary. --memory-swap is set equal to --memory so podman cannot
# silently swap-thrash under memory pressure. Falls back to no cap on
# absurdly small nodes (<2GiB MemTotal, implausible on cluster).
MEM_BYTES=$(awk '/MemTotal:/{{val = $2*1024 - 2*1024*1024*1024; if (val > 0) print val; else print ""}}' /proc/meminfo)
if [ -n "${{MEM_BYTES}}" ]; then
    MEM_FLAGS="--memory=${{MEM_BYTES}} --memory-swap=${{MEM_BYTES}}"
    echo "Container memory cap: ${{MEM_BYTES}} bytes (NodeRAM - 2GiB)"
else
    MEM_FLAGS=""
    echo "Container memory cap: disabled (MemTotal probe yielded non-positive headroom)"
fi
echo ""

# Resolve the compute node's peer-routable IPs so the secondary
# advertises addresses other cluster nodes can actually dial. The
# container runs with `--network host` so it shares this node's
# network namespace, but `hostname -I` in there still returns
# *every* configured non-loopback address — and on Krater-class
# nodes the first one is often a CNI bridge / podman-internal
# subnet (10.x.x.x) that's not routed off-host. Resolving the
# node's FQDN through NSS picks the canonical cluster address that
# slurmd, ssh, and DNS all agree on. Empty values are tolerated by
# the Rust env-hint reader (see network::detect_ipv4); a probe
# failure simply falls back to the legacy `hostname -I` first-token.
SLURM_NODE_NAME="${{SLURMD_NODENAME:-$(hostname -f)}}"
PRIMARY_NODE_IPV4=$(getent ahostsv4 "$SLURM_NODE_NAME" 2>/dev/null | awk '{{print $1; exit}}')
PRIMARY_NODE_IPV6=$(getent ahostsv6 "$SLURM_NODE_NAME" 2>/dev/null | awk '$1 ~ /:/ {{print $1; exit}}')
echo "Peer-routable IPv4: ${{PRIMARY_NODE_IPV4:-<unresolved, will fall back to hostname -I>}}"
echo "Peer-routable IPv6: ${{PRIMARY_NODE_IPV6:-<unresolved, will fall back to hostname -I or skip>}}"
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
{{
  echo "hostname=$HOSTNAME"
  echo "tunnel_port=$TUNNEL_PORT"
}} > "{connection_info_dir}/{secondary_id}.info"
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
        elif [ ! -p "{cmd_socket}" ]; then
            # FIFO disappeared with no SIGTERM from cleanup() — that's
            # corrupt state (external rm, filesystem eviction, etc.),
            # not a normal lifecycle event. Bail loud so the failure is
            # diagnosable instead of silently neutering the secondary.
            # During intentional cleanup, the trap's kill+wait sequence
            # exits this subshell via signal before the FIFO vanishes,
            # so this branch only fires on genuine unexpected loss.
            echo "ERROR: command relay FIFO {cmd_socket} disappeared unexpectedly; secondary cannot continue." >&2
            exit 1
        fi
    done
}} &
CMD_RELAY_PID=$!

echo "Copying image to local temp directory..."
LOCAL_IMAGE="$RNDTMP/{self.deployment.image_tar_basename}"
cp "{image_path}" "$LOCAL_IMAGE"
echo "Image copied to: $LOCAL_IMAGE"

echo "Loading image into container runtime..."
# Wrap the load command in an explicit failure check so the abort
# surfaces as a clear marker on STDOUT (the .out file consumers
# check first), not just an opaque set-e exit between the
# "Loading…" line and the cleanup trap. The container runtime's
# own stderr still ends up in the .err file as before.
if ! {self.packaging.get_load_command("$LOCAL_IMAGE", "$PODMAN_STORAGE", "$PODMAN_RUN")}; then
    echo "ERROR: image load failed; secondary cannot start. See the .err file for the runtime's diagnostic."
    echo "ERROR: image load failed; secondary cannot start." >&2
    exit 1
fi
echo "Image loaded successfully"

CONTAINER_NAME="{container_name}"

# Detached fallback teardown for the conmon-double-fork-escapes-
# cgroup case observed in the field: when SLURM proctrack/cgroup
# either isn't in use or doesn't track the container monitor's
# detached pid, scancel/timeout/SIGTERM doesn't propagate into
# the container — conmon and its children survive both the
# wrapper's death and the SLURM job's termination, leaking
# storage and worker processes on the compute node.
#
# Watchdog polls `squeue -j $SLURM_JOB_ID` once a second; when
# the job is gone (squeue exit 0 + empty stdout — distinct from
# transient query failure which exits nonzero), it issues
# `podman kill` then `podman rm -f` on this wrapper's container
# by name. Detached via `setsid -f` so it survives wrapper exit
# and (where possible) cgroup teardown of the wrapper's pidtree.
#
# If proctrack/cgroup is in fact tracking the watchdog too, the
# watchdog dies alongside the rest — but in that case the
# container is also dead, so there's nothing to clean up. Either
# branch leaves the system in a clean state.
#
# Skipped when SLURM_JOB_ID is empty (running outside SLURM):
# squeue would never find a matching job so the watchdog could
# never exit cleanly.
if [ -n "${{SLURM_JOB_ID:-}}" ]; then
    setsid -f bash -c '
        job_id="$1"
        cname="$2"
        storage="$3"
        runroot="$4"
        while true; do
            sleep 1
            if out=$(squeue -j "$job_id" -h -o "%i" 2>/dev/null); then
                [ -n "$out" ] || break
            fi
        done
        podman --root "$storage" --runroot "$runroot" kill --signal TERM "$cname" 2>/dev/null
        sleep 5
        podman --root "$storage" --runroot "$runroot" rm -f "$cname" 2>/dev/null
    ' watchdog "$SLURM_JOB_ID" "$CONTAINER_NAME" "$PODMAN_STORAGE" "$PODMAN_RUN" \
        </dev/null >/dev/null 2>&1
    echo "Spawned podman teardown watchdog for SLURM job $SLURM_JOB_ID (container $CONTAINER_NAME)"
fi

echo "Starting Docker container..."
echo "  Volumes:"
echo "    {src_tmp} -> /app/src-tmp"
echo "    {out_tmp} -> /app/out-tmp"
echo "    {log_tmp} -> /app/log-tmp"
echo "    {srcbins_network} -> /app/src-network (ro)"
echo "    {output_network} -> /app/out-network"
echo "    {log_network} -> /app/log-network"
{f'echo "    {dynrunner_network_host} -> /app/dynrunner-network"' if dynrunner_network_host else 'true'}
echo "    {socket_dir} -> /app/sockets"
echo "  Secondary ID: {secondary_id}"
"""

        if reverse_connection:
            script += f"""echo "  Mode: SSH ProxyJump (primary tunnels to secondary via gateway)"
echo ""

# `--pull=never`: if the local `podman load` was incomplete (image
# layers missing from the load), podman's default behaviour is to
# silently fall through to a registry pull and try docker.io —
# which on most institutional clusters returns "access denied"
# only after a multi-minute timeout, by which point the
# dispatcher has already given up with `timeout waiting for
# secondaries`. `--pull=never` makes that class of incomplete-load
# fail loud-and-fast with a clear "image not in local storage"
# error instead.
podman --root "$PODMAN_STORAGE" --runroot "$PODMAN_RUN" run --rm \
    --name "$CONTAINER_NAME" \
    --pull=never \
    --network host \
    --pids-limit=16384 \
    ${{MEM_FLAGS}} \
    -e PRIMARY_NODE_IPV4="$PRIMARY_NODE_IPV4" \
    -e PRIMARY_NODE_IPV6="$PRIMARY_NODE_IPV6" \
{dynrunner_env_block}    -v "{src_tmp}:/app/src-tmp" \
    -v "{out_tmp}:/app/out-tmp" \
    -v "{log_tmp}:/app/log-tmp" \
    -v "{srcbins_network}:/app/src-network:ro" \
    -v "{output_network}:/app/out-network" \
    -v "{log_network}:/app/log-network" \
{dynrunner_volume_block}    -v "{socket_dir}:/app/sockets" \
{extra_run_args_block}    {image_name}:{image_tag} \
    {self.deployment.secondary_module} --secondary tcp://localhost:$TUNNEL_PORT --secondary-id {secondary_id} --secondary-quic-port $QUIC_PORT"""
        else:
            script += f"""echo "  Gateway: {gateway_host}:{gateway_port}"
echo "  Mode: Standard (secondary connects to primary via gateway)"
echo ""

# `--pull=never`: see the reverse-mode block above for the
# rationale; same incomplete-load → registry-fallback pitfall.
podman --root "$PODMAN_STORAGE" --runroot "$PODMAN_RUN" run --rm \
    --name "$CONTAINER_NAME" \
    --pull=never \
    --network host \
    --pids-limit=16384 \
    ${{MEM_FLAGS}} \
    -e PRIMARY_NODE_IPV4="$PRIMARY_NODE_IPV4" \
    -e PRIMARY_NODE_IPV6="$PRIMARY_NODE_IPV6" \
{dynrunner_env_block}    -v "{src_tmp}:/app/src-tmp" \
    -v "{out_tmp}:/app/out-tmp" \
    -v "{log_tmp}:/app/log-tmp" \
    -v "{srcbins_network}:/app/src-network:ro" \
    -v "{output_network}:/app/out-network" \
    -v "{log_network}:/app/log-network" \
{dynrunner_volume_block}    -v "{socket_dir}:/app/sockets" \
{extra_run_args_block}    {image_name}:{image_tag} \
    {self.deployment.secondary_module} --secondary tcp://{gateway_host}:{gateway_port} --secondary-id {secondary_id} --secondary-quic-port $QUIC_PORT"""

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

        image_path = self._expand_path(image_metadata.remote_path)

        podman_storage = f"{rndtmp}/storage"
        podman_run = f"{rndtmp}/run"

        return f"""#!/usr/bin/env bash
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
# Also cleanup on SLURM-induced signals: SIGTERM is sent by sbatch
# at time-limit / scancel, SIGHUP by an ssh disconnect, SIGINT by
# Ctrl+C from interactive jobs. Without these, the trap fires only
# on graceful exit and SLURM-killed jobs leak /tmp/asm-XXXX dirs
# until the node's /tmp fills (observed in the field on multi-day
# clusters). EXIT alone misses every non-graceful termination.
trap cleanup EXIT TERM HUP INT

PODMAN_STORAGE="{podman_storage}"
PODMAN_RUN="{podman_run}"
mkdir -p "$PODMAN_STORAGE" "$PODMAN_RUN"
chmod 700 "$PODMAN_STORAGE" "$PODMAN_RUN"
export XDG_RUNTIME_DIR="$PODMAN_RUN"
echo "Podman storage: $PODMAN_STORAGE"
echo "Podman run root: $PODMAN_RUN"
echo "XDG_RUNTIME_DIR: $XDG_RUNTIME_DIR"
echo ""

echo "Copying image to local /tmp..."
LOCAL_IMAGE="$RNDTMP/{self.deployment.image_tar_basename}"
echo "  Source: {image_path}"
cp "{image_path}" "$LOCAL_IMAGE"
echo "  Size: $(du -h "$LOCAL_IMAGE" | cut -f1)"
echo ""

echo "Loading image..."
{self.packaging.get_load_command("$LOCAL_IMAGE", "$PODMAN_STORAGE", "$PODMAN_RUN")}
echo ""

echo "Verifying image is loaded..."
podman --root "$PODMAN_STORAGE" --runroot "$PODMAN_RUN" images | grep {image_name} || echo "WARNING: Image not found in listing"
echo ""

echo "Testing secondary entrypoint ({self.deployment.secondary_module} --help)..."
podman --root "$PODMAN_STORAGE" --runroot "$PODMAN_RUN" run --rm {image_name}:{image_tag} {self.deployment.secondary_module} --help | head -5
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
        self._rust.cancel_job(job_id)

    def cancel_all_jobs(self) -> None:
        """Cancel all submitted jobs."""
        for job_id in self.job_ids:
            self.cancel_job(job_id)
        self.job_ids.clear()

    def get_job_status(self, job_id: str) -> dict[str, str]:
        """Get status of SLURM job."""
        return self._rust.get_job_status(job_id)
