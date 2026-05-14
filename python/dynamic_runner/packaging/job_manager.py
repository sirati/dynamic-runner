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
from pathlib import Path
from typing import Any

from .. import _native
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
            return path_str.replace("~", str(self.gateway.remote_home), 1)
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
        cores_spec: str = "0",
        max_memory_spec: str = "-2G",
        forwarded_argv: list[str] | None = None,
        reverse_connection: bool = False,
        run_log_dir: str | None = None,
        is_observer: bool = False,
    ) -> str:
        """Generate the bash wrapper script for a SLURM job.

        This method's only concern is *flattening* the Python object
        graph (gateway tilde-expansion, ``PodmanPackaging`` callables,
        ``TaskDeploymentSpec`` consumer fields) into the flat-string
        kwargs the Rust generator expects. The actual bash-template
        rendering lives in ``crates/dynrunner-slurm/src/wrapper_script.rs``
        and is exposed via ``_native.generate_wrapper_script``.

        ``cores_spec`` is the verbatim ``--cores`` spec string
        (``"0"``, ``"N"``, ``"+N"``, ``"-N"``) forwarded to the
        secondary subprocess inside the container. Each secondary
        resolves it locally against its OWN container's detected
        CPU count via :func:`parse_cores`, preserving the per-machine
        semantic in heterogeneous SLURM deployments. Defaults to
        ``"0"`` (all detected cores) for back-compat with callers
        that haven't been updated to pass an explicit spec.

        ``max_memory_spec`` is the verbatim ``--max-memory`` spec
        string (``"16G"``, ``"4G"``, ``"-2G"``, ``"+1G"``, …)
        forwarded the same way; each secondary resolves it locally
        via :func:`parse_memory` against ITS host's
        ``/proc/meminfo:MemTotal`` (or cgroup-v2 ``memory.max``).
        Defaults to ``"-2G"`` (host minus 2 GiB headroom), matching
        the CLI's default. SLURM-only: the ``--multi-computer local``
        path INTENTIONALLY does not forward memory because all local
        secondaries share one host's RAM (double-counting); SLURM
        secondaries are each on a different host with their own
        budget so per-machine semantic applies.

        ``forwarded_argv`` is the dispatcher's ``sys.argv[1:]`` with
        the framework-regenerated flags removed (filtering owned by
        :func:`dynamic_runner._forwarded_argv.filter_framework_argv`).
        Each entry is bash-quoted by the Rust generator and spliced
        into the secondary's container-command argv after
        ``--src-network``, so the setup-promoted secondary's argparse
        re-parses task-specific filter flags (``--platform``,
        ``--compiler``, ``--name-regex``, …) and ``task.discover_items``
        sees them. Defaults to an empty list (back-compat with callers
        that haven't been updated).
        """
        connection_info_dir = (
            self._expand_path(f"{run_log_dir or self.slurm_config.get_log_dir()}/connection_info")
            if reverse_connection
            else None
        )
        return _native.generate_wrapper_script(
            root_folder=str(self.slurm_config.root_folder),
            image_path=self._expand_path(image_metadata.remote_path),
            secondary_id=secondary_id,
            image_name=self.packaging.get_image_name(),
            image_tag=self.packaging.get_image_tag(),
            image_tar_basename=self.deployment.image_tar_basename,
            load_command=self.packaging.get_load_command(
                "$LOCAL_IMAGE", "$PODMAN_STORAGE", "$PODMAN_RUN"
            ),
            container_command=self.deployment.secondary_module,
            srcbins_mount_source=self._expand_path(self.slurm_config.get_srcbins_mount_source()),
            output_dir=self._expand_path(self.slurm_config.get_output_dir()),
            cores_spec=cores_spec,
            max_memory_spec=max_memory_spec,
            forwarded_argv=list(forwarded_argv) if forwarded_argv else [],
            run_log_dir=self._expand_path(run_log_dir or self.slurm_config.get_log_dir()),
            dynrunner_network_dir=(
                self._expand_path(self.deployment.dynrunner_network_dir)
                if self.deployment.dynrunner_network_dir
                else None
            ),
            extra_run_args=list(self.deployment.extra_run_args),
            gateway_host=gateway_host,
            gateway_port=gateway_port,
            reverse_connection=reverse_connection,
            connection_info_dir=connection_info_dir,
            is_observer=is_observer,
        )

    def generate_test_wrapper_script(self, image_metadata: PodmanImageMetadata) -> str:
        """Generate the image-validation test wrapper script.

        Same flatten-then-delegate shape as
        :meth:`generate_wrapper_script`. Bash rendering lives in
        the Rust generator (``_native.generate_test_wrapper_script``).
        """
        return _native.generate_test_wrapper_script(
            image_path=self._expand_path(image_metadata.remote_path),
            image_name=self.packaging.get_image_name(),
            image_tag=self.packaging.get_image_tag(),
            image_tar_basename=self.deployment.image_tar_basename,
            load_command=self.packaging.get_load_command(
                "$LOCAL_IMAGE", "$PODMAN_STORAGE", "$PODMAN_RUN"
            ),
            container_command=self.deployment.secondary_module,
        )

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
