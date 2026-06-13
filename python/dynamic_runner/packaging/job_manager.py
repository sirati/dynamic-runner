# =====================================================================
# WARNING — PYTHON BRIDGE ONLY. NO LOGIC HERE.
# =====================================================================
# This file is a thin PyO3 / CLI / config bridge. ALL business logic,
# lifecycle, state-tracking, async orchestration, and process management
# lives in Rust under `crates/dynrunner-slurm/`. If you find yourself
# adding logic here — STOP. Put it in Rust and call it from this file
# via PyO3.
# =====================================================================
"""Python-facing SLURM job manager.

Thin shim over `dynamic_runner._native.RustSlurmJobManager` for the
SLURM job lifecycle primitives the Rust `dynrunner_slurm::SlurmJobManager`
owns (directory prep, job submit, per-job cancel, cancel-all, status
query, tracked job-id list). The remaining methods — source-binary
upload, image build/transfer, bash wrapper-script generation — keep
their Python implementations until their dedicated migration units
(L1.7 / L1.8 / L1.9) land and reconcile the Python ↔ Rust semantic
gaps. The public class name, attribute surface (`job_ids`), and method
signatures are preserved across the cutover so callers don't see the
move.
"""

from __future__ import annotations

import logging
from pathlib import Path
from typing import Any

from .. import _native
from .._native import RustSlurmJobManager
from ..deployment_spec import TaskDeploymentSpec
from .gateway import retry_transient
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
        # Rust-side delegate. Owns every SLURM lifecycle primitive
        # (prepare_directories, submit_job, cancel_job, cancel_all_jobs,
        # get_job_status) and the tracked-job-id list. The remaining
        # non-migrated methods on this class (wrapper-script generation,
        # image transfer, source-binary upload) still use the Python
        # ``gateway`` / ``slurm_config`` / ``packaging`` references
        # directly; those references stay on this object as part of
        # the public bridge surface.
        self._rust = RustSlurmJobManager(
            gateway,
            slurm_config,
            packaging_method,
            deployment,
        )

    @property
    def job_ids(self) -> list[str]:
        """Snapshot of the Rust-tracked SLURM job IDs.

        Preserves the historical attribute name so existing callers
        (today: just `cancel_all_jobs`; tests inspect the list as a
        public surface). Returns a fresh list each call — the
        authoritative state lives in Rust and is mutated by
        ``submit_job`` / ``cancel_all_jobs``.
        """
        return self._rust.job_ids

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

    def upload_shutdown_manager_binary(self) -> str:
        """Stage the bundled ``dynrunner-slurm-shutdown`` binary on the gateway.

        Resolves the local source path via
        :func:`dynamic_runner._shutdown_manager.bundled_binary_path`
        (env-var override > nix-bundled artifact) and hands the
        resolved path to the Rust upload primitive. Hard error when
        neither source is available — the SLURM dispatch path
        requires the binary for correct rootless-podman container
        teardown on ``scancel`` / TIMEOUT, and the previous opt-in
        ``DYNRUNNER_SLURM_SHUTDOWN_BIN_SOURCE``-only model silently
        disabled cleanup when consumer flakes did not set the env
        var (the exact failure mode the shutdown-manager was built
        to prevent).

        Returns the tilde-expanded remote path the gateway-side
        binary lives at. Tilde expansion happens here (mirrors
        :meth:`submit_job`'s ``run_log_dir`` expansion — see that
        method's docstring for the rationale: tilde resolution needs
        the Python gateway's ``remote_home`` attribute, which the
        Rust core can't see through ``PyGatewayAdapter``).
        """
        from .._shutdown_manager import bundled_binary_path, ENV_VAR

        local = bundled_binary_path()
        if local is None:
            raise RuntimeError(
                f"shutdown-manager binary not found. Either the framework "
                f"wheel was built without the bundled binary (non-nix "
                f"install) or {ENV_VAR} points at a missing path. "
                f"SLURM dispatch requires this binary for rootless-podman "
                f"orphan-container teardown on scancel / TIMEOUT."
            )
        raw = self._rust.upload_shutdown_manager_binary_from(str(local))
        return self._expand_path(raw)

    def upload_wrapper_binary(self) -> str:
        """Stage the bundled ``dynrunner-slurm-wrapper`` binary on the gateway.

        Resolves the local source path via
        :func:`dynamic_runner._wrapper_manager.bundled_binary_path`
        (env-var override > nix-bundled artifact) and hands the
        resolved path to the Rust upload primitive. Hard error when
        neither source is available — the SLURM dispatch path renders
        each per-job wrapper as a tiny stub that ``exec``s this binary
        to run the full secondary lifecycle, so a missing binary is
        misconfiguration, not a benign skip.

        Returns the tilde-expanded remote path the gateway-side
        binary lives at. Tilde expansion happens here (mirrors
        :meth:`upload_shutdown_manager_binary` — see that method's
        docstring for the rationale).
        """
        from .._wrapper_manager import bundled_binary_path, ENV_VAR

        local = bundled_binary_path()
        if local is None:
            raise RuntimeError(
                f"wrapper binary not found. Either the framework wheel "
                f"was built without the bundled binary (non-nix install) "
                f"or {ENV_VAR} points at a missing path. SLURM dispatch "
                f"renders each per-job wrapper as a stub that execs this "
                f"binary to run the secondary lifecycle."
            )
        raw = self._rust.upload_wrapper_binary_from(str(local))
        return self._expand_path(raw)

    @property
    def wrapper_remote_path(self) -> str | None:
        """Gateway-side path of the uploaded wrapper binary.

        Tilde-expanded (mirrors :meth:`upload_wrapper_binary`). ``None``
        only when :meth:`upload_wrapper_binary` has not yet been called
        on this manager. Read by the Rust preparation step when
        threading the value into per-secondary ``generate_wrapper_script``
        kwargs (initial cohort + respawn paths) as ``wrapper_bin_path``.
        """
        raw = self._rust.wrapper_bin_remote_path
        if raw is None:
            return None
        return self._expand_path(raw)

    @property
    def shutdown_manager_remote_path(self) -> str | None:
        """Gateway-side path of the uploaded shutdown-manager binary.

        Tilde-expanded (mirrors the
        :meth:`upload_shutdown_manager_binary` return shape — see
        that method's docstring for the rationale). ``None`` only
        when :meth:`upload_shutdown_manager_binary` has not yet been
        called on this manager — a successful upload always records
        a path (the upload step itself raises on missing binary
        rather than skipping silently). Read by the Rust preparation
        step when threading the value into per-secondary
        ``generate_wrapper_script`` kwargs (initial cohort + respawn
        paths).
        """
        raw = self._rust.shutdown_manager_remote_path
        if raw is None:
            return None
        return self._expand_path(raw)

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
            if self._stage_one_source_file(
                Path(binary.path), src_root, srcbins_dir, created_dirs
            ):
                uploaded += 1
        logger.info("Source-binary upload complete (%d/%d files)", uploaded, len(binaries))

    def _stage_one_source_file(
        self,
        raw: Path,
        src_root: Path,
        srcbins_dir: Path,
        created_dirs: set[str],
    ) -> bool:
        """Stage ONE source file to ``<srcbins>/<rel>`` on the gateway.

        The single-file core of :meth:`upload_source_binaries`, factored out
        UNCHANGED so the #336 upload-action callback (:meth:`upload_task_file`)
        reuses the SAME placement + per-blob ``retry_transient`` transfer for a
        per-task file upload rather than re-implementing it. The bulk walk
        loops this once per binary; the callback calls it once per
        file-setup-task.

        Returns ``True`` if transferred, ``False`` if skipped (no backing
        file, or out-of-tree — the same skip semantics the bulk walk and the
        Rust ``images.rs`` upload agree on). Raises the underlying
        ``OSError`` (transient class) / other (permanent) on a transfer that
        could not complete after the bounded retry.

        ``created_dirs`` is the shared mkdir-dedup set (the bulk walk's, or a
        per-call singleton).
        """
        # Resolve the on-disk read location: relative paths join against
        # source_root (post-Bug-B wire-id shape — mirrors the Rust
        # queue_initial_staging fix); absolute paths use the path verbatim.
        local = raw if raw.is_absolute() else src_root / raw
        # A discovered item with no backing file on disk is a
        # computed/producer item: a ``uses_file_based_items=False`` task
        # discovers items it will PRODUCE, not files to upload. Skip it — the
        # per-item authority for upload-stageability is "does this resolve to
        # a real file under --source?", and the task-class flag cannot
        # discriminate (a pure producer and a mixed composite are both
        # ``uses_file_based_items=False``), so the walk itself honours the
        # no-backing-file case rather than stat+scp blindly (which OSErrors
        # the whole dispatch). Mirrors the Rust upload (images.rs). The
        # StageFile/staging path stays strict — it is gated to file-based
        # tasks, whose files exist, and a genuinely-missing file-based source
        # SHOULD surface there as ``SourceUnreadable``.
        if not local.exists():
            logger.warning(
                "Binary %s (resolved %s) has no backing source file "
                "under --source %s; skipping upload (computed/producer "
                "item — nothing to stage).",
                raw,
                local,
                src_root,
            )
            return False
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
            return False
        remote = srcbins_dir / rel
        parent = str(remote.parent)
        if parent not in created_dirs:
            self.gateway.create_directory(parent)
            created_dirs.add(parent)
        # Same idempotent gateway-copy boundary as the image uploads — one
        # transient scp/ssh fault on one file must not kill the dispatch.
        retry_transient(
            lambda: self.gateway.transfer_file(local, str(remote)),
            what=f"source-binary upload of {rel}",
        )
        return True

    def upload_task_file(self, source: str, dest: str | None = None) -> None:
        """Upload ONE task file to the cluster — the #336 P1 upload-action
        callback target (registered Rust→Python; see the
        ``upload_action``/``UploadAction`` seam).

        ``source`` is the file's on-disk location on the source-owning member
        (the submitter / observer that holds it). ``dest`` is the cluster-side
        destination RELATIVE to the gateway srcbins dir; ``None`` derives it
        from ``source``'s basename. Either way the file lands under
        ``<srcbins>/...``, the SAME bind-mount root the bulk walk populates, so
        a secondary's ``src_network`` view resolves it identically.

        Reuses the bulk walk's per-blob ``retry_transient`` transfer — NOT
        re-implemented. Raises ``OSError`` (transient) / other (permanent) on
        a transfer that could not complete after the bounded retry; the Rust
        bridge classifies the exception for the upload action's retry/terminal
        decision.
        """
        srcbins_dir = self._expanded_remote_path(self.slurm_config.get_srcbins_dir())
        # The srcbins-relative tail: an explicit dest verbatim, else the
        # source's basename (P2 owns richer placement policy).
        rel = Path(dest) if dest is not None else Path(Path(source).name)
        remote = srcbins_dir / rel
        self.gateway.create_directory(str(remote.parent))
        retry_transient(
            lambda: self.gateway.transfer_file(Path(source), str(remote)),
            what=f"task-file upload of {source} -> {remote}",
        )

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
        reverse_connection: bool = False,
        run_log_dir: str | None = None,
        is_observer: bool = False,
        shutdown_manager_bin_path: str | None = None,
        wrapper_bin_path: str | None = None,
        name_prefix: str | None = None,
        mem_manager_reserved_bytes: int | None = None,
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

        The dispatcher's task-specific argv is NOT a parameter here: the
        container runs the framework bootstrap shim
        (``dynamic_runner._secondary_bootstrap``), which fetches the run
        config — including that argv — over the peer mesh and then
        ``runpy``s the consumer's real ``secondary_module``. The launch
        command line carries only the framework-regenerated flags plus
        ``--secondary-module``.

        ``shutdown_manager_bin_path`` is the gateway-side absolute path
        of the ``dynrunner-slurm-shutdown`` binary (as recorded by
        :meth:`upload_shutdown_manager_binary`). When set, the rendered
        wrapper spawns the shutdown manager under
        ``systemd-run --user --scope`` so it survives slurmd cgroup
        teardown. The ``None`` branch exists for renderer-internal
        unit tests and back-compat; in production
        :meth:`upload_shutdown_manager_binary` always populates the
        path (or raises on missing source), so the spawn block is
        always emitted on the SLURM dispatch path.

        ``wrapper_bin_path`` is the gateway-side absolute path of the
        ``dynrunner-slurm-wrapper`` binary (as recorded by
        :meth:`upload_wrapper_binary`). When set, the Rust renderer
        emits a tiny ``exec <bin> <args>`` stub instead of the legacy
        inline bash body; the binary parses the ``<args>`` flags and
        runs the full secondary lifecycle. ``None`` keeps the legacy
        bash body (back-compat / renderer-internal tests).

        ``name_prefix`` is the consumer program-identity prefix for the
        scratch dir (``/tmp/<name_prefix>-<suffix>``) and container name
        (``<name_prefix>-<suffix>-<secondary_id>``), replacing the
        framework's old hardcoded ``asm``. Defaults to the deployment
        spec's :attr:`TaskDeploymentSpec.effective_job_name_prefix`
        (``slurm_job_name_prefix`` or ``image_name``) — the same field
        that names the SLURM job ``{prefix}-{secondary_id}``.
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
            # Content key for the wrapper binary's node-local image cache:
            # the secondary reuses a digest-keyed local copy instead of
            # re-reading the shared-FS tarball per job (see image.rs).
            image_digest=image_metadata.image_hash,
            load_command=self.packaging.get_load_command(
                "$LOCAL_IMAGE", "$PODMAN_STORAGE", "$PODMAN_RUN"
            ),
            # The container entrypoint (`python -m`) runs the framework
            # bootstrap shim, which fetches the run config over the peer
            # mesh and then `runpy`s the consumer's real secondary module
            # (named via `secondary_module`). The dispatcher's
            # task-specific argv no longer rides the launch command line.
            container_command="dynamic_runner._secondary_bootstrap",
            secondary_module=self.deployment.secondary_module,
            srcbins_mount_source=self._expand_path(self.slurm_config.get_srcbins_mount_source()),
            output_dir=self._expand_path(self.slurm_config.get_output_dir()),
            cores_spec=cores_spec,
            max_memory_spec=max_memory_spec,
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
            shutdown_manager_bin_path=shutdown_manager_bin_path,
            wrapper_bin_path=wrapper_bin_path,
            name_prefix=name_prefix or self.deployment.effective_job_name_prefix,
            mem_manager_reserved_bytes=mem_manager_reserved_bytes,
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
        secondary_id: str,
        nodes: int = 1,
        run_log_dir: str | None = None,
    ) -> str:
        """Submit SLURM job — pure delegation to Rust.

        The only Python-side responsibility is resolving the two
        gateway-specific path concerns the Rust core stays agnostic of:

        * ``run_log_dir is None`` → fall back to
          ``slurm_config.get_log_dir()``. The Rust ``submit_job``
          requires a non-optional path; the None-fallback is bridge
          ergonomics (preserves the historical Python default-arg
          shape).
        * Tilde expansion of the resolved ``run_log_dir``. Bash does
          not expand ``~`` in ``--output=~/foo`` style arguments, so
          a tilde-bearing path must be resolved before being handed to
          sbatch. ``_expand_path`` consults the Python gateway's
          ``remote_home`` attribute (a ``PosixPath`` for
          ``LocalGateway``, ``str | None`` for ``SSHGateway``) — a
          shape the Rust core can't see through ``PyGatewayAdapter``.

        ``secondary_id`` anchors sbatch's own ``--output``/``--error``
        under ``<run_log_dir>/<secondary_id>/`` so the batch script's
        ``slurm_<jobid>.{out,err}`` live in the same per-secondary
        folder as the worker and role logs, not at the run-dir root.
        """
        return self._rust.submit_job(
            wrapper_script,
            job_name,
            secondary_id,
            nodes,
            self._expand_path(run_log_dir or self.slurm_config.get_log_dir()),
        )

    def cancel_job(self, job_id: str) -> None:
        """Cancel a single SLURM job — pure delegation to Rust."""
        self._rust.cancel_job(job_id)

    def cancel_all_jobs(self) -> None:
        """Cancel every tracked SLURM job — pure delegation to Rust."""
        self._rust.cancel_all_jobs()

    def get_job_status(self, job_id: str) -> dict[str, str]:
        """Get status of SLURM job — pure delegation to Rust."""
        return self._rust.get_job_status(job_id)
