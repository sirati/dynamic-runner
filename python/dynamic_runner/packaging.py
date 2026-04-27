"""Container image packaging + SLURM job pipeline.

Owns everything that prepares a SLURM cluster run: the dual-image podman
build (with cache marker for the base image), the gateway transfer, and
the slurm-pipeline driver that ties the build/transfer/submit/run steps
together.

The podman class is a faithful port of the legacy
`runtime_env.podman.podman_packaging` so the gateway protocol stays
byte-compatible during the legacy-deletion transition. A SLURM-only
factory (`make_packaging`) replaces `runtime_env.create_packaging_method`;
docker is intentionally not supported for SLURM runs and raises a
`ValueError` rather than producing a stub that fails later.

The slurm-pipeline driver currently delegates to the legacy
`SlurmPrimaryCoordinator`; the next port step rewrites it on top of the
typed Rust runner directly.
"""

from __future__ import annotations

import argparse
import hashlib
import logging
import subprocess
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any

from shared import process_selection_arguments

from .task_protocol import TaskDefinition

logger = logging.getLogger(__name__)


# ── Podman packaging ──────────────────────────────────────────────────────


@dataclass(frozen=True, slots=True)
class PodmanImageMetadata:
    """Metadata for remotely available Podman image artifacts."""

    base_remote_path: Path
    app_remote_path: Path
    base_hash: str
    base_uploaded: bool


class PodmanPackaging:
    """Podman-based packaging implementation for SLURM cluster environments."""

    BASE_IMAGE_NAME = "asm-tokenizer-base.tar"
    APP_IMAGE_NAME = "asm-tokenizer-app.tar"
    BASE_MARKER_NAME = "asm-tokenizer-base.sha256"

    def __init__(self) -> None:
        self.image_name = "asm-tokenizer"
        self.image_tag = "latest"

    def _normalize_path(self, path: str | Path) -> Path:
        if isinstance(path, Path):
            return path
        return Path(path)

    def _build_nix_target(self, local_project_root: Path, target: str, out_link: str) -> Path:
        build_cmd = ["nix", "build", target, "--out-link", out_link]
        result = subprocess.run(
            build_cmd,
            cwd=str(local_project_root),
            capture_output=True,
            text=True,
        )

        if result.returncode != 0:
            logger.error("Nix build failed for %s: %s", target, result.stderr)
            raise RuntimeError(f"Container image build failed for {target}: {result.stderr}")

        built_path = local_project_root / out_link
        if not built_path.exists():
            raise RuntimeError(f"Container image result not found after build for {target}")

        return built_path

    def _compute_sha256(self, file_path: Path) -> str:
        hasher = hashlib.sha256()
        with file_path.open("rb") as handle:
            for chunk in iter(lambda: handle.read(1024 * 1024), b""):
                hasher.update(chunk)
        digest = hasher.hexdigest()
        logger.info("Computed base image SHA-256: %s", digest)
        return digest

    def _read_remote_file(self, gateway: Any, remote_path: Path) -> str | None:
        returncode, stdout, _ = gateway.execute_command(f"cat {remote_path}")
        if returncode != 0:
            return None
        content = stdout.strip()
        return content if content else None

    def _remote_file_exists(self, gateway: Any, remote_path: Path) -> bool:
        returncode, _, _ = gateway.execute_command(f"test -f {remote_path}")
        return returncode == 0

    def _cleanup_symlink(self, path: Path) -> None:
        try:
            path.unlink()
            logger.debug("Removed local image symlink: %s", path)
        except Exception as exc:
            logger.warning("Failed to remove local result symlink %s: %s", path, exc)

    def _upload_artifact(self, gateway: Any, local_path: Path, remote_path: Path) -> None:
        logger.info("Transferring container image to gateway at %s...", remote_path)
        gateway.transfer_file(local_path, str(remote_path))

    def build_images(self, gateway: Any, local_project_root: Path, output_dir: str | Path) -> PodmanImageMetadata:
        """Build and transfer dual image artifacts with base-image cache marker logic."""
        logger.info("Building container images locally using Nix...")

        local_base_path = self._build_nix_target(
            local_project_root=local_project_root,
            target=".#dockerImageBase",
            out_link="docker-image-base-result",
        )
        local_app_path = self._build_nix_target(
            local_project_root=local_project_root,
            target=".#dockerImageApp",
            out_link="docker-image-app-result",
        )

        output_dir_path = self._normalize_path(output_dir)
        base_remote_path = output_dir_path / self.BASE_IMAGE_NAME
        app_remote_path = output_dir_path / self.APP_IMAGE_NAME
        marker_remote_path = output_dir_path / self.BASE_MARKER_NAME

        gateway.create_directory(str(output_dir_path))

        base_hash = self._compute_sha256(local_base_path)
        remote_marker_hash = self._read_remote_file(gateway, marker_remote_path)
        remote_base_exists = self._remote_file_exists(gateway, base_remote_path)

        should_upload_base = not (remote_marker_hash == base_hash and remote_base_exists)

        if should_upload_base:
            logger.info("Base image upload required (hash mismatch or missing remote artifact).")
            gateway.execute_command(f"rm -f {base_remote_path}")
            self._upload_artifact(gateway, local_base_path, base_remote_path)
            gateway.execute_command(f"printf '%s\n' '{base_hash}' > {marker_remote_path}")
            base_uploaded = True
            logger.info("Base image uploaded and marker updated at %s", marker_remote_path)
        else:
            base_uploaded = False
            logger.info("Base image cache hit: reusing remote base image %s", base_remote_path)

        logger.info("App image upload is always enabled.")
        gateway.execute_command(f"rm -f {app_remote_path}")
        self._upload_artifact(gateway, local_app_path, app_remote_path)

        self._cleanup_symlink(local_base_path)
        self._cleanup_symlink(local_app_path)

        logger.info("Container images ready on gateway")
        return PodmanImageMetadata(
            base_remote_path=base_remote_path,
            app_remote_path=app_remote_path,
            base_hash=base_hash,
            base_uploaded=base_uploaded,
        )

    def get_load_command(self, image_path: str, storage_root: str, run_root: str) -> str:
        return f"podman --root {storage_root} --runroot {run_root} --runtime /usr/bin/crun load < {image_path}"

    def get_run_command(
        self,
        image_name: str,
        image_tag: str,
        mounts: dict[str, str],
        ports: dict[int, int],
        entrypoint_args: list[str],
        storage_root: str,
        run_root: str,
    ) -> str:
        cmd_parts = [
            "podman",
            "--root",
            storage_root,
            "--runroot",
            run_root,
            "--runtime",
            "/usr/bin/crun",
            "run",
            "--rm",
        ]

        for host_path, container_path in mounts.items():
            cmd_parts.extend(["-v", f"{host_path}:{container_path}"])

        for host_port, container_port in ports.items():
            cmd_parts.extend(["-p", f"{host_port}:{container_port}"])

        cmd_parts.append(f"{image_name}:{image_tag}")
        cmd_parts.extend(entrypoint_args)
        return " ".join(cmd_parts)

    def get_images_command(self, storage_root: str, run_root: str) -> str:
        return f"podman --root {storage_root} --runroot {run_root} images"

    def get_version_command(self) -> str:
        return "podman --version"

    def get_image_name(self) -> str:
        return self.image_name

    def get_image_tag(self) -> str:
        return self.image_tag

    def requires_storage_paths(self) -> bool:
        return True


# ── Packaging factory (replaces runtime_env.create_packaging_method) ──────


@dataclass
class PackagingConfig:
    """Configuration for packaging method (one of: 'podman')."""

    method: str


def make_packaging(config: PackagingConfig) -> PodmanPackaging:
    """Factory: only podman is supported for SLURM cluster environments.

    Docker requires user-session systemd which isn't available in SLURM
    batch jobs; the legacy `DockerPackaging` was a stub that raised
    `NotImplementedError` on every method, so callers reaching it would
    crash anyway. We surface the constraint at config time instead.
    """
    if config.method == "podman":
        return PodmanPackaging()
    raise ValueError(
        f"Unknown or unsupported packaging method: {config.method!r}. "
        "Only 'podman' is supported for SLURM cluster runs."
    )


# ── SLURM pipeline driver ─────────────────────────────────────────────────


def _make_run_id() -> str:
    return f"run_{datetime.now().strftime('%Y%m%d_%H%M%S')}"


def run_slurm_pipeline(
    task: TaskDefinition,
    args: argparse.Namespace,
    logger: logging.Logger,
) -> None:
    """Build the image, transfer it, submit slurm jobs, then run the primary
    coordinator. Validates the required `--multi-computer slurm` flags before
    delegating.

    Currently delegates to the legacy `SlurmPrimaryCoordinator` (the single
    source of truth for the gateway transfer + slurm submission glue);
    the next port step rewrites it on top of the typed Rust runner.
    """
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

    # Lazy import — avoids paying the cost when slurm mode is not used and
    # keeps the legacy coordinator quarantined behind this single call site.
    from .slurm.primary import SlurmPrimaryCoordinator
    from shared import find_matching_binaries

    sel_result = process_selection_arguments(args)
    binaries = find_matching_binaries(
        sel_result.source_dir,
        sel_result.platforms,
        sel_result.compiler,
        sel_result.compiler_versions,
        sel_result.opt_levels,
    )
    if not binaries:
        logger.warning("No binaries found to process. Coordinator will run in test mode.")

    num_secondaries = args.jobs
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
    coordinator.run(num_secondaries=num_secondaries)
