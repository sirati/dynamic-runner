"""Podman container image build + dual-image artifact transfer.

Faithful port of the legacy `runtime_env.podman.podman_packaging` so the
gateway protocol stays byte-compatible during the legacy-deletion
transition. Sibling modules in the `packaging` package own SLURM job
submission (`job_manager`), gateway abstraction (`gateway/`), per-run
preparation (`preparation`), and the top-level pipeline (`pipeline`).
"""

from __future__ import annotations

import hashlib
import logging
import subprocess
from dataclasses import dataclass
from pathlib import Path
from typing import Any

logger = logging.getLogger(__name__)


@dataclass(frozen=True, slots=True)
class PodmanImageMetadata:
    """Metadata for remotely available Podman image artifacts.

    `base_uploaded` and `app_uploaded` reflect whether the local hash
    matched the remote marker (i.e. the upload was skipped). With
    layer caching enabled both images can be cache-hits, turning the
    "transfer" phase into a couple of `cat` commands instead of a
    1+ GB upload.
    """

    base_remote_path: Path
    app_remote_path: Path
    base_hash: str
    base_uploaded: bool
    app_hash: str = ""
    app_uploaded: bool = True


class PodmanPackaging:
    """Podman-based packaging implementation for SLURM cluster environments."""

    BASE_IMAGE_NAME = "asm-tokenizer-base.tar"
    APP_IMAGE_NAME = "asm-tokenizer-app.tar"
    BASE_MARKER_NAME = "asm-tokenizer-base.sha256"
    APP_MARKER_NAME = "asm-tokenizer-app.sha256"

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
        base_marker_remote_path = output_dir_path / self.BASE_MARKER_NAME
        app_marker_remote_path = output_dir_path / self.APP_MARKER_NAME

        gateway.create_directory(str(output_dir_path))

        # Per-image hash-marker cache: each image (base + app) keeps a
        # sha256 file on the gateway alongside the tarball. When the
        # local image's hash matches the marker AND the tarball still
        # exists, skip the upload entirely. With this, an app-only
        # change re-uploads ~MB of code instead of ~1 GB of base.
        base_uploaded, base_hash = self._maybe_upload(
            gateway,
            local_base_path,
            base_remote_path,
            base_marker_remote_path,
            label="Base",
        )

        app_uploaded, app_hash = self._maybe_upload(
            gateway,
            local_app_path,
            app_remote_path,
            app_marker_remote_path,
            label="App",
        )

        self._cleanup_symlink(local_base_path)
        self._cleanup_symlink(local_app_path)

        logger.info("Container images ready on gateway")
        return PodmanImageMetadata(
            base_remote_path=base_remote_path,
            app_remote_path=app_remote_path,
            base_hash=base_hash,
            base_uploaded=base_uploaded,
            app_hash=app_hash,
            app_uploaded=app_uploaded,
        )

    def _maybe_upload(
        self,
        gateway: Any,
        local_path: Path,
        remote_path: Path,
        marker_path: Path,
        label: str,
    ) -> tuple[bool, str]:
        """Upload `local_path` only if the local sha256 doesn't match the
        remote marker. Returns `(uploaded, local_hash)`.
        """
        local_hash = self._compute_sha256(local_path)
        remote_marker_hash = self._read_remote_file(gateway, marker_path)
        remote_exists = self._remote_file_exists(gateway, remote_path)

        if remote_marker_hash == local_hash and remote_exists:
            logger.info("%s image cache hit: reusing remote %s", label, remote_path)
            return (False, local_hash)

        logger.info("%s image upload required (hash mismatch or missing remote artifact).", label)
        gateway.execute_command(f"rm -f {remote_path}")
        self._upload_artifact(gateway, local_path, remote_path)
        gateway.execute_command(f"printf '%s\n' '{local_hash}' > {marker_path}")
        logger.info("%s image uploaded; marker updated at %s", label, marker_path)
        return (True, local_hash)

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
