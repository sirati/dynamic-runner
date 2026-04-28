"""Podman container image build + dual-image artifact transfer.

Faithful port of the legacy `runtime_env.podman.podman_packaging` so the
gateway protocol stays byte-compatible during the legacy-deletion
transition. Sibling modules in the `packaging` package own SLURM job
submission (`job_manager`), gateway abstraction (`gateway/`), per-run
preparation (`preparation`), and the top-level pipeline (`pipeline`).

Transfer strategy (two-tier cache):

1. **Per-image hash marker** — sha256 of the entire tar.gz. When the
   local hash matches the gateway-side marker AND the tarball is still
   present, skip the upload entirely. This is the byte-equality fast
   path; it costs one `cat` + one `test -f` and zero bytes on the wire.

2. **Per-layer content-addressed blob cache** (`layered_transfer`) —
   when the per-image hash misses (one bit changed in the tarball),
   decompose the docker-archive into its sha256-addressed layer.tar
   and config blobs, query the gateway for which blobs are already
   present in `<output_dir>/layer-cache/blobs/sha256/`, and only
   upload the missing ones. The reassembled tarball at the legacy
   path keeps the SLURM job script's `podman load < <path>`
   contract unchanged.

The 1+2 pairing matters: tier 1 catches the "no change at all" case
in one round-trip; tier 2 catches "small change in a big image"
(e.g. a 160 KB project-source layer change inside a 3 GB app image).
Without tier 1 we'd needlessly extract+hash multi-GB tarballs every
run; without tier 2 a one-line fix triggers a multi-GB upload.
"""

from __future__ import annotations

import hashlib
import logging
import subprocess
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from .layered_transfer import LayeredUploader, UploadStats, make_bundle_from_archive

logger = logging.getLogger(__name__)


def _human_bytes(n: int) -> str:
    """Render byte counts in the closest power-of-1024 unit."""
    f = float(n)
    for u in ("B", "KB", "MB", "GB", "TB"):
        if f < 1024.0 or u == "TB":
            return f"{int(f)} B" if u == "B" else f"{f:.1f} {u}"
        f /= 1024.0
    return f"{f:.1f} TB"


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
    LAYER_CACHE_SUBDIR = "layer-cache"

    def __init__(self, *, layered_transfer: bool = True) -> None:
        self.image_name = "asm-tokenizer"
        self.image_tag = "latest"
        # Layered transfer is opt-out (defaults on). Disable for
        # diagnostics / regression bisect by passing False.
        self.layered_transfer = layered_transfer

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

        # Shared blob cache for both base and app — when the app
        # image inherits layers from base via `fromImage`, the layer
        # blobs are byte-identical and dedupe automatically through
        # this single content-addressed cache.
        layer_cache_root = output_dir_path / self.LAYER_CACHE_SUBDIR

        # Per-image hash-marker cache (tier 1): each image keeps a
        # sha256 of the full tar.gz on the gateway. When local==remote
        # AND the tarball still exists, skip everything. Layered
        # transfer (tier 2) only kicks in when this fast path misses.
        base_uploaded, base_hash = self._maybe_upload(
            gateway,
            local_base_path,
            base_remote_path,
            base_marker_remote_path,
            label="Base",
            layer_cache_root=layer_cache_root,
        )

        app_uploaded, app_hash = self._maybe_upload(
            gateway,
            local_app_path,
            app_remote_path,
            app_marker_remote_path,
            label="App",
            layer_cache_root=layer_cache_root,
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
        layer_cache_root: Path | None = None,
    ) -> tuple[bool, str]:
        """Upload `local_path` only if the local sha256 doesn't match the
        remote marker. Returns `(uploaded, local_hash)`.

        On hash mismatch, route through the layered uploader if it's
        enabled and `layer_cache_root` is provided — only the layers
        not already in the cache get sent over the wire, and the
        legacy `remote_path` keeps its `podman load`-compatible
        contents via gateway-side reassembly.
        """
        local_hash = self._compute_sha256(local_path)
        remote_marker_hash = self._read_remote_file(gateway, marker_path)
        remote_exists = self._remote_file_exists(gateway, remote_path)

        if remote_marker_hash == local_hash and remote_exists:
            logger.info("%s image cache hit: reusing remote %s", label, remote_path)
            return (False, local_hash)

        logger.info("%s image upload required (hash mismatch or missing remote artifact).", label)
        if self.layered_transfer and layer_cache_root is not None:
            self._upload_layered(gateway, local_path, remote_path, layer_cache_root, label)
        else:
            gateway.execute_command(f"rm -f {remote_path}")
            self._upload_artifact(gateway, local_path, remote_path)
        gateway.execute_command(f"printf '%s\n' '{local_hash}' > {marker_path}")
        logger.info("%s image uploaded; marker updated at %s", label, marker_path)
        return (True, local_hash)

    def _upload_layered(
        self,
        gateway: Any,
        local_path: Path,
        remote_path: Path,
        layer_cache_root: Path,
        label: str,
    ) -> UploadStats:
        """Layered upload path: extract → push missing blobs → reassemble."""
        bundle, scratch = make_bundle_from_archive(local_path)
        try:
            uploader = LayeredUploader(gateway, layer_cache_root)
            stats = uploader.upload(bundle, remote_path)
            logger.info(
                "%s image layered upload: %d/%d blobs sent (%s on the wire, %s deduplicated, %.0f%% cache hit)",
                label,
                stats.blobs_uploaded,
                stats.blobs_total,
                _human_bytes(stats.bytes_uploaded),
                _human_bytes(stats.bytes_skipped),
                stats.hit_ratio * 100,
            )
            return stats
        finally:
            import shutil
            shutil.rmtree(scratch, ignore_errors=True)

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
