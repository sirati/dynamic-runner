"""Podman container image build + layered transfer.

Owns the single-image build (via `nix build .#dockerImage`) and
upload to the gateway. Sibling modules in the `packaging` package
own SLURM job submission (`job_manager`), gateway abstraction
(`gateway/`), per-run preparation (`preparation`), and the top-level
pipeline (`pipeline`).

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
(e.g. a 160 KB project-source layer change). Without tier 1 we'd
needlessly extract+hash multi-GB tarballs every run; without tier 2
a one-line fix triggers a multi-GB upload.

The flake's `dockerImage` package uses an explicit `layeringPipeline`
(see flake.nix) that puts each semantic concern in its own layer:
project code, rust wheel, ghidra, openjdk, each major python package,
basics. With this layout, an edit-then-rebuild typically invalidates
only one or two layers — usually <10 MB on the wire after layered
transfer's blob cache hits.
"""

from __future__ import annotations

import hashlib
import logging
import os
import subprocess
from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING, Any

from ..deployment_spec import TaskDeploymentSpec
from .gateway import expand_gateway_tilde, retry_transient
from .layered_transfer import LayeredUploader, UploadStats, make_bundle_from_archive

if TYPE_CHECKING:
    from .upload_milestones import UploadProgressReporter

logger = logging.getLogger(__name__)


# Default location for the partial-build layer-assignment cache
# (per developer machine; not committed). Lives next to the project
# root so different projects' caches don't collide.
DEFAULT_LAYER_CACHE_REL = ".docker-layer-cache.json"

# Legacy in-tree location for the layer-assignment extractor
# script. Used as the fallback when neither the explicit
# `layer_extractor_script_path` ctor arg nor the
# `DYNRUNNER_LAYER_EXTRACTOR_SCRIPT` env var is set.
#
# Pre-2026-04-29 every consumer kept a copy at
# `<project_root>/nix/extract-layer-assignment.py`. Then the
# script moved upstream to nix-docker-layered-image
# (`pkgs/extract-layer-assignment/extract-layer-assignment.py`)
# and is now exposed via that flake's `packages.<system>.
# extract-layer-assignment` derivation and its default overlay.
# Consumers using the upstream package point at the resolved
# store path via the explicit arg or the env var below; the
# legacy convention path is kept so consumers that still ship
# a vendored copy keep working.
LAYER_EXTRACTOR_SCRIPT_REL = Path("nix") / "extract-layer-assignment.py"

# Env var consumers set to point at the upstream
# `extract-layer-assignment` script. Resolution order in
# `_resolve_layer_extractor_script`: explicit ctor arg →
# env var → legacy in-tree path → not found.
LAYER_EXTRACTOR_ENV_VAR = "DYNRUNNER_LAYER_EXTRACTOR_SCRIPT"

# Env var name read by flake.nix's previousAssignment hook.
LAYER_CACHE_ENV_VAR = "NIX_DOCKER_LAYER_CACHE"


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
    """Metadata for the single remote Podman image artifact.

    `uploaded` reflects whether the local hash matched the remote
    marker (i.e. the upload was a cache hit and skipped). With
    layered transfer enabled, even on a cache miss the actual
    bytes-on-the-wire are usually a small fraction of the image
    size — see `layered_transfer.py`.
    """

    remote_path: Path
    image_hash: str
    uploaded: bool


class PodmanPackaging:
    """Podman-based packaging implementation for SLURM cluster environments.

    Task-package identity (image name/tag, nix build target, image tar
    basename, sha256 marker basename) is read from the consumer-supplied
    :class:`TaskDeploymentSpec`. The framework holds no defaults here —
    the spec is the single source of truth.
    """

    LAYER_CACHE_SUBDIR = "layer-cache"

    def __init__(
        self,
        deployment: TaskDeploymentSpec,
        *,
        layered_transfer: bool = True,
        layer_cache_path: Path | str | None = None,
        layer_extractor_script_path: Path | str | None = None,
    ) -> None:
        self.deployment = deployment
        # Layered transfer is opt-out (defaults on). Disable for
        # diagnostics / regression bisect by passing False.
        self.layered_transfer = layered_transfer
        # Local file path used for the partial-build layer-assignment
        # cache. When None we resolve to `<project_root>/.docker-layer-cache.json`
        # at build time. Pass an explicit path (or False to disable
        # entirely) to override.
        self._layer_cache_path = layer_cache_path
        # Absolute path to the layer-assignment extractor script.
        # Resolution order in `_resolve_layer_extractor_script`:
        #   1. this explicit arg (when set)
        #   2. `DYNRUNNER_LAYER_EXTRACTOR_SCRIPT` env var
        #   3. legacy `<project_root>/nix/extract-layer-assignment.py`
        # Consumers using the upstream `nix-docker-layered-image`
        # flake set this to the resolved store path (e.g. via
        # `pkgs.extract-layer-assignment.outPath + "/bin/..."` in
        # their flake / dispatch wrapper). See module-level
        # `LAYER_EXTRACTOR_ENV_VAR` doc for the env-var alternative.
        self._layer_extractor_script_path = layer_extractor_script_path

    @property
    def image_name(self) -> str:
        return self.deployment.image_name

    @property
    def image_tag(self) -> str:
        return self.deployment.image_tag

    def _normalize_path(self, gateway: Any, path: str | Path) -> Path:
        # Resolve a leading ``~`` against the gateway's remote home so a
        # ``~``-prefixed output dir never lands a literal ``~`` directory
        # once the path is ``shlex.quote``d into a remote ``mkdir``. The
        # production path (pipeline._make_slurm_config) already expands at
        # the config boundary; this is the defensive lower-level guard.
        return Path(expand_gateway_tilde(gateway, path))

    def _resolve_layer_cache_path(self, local_project_root: Path) -> Path | None:
        """Resolve the local file path for the partial-build cache.

        Returns None if caching is explicitly disabled (caller passed
        `layer_cache_path=False`). Otherwise resolves to the
        configured path or the default beside `local_project_root`.
        """
        if self._layer_cache_path is False:
            return None
        if self._layer_cache_path is None:
            return local_project_root / DEFAULT_LAYER_CACHE_REL
        return Path(self._layer_cache_path)

    def _resolve_layer_extractor_script(
        self, local_project_root: Path
    ) -> Path | None:
        """Resolve the absolute path to the layer-assignment
        extractor script. Returns None when no plausible path is
        configured — caller (`_refresh_layer_cache`) treats that as
        "skip cache refresh, next build is cold".

        Resolution order:
          1. The `layer_extractor_script_path` ctor arg (when set).
             Wins unconditionally — consumers that know exactly
             where their script lives pass this.
          2. The `DYNRUNNER_LAYER_EXTRACTOR_SCRIPT` env var. Lets a
             consumer point at the upstream nix-docker-layered-image
             store path via its flake / dispatch wrapper without
             threading the value through every framework
             construction call.
          3. The legacy in-tree convention
             `<project_root>/nix/extract-layer-assignment.py`.
             Pre-2026-04-29 every consumer kept a vendored copy
             there; consumers that still ship one keep working.

        Existence is checked by the caller — this method just
        returns the resolved Path. Returning None (no configured
        path at all) is a separate signal from "configured but
        missing" so the warning is specific.
        """
        if self._layer_extractor_script_path is not None:
            return Path(self._layer_extractor_script_path)
        env = os.environ.get(LAYER_EXTRACTOR_ENV_VAR)
        if env:
            return Path(env)
        return local_project_root / LAYER_EXTRACTOR_SCRIPT_REL

    def _build_nix_target(self, local_project_root: Path, target: str, out_link: str) -> Path:
        """Build a nix target. If a layer-assignment cache from a
        previous build exists, this run becomes incremental: the
        flake reads the cache via NIX_DOCKER_LAYER_CACHE (impure)
        and the layeringPipeline preserves each pre-existing layer's
        content grouping. After a successful build we refresh the
        cache from the new image so the NEXT run is incremental too.

        Without a cache (cold first build), the nix build runs in
        pure mode as usual.
        """
        cache_path = self._resolve_layer_cache_path(local_project_root)
        env = os.environ.copy()
        build_cmd = ["nix", "build", target, "--out-link", out_link]

        if cache_path is not None and cache_path.exists():
            logger.info(
                "Layer-cache hit at %s; running incremental nix build (--impure).",
                cache_path,
            )
            env[LAYER_CACHE_ENV_VAR] = str(cache_path.resolve())
            build_cmd.append("--impure")
        elif cache_path is not None:
            logger.info(
                "No layer-cache at %s; cold nix build. (Cache will be primed after this run.)",
                cache_path,
            )

        result = subprocess.run(
            build_cmd,
            cwd=str(local_project_root),
            capture_output=True,
            text=True,
            env=env,
        )

        if result.returncode != 0:
            logger.error("Nix build failed for %s: %s", target, result.stderr)
            raise RuntimeError(f"Container image build failed for {target}: {result.stderr}")

        built_path = local_project_root / out_link
        if not built_path.exists():
            raise RuntimeError(f"Container image result not found after build for {target}")

        # Refresh the layer-assignment cache from the new image so
        # the next build can run incrementally. Cache failures are
        # warnings, not fatal — the build itself succeeded and the
        # next run will fall back to a cold build.
        if cache_path is not None:
            self._refresh_layer_cache(local_project_root, built_path, cache_path)

        return built_path

    def _refresh_layer_cache(
        self,
        local_project_root: Path,
        built_image_path: Path,
        cache_path: Path,
    ) -> None:
        """Run the extract-layer-assignment script against the just-
        built image and write the result to `cache_path`. Atomic via
        temp + rename so an interrupted refresh can't leave a
        half-written cache that breaks the next build.
        """
        extractor = self._resolve_layer_extractor_script(local_project_root)
        if extractor is None or not extractor.exists():
            logger.warning(
                "Layer extractor not found at %s; skipping cache refresh. "
                "Next build will be cold (full ~1.5GB re-upload on cache "
                "miss). The script moved upstream to nix-docker-layered-image "
                "(pkgs.extract-layer-assignment) post-2026-04-29; consumers "
                "using that flake point at the resolved store path via the "
                "%s env var or `layer_extractor_script_path=` ctor arg on "
                "PodmanPackaging. The legacy in-tree path "
                "`%s/nix/extract-layer-assignment.py` is checked as a "
                "fallback for consumers that vendor a copy.",
                extractor,
                LAYER_EXTRACTOR_ENV_VAR,
                local_project_root,
            )
            return

        cache_path.parent.mkdir(parents=True, exist_ok=True)
        tmp_path = cache_path.with_suffix(cache_path.suffix + ".partial")
        try:
            # Use system python3 (not nix-develop) so the dev-shell
            # banner doesn't pollute stdout — the extractor only
            # uses stdlib.
            with tmp_path.open("wb") as out:
                subprocess.run(
                    ["python3", str(extractor), str(built_image_path.resolve())],
                    check=True,
                    stdout=out,
                )
            tmp_path.replace(cache_path)
            logger.info("Layer-cache refreshed at %s", cache_path)
        except (subprocess.CalledProcessError, OSError) as exc:
            logger.warning("Layer-cache refresh failed: %s", exc)
            try:
                tmp_path.unlink(missing_ok=True)
            except OSError:
                pass

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
        # Same idempotent-copy boundary as the layered uploader's
        # per-blob transfer — one transient scp/ssh fault must not
        # kill the dispatch.
        retry_transient(
            lambda: gateway.transfer_file(local_path, str(remote_path)),
            what=f"image artifact upload to {remote_path}",
        )

    def build_images(self, gateway: Any, local_project_root: Path, output_dir: str | Path) -> PodmanImageMetadata:
        """Build and transfer the single image artifact.

        The flake's `dockerImage` is a semantically-layered
        docker-archive with one layer per declared unit (see
        flake.nix and nix/semantic-layering.nix). The base/app split
        that earlier versions of this method handled is gone.

        Two-tier upload optimisation:

        1. The nix build itself runs INCREMENTALLY when a previous
           build's layer assignment is cached locally at
           `<project_root>/.docker-layer-cache.json` (configurable
           via `layer_cache_path` arg to PodmanPackaging). The flake
           reads the cache via NIX_DOCKER_LAYER_CACHE + --impure
           and preserves each pre-existing layer's grouping so
           unchanged units produce byte-identical layer.tar bytes.
           After a successful build, we refresh the cache from the
           new image for the next run.

        2. The upload itself is done via layered_transfer.py's
           content-addressed blob cache on the gateway: only layers
           whose sha256 differs from what's cached on the gateway
           get re-transmitted.

        Together these mean a typical edit-test cycle (one Python
        file changed) ships a few MB instead of the full ~3 GB image.
        """
        logger.info("Building container image locally using Nix...")

        local_image_path = self._build_nix_target(
            local_project_root=local_project_root,
            target=self.deployment.nix_build_target,
            out_link="docker-image-result",
        )

        output_dir_path = self._normalize_path(gateway, output_dir)
        remote_path = output_dir_path / self.deployment.image_tar_basename
        marker_remote_path = output_dir_path / self.deployment.image_marker_basename
        layer_cache_root = output_dir_path / self.LAYER_CACHE_SUBDIR

        gateway.create_directory(str(output_dir_path))

        uploaded, image_hash = self._maybe_upload(
            gateway,
            local_image_path,
            remote_path,
            marker_remote_path,
            label=self.deployment.image_name,
            layer_cache_root=layer_cache_root,
        )

        self._cleanup_symlink(local_image_path)

        logger.info("Container image ready on gateway")
        return PodmanImageMetadata(
            remote_path=remote_path,
            image_hash=image_hash,
            uploaded=uploaded,
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
        from .upload_milestones import UploadProgressReporter

        local_hash = self._compute_sha256(local_path)
        remote_marker_hash = self._read_remote_file(gateway, marker_path)
        remote_exists = self._remote_file_exists(gateway, remote_path)

        # Single owner of the upload bring-up milestones for this image:
        # one reporter drives the SKIPPED (cache hit / all-blobs-cached) vs
        # STARTED → per-minute PROGRESS → FINISHED milestone stream across
        # whichever transfer branch runs below. The transfer code stays
        # milestone-agnostic — the layered uploader receives the reporter
        # and notifies it per blob; the fallback brackets its single
        # transfer with start/finish.
        reporter = UploadProgressReporter(label)

        if remote_marker_hash == local_hash and remote_exists:
            logger.info("%s image cache hit: reusing remote %s", label, remote_path)
            reporter.skipped("cached (remote artifact matches local image hash)")
            return (False, local_hash)

        logger.info("%s image upload required (hash mismatch or missing remote artifact).", label)
        if self.layered_transfer and layer_cache_root is not None:
            self._upload_layered(gateway, local_path, remote_path, layer_cache_root, label, reporter)
        else:
            gateway.execute_command(f"rm -f {remote_path}")
            reporter.start(total_blobs=1, total_bytes=local_path.stat().st_size)
            try:
                self._upload_artifact(gateway, local_path, remote_path)
                reporter.blob_done(local_path.stat().st_size)
            finally:
                reporter.finish()
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
        reporter: "UploadProgressReporter | None" = None,
    ) -> UploadStats:
        """Layered upload path: extract → push missing blobs → reassemble.

        `reporter` (when given) receives the per-blob bring-up milestones
        from the uploader's transfer loop; `_maybe_upload` owns its
        lifecycle, so this method just threads it through.
        """
        bundle, scratch = make_bundle_from_archive(local_path)
        try:
            uploader = LayeredUploader(gateway, layer_cache_root)
            stats = uploader.upload(bundle, remote_path, reporter=reporter)
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
        # `--cgroup-manager=cgroupfs`: rootless podman defaults to the
        # systemd cgroup-manager, which depends on the user's
        # `user@<uid>.service` being healthy. Under SLURM with shared
        # accounts (or without `loginctl enable-linger`), that systemd
        # user instance start/stop-storms across consecutive job steps;
        # `podman load`'s pause-process registration drops mid-write
        # ("sendmsg: broken pipe") and the load reports success without
        # finalising the manifest — subsequent `podman run` then fails
        # with "image not known". cgroupfs sidesteps systemd entirely.
        return f"podman --root {storage_root} --runroot {run_root} --cgroup-manager=cgroupfs load < {image_path}"

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
            "--cgroup-manager=cgroupfs",
            "run",
            "--rm",
            # Podman's rootless default is pids_limit=2048 (from containers.conf).
            # Under SLURM, fork-heavy or thread-heavy workloads (JVM, parallel
            # compilers, autotools) exhaust that cap → clone() EAGAIN. Pass 0
            # (unlimited) explicitly so the silent builtin default never fires;
            # the host protections are the RAM cap and nice level.
            "--pids-limit=0",
        ]

        for host_path, container_path in mounts.items():
            cmd_parts.extend(["-v", f"{host_path}:{container_path}"])

        for host_port, container_port in ports.items():
            cmd_parts.extend(["-p", f"{host_port}:{container_port}"])

        cmd_parts.append(f"{image_name}:{image_tag}")
        cmd_parts.extend(entrypoint_args)
        return " ".join(cmd_parts)

    def get_images_command(self, storage_root: str, run_root: str) -> str:
        return f"podman --root {storage_root} --runroot {run_root} --cgroup-manager=cgroupfs images"

    def get_version_command(self) -> str:
        return "podman --version"

    def get_image_name(self) -> str:
        return self.image_name

    def get_image_tag(self) -> str:
        return self.image_tag

    def requires_storage_paths(self) -> bool:
        return True
