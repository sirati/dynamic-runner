"""Layered upload of docker-archive container images.

Single concern: minimise bytes-on-the-wire when transferring an updated
docker-archive tarball to a gateway. The runner doesn't change the
on-gateway artifact contract — the `<output_dir>/<image_name>.tar`
path (where `image_name` comes from the consumer's
:class:`TaskDeploymentSpec`) still holds a fully-formed
`podman load`-compatible tarball after each upload — only the bytes
physically transferred shrink to the layers and config blobs that
aren't already present in the gateway's blob cache.

## Why this matters

The flake's `dockerImage` is a layered docker-archive (~3 GB across
80+ explicit layers — see `flake.nix`'s layeringPipeline). Without
layered transfer, every image-hash mismatch (which happens whenever
ANY layer changes) re-uploads the whole tarball.

A one-line edit to the project source typically invalidates only the
project-code layer (~160 KB compressed) and possibly the customisation
layer (a few KB of symlinks + metadata). Layered transfer turns that
~3 GB upload into ~200 KB.

## Wire layout (gateway-side)

```
<cache_root>/
  blobs/sha256/<digest>    # one file per layer.tar OR config blob;
                           # filename equals the file's sha256 hex
  manifests/<image>.json   # the docker-archive manifest.json that
                           # ties them together (one per logical image)
```

The cache_root is shared across base and app — when app inherits
layers from base via `fromImage`, the layers are content-addressed
identically and dedupe automatically.

## Reassembly contract

After `LayeredUploader.upload(bundle, output_path)` returns success,
the gateway file at `output_path` is a freshly-built `tar.gz`
docker-archive containing the manifest + config + all referenced
layer.tar blobs. SLURM job scripts continue to do
`podman load < <output_path>` exactly as before — they don't see the
blob cache at all. Reassembly uses hardlinks from the cache so the
gateway never re-reads the blob bytes (only writes the new tar
container).

## Atomicity

Blob uploads land at `<digest>.partial`, then atomically `mv` to
`<digest>`. Tarball reassembly writes `<output>.partial` then `mv`s
to `<output>`. Two concurrent runs targeting the same image cannot
expose half-uploaded blobs or half-built tarballs to a SLURM job
that's mid-launch.

## Non-goals

- We don't garbage-collect orphan blobs. A separate `prune` operation
  would walk all manifests and delete blobs not referenced by any —
  not in scope for the initial cut.
- We don't push to a registry; the cache is plain files on the
  gateway's shared filesystem.
- We don't compress blobs further; layer.tar inside the docker-archive
  is already gzip-compressed when the outer archive is gzipped, and
  blobs are stored as-extracted (uncompressed) for cheap hardlinking.
"""

from __future__ import annotations

import base64
import hashlib
import json as _json
import logging
import os
import shlex
import shutil
import tarfile
import tempfile
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterable

from .gateway import expand_gateway_tilde

logger = logging.getLogger(__name__)


@dataclass(frozen=True, slots=True)
class LayerBlob:
    """A content-addressed blob extracted from a docker-archive tarball.

    `digest` is the lowercase-hex sha256 of the file at `local_path`,
    and equals the directory name the blob lived under inside the
    docker-archive (for layer blobs) or the basename minus `.json`
    (for the config blob).
    """

    digest: str
    local_path: Path
    size: int
    kind: str  # "layer" | "config"


@dataclass(frozen=True, slots=True)
class ImageBundle:
    """An extracted docker-archive image, ready for layered upload."""

    image_label: str  # consumer-supplied image_name from TaskDeploymentSpec — for logs
    manifest_json_bytes: bytes
    config_blob: LayerBlob
    layer_blobs: tuple[LayerBlob, ...]  # in manifest order
    extracted_root: Path  # caller cleans up

    @property
    def all_blobs(self) -> tuple[LayerBlob, ...]:
        return (self.config_blob, *self.layer_blobs)

    @property
    def manifest_digest(self) -> str:
        """Stable identifier for the *image identity* — joins all blob
        digests in manifest order. Independent of tar/gzip determinism,
        unlike a hash of the outer archive."""
        joined = self.config_blob.digest + ":" + ":".join(b.digest for b in self.layer_blobs)
        return hashlib.sha256(joined.encode("utf-8")).hexdigest()

    @property
    def total_blob_bytes(self) -> int:
        return sum(b.size for b in self.all_blobs)


def _sha256(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(1 << 20), b""):
            h.update(chunk)
    return h.hexdigest()


def extract_image(local_archive: Path, scratch_dir: Path) -> ImageBundle:
    """Decompress + extract a docker-archive tarball into `scratch_dir`,
    parse its manifest.json, sha256-verify each layer.tar matches its
    on-disk dirname, and return an `ImageBundle`.

    `scratch_dir` should be an empty directory the caller created and
    will clean up after `LayeredUploader.upload(...)` returns.

    Raises ValueError if the archive doesn't conform to the
    docker-archive format (missing manifest.json, mismatched layer
    sha, multiple images in one archive, etc.).
    """
    scratch_dir.mkdir(parents=True, exist_ok=True)
    if any(scratch_dir.iterdir()):
        raise ValueError(f"scratch_dir {scratch_dir} must be empty for safe extraction")

    logger.debug("Extracting %s into %s", local_archive, scratch_dir)
    with tarfile.open(local_archive, "r:*") as tf:
        # Disallow path traversal — every member must stay inside scratch_dir.
        for member in tf.getmembers():
            target = (scratch_dir / member.name).resolve()
            if not str(target).startswith(str(scratch_dir.resolve())):
                raise ValueError(f"refusing tar member outside scratch_dir: {member.name}")
        tf.extractall(scratch_dir)  # noqa: S202 — checked above

    manifest_path = scratch_dir / "manifest.json"
    if not manifest_path.exists():
        raise ValueError(f"{local_archive} is not a docker-archive: no manifest.json")

    manifest_bytes = manifest_path.read_bytes()
    manifest = _json.loads(manifest_bytes)
    if not isinstance(manifest, list) or len(manifest) != 1:
        raise ValueError(
            f"{local_archive}: expected exactly one image in manifest, got {len(manifest)}"
        )
    entry = manifest[0]

    # Config blob
    config_name: str = entry["Config"]
    config_path = scratch_dir / config_name
    if not config_path.exists():
        raise ValueError(f"manifest references missing config blob {config_name}")
    config_digest = _sha256(config_path)
    expected_config_digest = config_name.removesuffix(".json")
    # Note: docker-archive convention names the config file
    # `<sha>.json` where <sha> is the sha256 of the JSON contents
    # (NOT of the file with the .json suffix). We don't strictly
    # require equality — some tools name config differently — but warn
    # if they diverge so the gateway-side dedup behaviour is
    # predictable.
    if expected_config_digest != config_digest:
        logger.debug(
            "config filename %s does not match content sha256 %s; "
            "using content sha256 for cache addressing",
            config_name,
            config_digest,
        )
    config_blob = LayerBlob(
        digest=config_digest,
        local_path=config_path,
        size=config_path.stat().st_size,
        kind="config",
    )

    # Layer blobs — manifest gives paths like "<sha>/layer.tar"
    layer_blobs: list[LayerBlob] = []
    for layer_rel in entry["Layers"]:
        layer_path = scratch_dir / layer_rel
        if not layer_path.exists():
            raise ValueError(f"manifest references missing layer {layer_rel}")
        layer_digest = _sha256(layer_path)
        # Verify dirname matches contents (the dockerTools convention).
        expected = layer_rel.split("/", 1)[0]
        if expected != layer_digest:
            raise ValueError(
                f"layer {layer_rel}: dirname {expected} != sha256 {layer_digest}"
            )
        layer_blobs.append(
            LayerBlob(
                digest=layer_digest,
                local_path=layer_path,
                size=layer_path.stat().st_size,
                kind="layer",
            )
        )

    return ImageBundle(
        image_label=local_archive.name,
        manifest_json_bytes=manifest_bytes,
        config_blob=config_blob,
        layer_blobs=tuple(layer_blobs),
        extracted_root=scratch_dir,
    )


# nix's dockerTools sometimes ships a streamed gzip — extract_image
# handles both .tar and .tar.gz transparently via tarfile's "r:*". For
# the rare case where the local image is a pure-stream pipe, callers
# should materialise to disk first.


@dataclass(frozen=True, slots=True)
class UploadStats:
    """Returned from `LayeredUploader.upload(...)`."""

    blobs_total: int
    blobs_uploaded: int
    bytes_uploaded: int
    bytes_skipped: int
    reassembled: bool

    @property
    def hit_ratio(self) -> float:
        if self.blobs_total == 0:
            return 1.0
        return 1.0 - (self.blobs_uploaded / self.blobs_total)


class MissingBlobsError(RuntimeError):
    """Cache integrity check failed after blob upload, before reassembly.

    Raised from `LayeredUploader._verify_blobs_present` when one or
    more blobs the manifest references are absent from the gateway
    cache, or when their on-cache size does not match the locally
    measured blob size.

    Failure mode this guards: a previous interrupted upload may have
    left a `<output>.manifest-id` marker referring to digests that
    have since been pruned (or were never finished landing). Without
    this check, `upload()` returns success and the SLURM-side
    `podman load` later fails with "Found incomplete layer ...
    pulling from registry", surfacing as an opaque dispatch error.
    """

    def __init__(
        self,
        image_label: str,
        missing: tuple[str, ...],
        mismatched: tuple[tuple[str, int, int], ...],
    ):
        self.image_label = image_label
        self.missing = missing
        self.mismatched = mismatched
        super().__init__(self._format())

    def _format(self) -> str:
        parts = []
        if self.missing:
            parts.append(f"missing: {', '.join(d[:12] for d in self.missing)}")
        if self.mismatched:
            parts.append(
                "size-mismatch: "
                + ", ".join(
                    f"{d[:12]} (expected {e}, got {a})"
                    for d, e, a in self.mismatched
                )
            )
        return f"[{self.image_label}] cache integrity check failed: " + "; ".join(parts)


class LayeredUploader:
    """Pushes a single `ImageBundle` to a gateway as a docker-archive
    tarball at `output_path`, sending only blobs not already present
    in the layer cache.

    The uploader is stateless w.r.t. images — construct one per
    gateway+cache_root pair and reuse it for both base and app.
    """

    BLOB_SUBDIR = "blobs/sha256"
    MANIFEST_SUBDIR = "manifests"
    PARTIAL_SUFFIX = ".partial"

    def __init__(self, gateway: Any, cache_root: Path) -> None:
        self.gateway = gateway
        # Resolve a leading ``~`` against the gateway's remote home up
        # front, so every derived remote path (``_blob_dir`` etc.) is
        # absolute before it reaches a ``shlex.quote``d remote ``mkdir``
        # / ``mv``. A quoted ``~`` is never shell-expanded server-side,
        # which would otherwise create a literal ``~`` directory.
        self.cache_root = Path(expand_gateway_tilde(gateway, cache_root))

    # ── Remote layout helpers ───────────────────────────────────────

    def _blob_dir(self) -> str:
        return str(self.cache_root / self.BLOB_SUBDIR)

    def _manifest_dir(self) -> str:
        return str(self.cache_root / self.MANIFEST_SUBDIR)

    def _blob_path(self, digest: str) -> str:
        return f"{self._blob_dir()}/{digest}"

    def ensure_layout(self) -> None:
        """Create the cache directories on the gateway if missing."""
        self.gateway.execute_command(
            f"mkdir -p {shlex.quote(self._blob_dir())} {shlex.quote(self._manifest_dir())}"
        )

    def list_present_blobs(self) -> set[str]:
        """Return the set of digests already cached on the gateway.

        We list once per upload run and set-diff locally; a per-blob
        `test -f` round-trip would be O(N) round-trips and cripple SSH
        performance on the typical "all hits" path.
        """
        rc, out, _ = self.gateway.execute_command(
            f"ls {shlex.quote(self._blob_dir())} 2>/dev/null"
        )
        if rc != 0:
            return set()
        digests: set[str] = set()
        for line in out.splitlines():
            name = line.strip()
            # Skip in-flight `.partial` files and any non-hex entries.
            if not name or name.endswith(self.PARTIAL_SUFFIX):
                continue
            if len(name) == 64 and all(c in "0123456789abcdef" for c in name):
                digests.add(name)
        return digests

    # ── Public upload entry point ───────────────────────────────────

    def upload(
        self,
        bundle: ImageBundle,
        output_path: Path,
        *,
        force_reassemble: bool = False,
    ) -> UploadStats:
        """Upload missing blobs from `bundle`, then ensure
        `output_path` on the gateway is a fully-formed docker-archive
        tarball matching the bundle.

        If `force_reassemble` is False (default), reassembly is
        skipped when the gateway-side `<output_path>.manifest-id`
        marker already records this bundle's `manifest_digest` AND
        `<output_path>` exists. That makes a "no blobs changed" run
        an O(1)-network operation.
        """
        self.ensure_layout()

        present = self.list_present_blobs()
        missing = [b for b in bundle.all_blobs if b.digest not in present]
        bytes_skipped = sum(b.size for b in bundle.all_blobs if b.digest in present)

        logger.info(
            "[%s] layer cache: %d/%d blobs present (%s skipped, %s to upload)",
            bundle.image_label,
            len(bundle.all_blobs) - len(missing),
            len(bundle.all_blobs),
            _human(bytes_skipped),
            _human(sum(b.size for b in missing)),
        )

        bytes_uploaded = 0
        for blob in missing:
            bytes_uploaded += self._upload_blob(blob)

        # Gate reassembly on the marker so a second run with no blob
        # changes doesn't even rebuild the tarball.
        marker_remote = f"{output_path}.manifest-id"
        manifest_id = bundle.manifest_digest
        existing_marker = self._read_remote(marker_remote)
        existing_tarball = self._remote_exists(output_path)
        needs_reassembly = (
            force_reassemble
            or existing_marker != manifest_id
            or not existing_tarball
        )

        if needs_reassembly:
            self._reassemble(bundle, output_path)
            self._write_remote(marker_remote, manifest_id)
        else:
            logger.info(
                "[%s] tarball at %s already matches manifest-id %s; skipping reassembly",
                bundle.image_label,
                output_path,
                manifest_id[:12],
            )

        # Also record the manifest under manifests/ for future GC.
        manifest_record = f"{self._manifest_dir()}/{bundle.image_label}.manifest-id"
        self._write_remote(manifest_record, manifest_id)

        return UploadStats(
            blobs_total=len(bundle.all_blobs),
            blobs_uploaded=len(missing),
            bytes_uploaded=bytes_uploaded,
            bytes_skipped=bytes_skipped,
            reassembled=needs_reassembly,
        )

    # ── Internal helpers ────────────────────────────────────────────

    def _upload_blob(self, blob: LayerBlob) -> int:
        target = self._blob_path(blob.digest)
        tmp = target + self.PARTIAL_SUFFIX
        logger.info(
            "  uploading %s blob %s (%s)",
            blob.kind,
            blob.digest[:12],
            _human(blob.size),
        )
        # transfer_file is the gateway abstraction's atomic copy.
        self.gateway.transfer_file(blob.local_path, Path(tmp))
        # Atomic rename so a half-uploaded blob never appears under its
        # final digest name (concurrent runs could race otherwise).
        self.gateway.execute_command(f"mv {shlex.quote(tmp)} {shlex.quote(target)}")
        return blob.size

    def _reassemble(self, bundle: ImageBundle, output_path: Path) -> None:
        """Build the docker-archive tar at `output_path` on the
        gateway, hardlinking from the blob cache so no blob bytes are
        re-read.

        Failure mode: if a blob is missing from the cache (race with a
        concurrent prune), the tar command fails and the partial
        output is left under .partial, never promoted to the final
        path. Caller can retry.

        Raises:
            MissingBlobsError: cache integrity check before reassembly
                found a blob the bundle references is absent or has a
                size that disagrees with the local blob. Reassembly is
                aborted before any `<output>.partial` is created and
                before any `<output>.manifest-id` marker is written,
                so a subsequent run will see "no marker" and re-upload.
        """
        self._verify_blobs_present(bundle)
        scratch = f"{output_path}.reassemble.{os.getpid()}"
        manifest_b64 = base64.b64encode(bundle.manifest_json_bytes).decode("ascii")
        config_target = f"{scratch}/{bundle.config_blob.digest}.json"

        cmds: list[str] = [
            "set -e",
            f"rm -rf {shlex.quote(scratch)}",
            f"mkdir -p {shlex.quote(scratch)}",
            # Manifest from base64 → bytes (works for arbitrary JSON without quoting hell).
            f"printf %s {shlex.quote(manifest_b64)} | base64 -d > {shlex.quote(scratch + '/manifest.json')}",
            # Config blob: hardlink from cache.
            f"ln {shlex.quote(self._blob_path(bundle.config_blob.digest))} {shlex.quote(config_target)}",
        ]
        for blob in bundle.layer_blobs:
            layer_dir = f"{scratch}/{blob.digest}"
            cmds += [
                f"mkdir -p {shlex.quote(layer_dir)}",
                f"ln {shlex.quote(self._blob_path(blob.digest))} {shlex.quote(layer_dir + '/layer.tar')}",
            ]
        # Build the tarball deterministically so byte-identical inputs
        # produce byte-identical outputs (helps downstream caching).
        partial = f"{output_path}{self.PARTIAL_SUFFIX}"
        cmds += [
            f"tar --sort=name --mtime=@0 --owner=0 --group=0 --numeric-owner "
            f"-czf {shlex.quote(partial)} -C {shlex.quote(scratch)} .",
            f"mv {shlex.quote(partial)} {shlex.quote(str(output_path))}",
            f"rm -rf {shlex.quote(scratch)}",
        ]
        joined = " && ".join(cmds)
        logger.info(
            "[%s] reassembling tarball at %s from %d cached blobs",
            bundle.image_label,
            output_path,
            len(bundle.all_blobs),
        )
        rc, _, err = self.gateway.execute_command(joined)
        if rc != 0:
            raise RuntimeError(
                f"reassembly failed for {bundle.image_label} → {output_path}: {err}"
            )

    def _read_remote(self, remote_path: str | Path) -> str | None:
        rc, out, _ = self.gateway.execute_command(f"cat {shlex.quote(str(remote_path))}")
        if rc != 0:
            return None
        s = out.strip()
        return s or None

    def _write_remote(self, remote_path: str | Path, content: str) -> None:
        self.gateway.execute_command(
            f"printf %s {shlex.quote(content)} > {shlex.quote(str(remote_path))}"
        )

    def _remote_exists(self, remote_path: str | Path) -> bool:
        rc, _, _ = self.gateway.execute_command(f"test -f {shlex.quote(str(remote_path))}")
        return rc == 0

    def _verify_blobs_present(self, bundle: ImageBundle) -> None:
        """Confirm every blob the bundle references exists on the
        gateway with the expected size, in one batched stat command.

        Single concern: integrity check between blob upload and
        reassembly. Re-using `list_present_blobs` would only catch
        absence, not size mismatch (a truncated `.partial` left over
        from a crashed transfer that was later renamed by hand, for
        example). One round-trip via a printf-loop scales to thousands
        of blobs without the O(N) SSH cost of per-blob `test -f`.

        Raises:
            MissingBlobsError: at least one blob is absent or has a
                cached size differing from `LayerBlob.size`.
        """
        blob_dir = self._blob_dir()
        # POSIX-portable size probe: GNU `stat -c %s` works on Linux
        # gateways; busybox/macOS BSD differ. We stay on `stat -c %s`
        # because every realistic gateway here is Linux — but we keep
        # the unparseable-line branch defensive so any divergence
        # surfaces as "missing" rather than silently passing.
        blobs = bundle.all_blobs
        digest_args = " ".join(shlex.quote(b.digest) for b in blobs)
        cmd = (
            f"for d in {digest_args}; do "
            f"  p={shlex.quote(blob_dir)}/$d; "
            f'  if [ -f "$p" ]; then '
            f'    printf \'OK %s %s\\n\' "$d" "$(stat -c %s "$p")"; '
            f"  else "
            f'    printf \'MISS %s\\n\' "$d"; '
            f"  fi; "
            f"done"
        )
        _, out, _ = self.gateway.execute_command(cmd)

        # Parse: one line per blob. Treat anything we can't parse as
        # missing — cheaper than reasoning about partial successes.
        observed: dict[str, int | None] = {}
        for raw in out.splitlines():
            line = raw.strip()
            if not line:
                continue
            parts = line.split()
            if parts[0] == "OK" and len(parts) == 3:
                digest = parts[1]
                try:
                    observed[digest] = int(parts[2])
                except ValueError:
                    observed[digest] = None
            elif parts[0] == "MISS" and len(parts) == 2:
                observed[parts[1]] = None
            else:
                logger.warning(
                    "[%s] unparseable blob-check line: %r (treating as missing)",
                    bundle.image_label,
                    raw,
                )

        missing: list[str] = []
        mismatched: list[tuple[str, int, int]] = []
        for blob in blobs:
            seen = observed.get(blob.digest, None)
            if seen is None:
                missing.append(blob.digest)
            elif seen != blob.size:
                mismatched.append((blob.digest, blob.size, seen))

        if missing or mismatched:
            logger.error(
                "[%s] cache integrity check failed before reassembly: "
                "%d missing, %d size-mismatched (missing=%s mismatched=%s)",
                bundle.image_label,
                len(missing),
                len(mismatched),
                [d[:12] for d in missing],
                [(d[:12], e, a) for d, e, a in mismatched],
            )
            raise MissingBlobsError(
                image_label=bundle.image_label,
                missing=tuple(missing),
                mismatched=tuple(mismatched),
            )

        logger.debug(
            "[%s] verified %d blobs present in cache",
            bundle.image_label,
            len(blobs),
        )


def _human(n: int) -> str:
    """Render byte counts in the closest power-of-1024 unit."""
    units = ("B", "KB", "MB", "GB", "TB")
    f = float(n)
    for u in units:
        if f < 1024.0 or u == units[-1]:
            return f"{f:.1f} {u}" if u != "B" else f"{int(f)} B"
        f /= 1024.0
    return f"{f:.1f} {units[-1]}"


def make_bundle_from_archive(local_archive: Path) -> tuple[ImageBundle, Path]:
    """Convenience: extract `local_archive` into a fresh tempdir and
    return `(bundle, scratch_dir)`. Caller must `shutil.rmtree(scratch_dir)`
    after upload.

    Use this when the caller doesn't already have a scratch dir.
    """
    scratch = Path(tempfile.mkdtemp(prefix="layered-"))
    try:
        return extract_image(local_archive, scratch), scratch
    except Exception:
        shutil.rmtree(scratch, ignore_errors=True)
        raise


def upload_image_layered(
    gateway: Any,
    local_archive: Path,
    cache_root: Path,
    output_path: Path,
    image_label: str | None = None,
) -> UploadStats:
    """One-shot: extract → upload missing blobs → reassemble tarball.

    The caller still needs an idea of where the cache lives
    (`cache_root` — typically `<output_dir>/layer-cache`) and where
    the reassembled tarball should land (`output_path` — keeps
    backwards compat with existing SLURM job scripts that
    `podman load < <output_path>`).
    """
    bundle, scratch = make_bundle_from_archive(local_archive)
    if image_label is not None:
        bundle = ImageBundle(
            image_label=image_label,
            manifest_json_bytes=bundle.manifest_json_bytes,
            config_blob=bundle.config_blob,
            layer_blobs=bundle.layer_blobs,
            extracted_root=bundle.extracted_root,
        )
    try:
        uploader = LayeredUploader(gateway, cache_root)
        stats = uploader.upload(bundle, output_path)
        logger.info(
            "[%s] uploaded %d/%d blobs (%s) in %s mode (%.1f%% cache hit)",
            bundle.image_label,
            stats.blobs_uploaded,
            stats.blobs_total,
            _human(stats.bytes_uploaded),
            "reassembled" if stats.reassembled else "no-op",
            stats.hit_ratio * 100,
        )
        return stats
    finally:
        shutil.rmtree(scratch, ignore_errors=True)


def iter_referenced_digests(bundles: Iterable[ImageBundle]) -> set[str]:
    """For GC: collect every digest referenced by any of `bundles`.
    Anything in the cache outside this set can be safely removed.
    """
    out: set[str] = set()
    for b in bundles:
        out.add(b.config_blob.digest)
        for layer in b.layer_blobs:
            out.add(layer.digest)
    return out


def prune_orphan_blobs(
    gateway: Any,
    cache_root: Path,
    keep_digests: set[str],
    *,
    dry_run: bool = False,
) -> tuple[int, int]:
    """Delete blobs from the gateway cache that aren't in `keep_digests`.

    `keep_digests` is the union of digests referenced by every image
    you want to retain — typically `iter_referenced_digests` over the
    current `(base, app)` bundles, plus any older runs you still need.

    Returns `(blobs_pruned, bytes_pruned)`. With `dry_run=True`, only
    counts and logs without deleting.

    GC is a separate concern from the upload path; layered uploads
    never delete anything, so this exists for "the cache is full /
    needs trimming" maintenance ops only.
    """
    blob_dir = str(cache_root / LayeredUploader.BLOB_SUBDIR)
    rc, out, _ = gateway.execute_command(
        f"ls -la {shlex.quote(blob_dir)} 2>/dev/null | awk '{{print $5\" \"$NF}}'"
    )
    if rc != 0:
        return (0, 0)

    pruned = 0
    bytes_pruned = 0
    targets: list[str] = []
    for line in out.splitlines():
        parts = line.split(" ", 1)
        if len(parts) != 2:
            continue
        try:
            size = int(parts[0])
        except ValueError:
            continue
        digest = parts[1].strip()
        if len(digest) != 64 or any(c not in "0123456789abcdef" for c in digest):
            continue
        if digest in keep_digests:
            continue
        targets.append(digest)
        pruned += 1
        bytes_pruned += size

    if not targets:
        return (0, 0)

    logger.info(
        "%s prune of %d orphan blobs (%s)",
        "dry-run" if dry_run else "executing",
        pruned,
        _human(bytes_pruned),
    )
    if dry_run:
        return (pruned, bytes_pruned)

    # Delete in batches so the shell command stays under any sane
    # arg-length limit even with thousands of orphans.
    BATCH = 64
    for i in range(0, len(targets), BATCH):
        batch = targets[i : i + BATCH]
        joined = " ".join(shlex.quote(f"{blob_dir}/{d}") for d in batch)
        gateway.execute_command(f"rm -f {joined}")

    return (pruned, bytes_pruned)
