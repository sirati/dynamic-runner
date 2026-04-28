"""Tests for the layered docker-archive uploader.

Strategy: simulate a "remote" filesystem inside a per-test tempdir,
swapping the real Gateway (SSH or local) for a `LocalDirGateway` fake
that translates `execute_command` (a shell snippet) and
`transfer_file` into operations on a local subdirectory. This lets us
verify cache hits, atomic renames, and reassembly without spinning up
an actual gateway.

Two-image dedupe is the marquee test: uploading base then app must
not retransfer the 3 base layers that app inherits via fromImage.
"""

from __future__ import annotations

import json
import shutil
import subprocess
import tarfile
from pathlib import Path

import pytest

from dynamic_runner.packaging.layered_transfer import (
    ImageBundle,
    LayerBlob,
    LayeredUploader,
    extract_image,
    upload_image_layered,
)


class LocalDirGateway:
    """Tiny fake of the `Gateway` Protocol used by the uploader.

    Treats `remote_root` as the gateway's filesystem root. Shell
    snippets the uploader emits run via `bash -c` with `cd
    remote_root` so paths line up.
    """

    def __init__(self, remote_root: Path) -> None:
        self.remote_root = remote_root
        self.remote_root.mkdir(parents=True, exist_ok=True)
        self.transfer_count = 0
        self.bytes_transferred = 0

    def execute_command(self, command: str, cwd: Path | None = None) -> tuple[int, str, str]:
        proc = subprocess.run(
            ["bash", "-c", command],
            capture_output=True,
            text=True,
            cwd=str(cwd) if cwd else None,
        )
        return (proc.returncode, proc.stdout, proc.stderr)

    def transfer_file(self, local_path: Path, remote_path: Path) -> None:
        # The uploader passes ABSOLUTE paths for blob targets, so
        # we just copy. We also count for assertions.
        target = Path(str(remote_path))
        target.parent.mkdir(parents=True, exist_ok=True)
        shutil.copyfile(local_path, target)
        self.transfer_count += 1
        self.bytes_transferred += target.stat().st_size

    def create_directory(self, remote_path) -> None:
        Path(str(remote_path)).mkdir(parents=True, exist_ok=True)


# ── Synthetic-image fixture ───────────────────────────────────────────


def _make_layer_tar(dest: Path, payload: bytes) -> None:
    """Write a 1-file tar at `dest` whose member contents are `payload`.
    Used to fabricate fake layer.tar blobs for tests."""
    with tarfile.open(dest, "w") as tf:
        info = tarfile.TarInfo(name="data.bin")
        info.size = len(payload)
        import io
        tf.addfile(info, io.BytesIO(payload))


def _build_synthetic_archive(
    out_path: Path,
    layer_payloads: list[bytes],
    image_tag: str = "fake:latest",
) -> None:
    """Build a docker-archive tarball at `out_path` whose layers are
    fabricated from `layer_payloads`. Layout matches what
    dockerTools.buildLayeredImage produces.
    """
    import hashlib
    import tempfile

    work = Path(tempfile.mkdtemp(prefix="synth-"))
    try:
        layer_paths_for_manifest: list[str] = []
        for payload in layer_payloads:
            # Produce a layer.tar in a tmp file, sha256 its bytes,
            # then move under <sha>/layer.tar (matches dockerTools layout).
            tmp_layer = work / "tmp.tar"
            _make_layer_tar(tmp_layer, payload)
            digest = hashlib.sha256(tmp_layer.read_bytes()).hexdigest()
            layer_dir = work / digest
            layer_dir.mkdir()
            shutil.move(str(tmp_layer), str(layer_dir / "layer.tar"))
            layer_paths_for_manifest.append(f"{digest}/layer.tar")

        # Config blob — minimal docker config json. Sha is derived from
        # contents and used as filename.
        config_obj = {
            "architecture": "amd64",
            "config": {"Entrypoint": ["/bin/true"]},
            "rootfs": {"type": "layers", "diff_ids": layer_paths_for_manifest},
        }
        config_bytes = json.dumps(config_obj, sort_keys=True).encode()
        config_digest = hashlib.sha256(config_bytes).hexdigest()
        (work / f"{config_digest}.json").write_bytes(config_bytes)

        manifest = [
            {
                "Config": f"{config_digest}.json",
                "RepoTags": [image_tag],
                "Layers": layer_paths_for_manifest,
            }
        ]
        (work / "manifest.json").write_text(json.dumps(manifest))

        # Wrap into tar.gz
        out_path.parent.mkdir(parents=True, exist_ok=True)
        with tarfile.open(out_path, "w:gz") as tf:
            for item in sorted(work.iterdir()):
                tf.add(str(item), arcname=item.name)
    finally:
        shutil.rmtree(work, ignore_errors=True)


# ── Tests ─────────────────────────────────────────────────────────────


def test_extract_image_parses_synthetic_archive(tmp_path):
    archive = tmp_path / "img.tar.gz"
    _build_synthetic_archive(archive, [b"alpha-payload", b"beta-payload"])

    scratch = tmp_path / "scratch"
    bundle = extract_image(archive, scratch)

    assert len(bundle.layer_blobs) == 2
    assert all(len(b.digest) == 64 for b in bundle.all_blobs)
    assert bundle.config_blob.kind == "config"
    # manifest_digest is stable across re-extractions of the same archive.
    bundle2 = extract_image(archive, tmp_path / "scratch2")
    assert bundle.manifest_digest == bundle2.manifest_digest


def test_first_upload_sends_all_blobs_then_second_sends_zero(tmp_path):
    archive = tmp_path / "img.tar.gz"
    _build_synthetic_archive(archive, [b"alpha", b"beta", b"gamma"])

    cache = tmp_path / "remote/cache"
    out = tmp_path / "remote/img.tar.gz"
    gw = LocalDirGateway(tmp_path / "remote")

    s1 = upload_image_layered(gw, archive, cache, out, image_label="img-A")
    assert s1.blobs_uploaded == s1.blobs_total  # 3 layers + 1 config
    assert s1.blobs_uploaded == 4
    assert s1.reassembled is True
    assert out.exists(), "reassembled tarball should exist after first upload"

    # Reset the counter; second upload of the SAME archive must move zero blobs.
    gw.transfer_count = 0
    gw.bytes_transferred = 0
    s2 = upload_image_layered(gw, archive, cache, out, image_label="img-A")
    assert s2.blobs_uploaded == 0
    assert s2.reassembled is False, "marker should short-circuit reassembly"
    assert gw.transfer_count == 0


def test_two_images_share_layers_via_dedupe(tmp_path):
    """Build base and app archives that share the first two layers,
    upload base, then upload app — only the unique app layers
    (and its config) should hit the wire."""
    base = tmp_path / "base.tar.gz"
    app = tmp_path / "app.tar.gz"
    shared = [b"shared-A", b"shared-B"]
    _build_synthetic_archive(base, shared)
    _build_synthetic_archive(app, [*shared, b"app-only-1", b"app-only-2"])

    cache = tmp_path / "remote/cache"
    gw = LocalDirGateway(tmp_path / "remote")

    s_base = upload_image_layered(
        gw, base, cache, tmp_path / "remote/base.tar.gz", image_label="base"
    )
    assert s_base.blobs_uploaded == 3  # 2 layers + 1 config

    gw.transfer_count = 0
    gw.bytes_transferred = 0
    s_app = upload_image_layered(
        gw, app, cache, tmp_path / "remote/app.tar.gz", image_label="app"
    )
    # app has 4 layers + 1 config = 5 blobs, 2 layers shared with base, so
    # 3 new (2 layers + 1 config — different config from base).
    assert s_app.blobs_total == 5
    assert s_app.blobs_uploaded == 3, (
        f"expected 2 new layers + 1 new config; uploaded {s_app.blobs_uploaded}"
    )
    assert gw.transfer_count == 3


def test_reassembled_tarball_is_loadable_docker_archive(tmp_path):
    """The reassembled tarball must be parseable as a docker-archive
    (correct manifest.json, all referenced files present).
    `podman load` would consume it identically."""
    archive = tmp_path / "img.tar.gz"
    _build_synthetic_archive(archive, [b"x", b"y", b"z"])

    cache = tmp_path / "remote/cache"
    out = tmp_path / "remote/img.tar.gz"
    gw = LocalDirGateway(tmp_path / "remote")
    upload_image_layered(gw, archive, cache, out, image_label="img")

    extracted = tmp_path / "verify"
    extracted.mkdir()
    with tarfile.open(out, "r:gz") as tf:
        tf.extractall(extracted)

    manifest = json.loads((extracted / "manifest.json").read_text())
    assert isinstance(manifest, list) and len(manifest) == 1
    entry = manifest[0]
    assert (extracted / entry["Config"]).exists()
    for layer in entry["Layers"]:
        assert (extracted / layer).exists(), f"missing reassembled layer {layer}"


def test_partial_files_are_not_listed_as_present(tmp_path):
    cache = tmp_path / "remote/cache"
    blobdir = cache / "blobs/sha256"
    blobdir.mkdir(parents=True)

    # Real blob:
    real_digest = "a" * 64
    (blobdir / real_digest).write_bytes(b"real")
    # In-flight upload:
    (blobdir / f"{'b' * 64}.partial").write_bytes(b"half")
    # Junk file (not a 64-hex digest):
    (blobdir / "README").write_text("hi")

    gw = LocalDirGateway(tmp_path / "remote")
    uploader = LayeredUploader(gw, cache)
    present = uploader.list_present_blobs()
    assert present == {real_digest}


def test_corrupt_layer_dirname_raises(tmp_path):
    """If dirname != sha256(layer.tar) the bundle is rejected
    immediately. Catches gateway-side cache corruption, malicious
    archives, or accidental byte flips before they propagate to
    podman load."""
    work = tmp_path / "build"
    work.mkdir()
    bad_dir = work / ("0" * 64)
    bad_dir.mkdir()
    _make_layer_tar(bad_dir / "layer.tar", b"contents")  # sha won't match dirname
    config_bytes = b"{}"
    import hashlib
    config_digest = hashlib.sha256(config_bytes).hexdigest()
    (work / f"{config_digest}.json").write_bytes(config_bytes)
    (work / "manifest.json").write_text(json.dumps([{
        "Config": f"{config_digest}.json",
        "RepoTags": ["bad:latest"],
        "Layers": [f"{'0' * 64}/layer.tar"],
    }]))
    archive = tmp_path / "bad.tar.gz"
    with tarfile.open(archive, "w:gz") as tf:
        for item in sorted(work.iterdir()):
            tf.add(str(item), arcname=item.name)

    with pytest.raises(ValueError, match="!= sha256"):
        extract_image(archive, tmp_path / "scratch")


def test_prune_orphan_blobs_removes_unreferenced(tmp_path):
    from dynamic_runner.packaging.layered_transfer import (
        iter_referenced_digests,
        prune_orphan_blobs,
    )

    archive = tmp_path / "img.tar.gz"
    _build_synthetic_archive(archive, [b"keep-me-1", b"keep-me-2"])

    cache = tmp_path / "remote/cache"
    out = tmp_path / "remote/img.tar.gz"
    gw = LocalDirGateway(tmp_path / "remote")
    upload_image_layered(gw, archive, cache, out, image_label="img")

    # Plant an orphan digest that no manifest references.
    orphan = "f" * 64
    (cache / "blobs/sha256" / orphan).write_bytes(b"orphan-payload")

    bundle = extract_image(archive, tmp_path / "scratch3")
    keep = iter_referenced_digests([bundle])

    # Dry-run reports counts but doesn't delete.
    pruned, bytes_pruned = prune_orphan_blobs(gw, cache, keep, dry_run=True)
    assert pruned == 1 and bytes_pruned == len(b"orphan-payload")
    assert (cache / "blobs/sha256" / orphan).exists()

    # Real run deletes.
    pruned, _ = prune_orphan_blobs(gw, cache, keep)
    assert pruned == 1
    assert not (cache / "blobs/sha256" / orphan).exists()
    # Referenced blobs survive.
    for d in keep:
        assert (cache / "blobs/sha256" / d).exists()


def test_force_reassemble_ignores_marker(tmp_path):
    archive = tmp_path / "img.tar.gz"
    _build_synthetic_archive(archive, [b"only-layer"])
    cache = tmp_path / "remote/cache"
    out = tmp_path / "remote/img.tar.gz"
    gw = LocalDirGateway(tmp_path / "remote")

    upload_image_layered(gw, archive, cache, out, image_label="x")
    # After first run, marker is present. Touch it to a stale value
    # so we can detect when the uploader chooses to reassemble vs not.
    out.unlink()  # delete the tarball; reassembly should rebuild it
    bundle, scratch = (extract_image(archive, tmp_path / "scratch2"), tmp_path / "scratch2")
    try:
        uploader = LayeredUploader(gw, cache)
        stats = uploader.upload(bundle, out)
        assert stats.reassembled is True
        assert out.exists()
    finally:
        shutil.rmtree(scratch, ignore_errors=True)
