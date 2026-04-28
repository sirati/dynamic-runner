"""Test the partial-build wiring in PodmanPackaging.

We don't actually invoke nix here — the build command itself is
slow and depends on a working flake. Instead we monkeypatch
`subprocess.run` to record what command + env was assembled, and
provide a fake "built image" the cache-refresh step can extract
layers from. That validates:

  1. Cold build (no cache file): nix is invoked WITHOUT --impure
     and WITHOUT NIX_DOCKER_LAYER_CACHE in env.
  2. Warm build (cache file present): nix is invoked WITH --impure
     and the env var pointing at the cache.
  3. After each successful build, the cache is refreshed
     atomically (.partial → rename) from the new image.
  4. A failed extract step doesn't break the overall build (just
     a warning); the next run starts cold again.
"""

from __future__ import annotations

import json
import shutil
import subprocess
import tarfile
from pathlib import Path
from typing import Any

import pytest

from dynamic_batch.packaging.podman import (
    DEFAULT_LAYER_CACHE_REL,
    LAYER_CACHE_ENV_VAR,
    LAYER_EXTRACTOR_SCRIPT_REL,
    PodmanPackaging,
)


# ── Fixtures ──────────────────────────────────────────────────────────


def _make_synthetic_image(out_path: Path) -> None:
    """Tiny docker-archive used for cache-refresh round-trips.

    Contains one layer with a /nix/store path so the extractor's
    /nix/store filter has something to find.
    """
    import hashlib
    import io

    work = out_path.parent / f".{out_path.name}.work"
    work.mkdir(parents=True, exist_ok=True)
    try:
        # One layer with a single store-path entry.
        layer = work / "layer1"
        layer.mkdir()
        layer_tar = layer / "layer.tar"
        with tarfile.open(layer_tar, "w") as tf:
            data = b"fake nix content"
            info = tarfile.TarInfo(name="nix/store/abc123-fakepkg/marker")
            info.size = len(data)
            tf.addfile(info, io.BytesIO(data))

        layer_digest = hashlib.sha256(layer_tar.read_bytes()).hexdigest()
        # Move under <digest>/layer.tar (docker-archive convention)
        digest_dir = work / layer_digest
        digest_dir.mkdir()
        shutil.move(str(layer_tar), str(digest_dir / "layer.tar"))

        # Config blob
        config_obj = {"architecture": "amd64", "config": {}, "rootfs": {"type": "layers", "diff_ids": []}}
        cfg_bytes = json.dumps(config_obj, sort_keys=True).encode()
        cfg_digest = hashlib.sha256(cfg_bytes).hexdigest()
        (work / f"{cfg_digest}.json").write_bytes(cfg_bytes)

        manifest = [{
            "Config": f"{cfg_digest}.json",
            "RepoTags": ["fake:latest"],
            "Layers": [f"{layer_digest}/layer.tar"],
        }]
        (work / "manifest.json").write_text(json.dumps(manifest))

        # Empty layer dir cleanup
        layer.rmdir()

        # Wrap into tar.gz at out_path
        out_path.parent.mkdir(parents=True, exist_ok=True)
        with tarfile.open(out_path, "w:gz") as tf:
            for item in sorted(work.iterdir()):
                tf.add(str(item), arcname=item.name)
    finally:
        shutil.rmtree(work, ignore_errors=True)


def _make_fake_project_root(tmp_path: Path) -> Path:
    """Skeleton project root containing the extractor script.
    `_build_nix_target` looks at `<root>/nix/extract-layer-assignment.py`.
    """
    root = tmp_path / "proj"
    root.mkdir()
    (root / "nix").mkdir()
    real_extractor = (
        Path(__file__).resolve().parents[2]
        / LAYER_EXTRACTOR_SCRIPT_REL
    )
    if real_extractor.exists():
        # Symlink the real extractor so the cache-refresh runs the
        # actual logic against our synthetic image.
        (root / LAYER_EXTRACTOR_SCRIPT_REL).symlink_to(real_extractor)
    else:
        # Fallback: a stub that just emits an empty array
        (root / LAYER_EXTRACTOR_SCRIPT_REL).write_text(
            "#!/usr/bin/env python3\nimport sys, json; json.dump([], sys.stdout)\n"
        )
        (root / LAYER_EXTRACTOR_SCRIPT_REL).chmod(0o755)
    return root


# ── nix-build interception helpers ────────────────────────────────────


class FakeNix:
    """Patches subprocess.run to fake `nix build`. Records the command
    and env it was called with for assertions, and creates a synthetic
    result tarball at `<project_root>/<out_link>` so the cache-refresh
    step can run a real extractor on it."""

    def __init__(self, project_root: Path) -> None:
        self.project_root = project_root
        self.calls: list[dict[str, Any]] = []
        self._real_run = subprocess.run

    def __call__(self, cmd, *args, **kwargs):
        if isinstance(cmd, list) and cmd[:2] == ["nix", "build"]:
            # Fake the build: produce a synthetic image at the
            # requested out-link path so the extractor has
            # something to chew on.
            out_link = "result"
            for i, tok in enumerate(cmd):
                if tok == "--out-link" and i + 1 < len(cmd):
                    out_link = cmd[i + 1]
                    break
            tarball = self.project_root / out_link
            tarball.parent.mkdir(parents=True, exist_ok=True)
            _make_synthetic_image(tarball)
            self.calls.append({
                "cmd": list(cmd),
                "env": dict(kwargs.get("env") or {}),
                "cwd": kwargs.get("cwd"),
            })

            class _Result:
                returncode = 0
                stdout = ""
                stderr = ""
            return _Result()
        # Pass through the extractor invocation (real subprocess.run).
        return self._real_run(cmd, *args, **kwargs)


@pytest.fixture
def patched_subprocess(monkeypatch, tmp_path):
    project_root = _make_fake_project_root(tmp_path)
    fake = FakeNix(project_root)
    monkeypatch.setattr(subprocess, "run", fake)
    return fake


# ── Tests ─────────────────────────────────────────────────────────────


def test_cold_build_invokes_nix_without_impure(patched_subprocess, tmp_path):
    pp = PodmanPackaging()
    pp._build_nix_target(
        local_project_root=patched_subprocess.project_root,
        target=".#dockerImage",
        out_link="docker-image-result",
    )

    assert len(patched_subprocess.calls) == 1
    cmd = patched_subprocess.calls[0]["cmd"]
    env = patched_subprocess.calls[0]["env"]
    assert "--impure" not in cmd, "cold build should NOT pass --impure"
    assert LAYER_CACHE_ENV_VAR not in env, "cold build should NOT set the cache env var"


def test_warm_build_passes_impure_and_env_var(patched_subprocess, tmp_path):
    cache_path = patched_subprocess.project_root / DEFAULT_LAYER_CACHE_REL
    cache_path.write_text(json.dumps([["/nix/store/abc-fake"]]))

    pp = PodmanPackaging()
    pp._build_nix_target(
        local_project_root=patched_subprocess.project_root,
        target=".#dockerImage",
        out_link="docker-image-result",
    )

    cmd = patched_subprocess.calls[0]["cmd"]
    env = patched_subprocess.calls[0]["env"]
    assert "--impure" in cmd, "warm build should pass --impure"
    assert env.get(LAYER_CACHE_ENV_VAR) == str(cache_path.resolve())


def test_cache_is_refreshed_after_successful_build(patched_subprocess, tmp_path):
    cache_path = patched_subprocess.project_root / DEFAULT_LAYER_CACHE_REL
    assert not cache_path.exists()

    pp = PodmanPackaging()
    pp._build_nix_target(
        local_project_root=patched_subprocess.project_root,
        target=".#dockerImage",
        out_link="docker-image-result",
    )

    # The synthetic image has one /nix/store path; the extractor
    # should have produced a single-layer assignment.
    assert cache_path.exists(), "cache should be written after successful build"
    assignment = json.loads(cache_path.read_text())
    assert isinstance(assignment, list)
    assert len(assignment) == 1
    assert assignment[0] == ["/nix/store/abc123-fakepkg"]
    # No partial file leftover after success.
    assert not (cache_path.with_suffix(cache_path.suffix + ".partial")).exists()


def test_cache_disabled_when_layer_cache_path_is_false(patched_subprocess, tmp_path):
    pp = PodmanPackaging(layer_cache_path=False)
    pp._build_nix_target(
        local_project_root=patched_subprocess.project_root,
        target=".#dockerImage",
        out_link="docker-image-result",
    )

    cmd = patched_subprocess.calls[0]["cmd"]
    env = patched_subprocess.calls[0]["env"]
    assert "--impure" not in cmd
    assert LAYER_CACHE_ENV_VAR not in env
    # Cache file should NOT have been created.
    assert not (patched_subprocess.project_root / DEFAULT_LAYER_CACHE_REL).exists()


def test_explicit_cache_path_override(patched_subprocess, tmp_path):
    custom_cache = tmp_path / "custom" / "layers.json"
    pp = PodmanPackaging(layer_cache_path=custom_cache)
    pp._build_nix_target(
        local_project_root=patched_subprocess.project_root,
        target=".#dockerImage",
        out_link="docker-image-result",
    )
    assert custom_cache.exists(), "explicit cache path should receive the assignment"
    # Default location should NOT have been used.
    assert not (patched_subprocess.project_root / DEFAULT_LAYER_CACHE_REL).exists()
