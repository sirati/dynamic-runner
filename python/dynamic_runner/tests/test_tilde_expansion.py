"""Defensive tilde-expansion at the lower-level remote-path surfaces.

The production path expands a leading ``~`` once at the config
boundary (``pipeline._make_slurm_config`` against ``gateway.remote_home``).
These tests cover the lower-level guards that keep a ``~``-prefixed
remote root from ever creating a literal ``~`` directory if a caller
bypasses that boundary:

  * the shared :func:`expand_gateway_tilde` helper,
  * :meth:`PodmanPackaging._normalize_path`,
  * :class:`LayeredUploader`'s ``cache_root`` (and the remote layout
    paths derived from it).

A quoted ``~`` is never expanded by the remote shell, so without these
guards ``mkdir -p`` / ``mv`` would land a literal ``~`` directory under
``$HOME`` while server-side ``~``-expanding tools target the real home.
"""

from __future__ import annotations

from pathlib import Path

from dynamic_runner import TaskDeploymentSpec
from dynamic_runner.packaging.gateway import expand_gateway_tilde
from dynamic_runner.packaging.layered_transfer import LayeredUploader
from dynamic_runner.packaging.podman import PodmanPackaging


_TEST_DEPLOYMENT = TaskDeploymentSpec(
    secondary_module="test_pkg",
    image_name="test-image",
)


class _GatewayStub:
    """Minimal stand-in exposing only ``remote_home``."""

    def __init__(self, remote_home: object) -> None:
        self.remote_home = remote_home


# ── expand_gateway_tilde (shared helper) ──────────────────────────────


def test_expand_helper_replaces_leading_tilde() -> None:
    gw = _GatewayStub("/home/alice")
    assert expand_gateway_tilde(gw, "~/slurm/out") == "/home/alice/slurm/out"


def test_expand_helper_coerces_pathlib_remote_home() -> None:
    """``LocalGateway.remote_home`` is a ``PosixPath``; the helper must
    not ``TypeError`` on a non-str home (mirrors job_manager CCF-6)."""
    gw = _GatewayStub(Path("/home/bob"))
    assert expand_gateway_tilde(gw, "~/cache") == "/home/bob/cache"


def test_expand_helper_only_first_tilde() -> None:
    gw = _GatewayStub("/home/alice")
    # A non-leading literal ~ must be left intact.
    assert expand_gateway_tilde(gw, "~/a/~b") == "/home/alice/a/~b"


def test_expand_helper_passthrough_absolute() -> None:
    gw = _GatewayStub("/home/alice")
    assert expand_gateway_tilde(gw, "/abs/path") == "/abs/path"
    assert expand_gateway_tilde(gw, Path("/abs/path")) == "/abs/path"


def test_expand_helper_passthrough_when_no_home() -> None:
    """No remote_home (None / missing attr) → path returned unchanged
    rather than a half-expanded ``Nonefoo``."""
    assert expand_gateway_tilde(_GatewayStub(None), "~/x") == "~/x"
    assert expand_gateway_tilde(object(), "~/x") == "~/x"


# ── PodmanPackaging._normalize_path ───────────────────────────────────


def test_normalize_path_expands_tilde_against_gateway_home() -> None:
    pp = PodmanPackaging(deployment=_TEST_DEPLOYMENT)
    gw = _GatewayStub("/home/carol")
    out = pp._normalize_path(gw, "~/slurm/images")
    assert out == Path("/home/carol/slurm/images")
    # No literal ~ component survives.
    assert "~" not in out.parts


def test_normalize_path_passthrough_absolute() -> None:
    pp = PodmanPackaging(deployment=_TEST_DEPLOYMENT)
    gw = _GatewayStub("/home/carol")
    assert pp._normalize_path(gw, "/abs/out") == Path("/abs/out")
    assert pp._normalize_path(gw, Path("/abs/out")) == Path("/abs/out")


# ── LayeredUploader.cache_root ────────────────────────────────────────


def test_uploader_expands_cache_root_tilde() -> None:
    gw = _GatewayStub("/home/dave")
    uploader = LayeredUploader(gw, Path("~/layer-cache"))
    assert uploader.cache_root == Path("/home/dave/layer-cache")
    # Every derived remote path must be absolute, never literal-~.
    assert uploader._blob_dir() == "/home/dave/layer-cache/blobs/sha256"
    assert uploader._manifest_dir() == "/home/dave/layer-cache/manifests"
    assert "~" not in uploader._blob_path("deadbeef")


def test_uploader_passthrough_absolute_cache_root() -> None:
    gw = _GatewayStub("/home/dave")
    uploader = LayeredUploader(gw, Path("/abs/cache"))
    assert uploader.cache_root == Path("/abs/cache")
