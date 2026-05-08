"""Regression test for :meth:`SlurmJobManager._expand_path` against
gateways whose ``remote_home`` attribute is a :class:`pathlib.Path`
rather than a :class:`str`.

The bug (CCF-6, audit batch L4.7): ``_expand_path`` previously called
``str.replace("~", self.gateway.remote_home, 1)`` directly, which
TypeErrors on every tilde-prefixed path routed through ``LocalGateway``
because ``LocalGateway.remote_home`` is ``Path.home()`` (a
:class:`pathlib.PosixPath`) while ``str.replace`` requires a ``str``
replacement. ``SSHGateway.remote_home`` is ``str | None``, so the
TypeError only surfaced on the local-gateway path — but every
``_expand_path`` caller in :mod:`job_manager` (wrapper-script log dirs,
image paths, srcbins mount source, output dir, run log dir, network
dir) flows through it, so a single tilde-prefixed config field would
take the whole submission flow down.

The fix is a one-line ``str()`` coercion at the consumption site;
this test asserts the call returns the expanded path without raising.
"""

from __future__ import annotations

from pathlib import Path

import pytest

from dynamic_runner.deployment_spec import TaskDeploymentSpec
from dynamic_runner.packaging.gateway.local_gateway import LocalGateway
from dynamic_runner.packaging.job_manager import SlurmJobManager
from dynamic_runner.packaging.podman import PodmanPackaging
from dynamic_runner.packaging.slurm_config import SlurmConfig


_TEST_DEPLOYMENT = TaskDeploymentSpec(
    secondary_module="test_pkg",
    image_name="test-image",
)


@pytest.fixture
def manager(tmp_path: Path) -> SlurmJobManager:
    """Build a :class:`SlurmJobManager` backed by a real
    :class:`LocalGateway`. Using the real gateway is load-bearing for
    this test: ``LocalGateway.remote_home`` is a :class:`PosixPath`,
    which is exactly the type that triggered the original TypeError.
    A string stub would not reproduce the bug."""
    slurm_config = SlurmConfig(root_folder=str(tmp_path / "slurm-root"))
    packaging = PodmanPackaging(deployment=_TEST_DEPLOYMENT)
    return SlurmJobManager(
        gateway=LocalGateway(),
        slurm_config=slurm_config,
        packaging_method=packaging,
        deployment=_TEST_DEPLOYMENT,
    )


def test_expand_path_handles_pathlib_remote_home(
    manager: SlurmJobManager,
) -> None:
    """``_expand_path("~/foo")`` must return ``f"{Path.home()}/foo"``
    when the gateway exposes ``remote_home`` as a
    :class:`pathlib.Path`. Previously this raised
    ``TypeError: replace() argument 2 must be str, not PosixPath``."""
    expanded = manager._expand_path("~/foo")
    assert expanded == f"{Path.home()}/foo"
    assert isinstance(expanded, str)


def test_expand_path_passthrough_when_no_tilde(
    manager: SlurmJobManager,
) -> None:
    """Paths without a leading tilde must be returned unchanged
    (modulo ``str()`` coercion of :class:`Path` inputs). Guards
    against an over-eager fix that strips or rewrites non-tilde
    paths."""
    assert manager._expand_path("/abs/path") == "/abs/path"
    assert manager._expand_path(Path("/abs/path")) == "/abs/path"
