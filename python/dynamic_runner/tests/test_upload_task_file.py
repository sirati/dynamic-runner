"""The #336 P1 per-task upload callback (``SlurmJobManager.upload_task_file``).

The upload-action callback the Rust setup executor invokes for a file-setup
task uploads ONE file to the gateway srcbins dir, reusing the bulk walk's
per-blob ``retry_transient`` transfer (not re-implementing it). This suite
pins:

* an explicit ``dest`` lands at ``<srcbins>/<dest>``;
* ``dest=None`` derives the placement from the source's basename
  (``<srcbins>/<basename>``);
* a one-off transient ``OSError`` is retried (the #400 per-blob shield), then
  succeeds — the retry is NOT re-implemented, it is the shared helper.

Same direct-file module loading + stubbing as ``test_upload_skip_no_source``
so the suite runs in a bare ``nix develop`` shell without the compiled
``_native`` extension. The tested method touches only ``self.gateway`` /
``self.slurm_config``; the manager is built via ``__new__`` to bypass the
Rust ``__init__`` delegate.
"""

from __future__ import annotations

import enum
import importlib.util
import pathlib
import sys
import tempfile
import types
import unittest
from types import SimpleNamespace


class _UploadRootStub(enum.Enum):
    """Stand-in for the Rust ``_native.UploadRoot`` pyclass enum in the
    standalone (no-compiled-extension) test. Same member NAMES as the real
    one (``SOURCE`` / ``OUTPUT``) so the manager's mount-root mapping resolves
    identically."""

    SOURCE = "source"
    OUTPUT = "output"


def _setup_stubs() -> pathlib.Path:
    here = pathlib.Path(__file__).resolve()
    package_root = here.parent.parent  # python/dynamic_runner
    pkg_stubs = {
        "dynamic_runner": str(package_root),
        "dynamic_runner.packaging": str(package_root / "packaging"),
    }
    for name, path in pkg_stubs.items():
        if name not in sys.modules:
            pkg = types.ModuleType(name)
            pkg.__path__ = [path]
            sys.modules[name] = pkg
    leaf_stubs = {
        "dynamic_runner._native": {
            "RustSlurmJobManager": object,
            # The #644 mount-root selector. The real one is a Rust pyclass
            # enum; the standalone test (no compiled ``_native``) substitutes a
            # plain ``enum.Enum`` with the SAME member names so
            # ``upload_task_file``'s ``{UploadRoot.SOURCE: ..., ...}[root]``
            # mapping + the ``root=UploadRoot.SOURCE`` default resolve.
            "UploadRoot": _UploadRootStub,
        },
        "dynamic_runner.deployment_spec": {"TaskDeploymentSpec": object},
        "dynamic_runner.packaging.podman": {"PodmanImageMetadata": object},
    }
    for name, attrs in leaf_stubs.items():
        if name not in sys.modules:
            mod = types.ModuleType(name)
            for attr, val in attrs.items():
                setattr(mod, attr, val)
            sys.modules[name] = mod
    return package_root


def _load_job_manager():
    package_root = _setup_stubs()
    fullname = "dynamic_runner.packaging.job_manager"
    if fullname in sys.modules:
        return sys.modules[fullname]
    target = package_root / "packaging" / "job_manager.py"
    spec = importlib.util.spec_from_file_location(fullname, target)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[fullname] = module
    spec.loader.exec_module(module)
    return module


SlurmJobManager = _load_job_manager().SlurmJobManager


class _RecordingGateway:
    """Records ``transfer_file`` / ``create_directory``; never touches the
    real filesystem. ``transfer_fails_n`` makes the first ``n``
    ``transfer_file`` calls raise ``OSError`` (the transient class) before
    succeeding — to exercise the shared ``retry_transient`` shield."""

    def __init__(self, transfer_fails_n: int = 0) -> None:
        self.transfers: list[tuple[str, str]] = []
        self.created: list[str] = []
        self.remote_home = None
        self._fails_left = transfer_fails_n

    def create_directory(self, remote: str) -> None:
        self.created.append(remote)

    def transfer_file(self, local: object, remote: str) -> None:
        if self._fails_left > 0:
            self._fails_left -= 1
            raise OSError("scp stream reset (transient)")
        self.transfers.append((str(local), str(remote)))


def _manager(gateway: _RecordingGateway) -> SlurmJobManager:
    mgr = SlurmJobManager.__new__(SlurmJobManager)
    mgr.gateway = gateway
    mgr.slurm_config = SimpleNamespace(
        get_srcbins_dir=lambda: "/remote/srcbins",
        get_output_dir=lambda: "/remote/out-network",
    )
    return mgr


# The mount-root selector the manager imports from ``_native`` (here the stub).
UploadRoot = sys.modules["dynamic_runner._native"].UploadRoot


class UploadTaskFileTests(unittest.TestCase):
    def test_explicit_dest_lands_under_srcbins(self) -> None:
        gw = _RecordingGateway()
        mgr = _manager(gw)
        with tempfile.TemporaryDirectory() as tmp:
            src = pathlib.Path(tmp) / "libfoo.a"
            src.write_bytes(b"x")
            mgr.upload_task_file(str(src), dest="toolchains/gcc/libfoo.a")
        self.assertEqual(len(gw.transfers), 1)
        self.assertEqual(
            gw.transfers[0][1],
            "/remote/srcbins/toolchains/gcc/libfoo.a",
            "an explicit dest lands at <srcbins>/<dest>",
        )

    def test_none_dest_derives_from_basename(self) -> None:
        gw = _RecordingGateway()
        mgr = _manager(gw)
        with tempfile.TemporaryDirectory() as tmp:
            src = pathlib.Path(tmp) / "nested" / "libbar.a"
            src.parent.mkdir(parents=True)
            src.write_bytes(b"y")
            mgr.upload_task_file(str(src), dest=None)
        self.assertEqual(len(gw.transfers), 1)
        self.assertEqual(
            gw.transfers[0][1],
            "/remote/srcbins/libbar.a",
            "dest=None derives the srcbins-relative tail from the source basename",
        )

    def test_output_root_lands_under_output_dir(self) -> None:
        # #644: root=OUTPUT places the upload under the shared output mount
        # (where consumers' affine import gates read), NOT srcbins.
        gw = _RecordingGateway()
        mgr = _manager(gw)
        with tempfile.TemporaryDirectory() as tmp:
            src = pathlib.Path(tmp) / "gate.so"
            src.write_bytes(b"g")
            mgr.upload_task_file(str(src), dest="x/y", root=UploadRoot.OUTPUT)
        self.assertEqual(len(gw.transfers), 1)
        self.assertEqual(
            gw.transfers[0][1],
            "/remote/out-network/x/y",
            "root=OUTPUT lands at <output_dir>/<dest>",
        )

    def test_explicit_source_root_matches_default(self) -> None:
        # #644: root=SOURCE (explicit) is identical to the default — srcbins.
        gw = _RecordingGateway()
        mgr = _manager(gw)
        with tempfile.TemporaryDirectory() as tmp:
            src = pathlib.Path(tmp) / "libfoo.a"
            src.write_bytes(b"x")
            mgr.upload_task_file(str(src), dest="a/b.a", root=UploadRoot.SOURCE)
        self.assertEqual(len(gw.transfers), 1)
        self.assertEqual(
            gw.transfers[0][1],
            "/remote/srcbins/a/b.a",
            "root=SOURCE (explicit) lands at <srcbins>/<dest>, same as the default",
        )

    def test_transient_failure_is_retried_then_succeeds(self) -> None:
        # One transient OSError then success: the shared retry_transient
        # shield re-attempts the SAME copy. The callback does NOT
        # re-implement retry — it reuses the bulk walk's helper.
        gw = _RecordingGateway(transfer_fails_n=1)
        mgr = _manager(gw)
        with tempfile.TemporaryDirectory() as tmp:
            src = pathlib.Path(tmp) / "libbaz.a"
            src.write_bytes(b"z")
            mgr.upload_task_file(str(src), dest="libbaz.a")
        self.assertEqual(
            len(gw.transfers),
            1,
            "the retried transfer eventually records exactly one success",
        )


if __name__ == "__main__":
    unittest.main()
