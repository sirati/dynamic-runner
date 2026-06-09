"""The per-item source-upload walk skips items with no backing file.

A ``uses_file_based_items=False`` producer discovers items it will PRODUCE
(computed, e.g. ``matrix_eval__<binary>.json``) — they have no backing
source file under ``--source`` to upload. The per-item upload walk must SKIP
such items, not blindly ``scp`` a path that does not exist (which OSErrored
the whole SLURM dispatch before any job ran). Mirrors the Rust
``crates/dynrunner-slurm/tests/upload.rs::skip_in_tree_nonexistent``.

Direct-file module loading (mirroring `test_cli_validation.py`) so the suite
runs in a bare `nix develop` shell without the compiled `_native` extension:
``job_manager`` imports ``_native`` / ``deployment_spec`` / ``packaging.podman``
at module load, so those are stubbed before the target is loaded. The tested
method (``upload_source_binaries``) touches none of them — it reads only
``self.gateway`` / ``self.slurm_config`` — and the manager is built via
``__new__`` to bypass the ``__init__`` Rust delegate.
"""

from __future__ import annotations

import importlib.util
import pathlib
import sys
import tempfile
import types
import unittest
from types import SimpleNamespace


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
    # job_manager's module-level relative imports — stubbed (only referenced
    # in __init__ / non-tested methods, which this suite does not exercise).
    leaf_stubs = {
        "dynamic_runner._native": {"RustSlurmJobManager": object},
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
    """Records ``transfer_file`` / ``create_directory`` calls; never touches
    the real filesystem. ``remote_home = None`` disables the tilde-expansion
    branch in ``_expand_path``."""

    def __init__(self) -> None:
        self.transfers: list[tuple[str, str]] = []
        self.created: list[str] = []
        self.remote_home = None

    def create_directory(self, remote: str) -> None:
        self.created.append(remote)

    def transfer_file(self, local: object, remote: str) -> None:
        self.transfers.append((str(local), str(remote)))


def _manager(gateway: _RecordingGateway) -> SlurmJobManager:
    mgr = SlurmJobManager.__new__(SlurmJobManager)
    mgr.gateway = gateway
    mgr.slurm_config = SimpleNamespace(get_srcbins_dir=lambda: "/remote/srcbins")
    return mgr


class UploadSkipNoSourceTests(unittest.TestCase):
    def test_skip_in_tree_nonexistent(self) -> None:
        """A computed/producer item (no backing file under --source) is
        skipped, not stat+scp'd."""
        gw = _RecordingGateway()
        mgr = _manager(gw)
        with tempfile.TemporaryDirectory() as tmp:
            mgr.upload_source_binaries(
                [SimpleNamespace(path="matrix_eval__bzip2.json")],
                pathlib.Path(tmp),
            )
        self.assertEqual(gw.transfers, [], "nonexistent in-tree item must be skipped")

    def test_existing_uploads_nonexistent_skipped(self) -> None:
        """Selective skip: an existing in-tree file uploads while a
        nonexistent sibling in the same call is skipped and the loop
        continues."""
        gw = _RecordingGateway()
        mgr = _manager(gw)
        with tempfile.TemporaryDirectory() as tmp:
            (pathlib.Path(tmp) / "real.bin").write_bytes(b"x")
            mgr.upload_source_binaries(
                [
                    SimpleNamespace(path="real.bin"),
                    SimpleNamespace(path="matrix_eval__bzip2.json"),
                ],
                pathlib.Path(tmp),
            )
        self.assertEqual(len(gw.transfers), 1, "only the existing in-tree file uploads")
        self.assertEqual(gw.transfers[0][1], "/remote/srcbins/real.bin")


if __name__ == "__main__":
    unittest.main()
