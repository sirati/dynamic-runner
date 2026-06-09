"""The per-item source-upload walk skips items with no backing file.

A ``uses_file_based_items=False`` producer discovers items it will PRODUCE
(computed, e.g. ``matrix_eval__<binary>.json``) — they have no backing
source file under ``--source`` to upload. The per-item upload walk must SKIP
such items, not blindly ``scp`` a path that does not exist (which OSErrored
the whole SLURM dispatch before any job ran). Mirrors the Rust
``crates/dynrunner-slurm/tests/upload.rs::skip_in_tree_nonexistent``.

Drives the real production method (``SlurmJobManager.upload_source_binaries``)
with a recording gateway; constructs the manager via ``__new__`` so the
``__init__`` Rust delegate (which needs a real gateway/config) is bypassed —
the method only reads ``self.gateway`` / ``self.slurm_config``.
"""

from __future__ import annotations

from pathlib import Path
from types import SimpleNamespace

from dynamic_runner.packaging.job_manager import SlurmJobManager


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


def test_skip_in_tree_nonexistent(tmp_path: Path) -> None:
    """A computed/producer item (no backing file under --source) is skipped,
    not stat+scp'd."""
    gw = _RecordingGateway()
    mgr = _manager(gw)
    binaries = [SimpleNamespace(path="matrix_eval__bzip2.json")]

    mgr.upload_source_binaries(binaries, tmp_path)

    assert gw.transfers == [], "nonexistent in-tree item must be skipped"


def test_existing_uploads_nonexistent_skipped(tmp_path: Path) -> None:
    """Selective skip: an existing in-tree file uploads while a nonexistent
    sibling in the same call is skipped and the loop continues."""
    (tmp_path / "real.bin").write_bytes(b"x")
    gw = _RecordingGateway()
    mgr = _manager(gw)
    binaries = [
        SimpleNamespace(path="real.bin"),
        SimpleNamespace(path="matrix_eval__bzip2.json"),
    ]

    mgr.upload_source_binaries(binaries, tmp_path)

    assert len(gw.transfers) == 1, "only the existing in-tree file uploads"
    assert gw.transfers[0][1] == "/remote/srcbins/real.bin"
