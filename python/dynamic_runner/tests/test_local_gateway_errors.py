"""Locks in the `RuntimeError` exception class on local gateway copy
failures.

Pre-Rust-migration `LocalGateway.transfer_file` / `download_file`
wrapped any failure of the underlying `shutil.copy2` call (and the
preceding parent `mkdir`) in
``raise RuntimeError(f"File copy failed: {e}")``. Callers across the
framework `try/except RuntimeError`, so silently re-mapping these to
`OSError` after the migration would slip through unhandled. The Rust
`GatewayError::CopyFailed` variant + its PyO3 mapping is what
preserves the contract; this test fails if the mapping ever drifts
back to `OSError` (which is what a bare `?` on `tokio::fs::copy`
would produce via `GatewayError::Io`).
"""

from __future__ import annotations

from pathlib import Path

import pytest

from dynamic_runner.packaging.gateway.local_gateway import LocalGateway


@pytest.fixture
def gw() -> LocalGateway:
    g = LocalGateway()
    g.connect()
    yield g
    g.disconnect()


def test_transfer_file_missing_source_raises_runtime_error(
    tmp_path: Path, gw: LocalGateway
) -> None:
    """`tokio::fs::copy` failure (here: ENOENT on source) must surface
    as `RuntimeError`, mirroring pre-migration Python behavior."""
    missing = tmp_path / "no-such-source"
    dest = tmp_path / "dst.bin"
    with pytest.raises(RuntimeError):
        gw.transfer_file(missing, dest)


def test_download_file_missing_source_raises_runtime_error(
    tmp_path: Path, gw: LocalGateway
) -> None:
    """Symmetric to `transfer_file`: pre-migration the same
    `RuntimeError(f"File copy failed: ...")` was raised on
    `download_file` failure."""
    missing = tmp_path / "no-such-remote"
    dest = tmp_path / "out" / "copy.bin"
    with pytest.raises(RuntimeError):
        gw.download_file(missing, dest)


def test_transfer_file_succeeds(tmp_path: Path, gw: LocalGateway) -> None:
    """Sanity: success path still produces no exception and the bytes
    arrive intact. Without this check, a regression that turned every
    call into a copy failure would still satisfy the assertions
    above."""
    src = tmp_path / "src.bin"
    src.write_bytes(b"payload")
    dest = tmp_path / "nested" / "dst.bin"
    gw.transfer_file(src, dest)
    assert dest.read_bytes() == b"payload"
