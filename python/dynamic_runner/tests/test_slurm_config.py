"""Regression tests for ``SlurmConfig`` path-typed field acceptance.

Pre-Rust-migration the Python ``SlurmConfig`` dataclass typed
``root_folder: str | Path`` and downstream consumers (notably the
asm-tokenizer task config) relied on the ``Path`` arm. After the
``crates/dynrunner-pyo3`` migration the pyclass field defaulted to
``String``, which the PyO3 ``String`` extractor only fills from a
Python ``str`` — passing a ``pathlib.Path`` raised ``TypeError``. The
``PyPathStr`` wrapper restores ``str | os.PathLike`` acceptance for
``root_folder`` and ``prestaged_src_bins_path``; this test pins the
contract so a future field-type change can't silently regress it.

Read-back (the getter) is asserted to round-trip to ``str`` since the
Rust field stores a ``String`` internally and Python consumers built
paths via f-strings against the field value.
"""

from __future__ import annotations

import pathlib

from dynamic_runner.packaging.slurm_config import SlurmConfig


def test_root_folder_accepts_pathlib_path() -> None:
    cfg = SlurmConfig(root_folder=pathlib.Path("/tmp/foo"))

    assert cfg.root_folder == "/tmp/foo"
    assert isinstance(cfg.root_folder, str)


def test_root_folder_accepts_str() -> None:
    cfg = SlurmConfig(root_folder="/tmp/bar")

    assert cfg.root_folder == "/tmp/bar"
    assert isinstance(cfg.root_folder, str)


def test_prestaged_src_bins_path_accepts_pathlib_path() -> None:
    cfg = SlurmConfig(
        root_folder="/tmp/foo",
        prestaged_src_bins_path=pathlib.Path("/srv/srcbins"),
    )

    assert cfg.prestaged_src_bins_path == "/srv/srcbins"
    assert isinstance(cfg.prestaged_src_bins_path, str)


def test_prestaged_src_bins_path_accepts_none() -> None:
    cfg = SlurmConfig(root_folder="/tmp/foo")

    assert cfg.prestaged_src_bins_path is None


def test_root_folder_accepts_arbitrary_pathlike() -> None:
    """Anything implementing ``os.PathLike`` should work, not just
    ``pathlib.Path`` — that's the whole point of the protocol."""

    class CustomPath:
        def __fspath__(self) -> str:
            return "/var/custom"

    cfg = SlurmConfig(root_folder=CustomPath())

    assert cfg.root_folder == "/var/custom"


def test_root_folder_setter_accepts_pathlib_path() -> None:
    """``set_all`` on the pyclass exposes a setter; pre-migration the
    Python dataclass allowed ``cfg.root_folder = Path(...)`` and the
    ``PyPathStr`` field type preserves that.
    """
    cfg = SlurmConfig(root_folder="/tmp/foo")
    cfg.root_folder = pathlib.Path("/tmp/baz")

    assert cfg.root_folder == "/tmp/baz"
    assert isinstance(cfg.root_folder, str)
