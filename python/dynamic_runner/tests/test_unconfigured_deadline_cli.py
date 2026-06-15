"""Tests for the `--unconfigured-deadline-secs` operator override.

The flag lets an operator raise the pre-`Operational` setup deadline a
secondary waits before giving up (default 600s, owned by the Rust
`DistributedConfig.unconfigured_deadline_secs` field). The Python side's
whole job is the flag -> `DistributedConfig` kwarg mapping:

  * `cli.build_arg_parser` declares the flag (`type=float`, default
    `None`).
  * `run._build_distributed_config` forwards it as the
    `unconfigured_deadline_secs` kwarg when set, and returns `None` when
    unset so the Rust-side 600s default holds.

These tests load `cli` + `run` directly under a package stub (mirroring
`test_important_stdio_cli.py` / `test_spawn_secondary.py`) so they run in
a bare `nix develop` without a maturin build. `DistributedConfig` is a
capture-spy on the stub package; the in-function `import dynamic_runner
as _rs` in `run._build_distributed_config` resolves to it.

The spy installation is SCOPED to each TestCase via an `autouse` pytest
fixture (`_patch_distributed_config_spy`) that uses pytest's
`monkeypatch` for automatic teardown. A previous incarnation installed
the spy at MODULE import time and never restored it, which polluted
``sys.modules["dynamic_runner"].DistributedConfig`` for every test that
ran AFTER this file in the same pytest session — `_rs.DistributedConfig`
in unrelated tests (e.g. ``test_lifecycle_hooks``) then resolved to the
spy and failed PyO3's argument type-check with
``'_DistributedConfigSpy' object is not an instance of 'DistributedConfig'``.
Scoping the patch to a fixture's setup/teardown closes that leak.
"""

from __future__ import annotations

import argparse
import importlib.util
import pathlib
import sys
import types
import unittest

import pytest


_PACKAGE_ROOT = pathlib.Path(__file__).resolve().parent.parent


def _captured_distributed_config_kwargs() -> dict:
    """Last-call kwargs of the spy `DistributedConfig`. Cleared each call."""
    return _CAPTURED


_CAPTURED: dict = {}


class _DistributedConfigSpy:
    """Stand-in for the PyO3 `DistributedConfig` that records the kwargs
    it was constructed with so the flag->kwarg mapping can be asserted
    without the compiled extension."""

    def __init__(self, **kwargs) -> None:
        _CAPTURED.clear()
        _CAPTURED.update(kwargs)
        self.kwargs = kwargs


def _ensure_package_stub() -> None:
    """Create the `dynamic_runner` package stub if not already present.

    Idempotent: existing real-package or prior-stub registrations are left
    untouched. The spy attribute is installed PER-TEST by the autouse
    fixture below, not here — this function only owns the package skeleton
    needed by `_load_module_direct`.
    """
    if "dynamic_runner" not in sys.modules:
        pkg = types.ModuleType("dynamic_runner")
        pkg.__path__ = [str(_PACKAGE_ROOT)]
        sys.modules["dynamic_runner"] = pkg


def _load_module_direct(name: str, relpath: str):
    _ensure_package_stub()
    fullname = f"dynamic_runner.{name}"
    if fullname in sys.modules:
        return sys.modules[fullname]
    spec = importlib.util.spec_from_file_location(fullname, _PACKAGE_ROOT / relpath)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[fullname] = module
    spec.loader.exec_module(module)
    return module


cli = _load_module_direct("cli", "cli.py")
run = _load_module_direct("run", "run.py")


def _parse(argv: list[str]) -> argparse.Namespace:
    return cli.build_arg_parser("test").parse_args(argv)


@pytest.fixture(autouse=True)
def _patch_distributed_config_spy(monkeypatch: pytest.MonkeyPatch):
    """Install the spy onto the `dynamic_runner` stub package for one
    test, then restore on teardown.

    Using pytest's ``monkeypatch`` fixture (instead of a bare assignment
    at module-import time) means:

    1. Each test sees a freshly-installed spy with a cleared `_CAPTURED`
       buffer — no leakage between sibling tests in this file.
    2. AFTER the last test in this file, ``DistributedConfig`` is
       restored to whatever it was before (typically the real PyO3
       class, when other test files run later in the same session).

    The fixture is module-autouse so it applies to every test function
    in this file without needing to be referenced explicitly. The
    `dynamic_runner` package stub itself is left in place across tests
    (it carries the loaded `cli` / `run` submodules); only the
    `DistributedConfig` attribute is the per-test churn.
    """
    _ensure_package_stub()
    _CAPTURED.clear()
    monkeypatch.setattr(
        sys.modules["dynamic_runner"],
        "DistributedConfig",
        _DistributedConfigSpy,
        raising=False,
    )
    yield


class UnconfiguredDeadlineFlagShapeTests(unittest.TestCase):
    def test_flag_absent_defaults_to_none(self) -> None:
        args = _parse([])
        self.assertIsNone(args.unconfigured_deadline_secs)

    def test_flag_parses_as_float(self) -> None:
        args = _parse(["--unconfigured-deadline-secs", "1800"])
        self.assertEqual(args.unconfigured_deadline_secs, 1800.0)
        self.assertIsInstance(args.unconfigured_deadline_secs, float)


class UnconfiguredDeadlinePlumbingTests(unittest.TestCase):
    """The load-bearing flag->kwarg mapping in `_build_distributed_config`."""

    def test_unset_yields_none_config_so_rust_default_holds(self) -> None:
        # No deviating knob -> None, which makes the Rust side install
        # the stock `DistributedConfig::default()` (unconfigured_deadline
        # = 600s). Asserting None is the proof the default is NOT masked.
        args = _parse([])
        self.assertIsNone(run._build_distributed_config(args))

    def test_override_flows_into_kwarg(self) -> None:
        args = _parse(["--unconfigured-deadline-secs", "1800"])
        cfg = run._build_distributed_config(args)
        self.assertIsNotNone(cfg)
        self.assertEqual(
            _captured_distributed_config_kwargs().get("unconfigured_deadline_secs"),
            1800.0,
        )

    def test_override_does_not_leak_sibling_kwargs(self) -> None:
        # Only the deadline knob is set -> only that key is forwarded;
        # sibling retry knobs stay absent so their Rust defaults hold.
        args = _parse(["--unconfigured-deadline-secs", "900"])
        run._build_distributed_config(args)
        captured = _captured_distributed_config_kwargs()
        self.assertEqual(set(captured), {"unconfigured_deadline_secs"})


if __name__ == "__main__":
    unittest.main()
