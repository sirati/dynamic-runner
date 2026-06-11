"""Tests for `dynamic_runner._boot_banner` — the secondary's immediate
process-start banner (owner-spec line 1: "secondaries should immediately
write a log entry upon start").

The banner is the FIRST observable output of a mesh-launched secondary,
written to stderr at the bootstrap shim's entry (before fault-dump wiring,
before the consumer module runs) so a node that dies between launch and its
first framework log line is never mute. These tests pin the content
(identity fields), the argv scan forms, the emission target + flush, the
never-raises contract, and the shim's banner-first sequencing.

Loaded directly under a ``dynamic_runner`` package stub (mirroring
``test_secondary_bootstrap.py``) so they run in a bare ``nix develop``
WITHOUT a maturin build; unittest-based, matching the rest of the suite.
"""

from __future__ import annotations

import importlib.util
import io
import os
import pathlib
import sys
import types
import unittest
from unittest import mock

_PACKAGE_ROOT = pathlib.Path(__file__).resolve().parent.parent


def _setup_package_stub() -> None:
    if "dynamic_runner" not in sys.modules:
        pkg = types.ModuleType("dynamic_runner")
        pkg.__path__ = [str(_PACKAGE_ROOT)]
        sys.modules["dynamic_runner"] = pkg


def _load_module_direct(name: str, relpath: str):
    _setup_package_stub()
    fullname = f"dynamic_runner.{name}"
    if fullname in sys.modules:
        return sys.modules[fullname]
    spec = importlib.util.spec_from_file_location(fullname, _PACKAGE_ROOT / relpath)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[fullname] = module
    spec.loader.exec_module(module)
    return module


banner = _load_module_direct("_boot_banner", "_boot_banner.py")


class TestBannerContent(unittest.TestCase):
    def test_banner_identifies_the_node(self) -> None:
        line = banner.build_banner(["--secondary-id", "sec-3"])
        self.assertIn("dynamic_runner secondary process started", line)
        self.assertIn("host=", line)
        self.assertIn(f"pid={os.getpid()}", line)
        self.assertIn("time=", line)
        self.assertIn("secondary_id=sec-3", line)
        self.assertNotIn("\n", line, "the banner is exactly one line")

    def test_slurm_job_id_named_when_present(self) -> None:
        with mock.patch.dict(os.environ, {"SLURM_JOB_ID": "424242"}):
            self.assertIn("slurm_job_id=424242", banner.build_banner([]))
        with mock.patch.dict(os.environ, clear=True):
            self.assertNotIn("slurm_job_id", banner.build_banner([]))

    def test_secondary_id_scan_accepts_both_flag_forms(self) -> None:
        self.assertEqual(
            banner._secondary_id_from_argv(["--secondary-id", "sec-1"]), "sec-1"
        )
        self.assertEqual(
            banner._secondary_id_from_argv(["--secondary-id=sec-2"]), "sec-2"
        )
        # Absent / trailing-bare / empty forms yield None, never a raise.
        self.assertIsNone(banner._secondary_id_from_argv([]))
        self.assertIsNone(banner._secondary_id_from_argv(["--secondary-id"]))
        self.assertIsNone(banner._secondary_id_from_argv(["--secondary-id="]))


class TestAnnounceEmission(unittest.TestCase):
    def test_announce_writes_one_flushed_stderr_line(self) -> None:
        captured = io.StringIO()
        with mock.patch.object(sys, "stderr", captured):
            banner.announce_secondary_start(["--secondary-id", "sec-7"])
        out = captured.getvalue()
        self.assertEqual(out.count("\n"), 1)
        self.assertIn("secondary process started", out)
        self.assertIn("secondary_id=sec-7", out)

    def test_announce_never_raises(self) -> None:
        # Even with the identity probes broken, the banner must not break
        # the cold start (the best-effort contract `_fault_dumps` set).
        with mock.patch.object(
            banner.socket, "gethostname", side_effect=RuntimeError("no DNS")
        ):
            banner.announce_secondary_start([])  # must not raise


class TestShimSequencing(unittest.TestCase):
    def test_shim_announces_before_fault_dumps_and_consumer(self) -> None:
        """The shim emits the banner FIRST — before `enable_fault_dumps`
        and before the consumer module runs (the spec's "immediately upon
        start": nothing hang-capable precedes it)."""
        shim = _load_module_direct("_secondary_bootstrap", "_secondary_bootstrap.py")
        calls: list[str] = []
        with (
            mock.patch.object(
                shim,
                "announce_secondary_start",
                side_effect=lambda argv: calls.append("banner"),
            ),
            mock.patch.object(
                shim,
                "enable_fault_dumps",
                side_effect=lambda argv: calls.append("fault_dumps"),
            ),
            mock.patch.object(
                shim.runpy,
                "run_module",
                side_effect=lambda *a, **k: calls.append("consumer"),
            ),
        ):
            shim.main(["--secondary-module", "consumer.module"])
        self.assertEqual(calls, ["banner", "fault_dumps", "consumer"])


if __name__ == "__main__":
    unittest.main()
