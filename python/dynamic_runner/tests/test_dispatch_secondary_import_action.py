"""Regression pin for `_dispatch_secondary` forwarding the affine import action.

The bug this guards against (#501): `_dispatch_secondary` builds the
DISTRIBUTED/SLURM secondary by calling `_rs.run_secondary(...)`, but it
never forwarded the consumer's `task.import_action` callable. The native
`run_secondary` free function therefore built every distributed
`SecondaryCoordinator` with `import_action=None`, so the secondary's
affine gate had no importer and every `unmet_local_affine_dep` dependent
was pruned "upstream unfulfillable" before any dispatch — a fleet-wide
deadlock (`affine_import=0`).

The fix mirrors the existing `getattr(task, "fulfillability_matcher", None)`
forwarding idiom: `_dispatch_secondary` passes
`import_action=getattr(task, "import_action", None)` to `run_secondary`.
This test pins exactly that: the value reaching `run_secondary` is the
task's `import_action` attribute (and `None` when the task declares none).

unittest-based — pytest is not in the dev shell, so we stay stdlib to keep
the test runnable from any environment. The `dynamic_runner` package is
stubbed (no `_native` wheel build needed): `run.py` is loaded directly by
path, and its in-function ``import dynamic_runner as _rs`` resolves to the
stub package onto which we install fake config constructors + a capturing
``run_secondary``.
"""

from __future__ import annotations

import argparse
import importlib.util
import pathlib
import sys
import types
import unittest


def _setup_package_stub() -> pathlib.Path:
    """Register a minimal `dynamic_runner` package stub so relative imports
    inside `run.py` resolve without triggering the real package `__init__`
    (which imports the PyO3 `_native` extension)."""
    here = pathlib.Path(__file__).resolve()
    package_root = here.parent.parent  # …/python/dynamic_runner/
    if "dynamic_runner" not in sys.modules:
        pkg = types.ModuleType("dynamic_runner")
        pkg.__path__ = [str(package_root)]
        sys.modules["dynamic_runner"] = pkg
    return package_root


def _load_run_module():
    """Import `dynamic_runner.run` by absolute path, bypassing `__init__`.

    `run.py`'s module-level imports are all pure-Python siblings (none pull
    `_native` at import time — the `_native`-touching imports in
    `logging_setup` are deferred inside functions), so direct file import is
    safe in the bare `nix develop` shell.
    """
    package_root = _setup_package_stub()
    fullname = "dynamic_runner.run"
    if fullname in sys.modules:
        return sys.modules[fullname]
    spec = importlib.util.spec_from_file_location(fullname, package_root / "run.py")
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[fullname] = module
    spec.loader.exec_module(module)
    return module


_run = _load_run_module()


class _FakeRs:
    """The subset of the `dynamic_runner` PyO3 surface `_dispatch_secondary`
    reaches. Constructors are inert stand-ins; `run_secondary` records the
    kwargs it was called with so the test can assert the forwarded value."""

    def __init__(self) -> None:
        self.run_secondary_kwargs: dict | None = None

    # --- inert config constructors (only their existence + return matters) ---
    def SecondaryConfig(self, **_kw):  # noqa: N802 (mirror the native name)
        return types.SimpleNamespace(
            secondary_id=_kw.get("secondary_id"),
            src_network=None,
            src_tmp=None,
            output_dir=_kw.get("output_dir"),
        )

    def DistributedConfig(self, **_kw):  # noqa: N802
        return object()

    def ResourceMap(self, _m):  # noqa: N802
        return object()

    def SchedulerConfig(self, **_kw):  # noqa: N802
        return object()

    def parse_cores(self, _spec):
        return 1

    def parse_memory(self, _spec):
        return 1

    # --- the capture point ---
    def run_secondary(self, *args, **kwargs):
        self.run_secondary_kwargs = kwargs


def _install_fake_rs(fake: _FakeRs) -> None:
    """Mount the fake surface onto the stub package so every in-function
    ``import dynamic_runner as _rs`` inside `run.py` resolves to it."""
    pkg = sys.modules["dynamic_runner"]
    for name in (
        "SecondaryConfig",
        "DistributedConfig",
        "ResourceMap",
        "SchedulerConfig",
        "parse_cores",
        "parse_memory",
        "run_secondary",
    ):
        setattr(pkg, name, getattr(fake, name))


def _make_args() -> argparse.Namespace:
    """A complete-enough parsed-CLI Namespace for `_dispatch_secondary`."""
    return argparse.Namespace(
        secondary="ws://primary:8080",
        secondary_id="sec-0",
        secondary_quic_port=0,
        cores="1",
        max_memory="-2G",
        src_network=None,
        src_tmp=None,
        output_dir=None,
        skip_existing=False,
        mem_manager_reserved="",
        memprofile=False,
        log_dir=None,
        oom_cgroup_safety_margin="1G",
        oom_pressure_threshold="500M",
        panik_file_paths=[],
    )


class DispatchSecondaryImportActionTest(unittest.TestCase):
    def setUp(self) -> None:
        self._fake = _FakeRs()
        _install_fake_rs(self._fake)
        self._logger = __import__("logging").getLogger("test-dispatch-secondary")

    def test_forwards_task_import_action(self) -> None:
        """A task exposing `import_action` must have it reach `run_secondary`."""

        def _import_task(task_id, payload_json):  # the consumer's callable
            return None

        task = types.SimpleNamespace(import_action=_import_task)
        _run._dispatch_secondary(task, _make_args(), self._logger)

        kwargs = self._fake.run_secondary_kwargs
        self.assertIsNotNone(kwargs, "run_secondary must have been called")
        self.assertIn(
            "import_action",
            kwargs,
            "_dispatch_secondary must forward import_action to run_secondary",
        )
        self.assertIs(
            kwargs["import_action"],
            _import_task,
            "the forwarded import_action must be the task's callable verbatim",
        )

    def test_absent_import_action_forwards_none(self) -> None:
        """A task with no `import_action` attribute forwards None (the
        getattr default), never raising — out-of-tree / affine-free tasks."""
        task = types.SimpleNamespace()  # no import_action attribute
        _run._dispatch_secondary(task, _make_args(), self._logger)

        kwargs = self._fake.run_secondary_kwargs
        self.assertIsNotNone(kwargs, "run_secondary must have been called")
        self.assertIsNone(
            kwargs.get("import_action"),
            "a task without import_action must forward None",
        )


if __name__ == "__main__":
    unittest.main()
