"""Smoke tests for the `--reuse-workers` CLI flag.

Pins three contract points:

1. The flag parses as a boolean and ends up at `args.reuse_workers`.
2. The flag is `False` by default — the framework default is to restart
   each worker after a completed task, so the absence of the flag means
   "always restart".
3. The flag survives `filter_framework_argv` so the SLURM wrapper's
   `forwarded_argv` block re-emits it on the secondary command line
   verbatim, applying the same restart-vs-reuse policy cluster-wide. The
   actual policy decision lives entirely on the Rust side in
   `crates/dynrunner-manager-local/src/manager/events.rs` (a worker is
   restarted UNLESS `reuse_workers` is set); pinning it here would
   duplicate the policy across the FFI boundary.

unittest-based to stay runnable in a bare nix-develop shell (no
pytest in the dev environment by convention; see
`test_forwarded_argv.py`).
"""

from __future__ import annotations

import argparse
import importlib.util
import pathlib
import sys
import types
import unittest


def _setup_package_stub() -> pathlib.Path:
    """Register a minimal `dynamic_runner` package stub so the cli.py
    module under test can be loaded without triggering the real
    package `__init__` (which imports the PyO3 `_native` extension
    and would otherwise require a maturin build to import).

    Mirrors the stub pattern in `test_memprofile_cli.py`.
    """
    here = pathlib.Path(__file__).resolve()
    package_root = here.parent.parent
    if "dynamic_runner" not in sys.modules:
        pkg = types.ModuleType("dynamic_runner")
        pkg.__path__ = [str(package_root)]
        sys.modules["dynamic_runner"] = pkg
    return package_root


def _load_module_direct(name: str, relpath: str):
    package_root = _setup_package_stub()
    target = package_root / relpath
    fullname = f"dynamic_runner.{name}"
    if fullname in sys.modules:
        return sys.modules[fullname]
    spec = importlib.util.spec_from_file_location(fullname, target)
    assert spec is not None and spec.loader is not None, f"could not spec {target}"
    module = importlib.util.module_from_spec(spec)
    sys.modules[fullname] = module
    spec.loader.exec_module(module)
    return module


def _load_cli_module():
    package_root = _setup_package_stub()
    fullname = "dynamic_runner.cli"
    if fullname in sys.modules:
        return sys.modules[fullname]
    target = package_root / "cli.py"
    spec = importlib.util.spec_from_file_location(fullname, target)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[fullname] = module
    spec.loader.exec_module(module)
    return module


cli = _load_cli_module()
_forwarded_argv = _load_module_direct("_forwarded_argv", "_forwarded_argv.py")
filter_framework_argv = _forwarded_argv.filter_framework_argv


def _parse(argv: list[str]) -> argparse.Namespace:
    parser = cli.build_arg_parser("test")
    return parser.parse_args(argv)


class ReuseWorkersFlagShapeTests(unittest.TestCase):
    def test_flag_absent_default_false(self) -> None:
        # The framework default: restart the worker after each task. The
        # opt-in flag is absent, so `args.reuse_workers` is False and the
        # Rust policy in `events.rs` restarts on success.
        args = _parse([])
        self.assertFalse(args.reuse_workers)

    def test_flag_present_stores_true(self) -> None:
        # Opt into reusing worker processes across tasks.
        args = _parse(["--reuse-workers"])
        self.assertTrue(args.reuse_workers)


class ReuseWorkersForwardedArgvTests(unittest.TestCase):
    """`--reuse-workers` is NOT in `FRAMEWORK_REGENERATED_FLAGS`, so the
    SLURM wrapper's `forwarded_argv` block re-emits it on the secondary
    command line verbatim — applying the restart-vs-reuse policy on
    every node.
    """

    def test_flag_survives_filter(self) -> None:
        argv = [
            "--secondary",
            "tcp://x",
            "--reuse-workers",
            "--platform",
            "x64",
        ]
        # `--secondary` + value drop; `--reuse-workers` and the
        # task-specific filter must survive.
        self.assertEqual(
            filter_framework_argv(argv),
            ["--reuse-workers", "--platform", "x64"],
        )


if __name__ == "__main__":
    unittest.main()
