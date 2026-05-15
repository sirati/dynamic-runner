"""Smoke tests for the `--slurm-setup-deadline-secs` CLI flag and the
scale-aware default applied by the SLURM pipeline.

Pins three contract points:

1. The flag parses as an int and ends up at the documented attribute
   `args.slurm_setup_deadline_secs`.
2. The flag is absent by default (no behaviour change for operators
   who never touch the knob — the SLURM pipeline either injects the
   scale-aware default or, outside SLURM mode, lets the secondary's
   60s default take over).
3. The flag survives `filter_framework_argv`: it is NOT a
   framework-regenerated flag (the SLURM wrapper does not emit a
   fresh value per job; the operator-supplied OR pipeline-derived
   value rides through `forwarded_argv` verbatim).

The scale-aware FORMULA itself
(`max(60, num_secondaries * 15)`) lives in
`crates/dynrunner-slurm/src/pipeline.rs::compute_setup_deadline_secs`
and is exercised by Rust unit tests in that file — pinning it twice
across the FFI boundary would duplicate the heuristic.

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

    Mirrors the stub pattern in `test_forwarded_argv.py` and
    `test_observer_late_joiner_cli.py`.
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


class SetupDeadlineFlagShapeTests(unittest.TestCase):
    def test_flag_absent_default_is_none(self) -> None:
        # Headline contract: when the operator does not touch the knob,
        # the attribute is `None`. The SLURM pipeline reads `None` as
        # "apply the scale-aware default"; the secondary dispatch reads
        # `None` as "let DistributedConfig.setup_deadline_secs's stock
        # 60s default take over". Either path preserves the
        # pre-band-aid behaviour for callers that don't set the flag.
        args = _parse([])
        self.assertIsNone(args.slurm_setup_deadline_secs)

    def test_flag_stores_int(self) -> None:
        # Two-token form.
        args = _parse(["--slurm-setup-deadline-secs", "225"])
        self.assertEqual(args.slurm_setup_deadline_secs, 225)
        self.assertIsInstance(args.slurm_setup_deadline_secs, int)

    def test_flag_equals_form_also_parses(self) -> None:
        # Single-token form — the SLURM pipeline injection uses this
        # form (`--slurm-setup-deadline-secs=N`); argparse accepts
        # both for `type=int` arguments.
        args = _parse(["--slurm-setup-deadline-secs=225"])
        self.assertEqual(args.slurm_setup_deadline_secs, 225)

    def test_flag_rejects_non_int(self) -> None:
        parser = cli.build_arg_parser("test")
        with self.assertRaises(SystemExit):
            parser.parse_args(["--slurm-setup-deadline-secs", "not-a-number"])


class SetupDeadlineForwardedArgvTests(unittest.TestCase):
    """The flag is NOT in `FRAMEWORK_REGENERATED_FLAGS`, so
    `filter_framework_argv` must preserve operator-supplied values
    verbatim. The pipeline-derived value (appended on the dispatcher
    side) likewise rides through `forwarded_argv` unchanged.
    """

    def test_operator_supplied_value_survives_filter_two_token(self) -> None:
        argv = [
            "--secondary",
            "tcp://x",
            "--slurm-setup-deadline-secs",
            "225",
            "--platform",
            "x64",
        ]
        # `--secondary` + value drop; the deadline flag and the
        # task-specific filter must survive.
        self.assertEqual(
            filter_framework_argv(argv),
            ["--slurm-setup-deadline-secs", "225", "--platform", "x64"],
        )

    def test_operator_supplied_value_survives_filter_equals_form(self) -> None:
        argv = [
            "--secondary",
            "tcp://x",
            "--slurm-setup-deadline-secs=600",
            "--platform",
            "x64",
        ]
        self.assertEqual(
            filter_framework_argv(argv),
            ["--slurm-setup-deadline-secs=600", "--platform", "x64"],
        )


if __name__ == "__main__":
    unittest.main()
