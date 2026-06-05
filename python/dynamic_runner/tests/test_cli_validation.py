"""Tests for the framework's count/budget CLI range validation.

`add_framework_arguments` owns the framework's numeric flags now, so it
also owns their bounds. A consumer that adopts the framework CLI inherits
these flags and cannot re-attach its own validator (``set_defaults`` can
restore a default but not a ``type=`` converter), so a negative value would
otherwise flow unchecked to the Rust PyO3 ``u32``/``usize`` boundary and
surface as an ugly ``OverflowError`` instead of a clean argparse error.

These assert, per flag:
  * a NEGATIVE value exits at ``parse_args`` (argparse raises ``SystemExit``
    on ``ArgumentTypeError``);
  * the boundary value that IS valid (``0`` for the ``>= 0`` flags, ``1``
    for ``--jobs``) parses to exactly that int/float — proving valid inputs
    are unchanged (the value reaching Rust is identical, just validated).

Direct-file module loading (mirroring `test_cli_api.py`) so the suite runs
in a bare `nix develop` shell without the compiled `_native` extension.
"""

from __future__ import annotations

import argparse
import importlib.util
import pathlib
import sys
import types
import unittest


def _setup_package_stub() -> pathlib.Path:
    here = pathlib.Path(__file__).resolve()
    package_root = here.parent.parent
    if "dynamic_runner" not in sys.modules:
        pkg = types.ModuleType("dynamic_runner")
        pkg.__path__ = [str(package_root)]
        sys.modules["dynamic_runner"] = pkg
    return package_root


def _load(name: str, relpath: str):
    package_root = _setup_package_stub()
    target = package_root / relpath
    fullname = f"dynamic_runner.{name}"
    if fullname in sys.modules:
        return sys.modules[fullname]
    spec = importlib.util.spec_from_file_location(fullname, target)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[fullname] = module
    spec.loader.exec_module(module)
    return module


cli = _load("cli", "cli.py")


def _parse(argv: list[str]) -> argparse.Namespace:
    return cli.build_arg_parser("test").parse_args(argv)


# (flag, dest, valid-boundary token, expected parsed value, valid-is-float)
_NON_NEGATIVE_INT_FLAGS = [
    ("--secondary-quic-port", "secondary_quic_port", "0", 0),
    ("--retry-max-passes", "retry_max_passes", "0", 0),
    ("--oom-retry-max-passes", "oom_retry_max_passes", "0", 0),
    (
        "--unfulfillable-reinject-max-per-task",
        "unfulfillable_reinject_max_per_task",
        "0",
        0,
    ),
    ("--respawn-max-per-secondary", "respawn_max_per_secondary", "0", 0),
    ("--respawn-max-total", "respawn_max_total", "0", 0),
]

_POSITIVE_INT_FLAGS = [
    ("--jobs", "jobs", "1", 1),
    ("--slurm-cpus-per-task", "slurm_cpus_per_task", "1", 1),
]

_NON_NEGATIVE_FLOAT_FLAGS = [
    ("--panik-poll-interval-secs", "panik_poll_interval_secs", "0", 0.0),
    ("--debug-simulate-errors", "simulate_errors", "0", 0.0),
    ("--unconfigured-deadline-secs", "unconfigured_deadline_secs", "0", 0.0),
]


class HelperUnitTests(unittest.TestCase):
    """The shared converters in isolation — one helper, reused everywhere."""

    def test_non_negative_int_accepts_zero_rejects_negative(self) -> None:
        self.assertEqual(cli.non_negative_int("0"), 0)
        self.assertEqual(cli.non_negative_int("7"), 7)
        with self.assertRaises(argparse.ArgumentTypeError):
            cli.non_negative_int("-1")
        with self.assertRaises(argparse.ArgumentTypeError):
            cli.non_negative_int("nope")

    def test_positive_int_requires_at_least_one(self) -> None:
        self.assertEqual(cli.positive_int("1"), 1)
        with self.assertRaises(argparse.ArgumentTypeError):
            cli.positive_int("0")
        with self.assertRaises(argparse.ArgumentTypeError):
            cli.positive_int("-3")

    def test_non_negative_float_accepts_zero_rejects_negative(self) -> None:
        self.assertEqual(cli.non_negative_float("0"), 0.0)
        self.assertEqual(cli.non_negative_float("2.5"), 2.5)
        with self.assertRaises(argparse.ArgumentTypeError):
            cli.non_negative_float("-0.1")
        with self.assertRaises(argparse.ArgumentTypeError):
            cli.non_negative_float("nan-ish")


class NonNegativeIntFlagTests(unittest.TestCase):
    def test_negative_rejected(self) -> None:
        for flag, _dest, _ok, _val in _NON_NEGATIVE_INT_FLAGS:
            with self.subTest(flag=flag):
                with self.assertRaises(SystemExit):
                    _parse([flag, "-1"])

    def test_zero_accepted(self) -> None:
        for flag, dest, ok, val in _NON_NEGATIVE_INT_FLAGS:
            with self.subTest(flag=flag):
                args = _parse([flag, ok])
                self.assertEqual(getattr(args, dest), val)


class PositiveIntFlagTests(unittest.TestCase):
    def test_zero_rejected(self) -> None:
        for flag, _dest, _ok, _val in _POSITIVE_INT_FLAGS:
            with self.subTest(flag=flag):
                with self.assertRaises(SystemExit):
                    _parse([flag, "0"])

    def test_negative_rejected(self) -> None:
        for flag, _dest, _ok, _val in _POSITIVE_INT_FLAGS:
            with self.subTest(flag=flag):
                with self.assertRaises(SystemExit):
                    _parse([flag, "-1"])

    def test_one_accepted(self) -> None:
        for flag, dest, ok, val in _POSITIVE_INT_FLAGS:
            with self.subTest(flag=flag):
                args = _parse([flag, ok])
                self.assertEqual(getattr(args, dest), val)


class NonNegativeFloatFlagTests(unittest.TestCase):
    def test_negative_rejected(self) -> None:
        for flag, _dest, _ok, _val in _NON_NEGATIVE_FLOAT_FLAGS:
            with self.subTest(flag=flag):
                with self.assertRaises(SystemExit):
                    _parse([flag, "-1"])

    def test_zero_accepted(self) -> None:
        for flag, dest, ok, val in _NON_NEGATIVE_FLOAT_FLAGS:
            with self.subTest(flag=flag):
                args = _parse([flag, ok])
                self.assertEqual(getattr(args, dest), val)
                self.assertIsInstance(getattr(args, dest), float)


class ValidValuesUnchangedTests(unittest.TestCase):
    """A representative non-boundary value parses to the exact same int the
    plain ``type=int`` would have produced — validation is purely additive."""

    def test_typical_positive_values_unchanged(self) -> None:
        args = _parse(
            [
                "--jobs", "4",
                "--retry-max-passes", "2",
                "--unfulfillable-reinject-max-per-task", "10",
                "--unconfigured-deadline-secs", "1800",
            ]
        )
        self.assertEqual(args.jobs, 4)
        self.assertEqual(args.retry_max_passes, 2)
        self.assertEqual(args.unfulfillable_reinject_max_per_task, 10)
        self.assertEqual(args.unconfigured_deadline_secs, 1800.0)


if __name__ == "__main__":
    unittest.main()
