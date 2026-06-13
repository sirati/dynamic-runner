"""Tests for the `--stage-via-setup-tasks` flag plumbing (#489 P4).

The flag selects the framework's file-staging system: off (default) keeps
the legacy StageFile path; on routes staging through the setup-task model
(per-file pre-succeeded setup tasks + `TaskDep` gating; the #488-free path).
The Python side's job is the flag -> kwarg mapping + the forward
classification so a relocated/promoted secondary inherits the same selector:

  * `cli.build_arg_parser` declares the flag (`store_true`, default False,
    dest `stage_via_setup_tasks`).
  * It is a FORWARDABLE framework flag (NOT regenerated, NOT submitter-local),
    so `filter_framework_argv` carries it verbatim to every secondary — the
    relocate target reads it off its own `task_args` to pick the same
    `StagingStrategy`.
  * `run._dispatch_*` forwards `args.stage_via_setup_tasks` to the Rust
    `run_primary` / `run_distributed` entry points as the
    `stage_via_setup_tasks` kwarg (the PyO3 layer maps it to
    `PrimaryConfig.staging_strategy`).

stdlib `unittest` (pytest is not in the dev shell); modules are loaded
directly under a package stub, mirroring the sibling `*_cli.py` tests so
they run in a bare `nix develop` without a maturin build.
"""

from __future__ import annotations

import argparse
import importlib.util
import io
import pathlib
import sys
import types
import unittest
from contextlib import redirect_stderr


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


cli = _load_module_direct("cli", "cli.py")
_forwarded_argv = _load_module_direct("_forwarded_argv", "_forwarded_argv.py")
_framework_flags = _load_module_direct("_framework_flags", "_framework_flags.py")
filter_framework_argv = _forwarded_argv.filter_framework_argv


def _parse(argv: list[str]) -> argparse.Namespace:
    return cli.build_arg_parser("test").parse_args(argv)


def _parse_and_validate(argv: list[str]) -> argparse.Namespace:
    """Build, parse, AND run cross-flag validation — mirrors what
    `dynamic_runner.run.run` does so guard tests exercise the same path
    the operator hits.
    """
    parser = cli.build_arg_parser("test")
    args = parser.parse_args(argv)
    cli.validate_parsed_args(args, parser)
    return args


class StageViaSetupTasksFlagShapeTests(unittest.TestCase):
    def test_flag_absent_defaults_to_false(self) -> None:
        args = _parse([])
        self.assertFalse(args.stage_via_setup_tasks)

    def test_flag_present_parses_true(self) -> None:
        args = _parse(["--stage-via-setup-tasks"])
        self.assertTrue(args.stage_via_setup_tasks)
        self.assertIsInstance(args.stage_via_setup_tasks, bool)


class StageViaSetupTasksForwardingTests(unittest.TestCase):
    """The flag must reach secondaries so a relocate target picks the same
    `StagingStrategy` — it rides `forwarded_argv` like every other generic
    framework flag (NOT regenerated, NOT submitter-local)."""

    def test_flag_is_a_registered_framework_flag(self) -> None:
        self.assertIn(
            "--stage-via-setup-tasks",
            _framework_flags.framework_option_strings(),
        )

    def test_flag_not_regenerated_not_submitter_local(self) -> None:
        # Either classification would strip it on forward; the relocate
        # target would then default to the OLD path while the submitter
        # used the setup-task model — a split-brain staging selector.
        self.assertNotIn(
            "--stage-via-setup-tasks", _framework_flags.FRAMEWORK_REGENERATED_FLAGS
        )
        self.assertNotIn(
            "--stage-via-setup-tasks", _framework_flags.SUBMITTER_LOCAL_FLAGS
        )

    def test_flag_survives_forward_filter_verbatim(self) -> None:
        # The headline round-trip: a value-less framework flag the secondary
        # needs survives `filter_framework_argv` (the only channel to a
        # secondary). The flag is `store_true`, so the single token passes
        # through and the following task token is untouched.
        argv = ["--stage-via-setup-tasks", "--platform", "x64"]
        self.assertEqual(filter_framework_argv(argv), argv)

    def test_flag_mid_argv_survives_without_swallowing_neighbor(self) -> None:
        # Defensive ordering: a value-less flag must not consume the next
        # task token as if it took a value.
        argv = ["--platform", "x64", "--stage-via-setup-tasks", "--compiler", "gcc"]
        self.assertEqual(filter_framework_argv(argv), argv)


class StageViaSetupTasksGuardTests(unittest.TestCase):
    """The CLI guard: `--stage-via-setup-tasks` without `--source-already-staged`
    must fail fast with a clear error at config time, not silently at dispatch
    with a NonRecoverable.

    Invariant: mode-1 (framework-upload / files-on-submitter) staging via setup
    tasks suppresses the legacy StageFile physical-resolution fan-out but does
    NOT replace it — per-secondary file delivery is unwired. `validate_parsed_args`
    rejects the combination so operators learn immediately rather than at
    runtime dispatch.
    """

    def test_flag_on_without_source_already_staged_is_rejected(self) -> None:
        # Headline guard: flag-ON + no --source-already-staged → clear error,
        # NOT a silent pass or a runtime NonRecoverable.
        parser = cli.build_arg_parser("test")
        args = parser.parse_args(["--stage-via-setup-tasks"])
        stderr_buf = io.StringIO()
        with self.assertRaises(SystemExit) as cm, redirect_stderr(stderr_buf):
            cli.validate_parsed_args(args, parser)
        self.assertEqual(cm.exception.code, 2)
        msg = stderr_buf.getvalue()
        # The error message names the unsupported combination so the
        # operator knows exactly what is missing.
        self.assertIn("--stage-via-setup-tasks", msg)
        self.assertIn("--source-already-staged", msg)

    def test_flag_on_with_source_already_staged_passes_guard(self) -> None:
        # Valid mode-2 combo: flag-ON + --source-already-staged + a
        # distributed mode (--source-already-staged itself requires one).
        # The guard must NOT reject this combination.
        args = _parse_and_validate(
            [
                "--stage-via-setup-tasks",
                "--source-already-staged",
                "/nfs/corpus",
                "--multi-computer",
                "slurm",
            ]
        )
        self.assertTrue(args.stage_via_setup_tasks)
        self.assertEqual(args.source_already_staged, "/nfs/corpus")

    def test_flag_off_without_source_already_staged_passes_guard(self) -> None:
        # Default (flag absent) + no --source-already-staged is the
        # ordinary run path; must never be affected by the guard.
        args = _parse_and_validate([])
        self.assertFalse(args.stage_via_setup_tasks)
        self.assertIsNone(args.source_already_staged)


if __name__ == "__main__":
    unittest.main()
