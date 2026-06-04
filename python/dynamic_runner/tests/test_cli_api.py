"""Tests for the composable CLI API surface:

  * `add_framework_arguments(parser)` attaches cleanly to a TOP-level parser
    AND to a SUBPARSER (asm-dataset's submit/secondary subcommand shape).
  * `run(task, args=ns)` runs from a pre-parsed namespace and NEVER reads
    global `sys.argv`.
  * `run(task, argv=...)` forwards ONLY framework/task flags to the
    secondary; consumer flags never appear (here: the forward filter drops
    framework-regenerated + submitter-local, keeps task flags).
  * `cli_main` resolves a factory from parsed args and excludes consumer
    flags from the forwarded secondary argv.

unittest-based + direct-file module loading so the suite runs in a bare
nix-develop shell without the compiled `_native` extension. `run.dispatch`
and `logging_setup.setup_logging` are stubbed so no test reaches the Rust
dispatch path.
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
run_mod = _load("run", "run.py")
cli_main_mod = _load("cli_main", "cli_main.py")


class _FakeTask:
    """A minimal object satisfying the TaskDefinition Protocol's CLI side.

    Registers one task-specific filter flag (`--platform`) so forwarding
    behaviour can be observed. `discover_items` is never reached (dispatch
    is stubbed) but defined for structural-Protocol membership.
    """

    def add_task_arguments(self, parser) -> None:
        parser.add_argument("--platform", default=None)

    def discover_items(self, source_dir, args):  # pragma: no cover - never run
        return []


class _DispatchRecorder:
    """Stub `run.dispatch` + `run.setup_logging` to capture args without
    touching the Rust dispatch path, and restore them on exit."""

    def __enter__(self):
        self.calls: list[argparse.Namespace] = []
        self._saved_dispatch = run_mod.dispatch
        self._saved_setup = run_mod.setup_logging
        self._saved_cm_dispatch = cli_main_mod._dispatch
        self._saved_cm_setup = cli_main_mod.setup_logging

        def _rec_dispatch(task, args, deployment):
            self.calls.append(args)

        run_mod.dispatch = _rec_dispatch
        run_mod.setup_logging = lambda args: None
        cli_main_mod._dispatch = _rec_dispatch
        cli_main_mod.setup_logging = lambda args: None
        return self

    def __exit__(self, *exc):
        run_mod.dispatch = self._saved_dispatch
        run_mod.setup_logging = self._saved_setup
        cli_main_mod._dispatch = self._saved_cm_dispatch
        cli_main_mod.setup_logging = self._saved_cm_setup


class AddFrameworkArgumentsTests(unittest.TestCase):
    def test_composes_onto_top_parser(self) -> None:
        parser = argparse.ArgumentParser()
        cli.add_framework_arguments(parser)
        args = parser.parse_args(["--multi-computer", "slurm", "--cores", "4"])
        self.assertEqual(args.multi_computer, "slurm")
        self.assertEqual(args.cores, "4")

    def test_composes_onto_subparser(self) -> None:
        # asm-dataset shape: framework args attach to the `submit`/`secondary`
        # SUBPARSER, alongside the consumer's own subcommand args.
        parser = argparse.ArgumentParser()
        sub = parser.add_subparsers(dest="command", required=True)
        p_submit = sub.add_parser("submit")
        cli.add_framework_arguments(p_submit)
        p_submit.add_argument("--shared-fs")  # consumer-only flag

        args = parser.parse_args(
            ["submit", "--multi-computer", "slurm", "--shared-fs", "/run/x"]
        )
        self.assertEqual(args.command, "submit")
        self.assertEqual(args.multi_computer, "slurm")
        self.assertEqual(args.shared_fs, "/run/x")

    def test_subparser_and_top_parser_register_same_flags(self) -> None:
        top = argparse.ArgumentParser()
        cli.add_framework_arguments(top)
        sub_root = argparse.ArgumentParser()
        subp = sub_root.add_subparsers(dest="command").add_parser("secondary")
        cli.add_framework_arguments(subp)
        top_opts = {o for a in top._actions for o in a.option_strings}
        sub_opts = {o for a in subp._actions for o in a.option_strings}
        # The subparser carries every framework option the top parser does
        # (minus argparse's own auto `-h/--help`, present on both anyway).
        self.assertTrue(top_opts.issubset(sub_opts | {"-h", "--help"}))
        self.assertIn("--multi-computer", sub_opts)


class RunArgsNamespaceTests(unittest.TestCase):
    def test_run_with_args_does_not_read_sys_argv(self) -> None:
        # Poison sys.argv: if `run` reads it, parsing would explode on the
        # bogus token. A pre-parsed namespace must bypass argparse entirely.
        ns = argparse.Namespace(
            multi_computer=None,
            secondary=None,
            observer_join_from_peer_info_dir=None,
        )
        saved_argv = sys.argv
        sys.argv = ["prog", "--this-flag-does-not-exist", "boom"]
        try:
            with _DispatchRecorder() as rec:
                run_mod.run(_FakeTask(), args=ns)
            self.assertEqual(len(rec.calls), 1)
            # Pre-parsed namespace path carries no task-filter argv to relay.
            self.assertEqual(rec.calls[0].forwarded_argv, [])
        finally:
            sys.argv = saved_argv

    def test_run_argv_and_args_mutually_exclusive(self) -> None:
        with self.assertRaises(ValueError):
            run_mod.run(_FakeTask(), argv=[], args=argparse.Namespace())

    def test_run_default_argv_is_empty_not_sys_argv(self) -> None:
        # With neither argv nor args, run parses [] — it must NOT fall back
        # to global sys.argv. Poison sys.argv to prove it.
        saved_argv = sys.argv
        sys.argv = ["prog", "--this-flag-does-not-exist"]
        try:
            with _DispatchRecorder() as rec:
                run_mod.run(_FakeTask())
            self.assertEqual(len(rec.calls), 1)
            self.assertEqual(rec.calls[0].forwarded_argv, [])
        finally:
            sys.argv = saved_argv


class ForwardedArgvSeparationTests(unittest.TestCase):
    def test_only_framework_and_task_flags_forwarded(self) -> None:
        # argv mixes a framework-regenerated flag (--cores, dropped), a
        # submitter-local flag (--important-stdio-only, dropped), and a task
        # flag (--platform, kept). Only the kept task flag reaches the
        # forwarded secondary argv.
        argv = [
            "--cores", "4",
            "--important-stdio-only",
            "--platform", "x64",
        ]
        with _DispatchRecorder() as rec:
            run_mod.run(_FakeTask(), argv=argv)
        self.assertEqual(rec.calls[0].forwarded_argv, ["--platform", "x64"])


class CliMainTests(unittest.TestCase):
    def test_factory_form_picks_task_from_consumer_flag(self) -> None:
        chosen: dict = {}

        class _TaskA(_FakeTask):
            pass

        class _TaskB(_FakeTask):
            pass

        def factory(ns: argparse.Namespace):
            chosen["which"] = ns.which
            return _TaskA() if ns.which == "a" else _TaskB()

        def add_consumer_args(p: argparse.ArgumentParser) -> None:
            p.add_argument("--which", choices=["a", "b"], required=True)

        with _DispatchRecorder() as rec:
            cli_main_mod.cli_main(
                factory,
                add_consumer_args=add_consumer_args,
                argv=["--which", "b", "--platform", "arm", "--cores", "2"],
            )
        self.assertEqual(chosen["which"], "b")
        # Consumer flag `--which b` is excluded from the forwarded argv;
        # the framework-regenerated `--cores 2` is dropped too; only the
        # task `--platform arm` survives.
        self.assertEqual(rec.calls[0].forwarded_argv, ["--platform", "arm"])

    def test_plain_task_form(self) -> None:
        with _DispatchRecorder() as rec:
            cli_main_mod.cli_main(
                _FakeTask(),
                argv=["--platform", "mips"],
            )
        self.assertEqual(rec.calls[0].forwarded_argv, ["--platform", "mips"])


if __name__ == "__main__":
    unittest.main()
