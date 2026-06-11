"""Tests for the `--important-stdio-only` logging plumbing — explicit
PARAMETER path, no environment variables.

The flag drives a Rust dual-sink tracing subscriber (in
`crates/dynrunner-pyo3/src/logging.rs`) installed by the `init_logging(...)`
pyfunction. The Python side's whole job is:

1. Parse the flag (`cli.add_framework_arguments` / `build_arg_parser`).
2. After parse, call `init_logging(important_stdio_only, full_log_file,
   full_log_dir)` with the parsed knobs — NO env var, NO read at
   `_native` import.
3. Configure Python's OWN logging: when importance mode is on, drop the
   console StreamHandler and route Python logs to the full-log file; when
   off, behaviour is unchanged.

Pins those contract points. The Rust gate / role-split is exercised by Rust
unit tests in `logging.rs`; pinning it here would duplicate the gate across
the FFI boundary.

unittest-based to stay runnable in a bare nix-develop shell (no pytest in
the dev environment by convention; see `test_forwarded_argv.py`).
"""

from __future__ import annotations

import argparse
import importlib.util
import logging
import os
import pathlib
import sys
import types
import unittest


def _setup_package_stub() -> pathlib.Path:
    """Register a minimal `dynamic_runner` package stub so the modules
    under test load without triggering the real package `__init__` (which
    imports the PyO3 `_native` extension).
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


cli = _load_module_direct("cli", "cli.py")
logging_setup = _load_module_direct("logging_setup", "logging_setup.py")


def _parse(argv: list[str]) -> argparse.Namespace:
    parser = cli.build_arg_parser("test")
    return parser.parse_args(argv)


class ImportantStdioFlagShapeTests(unittest.TestCase):
    def test_flag_absent_default_false(self) -> None:
        args = _parse([])
        self.assertFalse(args.important_stdio_only)

    def test_flag_sets_true(self) -> None:
        args = _parse(["--important-stdio-only"])
        self.assertTrue(args.important_stdio_only)

    def test_full_log_file_and_dir_default_none(self) -> None:
        args = _parse([])
        self.assertIsNone(args.full_log_file)
        self.assertIsNone(args.full_log_dir)

    def test_full_log_file_and_dir_parse(self) -> None:
        args = _parse(
            ["--full-log-file", "/tmp/x.log", "--full-log-dir", "/app/log/sec-0"]
        )
        self.assertEqual(args.full_log_file, "/tmp/x.log")
        self.assertEqual(args.full_log_dir, "/app/log/sec-0")

    def test_flag_literal_matches_module_constant(self) -> None:
        self.assertEqual(
            logging_setup.IMPORTANT_STDIO_ONLY_FLAG, "--important-stdio-only"
        )

    def test_no_logging_env_vars_referenced(self) -> None:
        # The env mechanism is gone: the logging module must not name any
        # of the retired DYNRUNNER_* logging env vars anywhere in its API.
        for retired in (
            "IMPORTANT_STDIO_ONLY_ENV",
            "FULL_LOG_FILE_ENV",
            "FULL_LOG_DIR_ENV",
            "apply_important_stdio_env",
            "importance_mode_active",
        ):
            self.assertFalse(
                hasattr(logging_setup, retired),
                f"retired env-mechanism symbol still present: {retired}",
            )

    def test_submitter_only_filehandler_redirect_retired(self) -> None:
        # The submitter-only Python full-log FileHandler *redirect* is replaced
        # by the general per-role Python->tracing bridge; the redirect symbol
        # must stay gone so no caller resurrects the Python-side special-case.
        # (`resolve_full_log_file` / `DEFAULT_FULL_LOG_FILE` are reinstated for
        # the NATIVE full sink default — exercised in
        # `ResolveFullLogFileTests` — which is NOT a Python FileHandler.)
        self.assertFalse(
            hasattr(logging_setup, "_redirect_python_logs_to_full_log"),
            "retired submitter-only Python FileHandler redirect still present",
        )


class ResolveFullLogDirTests(unittest.TestCase):
    """The submitter's per-setup role-split dir default — independent of
    `--important-stdio-only`, so the bootstrap-primary / post-relocation
    observer logs are always captured for debugging."""

    @staticmethod
    def _ns(**kw: object) -> argparse.Namespace:
        base: dict[str, object] = {
            "secondary": None,
            "multi_computer": None,
            "slurm": False,
            "log_dir": None,
        }
        base.update(kw)
        return argparse.Namespace(**base)

    def test_explicit_dir_always_wins(self) -> None:
        # A compute node is launched with an explicit per-node dir; honour it
        # verbatim regardless of role flags.
        ns = self._ns(secondary=True, log_dir="/app/log-network")
        self.assertEqual(
            logging_setup.resolve_full_log_dir(ns, "/app/log-network/sec-0"),
            "/app/log-network/sec-0",
        )

    def test_slurm_submitter_defaults_to_log_dir_setup(self) -> None:
        ns = self._ns(slurm=True, log_dir="/app/log-network")
        self.assertEqual(
            logging_setup.resolve_full_log_dir(ns, None),
            str(pathlib.Path("/app/log-network") / logging_setup.SETUP_FULL_LOG_SUBDIR),
        )

    def test_multi_computer_submitter_defaults_to_log_dir_setup(self) -> None:
        ns = self._ns(multi_computer="local", log_dir="/var/log/run")
        self.assertEqual(
            logging_setup.resolve_full_log_dir(ns, None),
            str(pathlib.Path("/var/log/run") / "setup"),
        )

    def test_secondary_returns_none(self) -> None:
        # A secondary's dir is always forwarded explicitly; never defaulted.
        ns = self._ns(secondary=True, slurm=True, log_dir="/app/log-network")
        self.assertIsNone(logging_setup.resolve_full_log_dir(ns, None))

    def test_plain_local_run_returns_none(self) -> None:
        # Not a submitter (no multi_computer/slurm) → keep the single stdout
        # stream, no role-split dir.
        ns = self._ns(log_dir="/var/log/run")
        self.assertIsNone(logging_setup.resolve_full_log_dir(ns, None))

    def test_submitter_without_log_dir_returns_none(self) -> None:
        # No anchor to hang the per-setup dir on → None (stdout single stream).
        ns = self._ns(slurm=True, log_dir=None)
        self.assertIsNone(logging_setup.resolve_full_log_dir(ns, None))

    def test_blank_explicit_dir_falls_through_to_default(self) -> None:
        ns = self._ns(slurm=True, log_dir="/app/log-network")
        self.assertEqual(
            logging_setup.resolve_full_log_dir(ns, "  "),
            str(pathlib.Path("/app/log-network") / "setup"),
        )


class ResolveFullLogFileTests(unittest.TestCase):
    """The NATIVE full-sink single-file path resolution. The documented
    ``./dynrunner-full.log`` default under importance mode must feed the Rust
    full sink so a fatal dispatch error is captured to a file rather than lost
    to the importance-gated stdout (the on-cluster diagnosability defect)."""

    def test_explicit_file_always_wins(self) -> None:
        self.assertEqual(
            logging_setup.resolve_full_log_file(True, "/tmp/x.log", None),
            "/tmp/x.log",
        )
        # Even with importance mode OFF an explicit file is honoured.
        self.assertEqual(
            logging_setup.resolve_full_log_file(False, "/tmp/x.log", None),
            "/tmp/x.log",
        )

    def test_importance_on_no_knobs_defaults_to_dynrunner_full_log(self) -> None:
        # The headline fix: with importance mode on and neither file nor dir
        # set, the native full sink defaults to ./dynrunner-full.log so the
        # full record (and a fatal error) is durably captured.
        self.assertEqual(
            logging_setup.resolve_full_log_file(True, None, None),
            logging_setup.DEFAULT_FULL_LOG_FILE,
        )
        self.assertEqual(logging_setup.DEFAULT_FULL_LOG_FILE, "dynrunner-full.log")

    def test_importance_off_yields_none_single_stream(self) -> None:
        # Off importance mode stdout already carries everything → no implicit
        # file (today's single-stream behaviour preserved).
        self.assertIsNone(logging_setup.resolve_full_log_file(False, None, None))

    def test_dir_sink_suppresses_the_file_default(self) -> None:
        # The per-node --full-log-dir is its own higher-precedence sink; the
        # single-file default must NOT also kick in (no duplicate sink).
        self.assertIsNone(
            logging_setup.resolve_full_log_file(True, None, "/app/log/setup")
        )

    def test_blank_explicit_file_falls_through_to_default(self) -> None:
        self.assertEqual(
            logging_setup.resolve_full_log_file(True, "  ", None),
            logging_setup.DEFAULT_FULL_LOG_FILE,
        )


class _RootLoggerSandbox(unittest.TestCase):
    """Save/restore the root logger's handlers + level so logging-setup
    tests do not corrupt the shared root logger for the rest of the suite.
    Also stubs `dynamic_runner.init_logging` so `setup_logging` does not
    require the compiled `_native` extension and records its call args.
    """

    def setUp(self) -> None:
        root = logging.getLogger()
        self._saved_handlers = root.handlers[:]
        self._saved_level = root.level
        root.handlers.clear()
        # Stub the native init_logging the setup_logging lazy import reaches
        # (`from . import init_logging`). Record calls for assertions.
        self.init_calls: list[dict] = []
        pkg = sys.modules["dynamic_runner"]
        self._saved_init = getattr(pkg, "init_logging", None)
        pkg.init_logging = lambda **kw: self.init_calls.append(kw)
        # Stub the native py_log the bridge handler forwards to
        # (`_install_tracing_bridge` does `from . import py_log`). Record
        # forwarded records so tests can assert the Python->tracing bridge.
        self.py_log_calls: list[tuple] = []
        self._saved_py_log = getattr(pkg, "py_log", None)
        pkg.py_log = lambda *a: self.py_log_calls.append(a)
        # Stub the native py_log_important the fatal-surfacing path reaches
        # (`surface_fatal_errors` does `from . import py_log_important`).
        self.py_log_important_calls: list[str] = []
        self._saved_py_log_important = getattr(pkg, "py_log_important", None)
        pkg.py_log_important = lambda m: self.py_log_important_calls.append(m)
        # Stub the native flush_important_stdio the fatal-flush path reaches
        # (`_flush_all_logging` does `from . import flush_important_stdio`).
        # Record calls so a test can assert the debounce buffer is flushed
        # synchronously on the fatal path.
        self.flush_important_stdio_calls = 0
        self._saved_flush_important = getattr(pkg, "flush_important_stdio", None)

        def _record_flush() -> None:
            self.flush_important_stdio_calls += 1

        pkg.flush_important_stdio = _record_flush

    def tearDown(self) -> None:
        root = logging.getLogger()
        for h in root.handlers[:]:
            root.removeHandler(h)
            try:
                h.close()
            except Exception:
                pass
        for h in self._saved_handlers:
            root.addHandler(h)
        root.setLevel(self._saved_level)
        pkg = sys.modules["dynamic_runner"]
        if self._saved_init is None:
            if hasattr(pkg, "init_logging"):
                del pkg.init_logging
        else:
            pkg.init_logging = self._saved_init
        if self._saved_py_log is None:
            if hasattr(pkg, "py_log"):
                del pkg.py_log
        else:
            pkg.py_log = self._saved_py_log
        if self._saved_py_log_important is None:
            if hasattr(pkg, "py_log_important"):
                del pkg.py_log_important
        else:
            pkg.py_log_important = self._saved_py_log_important
        if self._saved_flush_important is None:
            if hasattr(pkg, "flush_important_stdio"):
                del pkg.flush_important_stdio
        else:
            pkg.flush_important_stdio = self._saved_flush_important


class InitLoggingParamPassthroughTests(_RootLoggerSandbox):
    def test_flag_off_passes_false_no_paths(self) -> None:
        logging_setup.setup_logging(_parse(["--debug"]))
        self.assertEqual(len(self.init_calls), 1)
        call = self.init_calls[0]
        self.assertFalse(call["important_stdio_only"])
        self.assertIsNone(call["full_log_file"])
        self.assertIsNone(call["full_log_dir"])

    def test_debug_flag_threads_into_init_logging(self) -> None:
        # `--debug` must reach the Rust subscriber via the init_logging
        # `debug` param — not just the Python root logger. Without this the
        # secondary's per-role `secondary.log` stayed INFO-only on a
        # `--debug` run (the on-cluster bug).
        logging_setup.setup_logging(_parse(["--debug"]))
        self.assertEqual(len(self.init_calls), 1)
        self.assertTrue(self.init_calls[0]["debug"])

    def test_no_debug_flag_passes_debug_false(self) -> None:
        # Absent `--debug`, the param defaults to False so the Rust sinks
        # keep the historical INFO ceiling.
        logging_setup.setup_logging(_parse([]))
        self.assertEqual(len(self.init_calls), 1)
        self.assertFalse(self.init_calls[0]["debug"])

    def test_flag_on_arms_importance_via_param_no_env(self) -> None:
        # Headline: --important-stdio-only arms importance via the
        # init_logging PARAM, and NO logging env var is set as a side
        # effect (the mechanism is purely parametric now).
        snapshot = dict(os.environ)
        logging_setup.setup_logging(_parse(["--important-stdio-only"]))
        self.assertEqual(len(self.init_calls), 1)
        self.assertTrue(self.init_calls[0]["important_stdio_only"])
        # No DYNRUNNER_* logging env var appeared.
        new_dynrunner = {
            k for k in os.environ if k.startswith("DYNRUNNER_") and k not in snapshot
        }
        self.assertEqual(
            new_dynrunner, set(), f"setup_logging leaked env vars: {new_dynrunner}"
        )

    def test_importance_on_defaults_full_log_file_into_init_logging(self) -> None:
        # The native full sink must default to ./dynrunner-full.log under
        # importance mode when no file/dir knob is set, so the full record
        # (and a fatal error) is captured to a file — not the gated stdout.
        logging_setup.setup_logging(_parse(["--important-stdio-only"]))
        self.assertEqual(len(self.init_calls), 1)
        self.assertEqual(
            self.init_calls[0]["full_log_file"], logging_setup.DEFAULT_FULL_LOG_FILE
        )

    def test_importance_off_passes_no_implicit_full_log_file(self) -> None:
        # Off importance mode the native full sink stays stdout (None) — the
        # single-stream default is unchanged.
        logging_setup.setup_logging(_parse([]))
        self.assertIsNone(self.init_calls[0]["full_log_file"])

    def test_full_log_dir_forwarded_to_init_logging(self) -> None:
        # The per-role-log feature is wired via the forwarded --full-log-dir
        # CLI arg → parsed → init_logging param (replacing the retired env
        # injection).
        logging_setup.setup_logging(
            _parse(["--secondary", "tcp://x", "--full-log-dir", "/app/log/sec-0"])
        )
        self.assertEqual(self.init_calls[0]["full_log_dir"], "/app/log/sec-0")


class PythonLoggingReconfigTests(_RootLoggerSandbox):
    """The Python side bridges its records into Rust tracing for ALL roles
    (the general per-role mechanism replacing the submitter-only FileHandler
    redirect), and additionally drops the console handler in importance mode."""

    def _file_handlers(self):
        return [
            h
            for h in logging.getLogger().handlers
            if isinstance(h, logging.FileHandler)
        ]

    def _bare_stream_handlers(self):
        return [
            h
            for h in logging.getLogger().handlers
            if isinstance(h, logging.StreamHandler)
            and not isinstance(h, logging.FileHandler)
        ]

    def _bridge_handlers(self):
        return [
            h
            for h in logging.getLogger().handlers
            if isinstance(h, logging_setup._TracingBridgeHandler)
        ]

    def test_flag_off_installs_bridge_keeps_console_no_file(self) -> None:
        logging_setup.setup_logging(_parse(["--debug"]))
        self.assertEqual(
            len(self._bridge_handlers()),
            1,
            "flag off must STILL install the per-role tracing bridge",
        )
        self.assertTrue(
            self._bare_stream_handlers(),
            "flag off must keep the console StreamHandler",
        )
        self.assertFalse(
            self._file_handlers(),
            "the submitter-only full-log FileHandler is retired — none must appear",
        )

    def test_flag_on_installs_bridge_removes_console_no_file(self) -> None:
        logging_setup.setup_logging(_parse(["--important-stdio-only"]))
        self.assertEqual(
            len(self._bridge_handlers()),
            1,
            "importance mode must install the per-role tracing bridge",
        )
        self.assertFalse(
            self._bare_stream_handlers(),
            "flag on must drop the console StreamHandler",
        )
        self.assertFalse(
            self._file_handlers(),
            "the FileHandler redirect is replaced by the bridge — none must appear",
        )

    def test_bridge_forwards_record_via_py_log(self) -> None:
        # The bridge's whole job: a Python record reaches the native `py_log`
        # (level name, logger name, rendered message). This is the Python end
        # of the Python->tracing bridge; the Rust side routes it to the
        # per-role file by the run future's role span.
        logging_setup.setup_logging(_parse(["--debug"]))
        logging.getLogger("consumer.task").warning("phase complete: %d", 7)
        self.assertTrue(self.py_log_calls, "bridge did not forward to py_log")
        level, name, message = self.py_log_calls[-1]
        self.assertEqual(level, "WARNING")
        self.assertEqual(name, "consumer.task")
        self.assertEqual(message, "phase complete: 7")

    def test_bridge_install_is_idempotent(self) -> None:
        # A re-entrant setup_logging (respawn / cli_main-then-run) must not
        # stack duplicate bridges.
        logging_setup.setup_logging(_parse([]))
        logging_setup.setup_logging(_parse([]))
        self.assertEqual(
            len(self._bridge_handlers()),
            1,
            "re-running setup_logging stacked duplicate bridge handlers",
        )


class SurfaceFatalErrorsTests(_RootLoggerSandbox):
    """A FATAL error reaching the dispatch boundary must ALWAYS surface — via
    the native IMPORTANT-target primitive (so the importance-mode stdio gate
    admits it) — be flushed, and re-raise unchanged. This is the on-cluster
    diagnosability defect: under ``--important-stdio-only`` a fatal dispatch
    error was logged through the ordinary Python bridge, gated OUT of stdio,
    and lost. The guard closes that gap without weakening the normal filter."""

    def test_clean_block_does_not_emit_fatal(self) -> None:
        # No exception → no fatal emit (the guard is inert on the happy path).
        with logging_setup.surface_fatal_errors():
            pass
        self.assertEqual(self.py_log_important_calls, [])

    def test_exception_routes_through_important_and_reraises(self) -> None:
        # REVERT-CHECK: remove the guard (or route the error through the plain
        # bridge instead of py_log_important) and the fatal error never reaches
        # the IMPORTANT target → undiagnosable under importance mode.
        with self.assertRaises(RuntimeError):
            with logging_setup.surface_fatal_errors():
                raise RuntimeError("SLURM dispatch failed: boom")

        self.assertEqual(
            len(self.py_log_important_calls),
            1,
            "fatal error did not reach the IMPORTANT-target primitive",
        )
        message = self.py_log_important_calls[0]
        # The message carries the exception text AND its traceback, so an
        # operator (or LLM) woken on stdio can diagnose the hard failure.
        self.assertIn("SLURM dispatch failed: boom", message)
        self.assertIn("RuntimeError", message)
        self.assertIn("Traceback", message)

    def test_fatal_path_flushes_debounced_stdio_buffer(self) -> None:
        # Under `--important-stdio-only` the operator-stdio sink coalesces
        # output behind a 500ms-quiet / 5s-max-delay debounce buffer. The
        # just-emitted fatal IMPORTANT line must NOT wait for that timer — the
        # surfacing path flushes the native buffer synchronously so the
        # diagnosable line is on the wire before teardown. REVERT-CHECK: drop
        # the `flush_important_stdio()` call from `_flush_all_logging` and this
        # records zero flushes.
        with self.assertRaises(RuntimeError):
            with logging_setup.surface_fatal_errors():
                raise RuntimeError("boom")
        self.assertGreaterEqual(
            self.flush_important_stdio_calls,
            1,
            "fatal path did not flush the debounced operator-stdio buffer",
        )

    def test_surfacing_never_masks_original_with_secondary_failure(self) -> None:
        # If the native primitive itself raises, the ORIGINAL fatal error must
        # still propagate (surfacing is best-effort; it never replaces the
        # real error with a secondary one).
        pkg = sys.modules["dynamic_runner"]

        def _boom(_m: str) -> None:
            raise OSError("stdout gone")

        pkg.py_log_important = _boom
        with self.assertRaises(ValueError):
            with logging_setup.surface_fatal_errors():
                raise ValueError("the real fatal error")


if __name__ == "__main__":
    unittest.main()
