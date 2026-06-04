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


class ResolveFullLogFileTests(unittest.TestCase):
    def test_off_returns_none(self) -> None:
        self.assertIsNone(logging_setup.resolve_full_log_file(False, "/tmp/x.log"))

    def test_on_uses_explicit_path(self) -> None:
        self.assertEqual(
            logging_setup.resolve_full_log_file(True, "/tmp/x.log"),
            pathlib.Path("/tmp/x.log"),
        )

    def test_on_falls_back_to_default(self) -> None:
        self.assertEqual(
            logging_setup.resolve_full_log_file(True, None),
            pathlib.Path(logging_setup.DEFAULT_FULL_LOG_FILE),
        )

    def test_on_blank_path_falls_back_to_default(self) -> None:
        self.assertEqual(
            logging_setup.resolve_full_log_file(True, "  "),
            pathlib.Path(logging_setup.DEFAULT_FULL_LOG_FILE),
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


class InitLoggingParamPassthroughTests(_RootLoggerSandbox):
    def test_flag_off_passes_false_no_paths(self) -> None:
        logging_setup.setup_logging(_parse(["--debug"]))
        self.assertEqual(len(self.init_calls), 1)
        call = self.init_calls[0]
        self.assertFalse(call["important_stdio_only"])
        self.assertIsNone(call["full_log_file"])
        self.assertIsNone(call["full_log_dir"])

    def test_flag_on_arms_importance_via_param_no_env(self) -> None:
        # Headline: --important-stdio-only arms importance via the
        # init_logging PARAM, and NO logging env var is set as a side
        # effect (the mechanism is purely parametric now).
        snapshot = dict(os.environ)
        logging_setup.setup_logging(_parse(["--important-stdio-only"]))
        try:
            self.assertEqual(len(self.init_calls), 1)
            self.assertTrue(self.init_calls[0]["important_stdio_only"])
            # No DYNRUNNER_* logging env var appeared.
            new_dynrunner = {
                k for k in os.environ if k.startswith("DYNRUNNER_") and k not in snapshot
            }
            self.assertEqual(
                new_dynrunner, set(), f"setup_logging leaked env vars: {new_dynrunner}"
            )
        finally:
            for h in self._file_handlers():
                h.close()
            stray = pathlib.Path(logging_setup.DEFAULT_FULL_LOG_FILE)
            if stray.exists():
                stray.unlink()

    def test_full_log_dir_forwarded_to_init_logging(self) -> None:
        # The per-role-log feature is wired via the forwarded --full-log-dir
        # CLI arg → parsed → init_logging param (replacing the retired env
        # injection).
        logging_setup.setup_logging(
            _parse(["--secondary", "tcp://x", "--full-log-dir", "/app/log/sec-0"])
        )
        self.assertEqual(self.init_calls[0]["full_log_dir"], "/app/log/sec-0")

    def _file_handlers(self):
        return [
            h
            for h in logging.getLogger().handlers
            if isinstance(h, logging.FileHandler)
        ]


class PythonLoggingReconfigTests(_RootLoggerSandbox):
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

    def test_flag_off_keeps_console_no_file(self) -> None:
        logging_setup.setup_logging(_parse(["--debug"]))
        self.assertTrue(
            self._bare_stream_handlers(),
            "flag off must keep the console StreamHandler",
        )
        self.assertFalse(
            self._file_handlers(), "flag off must NOT add a full-log FileHandler"
        )

    def test_flag_on_removes_console_adds_file(self) -> None:
        import tempfile

        with tempfile.TemporaryDirectory() as d:
            full_log = pathlib.Path(d) / "full.log"
            logging_setup.setup_logging(
                _parse(["--important-stdio-only", "--full-log-file", str(full_log)])
            )
            self.assertFalse(
                self._bare_stream_handlers(),
                "flag on must drop the console StreamHandler",
            )
            file_handlers = self._file_handlers()
            self.assertEqual(len(file_handlers), 1)
            self.assertEqual(
                pathlib.Path(file_handlers[0].baseFilename), full_log.resolve()
            )

    def test_flag_on_file_handler_uses_default_path(self) -> None:
        logging_setup.setup_logging(_parse(["--important-stdio-only"]))
        try:
            file_handlers = self._file_handlers()
            self.assertEqual(len(file_handlers), 1)
            self.assertEqual(
                pathlib.Path(file_handlers[0].baseFilename),
                pathlib.Path(logging_setup.DEFAULT_FULL_LOG_FILE).resolve(),
            )
        finally:
            for h in self._file_handlers():
                h.close()
            stray = pathlib.Path(logging_setup.DEFAULT_FULL_LOG_FILE)
            if stray.exists():
                stray.unlink()


if __name__ == "__main__":
    unittest.main()
