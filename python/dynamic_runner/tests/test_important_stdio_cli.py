"""Tests for the `--important-stdio-only` CLI flag and its logging plumbing.

The flag drives a Rust dual-sink tracing subscriber (merged in
`crates/dynrunner-pyo3/src/logging.rs`) that reads two env vars ONCE at
`_native` import time. The Python side's whole job is:

1. Parse the flag (`cli.build_arg_parser`).
2. Export `DYNRUNNER_IMPORTANT_STDIO_ONLY` + `DYNRUNNER_FULL_LOG_FILE`
   BEFORE the first `_native` import (the ordering is load-bearing — set
   them after import and the Rust subscriber has already read the
   defaults). Driven from `dynamic_runner.__init__` top.
3. Configure Python's OWN logging: when the flag is on, drop the console
   StreamHandler and route Python logs to the full-log file; when off,
   behaviour is unchanged.

Pins each of those contract points. The Rust gate itself is exercised by
Rust unit tests in `logging.rs`; pinning it here would duplicate the gate
across the FFI boundary.

unittest-based to stay runnable in a bare nix-develop shell (no pytest in
the dev environment by convention; see `test_forwarded_argv.py`).
"""

from __future__ import annotations

import argparse
import importlib
import importlib.util
import logging
import os
import pathlib
import sys
import types
import unittest


def _setup_package_stub() -> pathlib.Path:
    """Register a minimal `dynamic_runner` package stub so the modules
    under test load without triggering the real package `__init__`
    (which imports the PyO3 `_native` extension and would otherwise
    require a maturin build to import).

    Mirrors the stub pattern in `test_forwarded_argv.py`.
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
    return _load_module_direct("cli", "cli.py")


cli = _load_cli_module()
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

    def test_flag_literal_matches_module_constant(self) -> None:
        # The argparse declaration and the early argv lookahead scan the
        # same literal; the lookahead helper owns it as a constant. If
        # they ever drift, parsing and the env export would disagree.
        self.assertEqual(logging_setup.IMPORTANT_STDIO_ONLY_FLAG, "--important-stdio-only")


class _EnvSandbox(unittest.TestCase):
    """Save/restore the two env vars around each test so ordering and
    default tests do not leak into one another (or the wider suite)."""

    def setUp(self) -> None:
        self._saved = {
            k: os.environ.get(k)
            for k in (
                logging_setup.IMPORTANT_STDIO_ONLY_ENV,
                logging_setup.FULL_LOG_FILE_ENV,
            )
        }
        for k in self._saved:
            os.environ.pop(k, None)

    def tearDown(self) -> None:
        for k, v in self._saved.items():
            if v is None:
                os.environ.pop(k, None)
            else:
                os.environ[k] = v


class ApplyEnvTests(_EnvSandbox):
    def test_no_flag_exports_nothing(self) -> None:
        logging_setup.apply_important_stdio_env(["--debug"])
        self.assertNotIn(logging_setup.IMPORTANT_STDIO_ONLY_ENV, os.environ)
        self.assertNotIn(logging_setup.FULL_LOG_FILE_ENV, os.environ)

    def test_flag_exports_both_vars(self) -> None:
        logging_setup.apply_important_stdio_env(["--important-stdio-only"])
        self.assertEqual(os.environ[logging_setup.IMPORTANT_STDIO_ONLY_ENV], "1")
        self.assertEqual(
            os.environ[logging_setup.FULL_LOG_FILE_ENV],
            logging_setup.DEFAULT_FULL_LOG_FILE,
        )

    def test_operator_full_log_override_is_respected(self) -> None:
        os.environ[logging_setup.FULL_LOG_FILE_ENV] = "/tmp/operator-chosen.log"
        logging_setup.apply_important_stdio_env(["--important-stdio-only"])
        # The flag still arms importance mode...
        self.assertEqual(os.environ[logging_setup.IMPORTANT_STDIO_ONLY_ENV], "1")
        # ...but never clobbers the operator's pre-exported path.
        self.assertEqual(
            os.environ[logging_setup.FULL_LOG_FILE_ENV], "/tmp/operator-chosen.log"
        )

    def test_full_log_file_path_tracks_env(self) -> None:
        os.environ[logging_setup.FULL_LOG_FILE_ENV] = "/var/log/custom.log"
        self.assertEqual(
            logging_setup.full_log_file_path(), pathlib.Path("/var/log/custom.log")
        )

    def test_full_log_file_path_default_when_unset(self) -> None:
        self.assertEqual(
            logging_setup.full_log_file_path(),
            pathlib.Path(logging_setup.DEFAULT_FULL_LOG_FILE),
        )


class EnvVarConfigInterfaceTests(_EnvSandbox):
    """The env-var first-class config path for wrapping consumers: a
    truthy ``DYNRUNNER_IMPORTANT_STDIO_ONLY`` set WITHOUT the flag must
    produce identical submitter behaviour to passing the flag — one
    mechanism, not a parallel surface.
    """

    def test_env_only_arms_importance_mode(self) -> None:
        # No flag in argv; the consumer armed the mode via the env var.
        os.environ[logging_setup.IMPORTANT_STDIO_ONLY_ENV] = "1"
        self.assertTrue(logging_setup.importance_mode_active())

    def test_flag_not_in_argv_does_not_arm_without_env(self) -> None:
        self.assertFalse(logging_setup.importance_mode_active())

    def test_truthiness_mirrors_rust_convention(self) -> None:
        # Mirrors the Rust `is_truthy` set (1/true/yes/on, ci); anything
        # else — including the empty string and "0" — is off.
        for on in ("1", "true", "TRUE", "Yes", "on", " on "):
            os.environ[logging_setup.IMPORTANT_STDIO_ONLY_ENV] = on
            self.assertTrue(
                logging_setup.importance_mode_active(), f"{on!r} should be truthy"
            )
        for off in ("0", "false", "no", "off", "", "maybe"):
            os.environ[logging_setup.IMPORTANT_STDIO_ONLY_ENV] = off
            self.assertFalse(
                logging_setup.importance_mode_active(), f"{off!r} should be falsey"
            )

    def test_env_only_seeds_default_full_log(self) -> None:
        # apply_important_stdio_env converges the env path onto the same
        # contract the flag path produces: the default full-log file is
        # seeded so Rust's full sink and Python's redirect agree.
        os.environ[logging_setup.IMPORTANT_STDIO_ONLY_ENV] = "1"
        logging_setup.apply_important_stdio_env(["--debug"])
        self.assertEqual(
            os.environ[logging_setup.FULL_LOG_FILE_ENV],
            logging_setup.DEFAULT_FULL_LOG_FILE,
        )

    def test_env_only_respects_pre_exported_full_log(self) -> None:
        os.environ[logging_setup.IMPORTANT_STDIO_ONLY_ENV] = "1"
        os.environ[logging_setup.FULL_LOG_FILE_ENV] = "/tmp/consumer-chosen.log"
        logging_setup.apply_important_stdio_env(["--debug"])
        self.assertEqual(
            os.environ[logging_setup.FULL_LOG_FILE_ENV], "/tmp/consumer-chosen.log"
        )


class EnvExportedBeforeNativeImportTests(_EnvSandbox):
    """The load-bearing ordering: the env export must win the race
    against the `_native` pymodule init, which reads the two vars ONCE.

    Drive the REAL `dynamic_runner.__init__` with a FAKE `_native` whose
    module-level body snapshots `os.environ` at the instant Python
    executes `from ._native import (...)`. The snapshot must already show
    both vars set.
    """

    def setUp(self) -> None:
        super().setUp()
        # The real package object the other tests installed (a bare stub)
        # must be evicted so `import dynamic_runner` re-executes
        # `__init__.py` for real against our fake `_native`.
        self._saved_modules = {
            name: mod
            for name, mod in list(sys.modules.items())
            if name == "dynamic_runner" or name.startswith("dynamic_runner.")
        }
        for name in self._saved_modules:
            del sys.modules[name]
        self._saved_argv = sys.argv[:]

    def tearDown(self) -> None:
        sys.argv = self._saved_argv
        for name in [
            n
            for n in list(sys.modules)
            if n == "dynamic_runner" or n.startswith("dynamic_runner.")
        ]:
            del sys.modules[name]
        sys.modules.update(self._saved_modules)
        super().tearDown()

    def test_env_set_before_native_import(self) -> None:
        captured: dict = {}

        # A fake `_native` materialised through a meta-path finder. Its
        # `exec_module` runs at exactly the moment `__init__` executes
        # `from ._native import (...)`, so the env snapshot it takes there
        # proves the export already happened.
        import importlib.abc
        import importlib.machinery

        class _Finder(importlib.abc.MetaPathFinder, importlib.abc.Loader):
            def find_spec(self, name, path, target=None):
                if name == "dynamic_runner._native":
                    return importlib.machinery.ModuleSpec(name, self)
                return None

            def create_module(self, spec):
                return None

            def exec_module(self, module):
                # This runs exactly when `__init__` imports `_native`.
                captured["important"] = os.environ.get(
                    logging_setup.IMPORTANT_STDIO_ONLY_ENV
                )
                captured["full_log"] = os.environ.get(
                    logging_setup.FULL_LOG_FILE_ENV
                )
                for name in [
                    "BinaryIdentifier",
                    "DistributedConfig",
                    "FailedTask",
                    "LocalManagerConfig",
                    "LogPathConfig",
                    "PrimaryConfig",
                    "ProcessingStats",
                    "PublishError",
                    "PyCallbackResourceMonitor",
                    "PyCallbackWorkerFactory",
                    "ResourceMap",
                    "RustDistributedManager",
                    "RustLocalGateway",
                    "RustLocalManager",
                    "RustObserverLateJoiner",
                    "RustPrimaryCoordinator",
                    "RustSecondaryCoordinator",
                    "RustSlurmJobManager",
                    "SchedulerConfig",
                    "SecondaryConfig",
                    "SlurmConfig",
                    "WorkerSpec",
                    "compute_task_hash",
                    "parse_cores",
                    "parse_memory",
                    "pick_free_port",
                    "run_distributed",
                    "run_local",
                    "run_observer_late_joiner",
                    "run_primary",
                    "run_secondary",
                ]:
                    setattr(module, name, object())

        # The real `dynamic_runner` package lives at `python/dynamic_runner`;
        # its parent `python/` must be importable so `import dynamic_runner`
        # binds the REAL `__init__.py` (not the stub the other tests use).
        package_parent = str(pathlib.Path(__file__).resolve().parents[2])
        path_added = package_parent not in sys.path
        if path_added:
            sys.path.insert(0, package_parent)

        finder = _Finder()
        sys.meta_path.insert(0, finder)
        sys.argv = ["prog", "--important-stdio-only"]
        try:
            importlib.import_module("dynamic_runner")
        finally:
            sys.meta_path.remove(finder)
            if path_added:
                sys.path.remove(package_parent)

        self.assertEqual(
            captured.get("important"),
            "1",
            "DYNRUNNER_IMPORTANT_STDIO_ONLY was NOT set before the _native "
            "import — the Rust subscriber would read the wrong value.",
        )
        self.assertEqual(
            captured.get("full_log"),
            logging_setup.DEFAULT_FULL_LOG_FILE,
            "DYNRUNNER_FULL_LOG_FILE was NOT set before the _native import.",
        )


class _RootLoggerSandbox(_EnvSandbox):
    """Save/restore the root logger's handlers + level so logging-setup
    tests do not corrupt the shared root logger for the rest of the suite.
    """

    def setUp(self) -> None:
        super().setUp()
        root = logging.getLogger()
        self._saved_handlers = root.handlers[:]
        self._saved_level = root.level
        root.handlers.clear()

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
        super().tearDown()


class PythonLoggingReconfigTests(_RootLoggerSandbox):
    def _file_handlers(self):
        return [
            h
            for h in logging.getLogger().handlers
            if isinstance(h, logging.FileHandler)
        ]

    def _bare_stream_handlers(self):
        # FileHandler subclasses StreamHandler; the console handler is a
        # bare StreamHandler. Distinguish so "console removed" is precise.
        return [
            h
            for h in logging.getLogger().handlers
            if isinstance(h, logging.StreamHandler)
            and not isinstance(h, logging.FileHandler)
        ]

    def test_flag_off_keeps_console_no_file(self) -> None:
        logging_setup.setup_logging(["--debug"])
        root = logging.getLogger()
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
            os.environ[logging_setup.FULL_LOG_FILE_ENV] = str(full_log)
            # `apply_important_stdio_env` would normally have set this at
            # package import; emulate that contract for the unit test.
            logging_setup.apply_important_stdio_env(["--important-stdio-only"])

            logging_setup.setup_logging(["--important-stdio-only"])

            self.assertFalse(
                self._bare_stream_handlers(),
                "flag on must drop the console StreamHandler so Python "
                "chatter does not reach stdio",
            )
            file_handlers = self._file_handlers()
            self.assertEqual(
                len(file_handlers),
                1,
                "flag on must add exactly one full-log FileHandler",
            )
            self.assertEqual(
                pathlib.Path(file_handlers[0].baseFilename), full_log.resolve()
            )

    def test_env_only_removes_console_adds_file(self) -> None:
        # The env-var config path (no flag in argv) must drive the SAME
        # Python-side reconfiguration as the flag: console dropped, full-log
        # FileHandler added. This is what makes the env var a first-class
        # interface rather than a half-honoured one.
        import tempfile

        with tempfile.TemporaryDirectory() as d:
            full_log = pathlib.Path(d) / "full.log"
            os.environ[logging_setup.IMPORTANT_STDIO_ONLY_ENV] = "1"
            os.environ[logging_setup.FULL_LOG_FILE_ENV] = str(full_log)

            logging_setup.setup_logging(["--debug"])

            self.assertFalse(
                self._bare_stream_handlers(),
                "env-armed importance mode must drop the console handler",
            )
            file_handlers = self._file_handlers()
            self.assertEqual(len(file_handlers), 1)
            self.assertEqual(
                pathlib.Path(file_handlers[0].baseFilename), full_log.resolve()
            )

    def test_flag_on_file_handler_uses_default_path(self) -> None:
        # No operator override → the FileHandler targets the cwd default.
        logging_setup.apply_important_stdio_env(["--important-stdio-only"])
        try:
            logging_setup.setup_logging(["--important-stdio-only"])
            file_handlers = self._file_handlers()
            self.assertEqual(len(file_handlers), 1)
            self.assertEqual(
                pathlib.Path(file_handlers[0].baseFilename),
                pathlib.Path(logging_setup.DEFAULT_FULL_LOG_FILE).resolve(),
            )
        finally:
            # The default-path FileHandler creates ./dynrunner-full.log in
            # cwd; close handlers then remove the stray file.
            for h in self._file_handlers():
                h.close()
            stray = pathlib.Path(logging_setup.DEFAULT_FULL_LOG_FILE)
            if stray.exists():
                stray.unlink()


if __name__ == "__main__":
    unittest.main()
