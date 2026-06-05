"""Tests for the compact per-worker log format
(``dynamic_runner.worker.logging_setup``).

The worker analogue of the manager-side per-role files: the line shape is
``{h:mm:ss local} {LEVEL} W-<worker_id>  {message}`` — no date, no
microseconds, no target. The worker id is derived from the framework's
``worker_<id>.log`` ``--log-file`` path (the only stable worker identity).

The module is loaded by file path so the test does not pull the package
``__init__`` (and its compiled ``_native`` dependency) — the formatter is
pure stdlib.
"""
from __future__ import annotations

import importlib.util
import io
import logging
import re
import tempfile
import unittest
from pathlib import Path

_MODULE_PATH = Path(__file__).resolve().parents[1] / "worker" / "logging_setup.py"
_spec = importlib.util.spec_from_file_location(
    "dynamic_runner_worker_logging_setup_under_test", _MODULE_PATH
)
assert _spec is not None and _spec.loader is not None
logging_setup = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(logging_setup)


class WorkerIdDerivationTests(unittest.TestCase):
    def test_parses_id_from_framework_pattern(self) -> None:
        self.assertEqual(
            logging_setup.worker_id_from_log_file("/app/log/worker_3.log"), "3"
        )

    def test_parses_non_numeric_id(self) -> None:
        self.assertEqual(
            logging_setup.worker_id_from_log_file("/x/worker_sec-0-w2.log"),
            "sec-0-w2",
        )

    def test_unknown_when_absent_or_non_matching(self) -> None:
        self.assertEqual(logging_setup.worker_id_from_log_file(None), "?")
        self.assertEqual(logging_setup.worker_id_from_log_file(""), "?")
        self.assertEqual(logging_setup.worker_id_from_log_file("/x/custom.txt"), "?")


class SetupWorkerLoggingTests(unittest.TestCase):
    def setUp(self) -> None:
        self._root = logging.getLogger()
        self._saved_handlers = list(self._root.handlers)
        self._saved_level = self._root.level
        for handler in list(self._root.handlers):
            self._root.removeHandler(handler)

    def tearDown(self) -> None:
        for handler in list(self._root.handlers):
            self._root.removeHandler(handler)
            handler.close()
        for handler in self._saved_handlers:
            self._root.addHandler(handler)
        self._root.setLevel(self._saved_level)

    def test_emits_compact_local_line_with_worker_prefix(self) -> None:
        with tempfile.TemporaryDirectory() as d:
            log_file = str(Path(d) / "worker_7.log")
            logging_setup.setup_worker_logging(log_file)
            logging.getLogger("dynamic_runner.worker.runtime").info("task dispatched")
            for handler in self._root.handlers:
                handler.flush()
            line = Path(log_file).read_text().strip()

        # `{h:mm:ss} {LEVEL} W-<id>  {message}` — local HH:MM:SS, no date /
        # micros / `T` / `Z` / target, two spaces before the message.
        self.assertRegex(line, r"^\d{2}:\d{2}:\d{2} INFO W-7  task dispatched$")
        self.assertNotIn("T", line.split()[0])
        self.assertNotIn("Z", line.split()[0])
        self.assertNotIn("dynamic_runner.worker.runtime", line)

    def test_noop_when_consumer_already_configured_logging(self) -> None:
        # `on_args` runs first and wins: a pre-existing root handler means the
        # consumer owns logging, so the framework default must not fight it.
        pre = logging.StreamHandler(io.StringIO())
        self._root.addHandler(pre)
        with tempfile.TemporaryDirectory() as d:
            log_file = str(Path(d) / "worker_8.log")
            logging_setup.setup_worker_logging(log_file)
            self.assertFalse(
                Path(log_file).exists(),
                "framework installed a handler despite a consumer's own setup",
            )
            self.assertEqual(self._root.handlers, [pre])

    def test_noop_without_log_file(self) -> None:
        logging_setup.setup_worker_logging(None)
        self.assertEqual(self._root.handlers, [])


if __name__ == "__main__":
    unittest.main()
