"""Tests for `dynamic_runner._fault_dumps` — the per-process frame-dump wiring.

Two layers:
  * pure argv-extraction (`_full_log_dir_from_argv`) — the only input the dump
    target resolves from;
  * an end-to-end SMOKE in a CHILD process: register the handler, raise
    SIGUSR1, and assert a Python traceback landed in the target file. Run in a
    subprocess so `faulthandler`'s PROCESS-GLOBAL state (and the SIGUSR1
    handler) never leaks into the unittest runner.

Loaded under a `dynamic_runner` package stub (mirroring
`test_secondary_bootstrap.py`) so it runs in a bare `nix develop` WITHOUT a
maturin build — `_fault_dumps` imports nothing from `_native`.

unittest-based (pytest is not in the dev shell), matching the rest of the suite.
"""

from __future__ import annotations

import importlib.util
import os
import pathlib
import signal
import subprocess
import sys
import textwrap
import types
import unittest


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


fault_dumps = _load_module_direct("_fault_dumps", "_fault_dumps.py")


class FullLogDirFromArgvTests(unittest.TestCase):
    """`_full_log_dir_from_argv`: extract the one flag the dump target needs."""

    def test_two_token_form(self) -> None:
        argv = ["--secondary-id", "sec-0", "--full-log-dir", "/log/sec-0", "--cores=-2"]
        self.assertEqual(fault_dumps._full_log_dir_from_argv(argv), "/log/sec-0")

    def test_equals_form(self) -> None:
        argv = ["--full-log-dir=/log/sec-0", "--cores=-2"]
        self.assertEqual(fault_dumps._full_log_dir_from_argv(argv), "/log/sec-0")

    def test_absent_returns_none(self) -> None:
        argv = ["--secondary-id", "sec-0", "--cores=-2"]
        self.assertIsNone(fault_dumps._full_log_dir_from_argv(argv))

    def test_empty_value_returns_none(self) -> None:
        # A bare trailing `--full-log-dir` (no value) and an `=`-empty form both
        # resolve to None so the caller falls back to stderr.
        self.assertIsNone(fault_dumps._full_log_dir_from_argv(["--full-log-dir"]))
        self.assertIsNone(fault_dumps._full_log_dir_from_argv(["--full-log-dir="]))
        self.assertIsNone(fault_dumps._full_log_dir_from_argv(["--full-log-dir", "  "]))


# Child program: install the handler against a temp `--full-log-dir`, raise
# SIGUSR1 at itself, then exit. The parent reads the dump file. We pass the dir
# via argv exactly as the bootstrap shim does, exercising the real
# argv→target→register path end to end.
_CHILD_PROGRAM = textwrap.dedent(
    """
    import importlib.util, os, pathlib, signal, sys

    pkg_root = pathlib.Path(sys.argv[1])
    spec = importlib.util.spec_from_file_location(
        "fault_dumps_under_test", pkg_root / "_fault_dumps.py"
    )
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)

    log_dir = sys.argv[2]
    # Resolve via the SAME argv path the bootstrap uses.
    mod.enable_fault_dumps(["--full-log-dir", log_dir])

    # On-demand all-thread dump: deliver SIGUSR1 to ourselves. faulthandler's
    # handler writes a traceback synchronously and (chain=False) returns control
    # WITHOUT terminating — so we keep running and exit 0. A non-zero / killed
    # exit would mean the handler chained to SIGUSR1's default (terminate),
    # which is exactly the "no process exit" contract this guards.
    os.kill(os.getpid(), signal.SIGUSR1)
    sys.exit(0)
    """
)


class WriteCrashTracebackTests(unittest.TestCase):
    """`write_crash_traceback`: durable last-gasp record of the in-flight
    exception under `<full-log-dir>/bootstrap-crash.log`, strictly
    best-effort (never raises, never masks, no-op on clean exits)."""

    def _crash_file(self, log_dir: str) -> pathlib.Path:
        return pathlib.Path(log_dir) / fault_dumps._CRASH_BASENAME

    def _exit_file(self, log_dir: str) -> pathlib.Path:
        return pathlib.Path(log_dir) / fault_dumps._EXIT_BASENAME

    def test_writes_traceback_of_inflight_exception(self) -> None:
        import tempfile

        with tempfile.TemporaryDirectory() as tmp:
            try:
                raise RuntimeError("boom-bootstrap-crash")
            except RuntimeError:
                fault_dumps.write_crash_traceback(["--full-log-dir", tmp])
            body = self._crash_file(tmp).read_text()
            self.assertIn("==== bootstrap crash at ", body)
            self.assertIn("Traceback (most recent call last):", body)
            self.assertIn("RuntimeError: boom-bootstrap-crash", body)

    def test_appends_across_crashes(self) -> None:
        import tempfile

        with tempfile.TemporaryDirectory() as tmp:
            for marker in ("first-crash", "second-crash"):
                try:
                    raise RuntimeError(marker)
                except RuntimeError:
                    fault_dumps.write_crash_traceback(["--full-log-dir", tmp])
            body = self._crash_file(tmp).read_text()
            self.assertIn("first-crash", body)
            self.assertIn("second-crash", body)

    def test_noop_without_inflight_exception(self) -> None:
        import tempfile

        with tempfile.TemporaryDirectory() as tmp:
            fault_dumps.write_crash_traceback(["--full-log-dir", tmp])
            self.assertFalse(self._crash_file(tmp).exists())

    def test_noop_on_clean_systemexit(self) -> None:
        # `sys.exit(0)` / bare `sys.exit()` escaping the consumer is a
        # normal shutdown, not a crash — no file.
        import tempfile

        with tempfile.TemporaryDirectory() as tmp:
            for code in (0, None):
                try:
                    raise SystemExit(code)
                except SystemExit:
                    fault_dumps.write_crash_traceback(["--full-log-dir", tmp])
            self.assertFalse(self._crash_file(tmp).exists())
            # A clean exit is not even trace-worthy: no exit log either.
            self.assertFalse(self._exit_file(tmp).exists())

    def test_systemexit_any_code_is_never_crash_dumped(self) -> None:
        # `SystemExit` is by definition a deliberate interpreter exit
        # (raise-by-design) — regardless of code, it must NEVER land in
        # bootstrap-crash.log. The asm-dataset 2212c136 fire-drill: a
        # secondary's deliberate `sys.exit(1)` after the primary's
        # RunAborted was filed as a "bootstrap crash", sending the
        # operator hunting a phantom.
        import tempfile

        with tempfile.TemporaryDirectory() as tmp:
            for code in (1, 2, "secondary exiting: run aborted by primary"):
                try:
                    raise SystemExit(code)
                except SystemExit:
                    fault_dumps.write_crash_traceback(["--full-log-dir", tmp])
            self.assertFalse(self._crash_file(tmp).exists())

    def test_keyboardinterrupt_is_never_crash_dumped(self) -> None:
        # Operator-initiated interrupt: deliberate, not a crash.
        import tempfile

        with tempfile.TemporaryDirectory() as tmp:
            try:
                raise KeyboardInterrupt()
            except KeyboardInterrupt:
                fault_dumps.write_crash_traceback(["--full-log-dir", tmp])
            self.assertFalse(self._crash_file(tmp).exists())

    def test_nonzero_systemexit_leaves_one_line_exit_trace(self) -> None:
        # A deliberate NON-ZERO exit still leaves a durable one-line trace
        # (in `bootstrap-exit.log`, never the crash log) so the operator can
        # correlate the container's exit code without a phantom crash hunt.
        import tempfile

        with tempfile.TemporaryDirectory() as tmp:
            try:
                raise SystemExit(1)
            except SystemExit:
                fault_dumps.write_crash_traceback(["--full-log-dir", tmp])
            body = self._exit_file(tmp).read_text()
            self.assertIn("bootstrap exited deliberately rc=1", body)
            self.assertNotIn("Traceback", body)

    def test_never_raises_without_log_dir_or_with_bad_dir(self) -> None:
        # No `--full-log-dir` → silent no-op; an uncreatable dir → swallowed.
        # Either failure mode raising here would MASK the original error.
        try:
            raise RuntimeError("boom")
        except RuntimeError:
            fault_dumps.write_crash_traceback([])
            fault_dumps.write_crash_traceback(
                ["--full-log-dir", "/proc/definitely-not-writable/x"]
            )


@unittest.skipUnless(
    hasattr(signal, "SIGUSR1"), "SIGUSR1 not available on this platform"
)
class FaultDumpSmokeTests(unittest.TestCase):
    """SIGUSR1 with the handler registered writes a traceback to the target."""

    def test_sigusr1_writes_traceback_to_full_log_dir(self) -> None:
        import tempfile

        with tempfile.TemporaryDirectory() as tmp:
            log_dir = os.path.join(tmp, "sec-0")
            os.makedirs(log_dir, exist_ok=True)
            proc = subprocess.run(
                [sys.executable, "-c", _CHILD_PROGRAM, str(_PACKAGE_ROOT), log_dir],
                capture_output=True,
                text=True,
                timeout=30,
            )
            self.assertEqual(
                proc.returncode,
                0,
                f"child exited non-zero; stderr:\n{proc.stderr}",
            )
            dump_path = pathlib.Path(log_dir) / fault_dumps._DUMP_BASENAME
            self.assertTrue(
                dump_path.exists(),
                f"faulthandler dump file not created at {dump_path}",
            )
            body = dump_path.read_text()
            # faulthandler's on-demand dump emits a "Current thread" header and
            # a `File "...", line N in <fn>` frame line.
            self.assertIn("Current thread", body)
            self.assertIn('File "', body)


# Worker-shaped child program (#365): register the handler against an
# EXPLICIT per-worker dump file (the `dump_path` keyword the worker
# runtime derives from its `--log-file`), raise SIGUSR1 at itself,
# keep running (no exit), then exit 0. Exercises the
# explicit-target→register path the worker subprocess entry uses.
_WORKER_CHILD_PROGRAM = textwrap.dedent(
    """
    import importlib.util, os, pathlib, signal, sys

    pkg_root = pathlib.Path(sys.argv[1])
    spec = importlib.util.spec_from_file_location(
        "fault_dumps_under_test", pkg_root / "_fault_dumps.py"
    )
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)

    dump_path = sys.argv[2]
    # The worker runtime's path: explicit per-worker dump file, no argv
    # resolution.
    mod.enable_fault_dumps(dump_path=dump_path)

    os.kill(os.getpid(), signal.SIGUSR1)
    sys.exit(0)
    """
)


@unittest.skipUnless(
    hasattr(signal, "SIGUSR1"), "SIGUSR1 not available on this platform"
)
class WorkerFaultDumpSmokeTests(unittest.TestCase):
    """#365: the worker-subprocess shape — an explicit per-worker
    `worker_<id>-faulthandler.log` dump target — receives the SIGUSR1
    all-thread dump, and the process keeps running (no exit)."""

    def test_sigusr1_writes_traceback_to_explicit_worker_dump_path(self) -> None:
        import tempfile

        with tempfile.TemporaryDirectory() as tmp:
            dump_path = pathlib.Path(tmp) / "logs" / "worker_3-faulthandler.log"
            proc = subprocess.run(
                [
                    sys.executable,
                    "-c",
                    _WORKER_CHILD_PROGRAM,
                    str(_PACKAGE_ROOT),
                    str(dump_path),
                ],
                capture_output=True,
                text=True,
                timeout=30,
            )
            self.assertEqual(
                proc.returncode,
                0,
                f"child exited non-zero (USR1 must dump WITHOUT exiting); "
                f"stderr:\n{proc.stderr}",
            )
            self.assertTrue(
                dump_path.exists(),
                f"per-worker faulthandler dump not created at {dump_path}",
            )
            body = dump_path.read_text()
            self.assertIn("Current thread", body)
            self.assertIn('File "', body)

    def test_runtime_derives_worker_dump_sibling(self) -> None:
        """The worker runtime derives `<stem>-faulthandler.log` from
        its `--log-file` — the documented worker_N-faulthandler shape
        (`worker/runtime._derive_fault_dump_path`). Skipped in a bare
        shell: importing the runtime module requires the maturin-built
        `dynamic_runner._native`.
        """
        try:
            from dynamic_runner.worker.runtime import _derive_fault_dump_path
        except Exception:
            self.skipTest("dynamic_runner._native not built in this environment")
        self.assertEqual(
            _derive_fault_dump_path("/log/dir/worker_3.log"),
            "/log/dir/worker_3-faulthandler.log",
        )


if __name__ == "__main__":
    unittest.main()
