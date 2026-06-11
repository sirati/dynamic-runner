"""Worker-subprocess fault-dump integration tests (#365 parity).

Spawns a REAL worker subprocess through the genuine
``dynamic_runner.worker.run`` entry (via
``tests._faultdump_stub_worker``), speaks the manager side of the
manager-worker protocol over a named Unix socket, and asserts the
registration parity the secondary bootstrap already has:

* ``SIGUSR1`` mid-task → the worker SURVIVES, an all-thread traceback
  dump lands at the per-worker dump target, and the in-flight task
  still completes (DoneResponse) — both WITH ``--log-file`` (dump file
  is the ``worker_<id>-faulthandler.log`` sibling) and WITHOUT it
  (dump falls back to stderr, which the production spawner captures
  into the per-worker log).
* a fatal signal in the handler (``os.abort()``) → the dump target
  carries the fatal-error traceback even though the process dies.

Requires the maturin-built ``dynamic_runner._native`` (the wire codec
is native); the whole module skips cleanly in a bare shell, mirroring
the native-gated cases in ``test_fault_dumps.py``.

unittest-based (pytest is not in the dev shell), matching the suite.
"""

from __future__ import annotations

import json
import os
import pathlib
import signal
import subprocess
import sys
import tempfile
import time
import unittest

try:
    from dynamic_runner.comm import (
        DoneResponse,
        NamedSocketInterface,
        ProcessBinaryCommand,
        ReadyResponse,
        StopCommand,
    )

    _NATIVE_AVAILABLE = True
    _NATIVE_SKIP_REASON = ""
except Exception as _exc:  # pragma: no cover - bare-shell fallback
    _NATIVE_AVAILABLE = False
    _NATIVE_SKIP_REASON = f"dynamic_runner._native not importable: {_exc}"

_WORKER_MODULE = "dynamic_runner.tests._faultdump_stub_worker"
_DEADLINE = 30.0
_POLL = 0.02


def _wait_until(predicate, deadline: float, what: str):
    """Poll ``predicate`` until it returns a truthy value or ``deadline``
    (seconds) elapses. Returns the truthy value; fails the test via
    ``AssertionError`` on timeout."""
    end = time.monotonic() + deadline
    while time.monotonic() < end:
        value = predicate()
        if value:
            return value
        time.sleep(_POLL)
    raise AssertionError(f"timed out after {deadline}s waiting for {what}")


class _ManagerHarness:
    """Manager side of the worker protocol for ONE spawned worker.

    Owns the named-socket server, the worker subprocess, and the
    response stream. ``close()`` is unconditional (kill + reap) so a
    failing assertion never leaks a worker process.
    """

    def __init__(self, tmp: pathlib.Path, *, log_file: pathlib.Path | None):
        self.tmp = tmp
        self.log_file = log_file
        self.socket_path = tmp / "worker.sock"
        self.comm = NamedSocketInterface(self.socket_path, is_server=True)
        argv = [
            sys.executable,
            "-m",
            _WORKER_MODULE,
            "--socket-path",
            str(self.socket_path),
        ]
        if log_file is not None:
            argv += ["--log-file", str(log_file)]
        # stderr=PIPE plays the role of the production spawner's
        # stdio-capture (which appends OS-level stderr to the
        # per-worker log file): the no-log-file dump target.
        self.proc = subprocess.Popen(
            argv,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
        )
        self.responses: list = []

    # ── protocol ────────────────────────────────────────────────────
    def _drain(self) -> None:
        self.responses.extend(self.comm.receive_responses())

    def wait_for_response(self, kind) -> object:
        def _find():
            self._drain()
            for r in self.responses:
                if isinstance(r, kind):
                    return r
            if self.proc.poll() is not None:
                raise AssertionError(
                    f"worker exited (rc={self.proc.returncode}) before "
                    f"sending {kind.__name__}; stderr:\n{self._stderr_so_far()}"
                )
            return None

        return _wait_until(_find, _DEADLINE, kind.__name__)

    def wait_ready(self) -> None:
        _wait_until(self.comm.accept_connection, _DEADLINE, "worker connect")
        self.wait_for_response(ReadyResponse)

    def send_task(self, payload: dict) -> None:
        ok, err = self.comm.send_command(
            ProcessBinaryCommand(
                relative_path="fault-dump-task", payload=json.dumps(payload)
            )
        )
        assert ok, f"send_command failed: {err}"

    def stop(self) -> None:
        self.comm.send_command(StopCommand())

    # ── process ─────────────────────────────────────────────────────
    def _stderr_so_far(self) -> str:
        # Only safe once the process exited (communicate would block).
        if self.proc.poll() is None:
            return "<worker still running>"
        return self.proc.communicate()[1] or ""

    def wait_exit(self) -> tuple[int, str]:
        out, err = self.proc.communicate(timeout=_DEADLINE)
        return self.proc.returncode, err or ""

    def close(self) -> None:
        if self.proc.poll() is None:
            self.proc.kill()
        try:
            self.proc.communicate(timeout=5)
        except Exception:
            pass
        self.comm.close()


@unittest.skipUnless(_NATIVE_AVAILABLE, _NATIVE_SKIP_REASON)
@unittest.skipUnless(
    hasattr(signal, "SIGUSR1"), "SIGUSR1 not available on this platform"
)
class WorkerSigusr1SubprocessTests(unittest.TestCase):
    """An operator ``kill -USR1 <worker>`` must DUMP, not TERMINATE,
    and the in-flight task must complete normally."""

    def _run_usr1_scenario(self, *, with_log_file: bool) -> tuple[str, str]:
        """Drive one worker through: ready → task in flight → SIGUSR1 →
        task completes → stop → clean exit. Returns
        ``(dump_text, stderr_text)`` where ``dump_text`` is the
        per-worker dump file's content ('' when no ``--log-file``)."""
        with tempfile.TemporaryDirectory() as tmp_str:
            tmp = pathlib.Path(tmp_str)
            log_file = tmp / "logs" / "worker_0.log" if with_log_file else None
            if log_file is not None:
                log_file.parent.mkdir(parents=True)
            harness = _ManagerHarness(tmp, log_file=log_file)
            try:
                harness.wait_ready()
                marker = tmp / "task-started"
                barrier = tmp / "task-may-finish"
                harness.send_task(
                    {"marker": str(marker), "barrier": str(barrier)}
                )
                _wait_until(marker.exists, _DEADLINE, "task in flight")

                os.kill(harness.proc.pid, signal.SIGUSR1)

                barrier.write_text("go\n")
                harness.wait_for_response(DoneResponse)
                harness.stop()
                rc, stderr = harness.wait_exit()
                self.assertEqual(
                    rc,
                    0,
                    f"worker must survive SIGUSR1 and exit cleanly on Stop "
                    f"(rc={rc}); stderr:\n{stderr}",
                )
                dump_text = ""
                if log_file is not None:
                    dump_file = log_file.with_name("worker_0-faulthandler.log")
                    self.assertTrue(
                        dump_file.exists(),
                        f"per-worker dump file missing at {dump_file}",
                    )
                    dump_text = dump_file.read_text()
                return dump_text, stderr
            finally:
                harness.close()

    def test_usr1_with_log_file_dumps_to_sibling_and_task_completes(self) -> None:
        dump_text, _ = self._run_usr1_scenario(with_log_file=True)
        # faulthandler's on-demand dump: "Current thread" header + frame lines.
        self.assertIn("Current thread", dump_text)
        self.assertIn('File "', dump_text)
        self.assertIn("_faultdump_stub_worker", dump_text)

    def test_usr1_without_log_file_dumps_to_stderr_and_task_completes(self) -> None:
        # No --log-file: registration must still hold (stderr fallback —
        # the production spawner captures OS-stderr into the worker log).
        # Pre-parity behaviour: no handler at all → SIGUSR1's default
        # disposition TERMINATES the worker mid-task.
        _, stderr = self._run_usr1_scenario(with_log_file=False)
        self.assertIn("Current thread", stderr)
        self.assertIn("_faultdump_stub_worker", stderr)


@unittest.skipUnless(_NATIVE_AVAILABLE, _NATIVE_SKIP_REASON)
@unittest.skipUnless(
    os.name == "posix", "fatal-signal dump test relies on POSIX signal exits"
)
class WorkerFatalSignalSubprocessTests(unittest.TestCase):
    """A worker dying on a fatal signal must leave a traceback at the
    per-worker dump target (faulthandler.enable parity)."""

    def test_abort_in_handler_leaves_fatal_dump(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_str:
            tmp = pathlib.Path(tmp_str)
            log_file = tmp / "logs" / "worker_0.log"
            log_file.parent.mkdir(parents=True)
            harness = _ManagerHarness(tmp, log_file=log_file)
            try:
                harness.wait_ready()
                harness.send_task({"abort": True})
                rc, _ = harness.wait_exit()
                self.assertEqual(
                    rc,
                    -signal.SIGABRT,
                    f"worker should die on SIGABRT, got rc={rc}",
                )
                dump_file = log_file.with_name("worker_0-faulthandler.log")
                self.assertTrue(
                    dump_file.exists(),
                    f"fatal-signal dump missing at {dump_file}",
                )
                body = dump_file.read_text()
                self.assertIn("Fatal Python error: Aborted", body)
                self.assertIn("_faultdump_stub_worker", body)
            finally:
                harness.close()


if __name__ == "__main__":
    unittest.main()
