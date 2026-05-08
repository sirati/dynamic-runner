"""Heartbeat-writer thread.

Single concern: keep an external watcher informed that the driver is
making progress.

How an outer watcher uses this
------------------------------

The driver writes the current monotonic timestamp into
``heartbeat_file`` every ``HEARTBEAT_PERIOD_S`` seconds while the
dispatch's log is showing fresh activity. An outer watcher
(coordinator-side ``Monitor`` / shell wrapper / cron) reads the
file's mtime and alerts when it goes stale beyond a threshold —
typically 5 min — meaning either the dispatch is hung or the driver
itself is hung.

The protocol is intentionally crash-safe: an old heartbeat from a
prior run looks identical to a stuck driver. Outer watchers are
expected to consult the heartbeat file's existence + mtime, not
its contents, and to compare against the current run's expected
PID (``heartbeat_path_for_pid`` returns a unique path per
invocation so concurrent runs don't share state).
"""

from __future__ import annotations

import os
import threading
import time
from pathlib import Path
from typing import Callable


HEARTBEAT_PERIOD_S = 10.0
"""How often to update the heartbeat file. The constant is exposed
so a test could override it; production callers use the default."""


def heartbeat_path_for_pid(pid: int | None = None) -> Path:
    """Default heartbeat path for the current run.

    Includes the PID so two concurrent ``run_e2e.py`` invocations
    don't trample each other. The directory is ``/tmp`` because
    the watcher contract documented in ``run_e2e.py``'s timeout
    message references that path.
    """
    return Path(f"/tmp/dynrunner-e2e-heartbeat-{pid or os.getpid()}")


class HeartbeatWriter:
    """Background thread that touches a file every N seconds.

    Holds a ``stopped`` flag (set by :meth:`stop`) and a
    ``progress_check`` callable: when the callable returns False,
    the writer pauses (it does NOT touch the file) so a stuck
    dispatch's heartbeat correctly goes stale.

    The writer thread is daemonic — if the driver crashes hard
    without calling :meth:`stop`, the thread dies with the process
    and leaves the heartbeat file in place; outer watchers see a
    stale heartbeat which is the right signal.
    """

    def __init__(
        self,
        heartbeat_file: Path,
        *,
        progress_check: Callable[[], bool] = lambda: True,
        period_s: float = HEARTBEAT_PERIOD_S,
    ) -> None:
        self._heartbeat_file = heartbeat_file
        self._progress_check = progress_check
        self._period_s = period_s
        self._stop = threading.Event()
        self._thread: threading.Thread | None = None

    def set_progress_check(self, check: Callable[[], bool]) -> None:
        """Replace the progress check callable.

        Used by callers that swap in per-plan progress tracking
        (the driver resets the tracker between scenarios so a
        long quiescent earlier plan doesn't poison the next
        plan's heartbeat).
        """
        self._progress_check = check

    def start(self) -> None:
        """Start the writer thread. Idempotent — repeated calls
        are silently ignored."""
        if self._thread is not None:
            return
        self._heartbeat_file.parent.mkdir(parents=True, exist_ok=True)
        # Initial touch so a watcher sees the heartbeat exists
        # immediately on driver start, before the first period
        # elapses.
        self._touch()
        self._thread = threading.Thread(
            target=self._run,
            name="dynrunner-e2e-heartbeat",
            daemon=True,
        )
        self._thread.start()

    def stop(self) -> None:
        """Signal the writer to exit and join briefly.

        Brief join — if the writer is mid-touch we don't want to
        block driver shutdown for more than one period.
        """
        self._stop.set()
        if self._thread is not None:
            self._thread.join(timeout=self._period_s + 1.0)
            self._thread = None

    def _run(self) -> None:
        while not self._stop.wait(self._period_s):
            try:
                if self._progress_check():
                    self._touch()
            except Exception:  # noqa: BLE001 — heartbeat MUST never crash
                # An exception in the progress check shouldn't
                # take down the whole run. Silently swallow and
                # try again next tick.
                continue

    def _touch(self) -> None:
        """Write current monotonic timestamp + wallclock to the file.

        Wallclock for human readers, monotonic for any future
        watcher that wants to detect clock skew.
        """
        now_w = time.time()
        now_m = time.monotonic()
        try:
            self._heartbeat_file.write_text(
                f"wall={now_w:.3f} mono={now_m:.3f}\n",
                encoding="utf-8",
            )
        except OSError:
            # /tmp full, immutable, etc. The watcher will see a
            # stale mtime and alert; we don't need to.
            return


__all__ = [
    "HEARTBEAT_PERIOD_S",
    "HeartbeatWriter",
    "heartbeat_path_for_pid",
]
