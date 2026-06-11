"""Bring-up milestone observability for the image/source upload.

Single concern: surface the upload's bring-up milestones on the
``--important-stdio-only`` operator stream — START, a per-minute PROGRESS
update stating how much REMAINS, and the terminal FINISHED / SKIPPED
lines. Under importance mode the operator stdio is otherwise SILENT from
dispatch start through the (minutes-long) image upload; these milestones
replace that silence with a small number of information-dense lines so an
operator (or an LLM woken on the stream) can tell "uploading normally"
from "wedged".

## Module boundary

The upload loop (:class:`dynamic_runner.packaging.layered_transfer.LayeredUploader`
and the whole-tarball fallback in ``podman.py``) is the *single owner of
the transfer*; this module is the *single owner of the milestone
observability*. The boundary between them is the
:class:`UploadProgressReporter` API:

    reporter = UploadProgressReporter(label)
    reporter.start(total_blobs, total_bytes)   # arms the timer, emits START
    ...
    reporter.blob_done(blob_bytes)             # per-blob, thread-safe counter bump
    ...
    reporter.finish()                          # stops the timer, emits FINISHED

    # or, when nothing is transferred:
    reporter.skipped("cached")                 # emits SKIPPED, no timer

The upload code calls those four methods and knows NOTHING about the
IMPORTANT target, the 60s cadence, the remaining-bytes phrasing, or the
timer thread — all of which live here. Adding/altering a milestone never
touches the transfer loop.

## Why a timer thread (not a loop-bound check)

A single blob can be a multi-hundred-MB / multi-GB layer
(``layered_transfer`` documents ~3 GB across 80+ layers; individual
layers are realistically large). The transfer of ONE blob
(``gateway.transfer_file``) can therefore run for many minutes with no
return to the loop. A loop-bound "emit if a minute elapsed between blobs"
check would go SILENT for the whole duration of a single large transfer —
exactly the wedged-vs-uploading ambiguity the milestone exists to remove.
So the per-minute PROGRESS update is driven by a small daemon timer thread
that reads the shared (lock-guarded) ``bytes_done`` / ``blobs_done``
counters the transfer loop bumps, and wakes on a CLOCK-ANCHORED cadence
(``next_mark += 60`` from the start instant, never reset by blob activity)
so the cadence does not drift with transfer timing. The thread stops at
:meth:`finish`.

## Importance mechanism

Every milestone is emitted through the native ``py_log_important(message,
level="INFO")`` primitive — the same IMPORTANT-target channel the
fatal-surfacing path uses (``logging_setup.surface_fatal_errors``), at INFO
level so a routine milestone is not recorded as an error. The native
dual-sink routes the IMPORTANT target to operator stdio under
``--important-stdio-only`` and keeps it in the full log; the milestones ride
the native 500ms-quiet / 5s-max-delay debounce automatically, so no flush
logic is needed here. The per-blob DEBUG transfer logs in the upload loop
are untouched — the milestones replace the *silence*, not those logs.
"""

from __future__ import annotations

import threading
from dataclasses import dataclass

#: Per-minute PROGRESS cadence, in seconds. Clock-anchored from the START
#: instant (see the timer-thread rationale in the module docstring).
PROGRESS_INTERVAL_SECONDS = 60.0


def _human(n: int) -> str:
    """Render a byte count in the closest power-of-1024 unit.

    Local copy rather than reaching into ``layered_transfer._human`` /
    ``podman._human_bytes``: the milestone phrasing is THIS module's
    concern, so it owns its own rendering and does not couple to either
    transfer module's private helper. The output shape matches the
    operator-facing convention (``1234.5 MB``).
    """
    units = ("B", "KB", "MB", "GB", "TB")
    f = float(n)
    for u in units:
        if f < 1024.0 or u == units[-1]:
            return f"{f:.1f} {u}" if u != "B" else f"{int(f)} B"
        f /= 1024.0
    return f"{f:.1f} {units[-1]}"


def _emit(message: str) -> None:
    """Emit one milestone line on the IMPORTANT target at INFO.

    Local import of the native primitive: it lives on the package's
    re-exported surface (same rationale as ``logging_setup``'s
    ``py_log_important`` import — importing ``_native`` at module top
    would pull it into modules that import this one in isolation, e.g. the
    package-stub test harness, which substitutes the package's
    ``py_log_important`` attribute). Routing this through the package
    attribute is also what lets a test capture the milestone without the
    compiled extension.
    """
    from .. import py_log_important

    py_log_important(message, level="INFO")


@dataclass
class _Progress:
    """Lock-guarded shared progress the timer thread reads and the upload
    loop bumps. A single mutex covers both counters so the per-minute
    snapshot is internally consistent."""

    total_blobs: int
    total_bytes: int
    blobs_done: int = 0
    bytes_done: int = 0


class UploadProgressReporter:
    """Owns the upload bring-up milestones for one labelled upload.

    Construct one per logical upload (per image label). Call exactly one
    of :meth:`start` (a real transfer is about to run) or :meth:`skipped`
    (nothing will be transferred); for a started upload, call
    :meth:`blob_done` per completed blob and :meth:`finish` once at the
    end. Re-entrant calls are guarded so a double-``finish`` (e.g. a
    ``finally`` that also fires) is a no-op.
    """

    def __init__(self, label: str) -> None:
        self._label = label
        self._progress: _Progress | None = None
        self._lock = threading.Lock()
        self._stop = threading.Event()
        self._timer: threading.Thread | None = None
        self._finished = False

    # ── Milestone entry points (the upload loop's API surface) ──────────

    def start(self, total_blobs: int, total_bytes: int) -> None:
        """Emit the START milestone and arm the per-minute PROGRESS timer.

        ``total_blobs`` / ``total_bytes`` are the amount that will actually
        go on the wire (the cache-miss set), known up front. A zero total
        means there is nothing to transfer — callers should prefer
        :meth:`skipped` for that case, but ``start(0, 0)`` is tolerated and
        simply does not arm the timer.
        """
        self._progress = _Progress(total_blobs=total_blobs, total_bytes=total_bytes)
        _emit(
            f"image upload started [{self._label}]: "
            f"{total_blobs} blobs / {_human(total_bytes)} to transfer"
        )
        if total_blobs <= 0 and total_bytes <= 0:
            return
        self._timer = threading.Thread(
            target=self._progress_loop,
            name=f"upload-progress-{self._label}",
            daemon=True,
        )
        self._timer.start()

    def blob_done(self, blob_bytes: int) -> None:
        """Record one completed blob transfer. Thread-safe — the timer
        thread reads the same counters under the same lock."""
        if self._progress is None:
            return
        with self._lock:
            self._progress.blobs_done += 1
            self._progress.bytes_done += blob_bytes

    def finish(self) -> None:
        """Stop the PROGRESS timer and emit the FINISHED milestone.

        Idempotent: a second call (e.g. from a ``finally`` after an
        explicit call) is a no-op."""
        if self._finished:
            return
        self._finished = True
        self._stop.set()
        if self._timer is not None:
            # Bounded join: the loop wakes on the stop event, so this
            # returns promptly; the timeout is a defensive backstop only.
            self._timer.join(timeout=5.0)
        prog = self._progress
        if prog is None:
            # `finish` without a prior `start` — emit a minimal terminal so
            # the stream still closes the upload phase.
            _emit(f"image upload finished [{self._label}]")
            return
        with self._lock:
            blobs = prog.blobs_done
            sent = prog.bytes_done
        _emit(
            f"image upload finished [{self._label}]: "
            f"{blobs}/{prog.total_blobs} blobs, {_human(sent)} transferred"
        )

    def skipped(self, reason: str) -> None:
        """Emit the SKIPPED milestone — nothing went on the wire (a cache
        hit, or every blob already present). No timer is involved.

        Idempotent with :meth:`finish`: whichever terminal fires first
        marks the reporter done so the other is a no-op."""
        if self._finished:
            return
        self._finished = True
        _emit(f"image upload skipped [{self._label}]: {reason}")

    # ── Internal: the per-minute PROGRESS timer thread ──────────────────

    def _progress_loop(self) -> None:
        """Wake on a CLOCK-ANCHORED 60s cadence and emit how much REMAINS,
        until :meth:`finish` sets the stop event.

        The next-mark instant advances by a fixed ``PROGRESS_INTERVAL_SECONDS``
        from the previous mark (``Event.wait`` is given the remaining time to
        the next mark), so the cadence is anchored to wall-clock and does not
        drift with transfer timing — a slow blob does not push the next
        update later than its scheduled minute. The thread reads the shared
        counters under the lock for an internally consistent snapshot.
        """
        import time

        prog = self._progress
        assert prog is not None  # `start` set it before spawning the thread
        next_mark = time.monotonic() + PROGRESS_INTERVAL_SECONDS
        while not self._stop.is_set():
            now = time.monotonic()
            wait_for = next_mark - now
            if wait_for > 0 and self._stop.wait(timeout=wait_for):
                # Stop fired while waiting → finish() owns the terminal emit.
                return
            if self._stop.is_set():
                return
            with self._lock:
                blobs_done = prog.blobs_done
                bytes_done = prog.bytes_done
            remaining_bytes = max(prog.total_bytes - bytes_done, 0)
            _emit(
                f"image upload progress [{self._label}]: "
                f"{_human(remaining_bytes)} of {_human(prog.total_bytes)} remaining, "
                f"{blobs_done}/{prog.total_blobs} blobs done"
            )
            # Clock-anchor the next mark; if we fell behind by more than one
            # interval (a very slow snapshot), skip the missed marks so we
            # do not burst-emit to catch up.
            next_mark += PROGRESS_INTERVAL_SECONDS
            while next_mark <= time.monotonic():
                next_mark += PROGRESS_INTERVAL_SECONDS
