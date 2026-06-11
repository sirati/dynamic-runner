"""Tests for the upload bring-up milestones (#418).

The owner spec requires the ``--important-stdio-only`` stream to stop being
SILENT through the (minutes-long) image upload: it must emit when an upload
STARTS, a per-minute PROGRESS update stating how much REMAINS, and a
terminal FINISHED / SKIPPED line. This file pins each milestone fires on the
IMPORTANT primitive (``py_log_important``) at the right point with the right
content, and that the layered uploader drives the reporter per blob.

The reporter routes every milestone through the package's
``py_log_important`` attribute (the native IMPORTANT-target primitive); the
tests monkeypatch that attribute and assert on the captured lines, so they
need no real gateway and no operator stdout.

REVERT-CHECK: drop the ``reporter`` wiring from
``LayeredUploader.upload`` (or the emits from ``UploadProgressReporter``)
and the assertions below fail RED — the upload stream goes silent again.
"""

from __future__ import annotations

import time

import pytest

import dynamic_runner
from dynamic_runner.packaging import upload_milestones
from dynamic_runner.packaging.upload_milestones import UploadProgressReporter


@pytest.fixture
def captured(monkeypatch):
    """Capture every IMPORTANT milestone line the reporter emits.

    The reporter does ``from .. import py_log_important`` at emit time, so
    patching the package attribute intercepts the call regardless of the
    native module being present."""
    lines: list[str] = []

    def _record(message: str, level: str = "ERROR") -> None:
        # Milestones are routine, not errors — they must emit at INFO.
        assert level == "INFO", f"milestone emitted at non-INFO level {level!r}"
        lines.append(message)

    monkeypatch.setattr(dynamic_runner, "py_log_important", _record)
    return lines


def test_start_then_finish_emits_milestones(captured):
    reporter = UploadProgressReporter("app")
    reporter.start(total_blobs=3, total_bytes=3 * 1024 * 1024)
    reporter.blob_done(1024 * 1024)
    reporter.blob_done(1024 * 1024)
    reporter.blob_done(1024 * 1024)
    reporter.finish()

    assert any("started" in m and "app" in m and "3 blobs" in m for m in captured), captured
    assert any(
        "finished" in m and "app" in m and "3/3 blobs" in m for m in captured
    ), captured


def test_skipped_emits_single_milestone_no_finish(captured):
    reporter = UploadProgressReporter("base")
    reporter.skipped("cached")
    # A second terminal (a defensive finish in a finally) must not double-emit.
    reporter.finish()

    skip_lines = [m for m in captured if "skipped" in m]
    assert len(skip_lines) == 1, captured
    assert "base" in skip_lines[0] and "cached" in skip_lines[0]


def test_progress_emits_remaining_on_clock_cadence(captured, monkeypatch):
    # Shrink the per-minute cadence so the timer thread fires promptly in
    # the test. The reporter reads the module constant inside the loop.
    monkeypatch.setattr(upload_milestones, "PROGRESS_INTERVAL_SECONDS", 0.05)

    reporter = UploadProgressReporter("app")
    reporter.start(total_blobs=4, total_bytes=4 * 1024 * 1024)
    # Complete one blob, then idle long enough for >=1 progress tick to
    # fire WITHOUT any further blob activity (the single-large-blob case the
    # timer thread exists for — a loop-bound check would be silent here).
    reporter.blob_done(1024 * 1024)
    time.sleep(0.2)
    reporter.finish()

    progress = [m for m in captured if "progress" in m]
    assert progress, f"no per-minute progress milestone fired: {captured}"
    # The remaining figure must reflect the one completed blob: 3 MB of 4 MB
    # remain, 1/4 blobs done.
    assert any(
        "3.0 MB of 4.0 MB remaining" in m and "1/4 blobs done" in m for m in progress
    ), progress


def test_layered_uploader_drives_reporter_per_blob(captured, tmp_path):
    # Integration: the layered uploader's transfer loop must notify the
    # reporter START + one blob_done per uploaded blob + FINISHED. Reuses
    # the layered-transfer test fixtures (synthetic archive + local-dir
    # gateway) so the assertion rides the real upload loop, not a mock.
    from dynamic_runner.tests.test_layered_transfer import (
        LocalDirGateway,
        _build_synthetic_archive,
    )
    from dynamic_runner.packaging.layered_transfer import (
        LayeredUploader,
        make_bundle_from_archive,
    )

    archive = tmp_path / "image.tar"
    _build_synthetic_archive(archive, [b"layer-a" * 1000, b"layer-b" * 1000])
    gateway = LocalDirGateway(tmp_path / "remote")
    bundle, scratch = make_bundle_from_archive(archive)
    try:
        uploader = LayeredUploader(gateway, tmp_path / "cache")
        reporter = UploadProgressReporter("app")
        uploader.upload(bundle, tmp_path / "remote" / "out.tar.gz", reporter=reporter)
    finally:
        import shutil

        shutil.rmtree(scratch, ignore_errors=True)

    # config + 2 layers = 3 blobs, all cache-missing on a fresh cache.
    assert any("started" in m and "3 blobs" in m for m in captured), captured
    assert any("finished" in m and "3/3 blobs" in m for m in captured), captured


def test_layered_uploader_all_cached_emits_skipped(captured, tmp_path):
    # Second upload of the SAME bundle to a warm cache transfers nothing →
    # the layered path emits SKIPPED (the layered analogue of a whole-image
    # cache hit), not a 0-blob STARTED/FINISHED pair.
    from dynamic_runner.tests.test_layered_transfer import (
        LocalDirGateway,
        _build_synthetic_archive,
    )
    from dynamic_runner.packaging.layered_transfer import (
        LayeredUploader,
        make_bundle_from_archive,
    )

    archive = tmp_path / "image.tar"
    _build_synthetic_archive(archive, [b"layer-a" * 1000, b"layer-b" * 1000])
    gateway = LocalDirGateway(tmp_path / "remote")
    out = tmp_path / "remote" / "out.tar.gz"

    # Warm the cache with a first (reporter-less) upload.
    bundle, scratch = make_bundle_from_archive(archive)
    try:
        LayeredUploader(gateway, tmp_path / "cache").upload(bundle, out)
    finally:
        import shutil

        shutil.rmtree(scratch, ignore_errors=True)

    # Second upload: every blob already present → SKIPPED.
    bundle2, scratch2 = make_bundle_from_archive(archive)
    try:
        reporter = UploadProgressReporter("app")
        LayeredUploader(gateway, tmp_path / "cache").upload(
            bundle2, out, reporter=reporter
        )
    finally:
        import shutil

        shutil.rmtree(scratch2, ignore_errors=True)

    assert any("skipped" in m and "already cached" in m for m in captured), captured
    assert not any("started" in m for m in captured), captured
