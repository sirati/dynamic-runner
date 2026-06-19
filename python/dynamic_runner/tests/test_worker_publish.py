"""Tests for the Python-side worker publish API.

Covers the contract documented in ``dynamic_runner.worker.publish``:

* env-driven src_root/dst_root resolution with the slurm-wrapper
  defaults
* explicit ``(src, dst)`` delivery — there is NO implicit
  src_root→dst_root mirroring (removed footgun)
* ``publish_all(pairs)`` set-atomic batch over explicit pairs
* ``Task.publish`` / ``Task.publish_all`` method delegation
* ``PublishError`` raised on src-outside-root / missing-src
* ``publish(src)`` (dst omitted) is a hard error

Tests run unbound from the runtime loop — ``Task`` is constructed
directly, exercising the same path that consumer unit tests follow.
"""
from __future__ import annotations

import os
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import importlib

# `dynamic_runner.worker.__init__` re-exports the `publish` *function*,
# which shadows the `publish` *submodule* attribute on the package — so
# `import dynamic_runner.worker.publish as x` would bind the function.
# Resolve the module object explicitly to patch its module-level
# `_native_*` bindings.
publish_mod = importlib.import_module("dynamic_runner.worker.publish")
from dynamic_runner.worker import (
    PublishError,
    Task,
    publish,
    publish_all,
)
from dynamic_runner.worker.publish import (
    DEFAULT_DST_ROOT,
    DEFAULT_SRC_ROOT,
    ENV_DST_ROOT,
    ENV_SRC_ROOT,
    sweep_stale_tmps,
)


class _PublishFixture(unittest.TestCase):
    """Sets up a temp src_root / dst_root pair pinned via env, with
    a per-test cleanup that restores any pre-existing env values.
    """

    def setUp(self) -> None:
        self._src_dir = tempfile.TemporaryDirectory()
        self._dst_dir = tempfile.TemporaryDirectory()
        self.src_root = Path(self._src_dir.name)
        self.dst_root = Path(self._dst_dir.name)
        self._prev_src = os.environ.get(ENV_SRC_ROOT)
        self._prev_dst = os.environ.get(ENV_DST_ROOT)
        os.environ[ENV_SRC_ROOT] = str(self.src_root)
        os.environ[ENV_DST_ROOT] = str(self.dst_root)

    def tearDown(self) -> None:
        for key, prev in (
            (ENV_SRC_ROOT, self._prev_src),
            (ENV_DST_ROOT, self._prev_dst),
        ):
            if prev is None:
                os.environ.pop(key, None)
            else:
                os.environ[key] = prev
        self._src_dir.cleanup()
        self._dst_dir.cleanup()

    def _stage(self, rel: str, content: str = "x") -> Path:
        p = self.src_root / rel
        p.parent.mkdir(parents=True, exist_ok=True)
        p.write_text(content)
        return p


class PublishContractTests(_PublishFixture):
    def test_default_constants_match_slurm_wrapper(self):
        # The slurm wrapper hard-codes /app/out-tmp and /app/out-network.
        # If those constants drift, the framework default no longer
        # matches the deployment shape; flag here, not at runtime.
        self.assertEqual(DEFAULT_SRC_ROOT, "/app/out-tmp")
        self.assertEqual(DEFAULT_DST_ROOT, "/app/out-network")

    def test_publish_delivers_to_explicit_dst(self):
        src = self._stage("a/b/c.txt", "hello")
        dst = self.dst_root / "a/b/c.txt"
        publish(src, dst)
        self.assertEqual(dst.read_text(), "hello")
        self.assertFalse(src.exists())

    def test_publish_creates_dst_parent_dirs(self):
        src = self._stage("deep/nested/path/file", "x")
        dst = self.dst_root / "deep/nested/path/file"
        publish(src, dst)
        self.assertTrue(dst.exists())

    def test_publish_explicit_dst_can_differ_from_src_layout(self):
        # The scratch layout need not mirror the publish layout: a
        # src under a scratch subdir lands at its EXPLICIT dst, never
        # a mirrored `dst_root / (src - src_root)`.
        src = self._stage("scratch/work/a.json", "v")
        explicit_dst = self.dst_root / "renamed.json"
        publish(src, explicit_dst)
        self.assertEqual(explicit_dst.read_text(), "v")
        # No mirrored destination was created.
        self.assertFalse((self.dst_root / "scratch/work/a.json").exists())

    def test_publish_overwrites_existing_dst(self):
        src = self._stage("k", "new")
        dst = self.dst_root / "k"
        dst.write_text("old")
        publish(src, dst)
        self.assertEqual(dst.read_text(), "new")

    def test_publish_requires_dst_no_implicit_mirror(self):
        # The auto-mirror footgun is GONE: omitting `dst` is a hard
        # TypeError (required positional), not a silent mirror.
        src = self._stage("x.txt", "v")
        with self.assertRaises(TypeError):
            publish(src)  # type: ignore[call-arg]

    def test_publish_all_lands_both_pairs_and_consumes_srcs(self):
        a = self._stage("scratch/one.txt", "1")
        b = self._stage("other/two.txt", "2")
        d1 = self.dst_root / "published/first.txt"
        d2 = self.dst_root / "published/second.txt"
        publish_all([(a, d1), (b, d2)])
        self.assertEqual(d1.read_text(), "1")
        self.assertEqual(d2.read_text(), "2")
        self.assertFalse(a.exists())
        self.assertFalse(b.exists())
        # Explicit dsts only — no mirrored copies under the scratch
        # relative paths.
        self.assertFalse((self.dst_root / "scratch/one.txt").exists())
        self.assertFalse((self.dst_root / "other/two.txt").exists())

    def test_publish_all_empty_iterable_is_noop(self):
        # No native call, no error.
        publish_all([])

    def test_publish_missing_src_raises(self):
        with self.assertRaises(PublishError):
            publish(self.src_root / "no-such-file", self.dst_root / "out.txt")

    def test_publish_explicit_dst_still_validates_src_root(self):
        # The native layer's src_root canonicalize-and-check still
        # fires: an outside src must fail even with an explicit dst.
        outside = Path(self._dst_dir.name) / "outside.txt"
        outside.write_text("x")
        with self.assertRaises(PublishError):
            publish(outside, self.dst_root / "anywhere.txt")


class TaskMethodTests(_PublishFixture):
    def test_task_publish_method(self):
        src = self._stage("via-method.txt", "m")
        dst = self.dst_root / "via-method.txt"
        Task(relative_path="/x").publish(src, dst)
        self.assertEqual(dst.read_text(), "m")

    def test_task_publish_all_method(self):
        a = self._stage("a", "1")
        b = self._stage("b", "2")
        d1 = self.dst_root / "a"
        d2 = self.dst_root / "b"
        Task(relative_path="/x").publish_all([(a, d1), (b, d2)])
        self.assertEqual(d1.read_text(), "1")
        self.assertEqual(d2.read_text(), "2")

    def test_task_publish_works_without_emit_hook(self):
        # Constructed outside the runtime loop. publish() is
        # process-state, not task-state, so no comm channel is
        # required. Same path consumer unit tests use.
        t = Task(relative_path="/x")
        self.assertIsNone(t._emit)
        src = self._stage("hookless.txt", "ok")
        dst = self.dst_root / "hookless.txt"
        t.publish(src, dst)
        self.assertEqual(dst.read_text(), "ok")

    def test_task_publish_with_key_records_explicit_dst(self):
        # `publish(src, dst, key=)` records the post-publish
        # destination under the key. No mocks — the real publish()
        # delivers the file and returns the explicit dst the
        # accumulator captures. Guards the keyed-output accumulator
        # path survived the explicit-pairs migration.
        src = self._stage("scratch/keyed.bin", "payload")
        dst = self.dst_root / "outputs/keyed.bin"
        t = Task(relative_path="/x")
        t.publish(src, dst, key="out")
        self.assertEqual(
            t._outputs_accumulator,
            {"out": {"kind": "file", "value": str(dst)}},
        )
        # Tie the accumulator's recorded value to the real on-disk
        # delivery so a future regression can't silently desync them.
        self.assertTrue(dst.exists())
        self.assertEqual(dst.read_text(), "payload")

    def test_publish_returns_explicit_dst(self):
        # The Path-return contract: publish returns the explicit dst
        # verbatim — the single source of truth Task.publish captures
        # into the keyed-outputs accumulator.
        src = self._stage("explicit.txt", "v")
        explicit_dst = self.dst_root / "alias.txt"
        returned = publish(src, explicit_dst)
        self.assertEqual(returned, explicit_dst)


class EnvOverrideTests(unittest.TestCase):
    """Confirms env vars actually steer publish — not just module
    defaults read once at import time. Distinct from
    ``_PublishFixture`` because here we want to assert the env
    re-read on every call, including swapping mid-test.
    """

    def test_env_changes_take_effect_per_call(self):
        # src_root is re-read per call: a src valid under src_root A
        # but not under src_root B must fail once the env flips. The
        # native under-root check is what observes the swap.
        with tempfile.TemporaryDirectory() as src_a, \
             tempfile.TemporaryDirectory() as dst_a, \
             tempfile.TemporaryDirectory() as src_b, \
             tempfile.TemporaryDirectory() as dst_b:
            file_a = Path(src_a) / "x"
            file_a.write_text("from-a")
            file_b = Path(src_b) / "x"
            file_b.write_text("from-b")

            os.environ[ENV_SRC_ROOT] = src_a
            os.environ[ENV_DST_ROOT] = dst_a
            try:
                publish(file_a, Path(dst_a) / "x")
                self.assertEqual((Path(dst_a) / "x").read_text(), "from-a")

                # Flip src_root to B; file_a now lies outside it, so the
                # native validation must reject it (proving the per-call
                # env re-read), while file_b under B succeeds.
                os.environ[ENV_SRC_ROOT] = src_b
                os.environ[ENV_DST_ROOT] = dst_b
                with self.assertRaises(PublishError):
                    publish(file_a, Path(dst_b) / "a")
                publish(file_b, Path(dst_b) / "x")
                self.assertEqual((Path(dst_b) / "x").read_text(), "from-b")
            finally:
                os.environ.pop(ENV_SRC_ROOT, None)
                os.environ.pop(ENV_DST_ROOT, None)


class PublishAllBatchTests(_PublishFixture):
    """``publish_all(pairs)`` hands the whole batch to the native
    ``publish_all`` in ONE call — it does NOT fall back to N per-file
    ``publish_one`` calls, and it does NOT derive/mirror any dst.
    Stubs the native layer so the call shape (single batch, verbatim
    pairs, process-wide src_root) is asserted without touching disk.
    """

    def test_publish_all_calls_native_batch_once_with_explicit_pairs(self):
        a = self._stage("one.txt", "1")
        b = self._stage("sub/two.txt", "2")
        d1 = self.dst_root / "first.txt"
        d2 = self.dst_root / "nested/second.txt"
        with patch.object(publish_mod, "_native_publish_all") as batch, \
             patch.object(publish_mod, "_native_publish_one") as one:
            publish_all([(a, d1), (b, d2)])
        # Exactly one batch call, zero per-file publish_one calls.
        batch.assert_called_once()
        one.assert_not_called()
        items, src_root, staging = batch.call_args.args
        # Pairs passed through verbatim, in order — no mirroring.
        self.assertEqual(items, [(a, d1), (b, d2)])
        # The process-wide src_root is forwarded once for the native
        # cross-FS staging + under-root validation.
        self.assertEqual(src_root, self.src_root)
        # The hidden staging dir (`<dst_root>/.publish-tmp`) is forwarded
        # so cross-FS temps land out of the published content tree.
        self.assertEqual(staging, self.dst_root / publish_mod.STAGING_SUBDIR)

    def test_publish_all_empty_is_noop(self):
        with patch.object(publish_mod, "_native_publish_all") as batch:
            publish_all([])
        batch.assert_not_called()


class SweepStaleTmpsTests(_PublishFixture):
    """``sweep_stale_tmps`` is a thin pass-through to the native
    own-host/dead-pid sweep, targeting the configured dst_root.
    """

    def test_sweep_passes_resolved_dst_root_and_returns_count(self):
        with patch.object(
            publish_mod, "_native_sweep_stale_tmps", return_value=3
        ) as native:
            reaped = sweep_stale_tmps(publish_mod.dst_root())
        native.assert_called_once_with(self.dst_root)
        self.assertEqual(reaped, 3)

    def test_dst_root_reads_env(self):
        self.assertEqual(publish_mod.dst_root(), self.dst_root)

    def test_staging_dir_is_hidden_subdir_of_dst_root(self):
        # The staging dir — where cross-FS publishes stage temps and the
        # run-start sweep reaps orphans — is `<dst_root>/.publish-tmp`,
        # out of the published content tree but on the same FS.
        self.assertEqual(
            publish_mod.staging_dir(),
            self.dst_root / publish_mod.STAGING_SUBDIR,
        )


if __name__ == "__main__":
    unittest.main()
