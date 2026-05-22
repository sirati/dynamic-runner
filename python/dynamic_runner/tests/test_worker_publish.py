"""Tests for the Python-side worker publish API.

Covers the contract documented in ``dynamic_runner.worker.publish``:

* env-driven src_root/dst_root resolution with the slurm-wrapper
  defaults
* dst-mirror derivation (``src - src_root → dst_root``) when ``dst``
  is omitted
* explicit ``dst`` override
* ``Task.publish`` / ``Task.publish_all`` method delegation
* ``PublishError`` raised on src-outside-root, missing-src, and
  cross-tree dst-derivation cases

Tests run unbound from the runtime loop — ``Task`` is constructed
directly, exercising the same path that consumer unit tests follow.
"""
from __future__ import annotations

import os
import tempfile
import unittest
from pathlib import Path

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

    def test_publish_mirrors_path_under_dst_root(self):
        src = self._stage("a/b/c.txt", "hello")
        publish(src)
        dst = self.dst_root / "a/b/c.txt"
        self.assertEqual(dst.read_text(), "hello")
        self.assertFalse(src.exists())

    def test_publish_creates_dst_parent_dirs(self):
        src = self._stage("deep/nested/path/file", "x")
        publish(src)
        self.assertTrue((self.dst_root / "deep/nested/path/file").exists())

    def test_publish_explicit_dst_override(self):
        src = self._stage("a.json", "v")
        explicit_dst = self.dst_root / "renamed.json"
        publish(src, explicit_dst)
        self.assertEqual(explicit_dst.read_text(), "v")
        self.assertFalse((self.dst_root / "a.json").exists())

    def test_publish_overwrites_existing_dst(self):
        src = self._stage("k", "new")
        prior = self.dst_root / "k"
        prior.write_text("old")
        publish(src)
        self.assertEqual(prior.read_text(), "new")

    def test_publish_all_processes_each(self):
        a = self._stage("one.txt", "1")
        b = self._stage("two.txt", "2")
        publish_all(a, b)
        self.assertEqual((self.dst_root / "one.txt").read_text(), "1")
        self.assertEqual((self.dst_root / "two.txt").read_text(), "2")
        self.assertFalse(a.exists())
        self.assertFalse(b.exists())

    def test_publish_outside_src_root_raises(self):
        # An outside file resolves cleanly but lies outside the
        # configured staging root → derivation rejects it.
        outside = Path(self._dst_dir.name) / "notstaged.txt"
        outside.write_text("x")
        with self.assertRaises(PublishError):
            publish(outside)

    def test_publish_missing_src_raises(self):
        with self.assertRaises(PublishError):
            publish(self.src_root / "no-such-file")

    def test_publish_explicit_dst_still_validates_src_root(self):
        # Override path: dst-derivation is skipped, but the native
        # layer's src_root canonicalize-and-check still fires. An
        # outside src must fail even with an explicit dst.
        outside = Path(self._dst_dir.name) / "outside.txt"
        outside.write_text("x")
        with self.assertRaises(PublishError):
            publish(outside, self.dst_root / "anywhere.txt")


class TaskMethodTests(_PublishFixture):
    def test_task_publish_method(self):
        src = self._stage("via-method.txt", "m")
        Task(relative_path="/x").publish(src)
        self.assertEqual((self.dst_root / "via-method.txt").read_text(), "m")

    def test_task_publish_all_method(self):
        a = self._stage("a", "1")
        b = self._stage("b", "2")
        Task(relative_path="/x").publish_all(a, b)
        self.assertEqual((self.dst_root / "a").read_text(), "1")
        self.assertEqual((self.dst_root / "b").read_text(), "2")

    def test_task_publish_works_without_emit_hook(self):
        # Constructed outside the runtime loop. publish() is
        # process-state, not task-state, so no comm channel is
        # required. Same path consumer unit tests use.
        t = Task(relative_path="/x")
        self.assertIsNone(t._emit)
        src = self._stage("hookless.txt", "ok")
        t.publish(src)
        self.assertEqual((self.dst_root / "hookless.txt").read_text(), "ok")

    def test_task_publish_with_key_records_resolved_dst_when_dst_omitted(self):
        # End-to-end regression for the `dst=None` accumulator bug:
        # the underlying publish helper resolves the destination via
        # src_root/dst_root; the Task wrapper must record the
        # resolved path, not the literal `None` the caller passed.
        # No mocks here — the real publish() resolves the path,
        # delivers the file, and returns the resolved dst the
        # accumulator captures.
        src = self._stage("nested/a/b/keyed.bin", "payload")
        t = Task(relative_path="/x")
        t.publish(src, key="out")
        expected_dst = self.dst_root / "nested/a/b/keyed.bin"
        self.assertEqual(
            t._outputs_accumulator,
            {"out": {"kind": "file", "value": str(expected_dst)}},
        )
        # Bonus: confirm the file actually lives at that path —
        # ties the accumulator's recorded value to the real on-disk
        # delivery so a future regression can't silently desync them.
        self.assertTrue(expected_dst.exists())
        self.assertEqual(expected_dst.read_text(), "payload")

    def test_publish_returns_resolved_dst_with_explicit_override(self):
        # The Path-return contract holds for the explicit-dst path
        # too — callers like Task.publish capture this verbatim.
        src = self._stage("explicit.txt", "v")
        explicit_dst = self.dst_root / "alias.txt"
        returned = publish(src, explicit_dst)
        self.assertEqual(returned, explicit_dst)

    def test_publish_returns_resolved_dst_when_derived(self):
        # The Path-return contract for the derive-from-src_root path.
        # This is the single source of truth Task.publish relies on
        # when the caller omitted dst.
        src = self._stage("d/e/derive.txt", "v")
        returned = publish(src)
        self.assertEqual(returned, self.dst_root / "d/e/derive.txt")


class EnvOverrideTests(unittest.TestCase):
    """Confirms env vars actually steer publish — not just module
    defaults read once at import time. Distinct from
    ``_PublishFixture`` because here we want to assert the env
    re-read on every call, including swapping mid-test.
    """

    def test_env_changes_take_effect_per_call(self):
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
                publish(file_a)
                self.assertEqual((Path(dst_a) / "x").read_text(), "from-a")

                os.environ[ENV_SRC_ROOT] = src_b
                os.environ[ENV_DST_ROOT] = dst_b
                publish(file_b)
                self.assertEqual((Path(dst_b) / "x").read_text(), "from-b")
            finally:
                os.environ.pop(ENV_SRC_ROOT, None)
                os.environ.pop(ENV_DST_ROOT, None)


if __name__ == "__main__":
    unittest.main()
