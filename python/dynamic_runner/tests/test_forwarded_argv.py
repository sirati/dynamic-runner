"""Regression pins for `_forwarded_argv.filter_framework_argv`.

The bug this guards against (Tier-2 setup-promote dispatch repro):
``--multi-computer slurm --source-already-staged ...`` discovered
``tasks=0`` whenever the user supplied task-side filter flags
(e.g. ``--platform x64 --compiler gcc --name-regex foo``) on the
dispatcher CLI. Root cause: the SLURM wrapper plumbed only
``--cores`` and ``--max-memory`` through to the secondary, dropping
every other user-supplied argv token. The setup-promoted secondary
then ran ``task.discover_items`` against the full corpus and reported
zero matches because its argparse never saw the filter flags.

The fix forwards ``sys.argv[1:]`` (minus the framework-regenerated
flags the wrapper emits afresh) to the secondary's container_command.
This module owns the filter; the layers below it are dumb data
carriers.

unittest-based — pytest is not in the dev shell, so we stay stdlib
to keep the test runnable from any environment.
"""

from __future__ import annotations

import importlib.util
import pathlib
import sys
import types
import unittest


def _setup_package_stub() -> pathlib.Path:
    """Register a minimal `dynamic_runner` package stub so relative
    imports inside the module under test resolve, without triggering
    the real package `__init__` (which imports the PyO3 `_native`
    extension and would require a maturin build to load).
    """
    here = pathlib.Path(__file__).resolve()
    package_root = here.parent.parent  # …/python/dynamic_runner/
    if "dynamic_runner" not in sys.modules:
        pkg = types.ModuleType("dynamic_runner")
        pkg.__path__ = [str(package_root)]
        sys.modules["dynamic_runner"] = pkg
    return package_root


def _load_module_direct(name: str, relpath: str):
    """Import a single `dynamic_runner` source file by absolute path,
    bypassing the package `__init__`. `_forwarded_argv` is pure-Python
    with no external imports, so direct file import is safe and
    keeps the test runnable in a bare nix-develop shell.
    """
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


_forwarded_argv = _load_module_direct("_forwarded_argv", "_forwarded_argv.py")
filter_framework_argv = _forwarded_argv.filter_framework_argv


class FilterFrameworkArgvTests(unittest.TestCase):
    def test_empty_argv_returns_empty(self) -> None:
        self.assertEqual(filter_framework_argv([]), [])

    def test_task_only_argv_unchanged(self) -> None:
        # Pure task-specific argv: nothing in the framework-regenerated
        # set, so the filter is the identity.
        argv = ["--platform", "x64", "--compiler", "gcc", "--name-regex", "foo"]
        self.assertEqual(filter_framework_argv(argv), argv)

    def test_dispatcher_argv_drops_framework_regenerated_pairs(self) -> None:
        # Headline test from the task brief: the dispatcher argv mixes
        # framework-regenerated flags with task-specific filters; the
        # filtered list contains only the task-specific tail.
        argv = [
            "--secondary",
            "tcp://x",
            "--src-network",
            "/app",
            "--platform",
            "x64",
            "--compiler",
            "gcc",
            "--name-regex",
            "foo",
        ]
        self.assertEqual(
            filter_framework_argv(argv),
            ["--platform", "x64", "--compiler", "gcc", "--name-regex", "foo"],
        )

    def test_equals_form_also_dropped(self) -> None:
        # argparse accepts `--flag=VALUE` in addition to `--flag VALUE`;
        # the filter must recognise both shapes.
        argv = [
            "--secondary=tcp://x",
            "--cores=2",
            "--max-memory=4G",
            "--platform",
            "x64",
        ]
        self.assertEqual(filter_framework_argv(argv), ["--platform", "x64"])

    def test_mixed_forms_dropped(self) -> None:
        # Real CLI invocations frequently mix the two forms — argparse
        # accepts both and the user has no reason to prefer one over
        # the other. The filter must handle them in arbitrary order.
        argv = [
            "--secondary",
            "tcp://x",
            "--secondary-id=sec-0",
            "--secondary-quic-port",
            "4242",
            "--cores=2",
            "--max-memory",
            "4G",
            "--src-network=/app",
            "--debug",
            "--platform",
            "x64",
        ]
        self.assertEqual(filter_framework_argv(argv), ["--debug", "--platform", "x64"])

    def test_unknown_flags_pass_through_verbatim(self) -> None:
        # Flags the framework doesn't know about (task-side or
        # consumer-added) pass through unchanged. The secondary's
        # argparse will accept/reject them — the filter has no opinion.
        argv = ["--unknown", "value", "--some-flag", "--positional-tail"]
        self.assertEqual(filter_framework_argv(argv), argv)

    def test_values_with_special_chars_preserved(self) -> None:
        # The filter does not inspect values — it counts tokens. A
        # value containing shell metacharacters (glob, quotes) survives
        # unmodified for the downstream bash-quoter to handle.
        argv = ["--name-regex", "x64-gcc-*-*_minigzipsh"]
        self.assertEqual(filter_framework_argv(argv), argv)

    def test_trailing_bare_framework_flag_drops_only_flag(self) -> None:
        # Defensive: a malformed trailing `--cores` with no value would
        # have argparse error on the dispatcher (so we'd never reach
        # this filter on a real run), but the filter must not walk off
        # the end of the slice if it ever sees one. Drops the flag
        # alone rather than indexing past the end.
        argv = ["--platform", "x64", "--cores"]
        self.assertEqual(filter_framework_argv(argv), ["--platform", "x64"])

    def test_log_dir_is_framework_regenerated(self) -> None:
        # Log-mount split: `--log-dir` is regenerated by the SLURM
        # wrapper as `--log-dir=/app/log-network` (the container-
        # internal log-mount path). Forwarding the dispatcher's value
        # would duplicate the flag and confuse argparse on the
        # secondary; the filter MUST drop both shapes.
        argv = [
            "--log-dir=/host/log",
            "--platform",
            "x64",
            "--log-dir",
            "/another/path",
        ]
        self.assertEqual(filter_framework_argv(argv), ["--platform", "x64"])


if __name__ == "__main__":
    unittest.main()
