"""Regression pins for `spawn_secondary.build_subprocess_spawn`.

The bug this guards against (2026-05-11): the subprocess argv built
by `build_subprocess_spawn` was missing `--cores`, so every secondary
spawned by `--multi-computer local` re-auto-detected workers from
`std::thread::available_parallelism()` and ignored the operator's
`--cores N` request. asm-tokenizer + asm-dataset-nix both reported
the over-spawn symptom (32 workers per secondary on a 32-core host,
`--cores 2` silently dropped). The fix threads `--cores` into the
subprocess argv as a verbatim spec string, so each secondary
resolves it locally against its own detected CPU count.

unittest-based — pytest is not in the dev shell, so we stay stdlib
to keep the test runnable from any environment.
"""

from __future__ import annotations

import argparse
import importlib.util
import pathlib
import sys
import unittest


def _setup_package_stub():
    """Register a minimal `dynamic_runner` package stub so relative
    imports inside the modules under test resolve, without triggering
    the real package `__init__` (which imports the PyO3 `_native`
    extension and would require a maturin build to load).
    """
    here = pathlib.Path(__file__).resolve()
    package_root = here.parent.parent  # …/python/dynamic_runner/
    if "dynamic_runner" not in sys.modules:
        import types
        pkg = types.ModuleType("dynamic_runner")
        pkg.__path__ = [str(package_root)]
        sys.modules["dynamic_runner"] = pkg
    return package_root


def _load_module_direct(name: str, relpath: str):
    """Import a single `dynamic_runner` source file by absolute path,
    bypassing the package `__init__`. The modules this test
    exercises (`deployment_spec`, `subprocess_spec`, `spawn_secondary`)
    are pure-Python and import nothing from `_native`, so direct file
    import is safe. Used so the test runs in the bare `nix develop`
    shell without a wheel build."""
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


_deployment_spec = _load_module_direct("deployment_spec", "deployment_spec.py")
# Load `subprocess_spec` first; `spawn_secondary` imports it via
# `from .subprocess_spec import SubprocessSpec` so its module entry
# must already be in `sys.modules` under the package-qualified name.
_subprocess_spec_mod = _load_module_direct("subprocess_spec", "subprocess_spec.py")
_spawn_secondary = _load_module_direct("spawn_secondary", "spawn_secondary.py")
TaskDeploymentSpec = _deployment_spec.TaskDeploymentSpec
SubprocessSpec = _subprocess_spec_mod.SubprocessSpec
build_subprocess_spawn = _spawn_secondary.build_subprocess_spawn


def _make_deployment() -> TaskDeploymentSpec:
    return TaskDeploymentSpec(
        secondary_module="some_consumer.entrypoint",
        image_name="test-image",
    )


def _make_args(**overrides) -> argparse.Namespace:
    """argparse.Namespace mimicking the primary's parsed CLI args."""
    defaults = dict(
        secondary=None,
        secondary_id=None,
        secondary_quic_port=None,
        source=None,
        cores=None,
        raw_logs=False,
    )
    defaults.update(overrides)
    return argparse.Namespace(**defaults)


def _captured_argv(args: argparse.Namespace) -> list[str]:
    """Drive `spawn_secondary` and return the argv it produced.

    Post-refactor `spawn_secondary` returns a
    :class:`SubprocessSpec` describing what to spawn — no
    `subprocess.Popen` is constructed here (lifecycle is Rust-owned;
    see ``spawn_secondary.py`` header banner). The test simply
    inspects the returned spec's ``argv`` field.
    """
    deployment = _make_deployment()
    spawn = build_subprocess_spawn(deployment, args)
    spec = spawn("ws://primary:8080", "sec-0", 4242)
    assert isinstance(spec, SubprocessSpec), (
        f"spawn_secondary must return SubprocessSpec; got {type(spec).__name__}"
    )
    return list(spec.argv)


class TestSpawnSecondaryStdioModeThreadThrough(unittest.TestCase):
    """Pins the local-mode importance-gate plumbing (2026-06-10): a
    `--multi-computer local` secondary is spawned with INHERITED stdio —
    its stdout IS the operator's terminal — so the operator's
    `--important-stdio-only` must ride its argv. It is stripped from the
    generic `forwarded_argv` (correct for SLURM, where secondary stdio is
    a per-node sbatch capture), so the local spawn path must re-emit it
    explicitly via `logging_setup.stdio_mode_argv`. Pre-fix the secondary
    subprocess installed an UNGATED subscriber and flooded the operator's
    importance-only stdout with its full INFO firehose."""

    def test_important_stdio_only_threaded_when_set(self) -> None:
        argv = _captured_argv(_make_args(important_stdio_only=True))
        self.assertIn(
            "--important-stdio-only",
            argv,
            f"stdio-inheriting secondary lost the operator's stdio gate: {argv}",
        )

    def test_important_stdio_only_absent_when_off(self) -> None:
        argv = _captured_argv(_make_args(important_stdio_only=False))
        self.assertNotIn("--important-stdio-only", argv)

    def test_important_stdio_only_absent_when_attr_missing(self) -> None:
        # Programmatic callers may pass a namespace without the attr; never
        # synthesize the flag from nothing.
        argv = _captured_argv(_make_args())
        self.assertNotIn("--important-stdio-only", argv)

    def test_secondary_boot_parse_arms_importance_from_spawn_argv(self) -> None:
        """Wire-shape mirror: the SECONDARY-side framework parser, fed the
        verbatim argv this spawn path produced, must come out with
        `important_stdio_only=True` — the same parsed knob its
        `setup_logging` hands to the shared `init_logging` seam. This is
        the cross-process chain the per-layer tests can't see."""
        cli = _load_module_direct("cli", "cli.py")
        argv = _captured_argv(_make_args(important_stdio_only=True))
        # Drop the `python -m <module>` launcher prefix; argparse sees the rest.
        flags = argv[3:]
        boot_args = cli.build_arg_parser("test").parse_args(flags)
        self.assertTrue(
            boot_args.important_stdio_only,
            f"secondary boot parse did not arm importance mode from {flags}",
        )


class TestSpawnSecondaryCoresThreadThrough(unittest.TestCase):
    def test_cores_threaded_when_set(self) -> None:
        """argv MUST include `--cores <spec>` when the primary args
        carry a non-None `cores` value. Pre-fix this failed — the
        argv builder silently dropped `--cores`."""
        argv = _captured_argv(_make_args(cores="2"))
        self.assertIn("--cores", argv, f"--cores missing from argv: {argv}")
        self.assertEqual(argv[argv.index("--cores") + 1], "2")

    def test_cores_threaded_offset_form_verbatim(self) -> None:
        """Offset specs (`-N`) are forwarded as the literal string,
        not pre-resolved. This is what makes the per-machine
        semantic work on heterogeneous clusters — each secondary
        resolves the offset against ITS host's detected CPU count,
        not the primary's."""
        argv = _captured_argv(_make_args(cores="-2"))
        self.assertIn("--cores", argv)
        self.assertEqual(argv[argv.index("--cores") + 1], "-2")

    def test_cores_omitted_when_none(self) -> None:
        """If `cores` is absent from args (older callers,
        programmatic invocations), don't synthesize an empty
        `--cores ""` flag — let the secondary's argparse default
        apply."""
        argv = _captured_argv(_make_args(cores=None))
        self.assertNotIn("--cores", argv, f"--cores should be absent: {argv}")

    def test_cores_zero_threaded_verbatim(self) -> None:
        """`0` is the all-cores sentinel; it must reach the
        secondary intact (not be silently dropped as 'falsy').
        Without this the secondary subprocess would see no
        `--cores` and fall back to its own argparse default, which
        historically happened to also be all-cores — but that's a
        different code path. Defend the explicit-zero plumbing as
        the user-facing contract."""
        argv = _captured_argv(_make_args(cores="0"))
        self.assertIn("--cores", argv)
        self.assertEqual(argv[argv.index("--cores") + 1], "0")

    def test_other_flags_still_forwarded(self) -> None:
        """Negative space: adding `--cores` didn't break the
        existing `--src-network` / `--raw-logs` thread-through."""
        argv = _captured_argv(_make_args(
            source="/some/path",
            cores="4",
            raw_logs=True,
        ))
        self.assertIn("--src-network", argv)
        self.assertEqual(argv[argv.index("--src-network") + 1], "/some/path")
        self.assertIn("--cores", argv)
        self.assertEqual(argv[argv.index("--cores") + 1], "4")
        self.assertIn("--raw-logs", argv)

    def test_secondary_connection_args_always_present(self) -> None:
        """Sanity: the three secondary-connection flags are still
        the load-bearing argv — the primary URL, secondary ID, and
        QUIC port must always be in argv regardless of which
        optional flags are set."""
        argv = _captured_argv(_make_args(cores="2"))
        self.assertIn("--secondary", argv)
        self.assertEqual(argv[argv.index("--secondary") + 1], "ws://primary:8080")
        self.assertIn("--secondary-id", argv)
        self.assertEqual(argv[argv.index("--secondary-id") + 1], "sec-0")
        self.assertIn("--secondary-quic-port", argv)
        self.assertEqual(argv[argv.index("--secondary-quic-port") + 1], "4242")


if __name__ == "__main__":
    unittest.main()
