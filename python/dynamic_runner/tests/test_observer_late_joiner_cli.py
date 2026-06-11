"""Smoke tests for the `--observer-join-from-peer-info-dir` CLI flag
(transport-unification Step 9: late-joiner observer dispatcher).

Pins three contract points:

1. The flag parses as a string and ends up at the documented attribute
   `args.observer_join_from_peer_info_dir`.
2. Mutual exclusion with ``--secondary`` (the two roles overlap
   structurally — a regular secondary speaks the primary-secondary
   handshake the late-joiner is built to SKIP — so combining them
   surfaces a `parser.error` rather than silent precedence).
3. The flag is absent by default (no surprise downstream behaviour
   when the operator doesn't ask for it).

The Rust-side bootstrap RPC, snapshot restore, and run-loop drive are
covered by `crates/dynrunner-pyo3/src/managers/observer_late_joiner.rs`'s
own unit tests + the channel-transport snapshot_bootstrap integration
test in `crates/dynrunner-transport-channel/tests/snapshot_bootstrap.rs`;
those don't repeat here because they cross the PyO3 boundary in a
shape pytest can't ergonomically drive without a maturin build.

unittest-based to stay runnable in a bare nix-develop shell (no pytest
in the dev environment by convention; see `test_forwarded_argv.py`).
"""

from __future__ import annotations

import argparse
import importlib.util
import io
import pathlib
import sys
import types
import unittest
from contextlib import redirect_stderr


def _setup_package_stub() -> pathlib.Path:
    """Register a minimal `dynamic_runner` package stub so the cli.py
    module under test can be loaded without triggering the real
    package `__init__` (which imports the PyO3 `_native` extension
    and would otherwise require a maturin build to import).

    Mirrors the stub pattern in `test_forwarded_argv.py`.
    """
    here = pathlib.Path(__file__).resolve()
    package_root = here.parent.parent
    if "dynamic_runner" not in sys.modules:
        pkg = types.ModuleType("dynamic_runner")
        pkg.__path__ = [str(package_root)]
        sys.modules["dynamic_runner"] = pkg
    return package_root


def _load_cli_module():
    """Import `dynamic_runner.cli` by absolute path, bypassing the
    package `__init__`. cli.py's only intra-package import is the
    pure-Python `._shared.add_selection_arguments`, so direct file
    import resolves cleanly via the package-stub `__path__`.
    """
    package_root = _setup_package_stub()
    fullname = "dynamic_runner.cli"
    if fullname in sys.modules:
        return sys.modules[fullname]
    target = package_root / "cli.py"
    spec = importlib.util.spec_from_file_location(fullname, target)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[fullname] = module
    spec.loader.exec_module(module)
    return module


cli = _load_cli_module()


def _parse(argv: list[str]) -> argparse.Namespace:
    """Build the parser and parse — no validation. Used for shape
    assertions where validation would fire and short-circuit the
    test before we can inspect `args`.
    """
    parser = cli.build_arg_parser("test")
    return parser.parse_args(argv)


def _parse_and_validate(argv: list[str]) -> argparse.Namespace:
    """Build, parse, AND run cross-flag validation. The two-step
    matches what `dynamic_runner.run.run` does at the top of its
    body; this helper makes sure tests exercise the same path the
    operator hits.
    """
    parser = cli.build_arg_parser("test")
    args = parser.parse_args(argv)
    cli.validate_parsed_args(args, parser)
    return args


class ObserverLateJoinerFlagShapeTests(unittest.TestCase):
    def test_flag_absent_default_is_none(self) -> None:
        # Headline contract: the dispatcher checks
        # `args.observer_join_from_peer_info_dir` as a truthy
        # predicate to decide whether to route through
        # `_dispatch_late_joiner`. Default must be `None` so existing
        # invocations (every pre-Step-9 caller) keep falling through
        # to the multi-computer / local dispatchers unchanged.
        args = _parse([])
        self.assertIsNone(args.observer_join_from_peer_info_dir)

    def test_flag_stores_path_string(self) -> None:
        # The Rust-side `PyObserverLateJoiner.new` takes a `PathBuf`,
        # which PyO3 builds from any `os.PathLike` or string. argparse
        # gives us the string verbatim; cli.py does NOT path-resolve
        # at parse time (the dir might not yet exist when the user
        # types the command; existence is checked Rust-side inside
        # `read_peer_info_dir_v2`, where the operator-visible error
        # shape is best surfaced).
        args = _parse(["--observer-join-from-peer-info-dir", "/tmp/connection_info"])
        self.assertEqual(
            args.observer_join_from_peer_info_dir, "/tmp/connection_info"
        )

    def test_underscored_attribute_name(self) -> None:
        # argparse converts dashes to underscores in `dest`. Pin the
        # exact attribute name so a future flag rename can't silently
        # break the dispatcher's `if args.observer_join_from_peer_info_dir:`
        # branch.
        args = _parse(["--observer-join-from-peer-info-dir", "/anywhere"])
        self.assertTrue(hasattr(args, "observer_join_from_peer_info_dir"))


class MutualExclusionTests(unittest.TestCase):
    """`--observer-join-from-peer-info-dir` is incompatible with
    `--secondary`: the late-joiner observer is a peer-mesh-only role
    that SKIPS the primary-secondary handshake `--secondary` exists
    to perform. The validator catches the combination up-front so the
    operator gets a clear `parser.error` instead of one flag silently
    winning.
    """

    def test_secondary_alone_passes(self) -> None:
        # Sanity: a plain `--secondary` invocation still validates.
        args = _parse_and_validate(
            ["--secondary", "tcp://primary:40000", "--secondary-id", "sec-0"]
        )
        self.assertEqual(args.secondary, "tcp://primary:40000")
        self.assertIsNone(args.observer_join_from_peer_info_dir)

    def test_observer_alone_passes(self) -> None:
        # Sanity: a plain `--observer-join-from-peer-info-dir`
        # invocation also validates (no companion knobs required).
        args = _parse_and_validate(
            ["--observer-join-from-peer-info-dir", "/tmp/ci"]
        )
        self.assertEqual(args.observer_join_from_peer_info_dir, "/tmp/ci")
        self.assertIsNone(args.secondary)

    def test_secondary_plus_observer_rejected(self) -> None:
        # The headline mutual-exclusion check. argparse's `parser.error`
        # calls `sys.exit(2)` AND writes the message to stderr; we
        # capture stderr so the assertion can inspect the failure
        # message AND assert on the exit code without polluting
        # test output.
        parser = cli.build_arg_parser("test")
        args = parser.parse_args(
            [
                "--secondary",
                "tcp://x:1",
                "--secondary-id",
                "sec-0",
                "--observer-join-from-peer-info-dir",
                "/tmp/ci",
            ]
        )
        stderr_buf = io.StringIO()
        with self.assertRaises(SystemExit) as cm, redirect_stderr(stderr_buf):
            cli.validate_parsed_args(args, parser)
        self.assertEqual(cm.exception.code, 2)
        msg = stderr_buf.getvalue()
        # The error message names BOTH flags so the operator knows
        # exactly which two are conflicting.
        self.assertIn("--observer-join-from-peer-info-dir", msg)
        self.assertIn("--secondary", msg)

    def test_observer_with_multi_computer_passes(self) -> None:
        # `--multi-computer` selects the dispatch path for STARTING
        # a cluster (primary + N secondaries). The observer joins
        # an ALREADY-running cluster, but the two flags don't
        # mechanically collide — the dispatcher's
        # `if args.observer_join_from_peer_info_dir:` early-return
        # in `run.py` short-circuits before the multi-computer
        # branch fires. The validator currently does not reject
        # the combination; this test pins that decision so future
        # changes that DO reject it land an explicit migration
        # rather than silent breakage of any operator scripts
        # that rely on the present behaviour.
        args = _parse_and_validate(
            [
                "--observer-join-from-peer-info-dir",
                "/tmp/ci",
                "--multi-computer",
                "slurm",
            ]
        )
        self.assertEqual(args.observer_join_from_peer_info_dir, "/tmp/ci")
        self.assertEqual(args.multi_computer, "slurm")


class GatewayModeTests(unittest.TestCase):
    """`--gateway` composes with the late-joiner flag (the desktop
    path: DIR is gateway-side, peers are reached over per-peer
    `ssh -L` local-forward tunnels). The combination is legal and
    MEANINGFUL — pre-fix it parsed but the gateway was silently
    ignored, leaving the late-joiner unusable from a desktop whose
    only reachable host is the gateway.
    """

    def test_gateway_plus_observer_validates(self) -> None:
        args = _parse_and_validate(
            [
                "--observer-join-from-peer-info-dir",
                "~/runs/run_x/connection_info",
                "--gateway",
                "ssh://alice@gw.example.org",
            ]
        )
        self.assertEqual(args.gateway, "ssh://alice@gw.example.org")
        self.assertEqual(
            args.observer_join_from_peer_info_dir, "~/runs/run_x/connection_info"
        )

    def test_help_documents_dir_semantics_per_mode(self) -> None:
        # The flag's help must tell the operator what DIR means in each
        # mode: LOCAL path without --gateway, GATEWAY-SIDE path with it.
        parser = cli.build_arg_parser("test")
        # argparse re-wraps help text at arbitrary points; normalise
        # whitespace so the assertion is wrap-insensitive.
        help_text = " ".join(parser.format_help().split())
        self.assertIn("GATEWAY-SIDE path", help_text)
        self.assertIn("LOCAL path", help_text)

    def test_dispatcher_forwards_gateway_kwargs(self) -> None:
        # `_dispatch_late_joiner` must hand the gateway knobs through to
        # the Rust pyfunction — the per-layer plumbing gap is exactly
        # what made the flag a silent no-op before.
        run_mod = _load_run_module()
        recorded: dict = {}

        def fake_run_observer_late_joiner(peer_info_dir, **kwargs):
            recorded["peer_info_dir"] = peer_info_dir
            recorded.update(kwargs)
            return {"completed": 0}

        pkg = sys.modules["dynamic_runner"]
        pkg.run_observer_late_joiner = fake_run_observer_late_joiner
        try:
            args = _parse(
                [
                    "--observer-join-from-peer-info-dir",
                    "/gw/run/connection_info",
                    "--gateway",
                    "ssh://alice@gw:2222",
                    "--ssh-identity-file",
                    "/home/x/key",
                    "--ssh-config",
                    "/home/x/cfg",
                ]
            )
            run_mod._dispatch_late_joiner(None, args, _SilentLogger())
        finally:
            del pkg.run_observer_late_joiner
        self.assertEqual(recorded["peer_info_dir"], "/gw/run/connection_info")
        self.assertEqual(recorded["gateway_url"], "ssh://alice@gw:2222")
        self.assertEqual(recorded["ssh_identity_file"], "/home/x/key")
        self.assertEqual(recorded["ssh_config_file"], "/home/x/cfg")

    def test_dispatcher_passes_none_gateway_when_unset(self) -> None:
        # Local mode: the kwargs are still passed, as None — the Rust
        # constructor treats None as "no gateway" and keeps the local
        # direct-dial path byte-identical.
        run_mod = _load_run_module()
        recorded: dict = {}

        def fake_run_observer_late_joiner(peer_info_dir, **kwargs):
            recorded["peer_info_dir"] = peer_info_dir
            recorded.update(kwargs)
            return {"completed": 0}

        pkg = sys.modules["dynamic_runner"]
        pkg.run_observer_late_joiner = fake_run_observer_late_joiner
        try:
            args = _parse(["--observer-join-from-peer-info-dir", "/tmp/ci"])
            run_mod._dispatch_late_joiner(None, args, _SilentLogger())
        finally:
            del pkg.run_observer_late_joiner
        self.assertIsNone(recorded["gateway_url"])
        self.assertIsNone(recorded["ssh_identity_file"])
        self.assertIsNone(recorded["ssh_config_file"])


class _SilentLogger:
    """Minimal logger stand-in for dispatcher-plumbing tests."""

    def info(self, *_a, **_k) -> None:
        pass


def _load_run_module():
    """Load `dynamic_runner.run` by absolute path through the package
    stub (same pattern as `test_cli_api.py`) — run.py's intra-package
    imports are all pure-Python at import time.
    """
    package_root = _setup_package_stub()
    fullname = "dynamic_runner.run"
    if fullname in sys.modules:
        return sys.modules[fullname]
    target = package_root / "run.py"
    spec = importlib.util.spec_from_file_location(fullname, target)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[fullname] = module
    spec.loader.exec_module(module)
    return module


if __name__ == "__main__":
    unittest.main()
