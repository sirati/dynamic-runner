"""Cold-start tests for `dynamic_runner._secondary_bootstrap`.

The shim's whole job: fetch the cluster-wide ``forwarded_argv`` from the
mesh (the Rust ``_native.fetch_run_config`` driver), splice it onto
``sys.argv``, and ``runpy`` the consumer module as ``__main__`` — so the
consumer sees a ``sys.argv`` byte-identical to a full command-line launch.

These tests load the shim directly under a ``dynamic_runner`` package stub
(mirroring ``test_spawn_secondary.py`` / ``test_unconfigured_deadline_cli.py``)
so they run in a bare ``nix develop`` WITHOUT a maturin build:
``_native.fetch_run_config`` is a capture/return spy and
``_native.DistributedConfig`` records its kwargs. The shim imports both via
``from ._native import …`` (the ``worker/publish.py`` idiom), so the stub
lives at ``dynamic_runner._native``.

unittest-based (pytest is not in the dev shell), matching the rest of the
suite.
"""

from __future__ import annotations

import argparse
import importlib.util
import pathlib
import sys
import types
import unittest


_PACKAGE_ROOT = pathlib.Path(__file__).resolve().parent.parent


# --------------------------------------------------------------------------
# `_native` stub: a capture/return spy for `fetch_run_config` plus a
# kwarg-recording `DistributedConfig`. The shim's only two `_native`
# dependencies.
# --------------------------------------------------------------------------
_FETCH_CALLS: list[tuple] = []
_FETCH_RETURN: list[str] = []
_FETCH_RAISES: BaseException | None = None
_DC_KWARGS: dict = {}


class _DistributedConfigSpy:
    def __init__(self, **kwargs) -> None:
        _DC_KWARGS.clear()
        _DC_KWARGS.update(kwargs)
        self.kwargs = kwargs


def _fetch_run_config_spy(primary_url, secondary_id, distributed_config=None):
    _FETCH_CALLS.append((primary_url, secondary_id, distributed_config))
    if _FETCH_RAISES is not None:
        raise _FETCH_RAISES
    return list(_FETCH_RETURN)


def _setup_package_stub() -> None:
    if "dynamic_runner" not in sys.modules:
        pkg = types.ModuleType("dynamic_runner")
        pkg.__path__ = [str(_PACKAGE_ROOT)]
        sys.modules["dynamic_runner"] = pkg
    if "dynamic_runner._native" not in sys.modules:
        native = types.ModuleType("dynamic_runner._native")
        native.fetch_run_config = _fetch_run_config_spy
        native.DistributedConfig = _DistributedConfigSpy
        sys.modules["dynamic_runner._native"] = native
        sys.modules["dynamic_runner"]._native = native


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


bootstrap = _load_module_direct("_secondary_bootstrap", "_secondary_bootstrap.py")
# `cli` is pure-Python (imports `_shared` + `logging_setup`, no `_native`),
# so the real framework parser loads under the stub. We use it to prove the
# reconstructed argv yields the SAME consumer namespace as a full-CLI launch.
cli = _load_module_direct("cli", "cli.py")


def _consumer_parser() -> argparse.ArgumentParser:
    """Framework parser + a representative task-filter flag, mirroring what
    a consumer's `cli_main` builds (framework flags + `task.add_task_arguments`).
    `--platform` stands in for the forwarded task flags."""
    parser = cli.build_arg_parser("test")
    parser.add_argument("--platform", type=str, default=None)
    return parser


# The framework-regenerated + binary-injected flags the wrapper leaves on
# the bootstrap CLI today (the post-#238 minimal-on-CLI set), PLUS the new
# `--secondary-module`. Order mirrors `build_run_argv`.
_MINIMAL_CLI = [
    "--secondary-module",
    "asm_tokenizer.secondary",
    "--secondary",
    "tcp://gw.cluster:4433",
    "--secondary-id",
    "sec-0",
    "--secondary-quic-port",
    "7777",
    "--cores=-2",
    "--max-memory=-2G",
    "--src-network=/app/src-network",
    "--log-dir=/app/log-network",
    "--full-log-dir=/app/log-network/sec-0",
    "--panik-file",
    "/app/log-tmp/.dynrunner-reaper.panik",
    "--mem-manager-reserved=524288000",
]

# The task-filter / non-regenerated flags the primary holds and answers
# over the mesh — the consumer's argparse must re-parse these.
_FORWARDED = ["--platform", "x86", "--memprofile"]

# The pre-#238 FULL command line the consumer saw on a baked container
# command: the minimal-on-CLI framework flags (minus `--secondary-module`,
# which never existed pre-#238) followed by the forwarded args.
_FULL_CLI = [
    "--secondary",
    "tcp://gw.cluster:4433",
    "--secondary-id",
    "sec-0",
    "--secondary-quic-port",
    "7777",
    "--cores=-2",
    "--max-memory=-2G",
    "--src-network=/app/src-network",
    "--log-dir=/app/log-network",
    "--full-log-dir=/app/log-network/sec-0",
    "--panik-file",
    "/app/log-tmp/.dynrunner-reaper.panik",
    "--mem-manager-reserved=524288000",
    "--platform",
    "x86",
    "--memprofile",
]


def _reset_spies() -> None:
    _FETCH_CALLS.clear()
    _DC_KWARGS.clear()
    global _FETCH_RETURN, _FETCH_RAISES
    _FETCH_RETURN = []
    _FETCH_RAISES = None


class ArgvReconstructionTests(unittest.TestCase):
    """`_reconstruct_consumer_argv`: byte-identical to the full-CLI case."""

    def test_strips_secondary_module_two_token_form(self) -> None:
        out = bootstrap._reconstruct_consumer_argv(_MINIMAL_CLI, _FORWARDED)
        self.assertNotIn("--secondary-module", out)
        self.assertNotIn("asm_tokenizer.secondary", out)

    def test_strips_secondary_module_equals_form(self) -> None:
        argv = ["--secondary-module=asm_tokenizer.secondary", "--secondary", "tcp://x:1"]
        out = bootstrap._reconstruct_consumer_argv(argv, [])
        self.assertNotIn("--secondary-module=asm_tokenizer.secondary", out)
        self.assertEqual(out, ["--secondary", "tcp://x:1"])

    def test_reconstructed_argv_is_byte_identical_to_full_cli(self) -> None:
        out = bootstrap._reconstruct_consumer_argv(_MINIMAL_CLI, _FORWARDED)
        self.assertEqual(out, _FULL_CLI)

    def test_forwarded_appended_last_preserving_order(self) -> None:
        out = bootstrap._reconstruct_consumer_argv(_MINIMAL_CLI, _FORWARDED)
        self.assertEqual(out[-len(_FORWARDED):], _FORWARDED)

    def test_unknown_flags_pass_through_verbatim(self) -> None:
        argv = ["--secondary-module", "m", "--some-unknown", "v", "--flagless"]
        out = bootstrap._reconstruct_consumer_argv(argv, [])
        self.assertEqual(out, ["--some-unknown", "v", "--flagless"])


class ConsumerNamespaceEqualityTests(unittest.TestCase):
    """Audit H6: the reconstructed argv must yield the SAME consumer
    namespace as a full-CLI launch — and must NOT pass vacuously empty."""

    def test_namespace_equals_full_cli(self) -> None:
        reconstructed = bootstrap._reconstruct_consumer_argv(_MINIMAL_CLI, _FORWARDED)
        from_mesh = _consumer_parser().parse_args(reconstructed)
        from_full = _consumer_parser().parse_args(_FULL_CLI)
        self.assertEqual(vars(from_mesh), vars(from_full))

    def test_load_bearing_fields_present(self) -> None:
        reconstructed = bootstrap._reconstruct_consumer_argv(_MINIMAL_CLI, _FORWARDED)
        ns = _consumer_parser().parse_args(reconstructed)
        # cores / mem / src-network / the connect coords + task-filter flags.
        self.assertEqual(ns.cores, "-2")
        self.assertEqual(ns.max_memory, "-2G")
        self.assertEqual(ns.src_network, "/app/src-network")
        self.assertEqual(ns.secondary, "tcp://gw.cluster:4433")
        self.assertEqual(ns.secondary_id, "sec-0")
        self.assertEqual(ns.platform, "x86")  # forwarded task flag
        self.assertTrue(ns.memprofile)  # forwarded framework flag

    def test_non_empty_when_run_had_forwarded_args(self) -> None:
        # H6: an empty fetch when the run genuinely had forwarded args is a
        # failure to surface, not a silent empty run. Here the forwarded set
        # is non-empty, so the reconstructed argv MUST carry it through.
        reconstructed = bootstrap._reconstruct_consumer_argv(_MINIMAL_CLI, _FORWARDED)
        ns = _consumer_parser().parse_args(reconstructed)
        self.assertIsNotNone(ns.platform)
        self.assertTrue(ns.memprofile)


class FetchDistributedConfigTests(unittest.TestCase):
    def setUp(self) -> None:
        _reset_spies()

    def _parsed(self, argv: list[str]) -> argparse.Namespace:
        return bootstrap._build_bootstrap_parser().parse_known_args(argv)[0]

    def test_none_when_no_knob_deviates(self) -> None:
        args = self._parsed(_MINIMAL_CLI)
        self.assertIsNone(bootstrap._build_fetch_distributed_config(args))

    def test_deadline_override_flows_into_kwarg(self) -> None:
        args = self._parsed(_MINIMAL_CLI + ["--unconfigured-deadline-secs", "1800"])
        cfg = bootstrap._build_fetch_distributed_config(args)
        self.assertIsNotNone(cfg)
        self.assertEqual(_DC_KWARGS.get("unconfigured_deadline_secs"), 1800.0)

    def test_disable_overlay_flows_into_kwarg(self) -> None:
        args = self._parsed(_MINIMAL_CLI + ["--disable-peer-overlay"])
        bootstrap._build_fetch_distributed_config(args)
        self.assertTrue(_DC_KWARGS.get("disable_peer_overlay"))

    def test_only_fetch_relevant_kwargs_forwarded(self) -> None:
        args = self._parsed(_MINIMAL_CLI + ["--unconfigured-deadline-secs", "900"])
        bootstrap._build_fetch_distributed_config(args)
        self.assertEqual(set(_DC_KWARGS), {"unconfigured_deadline_secs"})


class ColdStartMainTests(unittest.TestCase):
    """End-to-end `main`: fetch (spied) → splice sys.argv → runpy the
    consumer module. The consumer is a throwaway module that records the
    `sys.argv` it observed."""

    def setUp(self) -> None:
        _reset_spies()
        self._saved_argv = list(sys.argv)
        # A throwaway consumer module on sys.path that records sys.argv.
        self._mod_dir = pathlib.Path(__file__).resolve().parent
        self._mod_name = "_bootstrap_test_consumer"
        (self._mod_dir / f"{self._mod_name}.py").write_text(
            "import sys\n"
            "import json\n"
            "import pathlib\n"
            "pathlib.Path(__file__).with_suffix('.observed').write_text(\n"
            "    json.dumps(sys.argv))\n"
        )
        if str(self._mod_dir) not in sys.path:
            sys.path.insert(0, str(self._mod_dir))

    def tearDown(self) -> None:
        sys.argv = self._saved_argv
        for suffix in (".py", ".observed"):
            p = self._mod_dir / f"{self._mod_name}{suffix}"
            if p.exists():
                p.unlink()
        sys.modules.pop(self._mod_name, None)

    def _run_main(self, bootstrap_argv: list[str]) -> list[str]:
        import json

        bootstrap.main(bootstrap_argv)
        observed = (self._mod_dir / f"{self._mod_name}.observed").read_text()
        return json.loads(observed)

    def test_main_fetches_and_runs_consumer_with_full_argv(self) -> None:
        global _FETCH_RETURN
        _FETCH_RETURN = list(_FORWARDED)
        argv = list(_MINIMAL_CLI)
        argv[1] = self._mod_name  # point --secondary-module at the throwaway
        observed = self._run_main(argv)
        # observed[0] is runpy's module-file (alter_sys=True); the
        # load-bearing assertion is observed[1:] == the full-CLI argv.
        self.assertEqual(observed[1:], _FULL_CLI)

    def test_main_passes_primary_url_and_id_to_fetch(self) -> None:
        global _FETCH_RETURN
        _FETCH_RETURN = list(_FORWARDED)
        argv = list(_MINIMAL_CLI)
        argv[1] = self._mod_name
        self._run_main(argv)
        self.assertEqual(len(_FETCH_CALLS), 1)
        primary_url, secondary_id, _dc = _FETCH_CALLS[0]
        self.assertEqual(primary_url, "tcp://gw.cluster:4433")
        self.assertEqual(secondary_id, "sec-0")

    def test_fetch_failure_exits_nonzero(self) -> None:
        global _FETCH_RAISES
        _FETCH_RAISES = RuntimeError("dial budget exhausted")
        argv = list(_MINIMAL_CLI)
        argv[1] = self._mod_name
        with self.assertRaises(SystemExit) as ctx:
            bootstrap.main(argv)
        # SystemExit with a string message is a non-zero exit; the message
        # carries the setup-deadline-style diagnostic.
        self.assertIsInstance(ctx.exception.code, str)
        self.assertIn("never joined", ctx.exception.code)
        self.assertIn("dial budget exhausted", ctx.exception.code)

    def test_missing_secondary_url_exits_nonzero(self) -> None:
        argv = ["--secondary-module", self._mod_name, "--secondary-id", "sec-0"]
        with self.assertRaises(SystemExit) as ctx:
            bootstrap.main(argv)
        self.assertIsInstance(ctx.exception.code, str)
        self.assertIn("--secondary", ctx.exception.code)

    def test_missing_secondary_id_exits_nonzero(self) -> None:
        argv = ["--secondary-module", self._mod_name, "--secondary", "tcp://x:1"]
        with self.assertRaises(SystemExit) as ctx:
            bootstrap.main(argv)
        self.assertIsInstance(ctx.exception.code, str)
        self.assertIn("--secondary-id", ctx.exception.code)


if __name__ == "__main__":
    unittest.main()
