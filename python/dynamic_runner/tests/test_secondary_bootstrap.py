"""Cold-start tests for `dynamic_runner._secondary_bootstrap`.

The shim's whole job (post-#238-reverse removal): strip the shim-private
``--secondary-module`` flag off ``sys.argv`` and ``runpy`` the named
consumer module as ``__main__`` — so the consumer sees a ``sys.argv``
equal to the bootstrap argv minus that one shim-private pair. Cold-start
run-config delivery (the old mesh fetch) is GONE; the reimpl restores it
via a primary-push.

These tests load the shim directly under a ``dynamic_runner`` package stub
(mirroring ``test_spawn_secondary.py`` / ``test_unconfigured_deadline_cli.py``)
so they run in a bare ``nix develop`` WITHOUT a maturin build. The shim no
longer imports anything from ``_native``, so the stub is just an empty
package node so ``from . import …`` style relative loading resolves.

unittest-based (pytest is not in the dev shell), matching the rest of the
suite.
"""

from __future__ import annotations

import argparse
import importlib.util
import pathlib
import sys
import textwrap
import types
import unittest


_PACKAGE_ROOT = pathlib.Path(__file__).resolve().parent.parent


def _setup_package_stub() -> None:
    if "dynamic_runner" not in sys.modules:
        pkg = types.ModuleType("dynamic_runner")
        pkg.__path__ = [str(_PACKAGE_ROOT)]
        sys.modules["dynamic_runner"] = pkg


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
# stripped argv yields the expected consumer namespace.
cli = _load_module_direct("cli", "cli.py")


def _consumer_parser() -> argparse.ArgumentParser:
    """Framework parser + a representative task-filter flag, mirroring what
    a consumer's `cli_main` builds (framework flags + `task.add_task_arguments`).
    `--platform` stands in for the task flags that arrive on the CLI."""
    parser = cli.build_arg_parser("test")
    parser.add_argument("--platform", type=str, default=None)
    return parser


# The bootstrap argv the wrapper leaves on the CLI: `--secondary-module`
# plus the framework-regenerated + binary-injected boot flags.
_BOOTSTRAP_CLI = [
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

# The same argv with ONLY the shim-private `--secondary-module <m>` pair
# removed — what the consumer module must see as `sys.argv[1:]`.
_CONSUMER_CLI = [
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


class StripSecondaryModuleTests(unittest.TestCase):
    """`_strip_secondary_module`: drop ONLY the shim-private flag pair."""

    def test_strips_secondary_module_two_token_form(self) -> None:
        out = bootstrap._strip_secondary_module(_BOOTSTRAP_CLI)
        self.assertNotIn("--secondary-module", out)
        self.assertNotIn("asm_tokenizer.secondary", out)

    def test_strips_secondary_module_equals_form(self) -> None:
        argv = ["--secondary-module=asm_tokenizer.secondary", "--secondary", "tcp://x:1"]
        out = bootstrap._strip_secondary_module(argv)
        self.assertNotIn("--secondary-module=asm_tokenizer.secondary", out)
        self.assertEqual(out, ["--secondary", "tcp://x:1"])

    def test_stripped_argv_equals_consumer_cli(self) -> None:
        out = bootstrap._strip_secondary_module(_BOOTSTRAP_CLI)
        self.assertEqual(out, _CONSUMER_CLI)

    def test_unknown_flags_pass_through_verbatim(self) -> None:
        argv = ["--secondary-module", "m", "--some-unknown", "v", "--flagless"]
        out = bootstrap._strip_secondary_module(argv)
        self.assertEqual(out, ["--some-unknown", "v", "--flagless"])


class ConsumerNamespaceTests(unittest.TestCase):
    """The stripped argv parses into the expected boot-flag namespace."""

    def test_load_bearing_boot_fields_present(self) -> None:
        stripped = bootstrap._strip_secondary_module(_BOOTSTRAP_CLI)
        ns = _consumer_parser().parse_args(stripped)
        self.assertEqual(ns.cores, "-2")
        self.assertEqual(ns.max_memory, "-2G")
        self.assertEqual(ns.src_network, "/app/src-network")
        self.assertEqual(ns.secondary, "tcp://gw.cluster:4433")
        self.assertEqual(ns.secondary_id, "sec-0")


class ColdStartMainTests(unittest.TestCase):
    """End-to-end `main`: strip sys.argv → runpy the consumer module. The
    consumer is a throwaway module that records the `sys.argv` it observed."""

    def setUp(self) -> None:
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

    def test_main_runs_consumer_with_stripped_argv(self) -> None:
        argv = list(_BOOTSTRAP_CLI)
        argv[1] = self._mod_name  # point --secondary-module at the throwaway
        observed = self._run_main(argv)
        # observed[0] is runpy's module-file (alter_sys=True); the
        # load-bearing assertion is observed[1:] == the consumer argv.
        self.assertEqual(observed[1:], _CONSUMER_CLI)

    def test_missing_module_exits_nonzero(self) -> None:
        # `--secondary-module` is required; argparse raises SystemExit when
        # it is absent from the bootstrap argv.
        argv = ["--secondary", "tcp://x:1", "--secondary-id", "sec-0"]
        with self.assertRaises(SystemExit):
            bootstrap.main(argv)


# Child program for the crash-visibility end-to-end: load the shim under the
# package stub (it does relative imports), put a throwaway consumer module on
# sys.path, and run `main` with a real bootstrap argv. The PARENT asserts on
# the child's exit code + the durable `bootstrap-crash.log` — i.e. exactly
# what an operator (and the container runtime) observe.
_CRASH_CHILD_PROGRAM = textwrap.dedent(
    """
    import importlib.util, pathlib, sys, types

    pkg_root = pathlib.Path(sys.argv[1])
    consumer_dir = sys.argv[2]
    consumer_name = sys.argv[3]
    log_dir = sys.argv[4]

    pkg = types.ModuleType("dynamic_runner")
    pkg.__path__ = [str(pkg_root)]
    sys.modules["dynamic_runner"] = pkg

    def load(name):
        spec = importlib.util.spec_from_file_location(
            f"dynamic_runner.{name}", pkg_root / f"{name}.py"
        )
        mod = importlib.util.module_from_spec(spec)
        sys.modules[f"dynamic_runner.{name}"] = mod
        spec.loader.exec_module(mod)
        return mod

    load("_fault_dumps")
    bootstrap = load("_secondary_bootstrap")

    sys.path.insert(0, consumer_dir)
    bootstrap.main(
        [
            "--secondary-module",
            consumer_name,
            "--secondary",
            "tcp://gw.cluster:4433",
            "--secondary-id",
            "sec-0",
            "--full-log-dir",
            log_dir,
        ]
    )
    """
)


class CrashLogEndToEndTests(unittest.TestCase):
    """A consumer-module crash escaping the shim lands in
    `<full-log-dir>/bootstrap-crash.log` AND the process still exits
    non-zero with the traceback on stderr (the handler records, never
    swallows). A clean exit writes nothing."""

    def _run_child(self, consumer_body: str) -> tuple:
        import subprocess
        import tempfile

        self._tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self._tmp.cleanup)
        tmp = pathlib.Path(self._tmp.name)
        log_dir = tmp / "sec-0"
        log_dir.mkdir()
        consumer_name = "_bootstrap_crash_consumer"
        (tmp / f"{consumer_name}.py").write_text(consumer_body)
        proc = subprocess.run(
            [
                sys.executable,
                "-c",
                _CRASH_CHILD_PROGRAM,
                str(_PACKAGE_ROOT),
                str(tmp),
                consumer_name,
                str(log_dir),
            ],
            capture_output=True,
            text=True,
            timeout=60,
        )
        return proc, log_dir / "bootstrap-crash.log"

    def test_consumer_crash_writes_crash_log_and_exits_nonzero(self) -> None:
        proc, crash_log = self._run_child(
            'raise RuntimeError("boom-from-consumer")\n'
        )
        self.assertNotEqual(proc.returncode, 0, "crash must keep a non-zero exit")
        # The re-raise still reaches stderr unchanged (container-visible).
        self.assertIn("boom-from-consumer", proc.stderr)
        # AND the durable per-node record exists with the full traceback.
        self.assertTrue(
            crash_log.exists(), f"bootstrap-crash.log not created at {crash_log}"
        )
        body = crash_log.read_text()
        self.assertIn("Traceback (most recent call last):", body)
        self.assertIn("RuntimeError: boom-from-consumer", body)

    def test_clean_exit_writes_no_crash_log(self) -> None:
        proc, crash_log = self._run_child("import sys\nsys.exit(0)\n")
        self.assertEqual(proc.returncode, 0, f"stderr:\n{proc.stderr}")
        self.assertFalse(crash_log.exists(), "clean exit must not be recorded")

    def test_deliberate_nonzero_exit_writes_no_crash_log(self) -> None:
        # The asm-dataset 2212c136 shape: a secondary deliberately
        # `sys.exit(1)`s after the primary broadcast RunAborted. SystemExit
        # (any code) is raise-by-design — the exit code must survive
        # unchanged, but it is NOT a crash and must never be crash-dumped.
        proc, crash_log = self._run_child("import sys\nsys.exit(1)\n")
        self.assertEqual(proc.returncode, 1, f"stderr:\n{proc.stderr}")
        self.assertFalse(
            crash_log.exists(),
            "deliberate SystemExit(1) must not be filed as a bootstrap crash",
        )


if __name__ == "__main__":
    unittest.main()
