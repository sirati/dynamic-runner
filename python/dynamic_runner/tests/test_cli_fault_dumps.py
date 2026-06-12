"""The CLI dispatch chokepoint installs the SIGUSR1 fault handler.

`dynamic_runner.run.dispatch` is the single chokepoint both framework entry
points (`run` / `cli_main`) pass through. `_secondary_bootstrap.main` installs
`enable_fault_dumps` for a mesh-launched secondary, but the CLI main paths
(submitter, late-joiner observer, local) never run that shim — so before this
fix the documented operator `kill -USR1` (frame dump) landed on SIGUSR1's
default disposition and KILLED the main process.

This pins the fix: after `dispatch` runs, SIGUSR1 dumps a traceback and the
process SURVIVES (faulthandler's `chain=False` returns control without
terminating). Run in a CHILD process — `faulthandler`'s SIGUSR1 handler is
PROCESS-GLOBAL and must never leak into the unittest runner — and stub the
route body so no Rust dispatch path is reached.

unittest-based + direct-file module loading so the suite runs in a bare
`nix develop` shell without the compiled `_native` extension (the same
convention as `test_fault_dumps.py` / `test_cli_api.py`).
"""

from __future__ import annotations

import os
import pathlib
import signal
import subprocess
import sys
import tempfile
import textwrap
import unittest


_PACKAGE_ROOT = pathlib.Path(__file__).resolve().parent.parent


# Child program: load `run.py` under a package stub, stub `_dispatch_route`
# (so dispatch's mode-selection never touches Rust), build a minimal args
# namespace carrying `_boot_argv` with `--full-log-dir`, call the real
# `dispatch`, then raise SIGUSR1 at ourselves. Surviving to `sys.exit(0)`
# proves `dispatch` installed the non-fatal faulthandler — pre-fix the
# default disposition would terminate here. The parent then asserts the
# dump landed in the `--full-log-dir` target (the same argv resolution the
# secondary bootstrap uses).
_CHILD_PROGRAM = textwrap.dedent(
    """
    import argparse, importlib.util, os, pathlib, signal, sys, types

    pkg_root = pathlib.Path(sys.argv[1])
    log_dir = sys.argv[2]

    pkg = types.ModuleType("dynamic_runner")
    pkg.__path__ = [str(pkg_root)]
    sys.modules["dynamic_runner"] = pkg

    def _load(name, relpath):
        fullname = "dynamic_runner." + name
        spec = importlib.util.spec_from_file_location(fullname, pkg_root / relpath)
        mod = importlib.util.module_from_spec(spec)
        sys.modules[fullname] = mod
        spec.loader.exec_module(mod)
        return mod

    run_mod = _load("run", "run.py")

    # Stub the route body: dispatch's only job under test is the faulthandler
    # install at its chokepoint; the mode selection must not reach Rust.
    run_mod._dispatch_route = lambda task, args, deployment: None

    args = argparse.Namespace(_boot_argv=["--full-log-dir", log_dir])
    run_mod.dispatch(object(), args, None)

    # Operator `kill -USR1`: faulthandler dumps synchronously and (chain=False)
    # returns control WITHOUT terminating. A killed / non-zero exit would mean
    # dispatch did NOT arm the handler and SIGUSR1 hit its default (terminate)
    # — exactly the hole this guards.
    os.kill(os.getpid(), signal.SIGUSR1)
    sys.exit(0)
    """
)


@unittest.skipUnless(
    hasattr(signal, "SIGUSR1"), "SIGUSR1 not available on this platform"
)
class CliDispatchFaultDumpTests(unittest.TestCase):
    """`dispatch` arms faulthandler so an operator SIGUSR1 dumps, not kills."""

    def test_dispatch_installs_sigusr1_faulthandler(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            log_dir = os.path.join(tmp, "setup")
            os.makedirs(log_dir, exist_ok=True)
            proc = subprocess.run(
                [sys.executable, "-c", _CHILD_PROGRAM, str(_PACKAGE_ROOT), log_dir],
                capture_output=True,
                text=True,
                timeout=30,
            )
            # Survival IS the headline: pre-fix SIGUSR1 terminates the child.
            self.assertEqual(
                proc.returncode,
                0,
                f"child exited non-zero (SIGUSR1 not handled by dispatch?); "
                f"stderr:\n{proc.stderr}",
            )
            # The dump resolves to `<full-log-dir>/faulthandler.log` via the
            # SAME argv path the secondary bootstrap uses.
            dump_path = pathlib.Path(log_dir) / "faulthandler.log"
            self.assertTrue(
                dump_path.exists(),
                f"faulthandler dump file not created at {dump_path}",
            )
            body = dump_path.read_text()
            self.assertIn("Current thread", body)
            self.assertIn('File "', body)


if __name__ == "__main__":
    unittest.main()
