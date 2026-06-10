"""Local-mode output-delivery pin (``--multi-computer local``).

Runs the REAL dispatch end-to-end as a subprocess — network primary,
``build_subprocess_spawn``-spawned secondary subprocess, real workers —
with the payload-only consumer (``_localout_consumer``) whose worker
publishes one file per task into its framework-threaded ``--output``
directory.

The pin: those files must land in the OPERATOR's ``--output``
directory. On SLURM every secondary's publish target is the one
user-visible directory (the wrapper bind-mounts it at
``/app/out-network``); local mode shares the host filesystem, so the
secondaries' ``SecondaryConfig.output_dir`` must BE the operator's
resolved ``--output``. Pre-fix the secondary fell back to the
``<TMPDIR>/secondary-<id>-<pid>-out`` auto-resolution and every
artifact died with the per-secondary temp dir while the operator's
output directory stayed empty (consumer-validated at 2212c136).

Payload-only on purpose: ``uses_file_based_items = False`` keeps
StageFile traffic out of the run, so this pin exercises EXACTLY the
output-delivery seam and cannot be confounded by staging behaviour.

Requires the built extension (``maturin develop``) — importorskip-gated
like the other integration tests in this directory.
"""

from __future__ import annotations

import os
import subprocess
import sys
from pathlib import Path

import pytest

pytest.importorskip(
    "dynamic_runner",
    reason=(
        "dynamic_runner not installed; run `maturin develop --release` "
        "in this worktree first."
    ),
)

from dynamic_runner.tests._localout_consumer import output_filename  # noqa: E402


_NUM_TASKS = 2
# Generous wallclock cap: the run itself takes a few seconds; the cap
# only exists so a wedged dispatch fails the test instead of hanging
# the suite.
_DISPATCH_TIMEOUT_S = 180


def test_local_multi_computer_publishes_into_operator_output(tmp_path: Path) -> None:
    source = tmp_path / "src"
    output = tmp_path / "out"
    staging = tmp_path / "staging"
    source.mkdir()
    staging.mkdir()

    env = os.environ.copy()
    # The worker-publish staging contract (same env the SLURM wrapper /
    # e2e harness use): workers stage under this root and publish out
    # of it. Inherited dispatcher -> secondary -> worker.
    env["DYNRUNNER_PUBLISH_SRC_ROOT"] = str(staging)

    argv = [
        sys.executable,
        "-m",
        "dynamic_runner.tests._localout_consumer",
        "--multi-computer",
        "local",
        "--source",
        str(source),
        "--output",
        str(output),
        "--jobs",
        "1",
        "--cores",
        "1",
        "--num-tasks",
        str(_NUM_TASKS),
    ]
    proc = subprocess.run(
        argv,
        env=env,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        timeout=_DISPATCH_TIMEOUT_S,
    )
    tail = "\n".join(proc.stdout.splitlines()[-60:])
    assert proc.returncode == 0, (
        f"local dispatch exited {proc.returncode}; last lines:\n{tail}"
    )

    missing = [
        name
        for name in (output_filename(i) for i in range(_NUM_TASKS))
        if not (output / name).is_file()
    ]
    assert not missing, (
        f"published artifacts missing from the operator --output "
        f"({output}): {missing}; the secondary published into a "
        f"per-secondary temp dir instead. Dispatch tail:\n{tail}"
    )
