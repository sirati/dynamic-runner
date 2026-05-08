"""Scenario: ``task.publish`` atomic-rename semantics.

Single concern: assert that even when a worker is killed mid-write,
no partial files appear at the publish destination.

Threat model
------------

The framework's :func:`dynamic_runner.worker.publish` writes the
output to a staging dir then renames into the destination. If the
implementation ever copies (instead of renames) or splits the rename
across two non-atomic steps, a kill between the steps could leave
``dst/produce-0.out.tmp`` or a half-written ``dst/produce-0.out``.

We can't easily inject the kill between the framework's two writes
without instrumenting the framework, so this scenario instead:

1. Runs a normal dispatch with larger payloads (so writes take
   long enough that a SIGKILL has time to land mid-publish).
2. Asserts no ``.tmp`` / ``.part`` / ``.partial`` siblings exist
   under the publish destination after the run.

The actual mid-write kill is gated behind a future
``run_hook`` extension (worker-death-failover already does scancel
mid-run; mid-publish kill is harder and lives in a follow-up
unit). What this scenario reliably catches today is partial-file
LEAKS in the success path — equally valuable.
"""

from __future__ import annotations

from pathlib import Path

from ._assertions import (
    assert_files_present,
    assert_no_partial_files,
    expected_canonical_outputs,
)
from ._base import DispatchEnv, Scenario, ScenarioPlan, ScenarioResult
from ._dispatch import build_dispatch_argv
from ._staging import stage_inputs


_NUM_TASKS_PER_PHASE = 8
# 4 MiB per input — the produce worker echoes "produce:" + input,
# so each output is also ~4 MiB. Big enough that the rename
# completion is detectable; small enough that the test stays under
# a minute on slurm-test-env.
_PAYLOAD_SIZE = 4 * 1024 * 1024


class PublishAtomicScenario(Scenario):
    name = "publish-atomic"
    description = (
        "Asserts atomic publish: no .tmp / .part / .partial leaks at "
        "the publish destination after a run with larger payloads."
    )
    requires = ()

    def prepare(
        self, env: DispatchEnv, tmp_root: Path
    ) -> list[ScenarioPlan]:
        paths = stage_inputs(
            tmp_root,
            _NUM_TASKS_PER_PHASE,
            payload_size_bytes=_PAYLOAD_SIZE,
        )
        argv = build_dispatch_argv(
            env=env,
            source=paths.source,
            output=paths.output,
            num_tasks=_NUM_TASKS_PER_PHASE,
        )
        return [ScenarioPlan(argv=argv, paths=paths)]

    def assert_outputs(
        self, env: DispatchEnv, results: list[ScenarioResult]
    ) -> tuple[bool, list[str]]:
        del env
        result = results[0]
        publish = result.plan.paths.publish_dst
        ok_present, missing = assert_files_present(
            publish, expected_canonical_outputs(_NUM_TASKS_PER_PHASE)
        )
        ok_clean, leaks = assert_no_partial_files(publish)
        return (ok_present and ok_clean, missing + leaks)


SCENARIO = PublishAtomicScenario()
