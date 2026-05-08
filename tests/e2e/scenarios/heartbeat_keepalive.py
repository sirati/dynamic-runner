"""Scenario: 10-minute sustained run, assert no heartbeat misses.

Single concern: the framework's heartbeat / keepalive code path
under sustained load.

Setup
-----

Many small tasks (~120) so the run keeps secondaries continuously
busy for ~10 minutes. The exact wall-time depends on the cluster's
throughput but the assertion is content-based, not time-based:
after the dispatch finishes, we grep the log file for
``keepalive miss`` and ``heartbeat miss`` strings. Either appearing
is a fail.

Why this is its own scenario
----------------------------

A short run never stresses the heartbeat machinery — secondaries
finish their work before the keepalive interval matters. To exercise
the relevant code paths we need the dispatch to last long enough
that several keepalive intervals tick by while workers are alive.
"""

from __future__ import annotations

import re
from pathlib import Path

from ._assertions import assert_files_present, expected_canonical_outputs
from ._base import DispatchEnv, Scenario, ScenarioPlan, ScenarioResult
from ._dispatch import build_dispatch_argv
from ._staging import stage_inputs


_NUM_TASKS_PER_PHASE = 60  # 120 total — long enough to span keepalive intervals


# Match both the manager-side miss warnings and the worker-side
# loss reports. Conservative: any line containing either substring
# triggers the assertion.
_MISS_PATTERN = re.compile(
    r"(keepalive\s+miss|heartbeat\s+miss|keepalive\s+lost|"
    r"heartbeat\s+lost)",
    re.IGNORECASE,
)


class HeartbeatKeepaliveScenario(Scenario):
    name = "heartbeat-keepalive"
    description = (
        f"Sustained {2 * _NUM_TASKS_PER_PHASE}-task run; asserts the "
        "dispatch log contains no keepalive/heartbeat miss warnings."
    )
    requires = ()

    def prepare(
        self, env: DispatchEnv, tmp_root: Path
    ) -> list[ScenarioPlan]:
        paths = stage_inputs(tmp_root, _NUM_TASKS_PER_PHASE)
        argv = build_dispatch_argv(
            env=env,
            source=paths.source,
            output=paths.output,
            num_tasks=_NUM_TASKS_PER_PHASE,
            jobs=env.workers,
        )
        return [ScenarioPlan(argv=argv, paths=paths)]

    def assert_outputs(
        self, env: DispatchEnv, results: list[ScenarioResult]
    ) -> tuple[bool, list[str]]:
        del env
        result = results[0]
        ok_present, missing = assert_files_present(
            result.plan.paths.publish_dst,
            expected_canonical_outputs(_NUM_TASKS_PER_PHASE),
        )
        if not ok_present:
            return (False, missing)

        misses = _grep_misses(result.log_file)
        if misses:
            return (
                False,
                [f"heartbeat/keepalive miss in log: {line}" for line in misses],
            )
        return (True, [])


def _grep_misses(log_file: Path) -> list[str]:
    """Return matching lines for the miss pattern.

    Bounded to first 50 matches — any failure surfaces immediately;
    we don't need a megabyte of repeated warnings in the report.
    """
    out: list[str] = []
    try:
        with log_file.open("r", encoding="utf-8", errors="replace") as f:
            for line in f:
                if _MISS_PATTERN.search(line):
                    out.append(line.strip())
                    if len(out) >= 50:
                        break
    except OSError as e:
        return [f"<could not read log: {e}>"]
    return out


SCENARIO = HeartbeatKeepaliveScenario()
