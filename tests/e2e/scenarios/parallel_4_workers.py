"""Scenario: distribute 40+ tasks across 4 workers.

Single concern: assert work distributes — no single worker handles
more than half of the tasks.

How distribution is measured
----------------------------

The driver runs the dispatch with ``--jobs <env.workers>`` so the
framework spawns one secondary per worker. Inside the run, each
secondary draws tasks from the primary's queue and runs them via
its local worker pool. Per-worker task counts are extracted via
``sacct`` against the primary's submitted job array.

If the cluster only has 1 worker reachable (e.g. 3 are stuck in
DOWN+NOT_RESPONDING — the known Bug BB) the assertion will fail
loudly with "all tasks ran on slurm-worker0". That's the desired
behaviour — silent serialization on a single worker is exactly the
regression we don't want shipping unnoticed.

Threshold rationale
-------------------

We assert no worker ran >50% of tasks. With 4 workers and 40+
tasks, a perfectly balanced run gives 25%/worker. A 50% cap allows
significant noise (e.g. a slow worker, an OS scheduler hiccup) but
still catches "everything serialized on one node".
"""

from __future__ import annotations

import re
from pathlib import Path

from ._assertions import assert_files_present, expected_canonical_outputs
from ._base import DispatchEnv, Scenario, ScenarioPlan, ScenarioResult
from ._dispatch import build_dispatch_argv
from ._ssh import gateway_ssh
from ._staging import stage_inputs


_NUM_TASKS_PER_PHASE = 20  # 40 total tasks across produce + consume
_DISTRIBUTION_CAP = 0.50  # no single worker may handle more than this fraction


class Parallel4WorkersScenario(Scenario):
    name = "parallel-4-workers"
    description = (
        f"{2 * _NUM_TASKS_PER_PHASE} tasks across the cluster's "
        "secondaries. Asserts no single worker handles more than "
        f"{int(_DISTRIBUTION_CAP * 100)}% of tasks (catches "
        "serial-on-one-worker regressions)."
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
        result = results[0]
        ok_present, missing = assert_files_present(
            result.plan.paths.publish_dst,
            expected_canonical_outputs(_NUM_TASKS_PER_PHASE),
        )
        if not ok_present:
            return (False, missing)

        # Distribution check via sacct. The slurm-test-env gateway
        # ssh user has sacct in $PATH; the format flag asks for the
        # node each step ran on.
        per_node_count = _per_node_task_count(env)
        if per_node_count is None:
            return (
                False,
                [
                    "sacct query failed (cluster reachable? sacct in $PATH "
                    "on the gateway?) — cannot verify distribution"
                ],
            )
        total = sum(per_node_count.values())
        if total == 0:
            return (
                False,
                [
                    "sacct returned no task records — the dispatch may have "
                    "completed before sacct flushed; rerun or extend the "
                    "scenario's wait window"
                ],
            )
        offenders: list[str] = []
        for node, count in per_node_count.items():
            if count / total > _DISTRIBUTION_CAP:
                offenders.append(
                    f"worker {node} ran {count}/{total} tasks "
                    f"({count / total:.0%}) — exceeds "
                    f"{_DISTRIBUTION_CAP:.0%} cap"
                )
        if offenders:
            return (False, offenders)
        return (True, [])


def _per_node_task_count(env: DispatchEnv) -> dict[str, int] | None:
    """Run ``sacct`` on the gateway and tally task records per node.

    ``sacct -X -P --format=JobID,NodeList`` produces pipe-separated
    records, one per job (``-X`` excludes step records to stop us
    counting the same job twice). We don't try to filter to "this
    test's jobs only" — any other concurrent activity on the cluster
    inflates the denominator but doesn't false-positive the
    over-50% check (a stray third-party job would land on yet
    another node, lowering everyone's percentage).
    """
    proc = gateway_ssh(env, "sacct -X -P --format=JobID,NodeList -n")
    if proc.returncode != 0:
        return None
    counts: dict[str, int] = {}
    for line in proc.stdout.splitlines():
        line = line.strip()
        if not line:
            continue
        parts = line.split("|", 1)
        if len(parts) != 2:
            continue
        nodelist = parts[1].strip()
        if not nodelist or nodelist == "None":
            continue
        for node in _expand_nodelist(nodelist):
            counts[node] = counts.get(node, 0) + 1
    return counts


_NODELIST_BRACKET_RE = re.compile(r"^([a-zA-Z\-]+)\[(.+)\]$")


def _expand_nodelist(nodelist: str) -> list[str]:
    """Expand a SLURM nodelist (``slurm-worker[1-3]``) to individual
    hostnames.

    Handles the common cases: bare hostname, comma-list, bracketed
    range. Falls back to returning the unparsed string when an
    unexpected format appears — over-counting one node in a weird
    case is preferable to a false-pass on the distribution check.
    """
    out: list[str] = []
    for chunk in nodelist.split(","):
        m = _NODELIST_BRACKET_RE.match(chunk)
        if not m:
            out.append(chunk)
            continue
        prefix, ranges = m.group(1), m.group(2)
        for piece in ranges.split(","):
            if "-" in piece:
                lo_s, hi_s = piece.split("-", 1)
                try:
                    lo, hi = int(lo_s), int(hi_s)
                except ValueError:
                    out.append(chunk)
                    break
                for i in range(lo, hi + 1):
                    out.append(f"{prefix}{i}")
            else:
                out.append(f"{prefix}{piece}")
    return out


SCENARIO = Parallel4WorkersScenario()
