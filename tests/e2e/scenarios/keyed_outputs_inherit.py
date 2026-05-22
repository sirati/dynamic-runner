"""Scenario: ``TaskDep(inherit_outputs=True)`` round-trip A->B->C.

Single concern: assert the framework's transitive-ancestry dispatch
exposes A's published outputs to C even though the direct A->C edge
does not exist (A is only reachable through B). The companion plain
A->B scenario (``keyed_outputs.py``) covers the direct-predecessor
read; this one is the orthogonal test of the inherit flag.

Topology (declared in :mod:`tests.e2e.inherit_consumer.task`)::

    phase-a:  a                    (no deps, publishes "nonce-a")
    phase-b:  b   depends_on a     (publishes "nonce-b",
                                    asserts predecessor_outputs[a])
    phase-c:  c   depends_on b
                  AND TaskDep("a", inherit_outputs=True)
                  asserts predecessor_outputs has BOTH b AND a

C is the load-bearing task: without the
``TaskDep("a", inherit_outputs=True)`` edge on the consumer side and
the framework's transitive-ancestry walker on the primary side, C
would only see ``b`` in its ``predecessor_outputs`` and the worker
would raise ``NonRecoverableError`` ("missing 'a' key"). The
scenario's exit-zero gate is therefore equivalent to "the entire
publish_string -> result_data -> cluster cache -> dispatcher-with-
inherit-walk -> predecessor_outputs_json chain works end-to-end".

Why a separate consumer
-----------------------

The plain ``test_consumer`` ships a 2-phase produce/consume topology.
Extending it to a 3-task A->B->C chain when ``--keyed-outputs-inherit``
is set would force every dispatch path in that consumer
(``get_phases`` / ``discover_items`` / ``build_worker_command_args`` /
the worker's ``handle`` dispatch) to branch on the flag, violating
one-concern. A separate :mod:`tests.e2e.inherit_consumer` owns the
3-task topology declaratively; this scenario merely points the
dispatch builder at it.
"""

from __future__ import annotations

import tempfile
from pathlib import Path

from ._assertions import assert_files_present
from ._base import (
    DispatchEnv,
    DispatchPaths,
    Scenario,
    ScenarioPlan,
    ScenarioResult,
)
from ._dispatch import build_dispatch_argv


# Three tasks form the A->B->C chain. The constant lives here (not in
# the consumer) because the staging helper below uses it to lay down
# the matching ``input-{a|b|c}.txt`` files.
_TASK_IDS: tuple[str, ...] = ("a", "b", "c")


def _stage_inherit_inputs(tmp_root: Path) -> DispatchPaths:
    """Materialise ``input-{a|b|c}.txt`` plus the four staging dirs.

    The shared :func:`tests.e2e.scenarios._staging.stage_inputs` lays
    down ``input-{i}.txt`` files keyed by an integer index; the
    inherit consumer's :func:`task._input_filename` expects task-id
    keyed names instead. This helper owns the inherit-consumer's input
    naming convention; ``stage_inputs`` keeps owning the legacy index
    convention.
    """
    source = Path(tempfile.mkdtemp(prefix="src-", dir=tmp_root))
    output = Path(tempfile.mkdtemp(prefix="out-", dir=tmp_root))
    publish_src = Path(tempfile.mkdtemp(prefix="pubsrc-", dir=tmp_root))
    publish_dst = Path(tempfile.mkdtemp(prefix="pubdst-", dir=tmp_root))
    for task_id in _TASK_IDS:
        (source / f"input-{task_id}.txt").write_bytes(
            f"input-{task_id}-payload\n".encode()
        )
    return DispatchPaths(
        source=source,
        output=output,
        publish_src=publish_src,
        publish_dst=publish_dst,
    )


class KeyedOutputsInheritScenario(Scenario):
    name = "keyed-outputs-inherit"
    description = (
        "Three-task A->B->C inherit_outputs round-trip: C asserts "
        "predecessor_outputs carries BOTH the direct predecessor B "
        "AND the transitive ancestor A via "
        "TaskDep('a', inherit_outputs=True). Failure mode: worker "
        "raises NonRecoverableError and dispatch exits non-zero."
    )
    requires = ()

    def prepare(
        self, env: DispatchEnv, tmp_root: Path
    ) -> list[ScenarioPlan]:
        paths = _stage_inherit_inputs(tmp_root)
        argv = build_dispatch_argv(
            env=env,
            source=paths.source,
            output=paths.output,
            num_tasks=len(_TASK_IDS),
            # Point at the dedicated inherit consumer rather than the
            # canonical 2-phase one. The shared dispatch builder
            # threads ``--num-tasks`` regardless; the inherit consumer
            # accepts-and-ignores it because its topology is fixed.
            consumer_module="tests.e2e.inherit_consumer",
        )
        return [ScenarioPlan(argv=argv, paths=paths)]

    def assert_outputs(
        self, env: DispatchEnv, results: list[ScenarioResult]
    ) -> tuple[bool, list[str]]:
        del env
        result = results[0]
        # If C tripped its inherit-outputs assertion, the worker raises
        # ``NonRecoverableError`` -> the framework surfaces a non-zero
        # exit BEFORE C's publish runs -> ``c.out`` is absent under the
        # publish destination. The single file-presence check therefore
        # covers BOTH "all three tasks ran to publish" AND
        # "C's predecessor_outputs carried A's nonce" (the latter by
        # transitive failure mode, identical to the keyed-outputs
        # scenario's gate shape).
        expected = [f"{task_id}.out" for task_id in _TASK_IDS]
        return assert_files_present(result.plan.paths.publish_dst, expected)


SCENARIO = KeyedOutputsInheritScenario()
