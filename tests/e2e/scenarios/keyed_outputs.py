"""Scenario: keyed task outputs across a cross-phase A → B edge.

Single concern: assert ``Task.publish_string`` on the producer plus
``Task.predecessor_outputs`` on the consumer round-trip cleanly through
the framework's wire stack — producer ``DoneResponse.result_data`` →
primary ``ClusterState.task_outputs`` cache → dispatcher
``TaskAssignment.predecessor_outputs`` → secondary
``ProcessTaskCommand.predecessor_outputs_json`` → worker
``Task.predecessor_outputs`` dict.

What the scenario gates
-----------------------

Each ``produce-{i}`` task calls
``Task.publish_string("nonce", f"nonce-{i}")``. Each ``consume-{i}``
task reads
``Task.predecessor_outputs["produce-{i}"]["nonce"]["value"]`` and
``["kind"]`` and asserts both match the expected
``("nonce-{i}", "inline")``. A mismatch — or a missing key at either
level — raises ``NonRecoverableError`` in the consumer worker, which
the framework surfaces as a non-zero exit. The scenario's exit-zero
plus the published-files check is the gate.

The producer-output file assertion still runs alongside the keyed
outputs check; it would also fail if the consumer aborted before
publishing. This is intentional belt-and-braces — a regression that
masked the keyed-outputs assertion error somehow (silent swallow at
the worker shim) would still trip on the missing ``consume-{i}.out``
under the publish destination.

Three-step ``A → B → C`` ``inherit_outputs`` follow-up
------------------------------------------------------

The plan's wider Phase-7c outline also covers an inherit-outputs case
where a third hop pulls A's outputs through B's edge. That case is
deferred from this scenario: at the time of writing the Python-side
``TaskInfo.task_depends_on`` is a ``tuple[str, ...]`` and the PyO3
extractor lifts each entry into a ``TaskDep`` with the legacy
``inherit_outputs=False`` (see
``crates/dynrunner-pyo3/src/pytypes/extract.rs::extract_binaries``).
The Python API surface today does NOT expose the
``inherit_outputs=True`` flag to consumer code; reaching the
transitive-ancestry dispatch path requires a framework-side API
extension (a Python ``TaskDep`` dataclass, or a tuple-of-dict shape
the PyO3 extractor decodes). That follow-up belongs in a separate
plan item, not the scenario suite — adding it here would require
mutating framework code, which the Phase-7c brief explicitly forbids.
"""

from __future__ import annotations

from pathlib import Path

from ._assertions import assert_files_present, expected_canonical_outputs
from ._base import DispatchEnv, Scenario, ScenarioPlan, ScenarioResult
from ._dispatch import build_dispatch_argv
from ._staging import stage_inputs


# Small N keeps the cross-phase write-then-read round-trip tight (each
# produce/consume pair exercises one full keyed-outputs hop). Bigger N
# would re-exercise the same code path without surfacing extra coverage.
_NUM_TASKS_PER_PHASE = 4


class KeyedOutputsScenario(Scenario):
    name = "keyed-outputs"
    description = (
        "Cross-phase keyed-outputs round-trip: produce-{i} commits an "
        "inline nonce via Task.publish_string, consume-{i} asserts the "
        "value-and-kind via Task.predecessor_outputs. Failure mode: the "
        "worker raises NonRecoverableError and dispatch exits non-zero."
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
            # ``--keyed-outputs`` opts the consumer into the
            # publish_string + predecessor_outputs assertion path
            # (see tests.e2e.test_consumer.task::add_task_arguments).
            extra_args=("--keyed-outputs",),
        )
        return [ScenarioPlan(argv=argv, paths=paths)]

    def assert_outputs(
        self, env: DispatchEnv, results: list[ScenarioResult]
    ) -> tuple[bool, list[str]]:
        del env
        result = results[0]
        # If any consume worker tripped its keyed-outputs assertion,
        # it raised NonRecoverableError → the framework surfaces a
        # non-zero exit before the consume publish ever happens →
        # the ``consume-{i}.out`` file is absent here. So this single
        # check covers both "files published" AND "keyed-outputs
        # round-trip succeeded" (the latter by transitive failure mode).
        return assert_files_present(
            result.plan.paths.publish_dst,
            expected_canonical_outputs(_NUM_TASKS_PER_PHASE),
        )


SCENARIO = KeyedOutputsScenario()
