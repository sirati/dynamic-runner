"""Synthetic 3-phase consumer for the PhaseSpec.barrier=False e2e test.

Single concern: a workload whose phase topology lets the slurm-test-env
e2e harness assert the barrier feature shipped under #540 behaves as
declared.

Topology::

    phase_a (barrier=True default, 2 slow tasks)
      └── phase_b (barrier=False, 5 tasks, depends_on=phase_a)
            └── phase_c (barrier=True default, 5 tasks, depends_on=phase_b)

The ordering signal the test asserts on:

* phase_b's tasks dispatch WHILE phase_a's tasks are still running
  (proves the no-barrier opt-in actually lifts the implicit
  phase-A-completes-before-phase-B-starts gate).
* phase_c's tasks dispatch ONLY AFTER phase_b is drained (proves
  default barrier=True still gates downstream dispatch).
* No ``SpawnError::BarrierViolation`` is surfaced for this valid
  configuration (the runtime-spawn interlock accepts every task because
  phase_b is in the no-barrier set and phase_c's predecessor phase_b
  reaches Drained|Done before any phase_c task is spawned).

Workers sleep on phase_a (``DYNRUNNER_TEST540_PHASE_A_SLEEP_S``,
default 5s) so the dispatch overlap is observable in the
primary log's wall-clock timestamps.
"""

from .task import BarrierConsumerTask

__all__ = ["BarrierConsumerTask"]
