"""Synthetic dynamic_runner consumer used by ``tests/e2e/run_e2e.py``.

The job of this package is NOT to do useful work — it is to exercise
the framework's feature surface end-to-end through the slurm-test-env
so that a real-world dispatch flow keeps working post-migration.

Concerns covered, one each:

* :class:`SyntheticTask` declares two phases (``produce`` and
  ``consume``) with an explicit ``PhaseSpec(depends_on=...)`` edge,
  letting the framework's phase-state machine drive the cross-phase
  barrier.
* :meth:`SyntheticTask.discover_items` builds tasks that exercise both
  intra-phase ``task_depends_on`` (siblings inside ``produce``) and
  cross-phase ``task_depends_on`` (``consume`` tasks naming a specific
  ``produce`` task).
* The worker module (``tests.e2e.test_consumer.worker``) calls
  :func:`dynamic_runner.worker.publish` for each output, exercising the
  atomic stage→destination contract.
* The worker checks for the producer's output before it does its own
  work — failing loud if a downstream task ran before its declared
  prerequisite finished publishing.

The ``__main__`` module is the framework entry point — running
``python -m tests.e2e.test_consumer`` parses the standard
``dynamic_runner`` CLI and dispatches via the chosen mode.
"""

from .task import SyntheticTask

__all__ = ["SyntheticTask"]
