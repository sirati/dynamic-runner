"""Framework entry point for the 3-phase barrier-false e2e consumer.

Invoked as ``python -m tests.e2e.test_consumer_barrier_540 ...``.
Forwards the command line to :func:`dynamic_runner.cli_main`, which
parses the framework + task flags and decides between local /
single-process / multi-computer-local / SLURM dispatch based on
``--multi-computer``.
"""

from __future__ import annotations

from dynamic_runner import TaskDeploymentSpec, cli_main

from .task import BarrierConsumerTask


def main() -> None:
    cli_main(
        BarrierConsumerTask(),
        deployment=TaskDeploymentSpec(
            secondary_module="tests.e2e.test_consumer_barrier_540",
            image_name="dynrunner-e2e-test",
        ),
        description=(
            "Three-phase synthetic consumer exercising PhaseSpec.barrier=False "
            "on the middle phase. Used by the slurm-test-env test-540 "
            "assertion harness."
        ),
    )


if __name__ == "__main__":
    main()
