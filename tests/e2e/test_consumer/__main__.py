"""Framework entry point for the synthetic e2e consumer.

Invoked as ``python -m tests.e2e.test_consumer ...``. Forwards the
command line to :func:`dynamic_runner.cli_main`, which parses the
framework + task flags and decides between local / single-process /
multi-computer-local / SLURM dispatch based on ``--multi-computer``.
"""

from __future__ import annotations

from dynamic_runner import TaskDeploymentSpec, cli_main

from .task import SyntheticTask


def main() -> None:
    cli_main(
        SyntheticTask(),
        deployment=TaskDeploymentSpec(
            secondary_module="tests.e2e.test_consumer",
            image_name="dynrunner-e2e-test",
        ),
        description=(
            "Synthetic dynamic_runner consumer for end-to-end testing. "
            "Exercises phase deps, intra/cross-phase task deps, "
            "task.publish, and skip-existing idempotency."
        ),
    )


if __name__ == "__main__":
    main()
