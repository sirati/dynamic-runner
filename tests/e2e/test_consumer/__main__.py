"""Framework entry point for the synthetic e2e consumer.

Invoked as ``python -m tests.e2e.test_consumer ...``. Forwards the
parsed CLI to :func:`dynamic_runner.run`, which decides between local
/ single-process / multi-computer-local / SLURM dispatch based on
``--multi-computer``.
"""

from __future__ import annotations

from dynamic_runner import TaskDeploymentSpec, run

from .task import SyntheticTask


def main() -> None:
    run(
        task=SyntheticTask(),
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
