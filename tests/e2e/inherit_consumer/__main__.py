"""Framework entry point for the inherit-outputs e2e consumer.

Invoked as ``python -m tests.e2e.inherit_consumer ...``. Forwards the
parsed CLI to :func:`dynamic_runner.run`, which decides between local
/ single-process / multi-computer-local / SLURM dispatch based on
``--multi-computer``. Mirrors :mod:`tests.e2e.test_consumer.__main__`
verbatim with the consumer module name swapped — the secondary
``image_name`` is shared because the slurm-test-env container layout
is the same regardless of which Python topology dispatches.
"""

from __future__ import annotations

from dynamic_runner import TaskDeploymentSpec, run

from .task import InheritSyntheticTask


def main() -> None:
    run(
        task=InheritSyntheticTask(),
        deployment=TaskDeploymentSpec(
            secondary_module="tests.e2e.inherit_consumer",
            image_name="dynrunner-e2e-test",
        ),
        description=(
            "Synthetic dynamic_runner consumer for end-to-end testing "
            "of TaskDep(..., inherit_outputs=True). Exercises a three-"
            "task A->B->C chain where C reads B's AND A's published "
            "outputs via the framework's transitive-ancestry dispatch."
        ),
    )


if __name__ == "__main__":
    main()
