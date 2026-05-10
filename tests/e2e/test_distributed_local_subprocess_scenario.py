"""Unit tests for the distributed-local-subprocess scenario.

Single concern: pin the scenario's API-surface contract so a future
refactor of the dispatch builder, the consumer module name, or the
``--multi-computer local`` flag set cannot silently change the argv
shape this regression gate produces.

Why a unit test in addition to the live e2e run:
the live run exercises the full dispatch (primary spawn + secondary
subprocess + framework auto-stage) and is the authoritative gate, but
it requires a working environment for the local-subprocess spawn path.
A unit test that drives ``prepare()`` and inspects the argv pins the
scenario's intent at the API surface, so even when the live run is
skipped the regression sensitivity stays under version control.

Why under ``tests/e2e/`` rather than ``python/dynamic_runner/tests/``:
this is e2e-driver-side scenario coverage, not framework code; mirrors
the placement of ``test_worker_leak_gate.py``.
"""

from __future__ import annotations

import sys
import tempfile
from pathlib import Path


# Bring repo root onto sys.path so the absolute import works whether
# pytest is invoked from the repo root or from this dir. Mirrors the
# dance ``run_e2e.py`` itself does.
_REPO_ROOT = Path(__file__).resolve().parents[2]
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

from tests.e2e.scenarios._base import DispatchEnv  # noqa: E402
from tests.e2e.scenarios.distributed_local_subprocess import (  # noqa: E402
    SCENARIO,
)


def _env(mode: str = "slurm") -> DispatchEnv:
    """Minimal DispatchEnv. The scenario must override ``mode`` to
    ``local`` regardless of what the harness passes — that's exactly
    the property this test pins.
    """
    return DispatchEnv(
        instance_id="e2e",
        ssh_port=2222,
        slurm_root_folder="/home/e2e-user/dynrunner-e2e",
        workers=4,
        mode=mode,
        ssh_user="e2e-user",
    )


def test_scenario_name_and_registry_match() -> None:
    """Module's ``SCENARIO`` declares the expected name."""
    assert SCENARIO.name == "distributed-local-subprocess"


def test_prepare_emits_one_plan_with_local_dispatch_flags() -> None:
    """``prepare()`` returns a single plan whose argv contains
    ``--multi-computer local --jobs 2``.

    The exact ``--jobs`` value (2) matters: a ``--jobs 1`` argv would
    exercise the loop body but not the per-secondary fan-out shape
    that the staging-walk regression depends on; a future edit
    silently dropping the count to 1 would mask the bug under test.
    """
    with tempfile.TemporaryDirectory() as raw_tmp:
        tmp_root = Path(raw_tmp)
        # Drive prepare() under every harness mode the driver supports
        # (slurm / single-process / in-process) so the scenario's
        # mode-override behaviour is pinned independent of the
        # operator's wider --mode choice.
        for harness_mode in ("slurm", "single-process", "in-process"):
            plans = SCENARIO.prepare(_env(harness_mode), tmp_root)
            assert len(plans) == 1, (
                f"expected exactly one plan under harness mode "
                f"{harness_mode!r}, got {len(plans)}"
            )
            plan = plans[0]

            # Sanity check: the canonical consumer module is on the argv.
            joined = " ".join(plan.argv)
            assert "tests.e2e.test_consumer" in joined, (
                f"expected canonical consumer module in argv, got {plan.argv!r}"
            )

            # Pin the local-mode flag pair as a contiguous pair, so a
            # future builder edit that splits them apart still flags
            # against this test.
            assert (
                "--multi-computer" in plan.argv
            ), f"missing --multi-computer flag in argv: {plan.argv!r}"
            mc_idx = plan.argv.index("--multi-computer")
            assert plan.argv[mc_idx + 1] == "local", (
                f"expected --multi-computer local, got "
                f"{plan.argv[mc_idx + 1]!r} in argv: {plan.argv!r}"
            )

            assert "--jobs" in plan.argv, (
                f"missing --jobs flag in argv: {plan.argv!r}"
            )
            jobs_idx = plan.argv.index("--jobs")
            assert plan.argv[jobs_idx + 1] == "2", (
                f"expected --jobs 2 to exercise per-secondary fan-out, "
                f"got --jobs {plan.argv[jobs_idx + 1]!r}"
            )


def test_prepare_inputs_are_staged_under_tmp_root() -> None:
    """The scenario's plan paths point inside the per-scenario tmp root.

    Pins the contract that the scenario does not reach for absolute
    paths outside the driver-allocated tmpdir (the driver owns
    cleanup; a scenario writing elsewhere would leak).
    """
    with tempfile.TemporaryDirectory() as raw_tmp:
        tmp_root = Path(raw_tmp)
        plans = SCENARIO.prepare(_env(), tmp_root)
        plan = plans[0]
        for path in (
            plan.paths.source,
            plan.paths.output,
            plan.paths.publish_src,
            plan.paths.publish_dst,
        ):
            assert tmp_root in path.parents, (
                f"path {path!r} is not under tmp_root {tmp_root!r}"
            )
