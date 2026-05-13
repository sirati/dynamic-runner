"""Unit tests for the source-already-staged scenario.

Single concern: pin the scenario's argv shape across the cross-mode
plans so a future builder/registry refactor cannot silently break the
``--source-already-staged`` discriminator coverage. Mirrors
``test_distributed_local_subprocess_scenario.py``'s pattern.

Why a unit test in addition to the live e2e run:
the live run requires an in-process distributed pipeline (cheap) plus
a local-subprocess pipeline (Python env contract); a unit test that
drives ``prepare()`` and inspects the argv pins the framework-CLI
contract independent of the env.
"""

from __future__ import annotations

import sys
import tempfile
from pathlib import Path


# Bring repo root onto sys.path so the absolute import works whether
# pytest is invoked from the repo root or from this dir. Same dance
# as the sibling distributed-local-subprocess test.
_REPO_ROOT = Path(__file__).resolve().parents[2]
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

from tests.e2e.scenarios._base import DispatchEnv  # noqa: E402
from tests.e2e.scenarios.source_already_staged import (  # noqa: E402
    SCENARIO,
)


def _env(mode: str = "slurm") -> DispatchEnv:
    """Minimal DispatchEnv. The scenario emits plans for
    ``single-process`` and ``local`` regardless of what the harness
    passes — pinned by the cross-mode test below.
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
    assert SCENARIO.name == "source-already-staged"


def test_prepare_emits_one_plan_per_mode() -> None:
    """``prepare()`` returns two plans, one per cross-mode arm.

    The scenario's value is parametrizing over the dispatch helpers
    (single-process and local subprocess) with the same setup-promote
    handshake. Dropping a mode silently would mask a regression that
    surfaced ONLY on the dropped variant.
    """
    with tempfile.TemporaryDirectory() as raw_tmp:
        tmp_root = Path(raw_tmp)
        for harness_mode in ("slurm", "single-process", "in-process"):
            plans = SCENARIO.prepare(_env(harness_mode), tmp_root)
            labels = sorted(p.label for p in plans)
            assert labels == ["local", "single-process"], (
                f"expected one plan each for single-process + local, "
                f"got labels={labels!r} under harness mode "
                f"{harness_mode!r}"
            )


def test_every_plan_carries_source_already_staged_flag() -> None:
    """The load-bearing CLI flag is on every plan's argv.

    Without ``--source-already-staged`` the framework's setup-promote
    discriminator (``required_setup_on_promote`` in
    ``PyPrimaryCoordinator``) never flips, and the scenario silently
    reverts to legacy bootstrap mode. The test guards against an
    accidental flag drop in the builder.
    """
    with tempfile.TemporaryDirectory() as raw_tmp:
        tmp_root = Path(raw_tmp)
        plans = SCENARIO.prepare(_env(), tmp_root)
        for plan in plans:
            assert "--source-already-staged" in plan.argv, (
                f"plan {plan.label!r} missing the load-bearing flag: "
                f"argv={plan.argv!r}"
            )
            idx = plan.argv.index("--source-already-staged")
            staged_path = plan.argv[idx + 1]
            # Path value must be the same dir as --source (the
            # single-host pipelines reuse the same dir for both
            # flags; see module docstring "Why both flags point at
            # the same dir").
            source_idx = plan.argv.index("--source")
            source_path = plan.argv[source_idx + 1]
            assert staged_path == source_path, (
                f"plan {plan.label!r}: --source-already-staged ({staged_path!r}) "
                f"must point at the same dir as --source ({source_path!r}) "
                f"for single-host pipelines"
            )


def test_each_plan_pins_multi_computer_mode() -> None:
    """Each plan's ``--multi-computer`` value matches its label.

    ``label='single-process'`` carries ``--multi-computer single-process``;
    ``label='local'`` carries ``--multi-computer local``. A future builder
    edit that misroutes a label to a different mode would mask which
    dispatch helper the framework actually exercises.
    """
    with tempfile.TemporaryDirectory() as raw_tmp:
        tmp_root = Path(raw_tmp)
        plans = SCENARIO.prepare(_env(), tmp_root)
        for plan in plans:
            assert "--multi-computer" in plan.argv, (
                f"plan {plan.label!r} missing --multi-computer in argv: "
                f"{plan.argv!r}"
            )
            mc_idx = plan.argv.index("--multi-computer")
            actual = plan.argv[mc_idx + 1]
            assert actual == plan.label, (
                f"plan {plan.label!r} carries --multi-computer "
                f"{actual!r} — label and mode must match"
            )


def test_inputs_are_staged_under_tmp_root() -> None:
    """The scenario's plan paths point inside the per-scenario tmp root.

    Mirrors the sibling scenario tests' cleanup-safety pin: a scenario
    writing outside the driver-allocated tmpdir would leak.
    """
    with tempfile.TemporaryDirectory() as raw_tmp:
        tmp_root = Path(raw_tmp)
        plans = SCENARIO.prepare(_env(), tmp_root)
        assert plans, "prepare() must emit at least one plan"
        for plan in plans:
            for path in (
                plan.paths.source,
                plan.paths.output,
                plan.paths.publish_src,
                plan.paths.publish_dst,
            ):
                assert tmp_root in path.parents, (
                    f"plan {plan.label!r}: path {path!r} is not under "
                    f"tmp_root {tmp_root!r}"
                )
