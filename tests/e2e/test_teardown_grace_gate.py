"""Unit tests for the post-scenario teardown gate's grace resolution.

Single concern: ``_teardown_grace_for`` — the pure function that turns
a scenario's optional ``teardown_grace_s`` override into the wall-clock
window the driver's SLURM-job / worker-leak teardown gates poll for.

Why the override exists (#510): the baseline grace is sized for a CLEAN
run — one primary originates ``RunComplete`` the instant its ledger
drains, and a fixed handful of wrappers tear down. The
primary-death-failover scenario's terminal is heavier: the PROMOTED
primary must first inherit and finalize the (possibly large) ledger
before it can originate ``RunComplete``, then fan the terminal out to a
re-formed surviving fleet whose wrappers each drain. With a large
#504/#497 ledger that drain legitimately runs past the flat baseline, so
the gate false-flagged a healthy-but-slow teardown as a leftover-job
regression (run: 600/600 drained, zero stranded — only the grace gate
tripped). The scenario now widens the gate in step with the ledger,
mirroring how it already scales its post-kill convergence budget.

Why under ``tests/e2e/`` rather than ``python/dynamic_runner/tests/``:
this is e2e-driver infrastructure, not framework code (mirrors
``test_plan_exit_gate.py`` / ``test_worker_leak_gate.py``).
"""

from __future__ import annotations

import sys
from pathlib import Path


# Bring repo root onto sys.path so the absolute import works whether
# pytest is invoked from the repo root or from this dir. Mirrors the
# dance ``run_e2e.py`` itself does.
_REPO_ROOT = Path(__file__).resolve().parents[2]
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

from tests.e2e.run_e2e import _TEARDOWN_GRACE_S, _teardown_grace_for  # noqa: E402
from tests.e2e.scenarios._base import (  # noqa: E402
    DispatchEnv,
    Scenario,
    ScenarioPlan,
    ScenarioResult,
)
from tests.e2e.scenarios.primary_death_failover import (  # noqa: E402
    PrimaryDeathFailoverScenario,
)


def _env() -> DispatchEnv:
    return DispatchEnv(
        instance_id="t",
        ssh_port=0,
        slurm_root_folder="/nonexistent",
        workers=3,
        mode="slurm",
    )


class _DefaultScenario(Scenario):
    """A scenario that does NOT override the teardown grace."""

    name = "default-grace"

    def prepare(self, env: DispatchEnv, tmp_root: Path) -> list[ScenarioPlan]:
        del env, tmp_root
        return []

    def assert_outputs(
        self, env: DispatchEnv, results: list[ScenarioResult]
    ) -> tuple[bool, list[str]]:
        del env, results
        return (True, [])


class _WideningScenario(_DefaultScenario):
    """Overrides the grace with a value WIDER than the baseline."""

    name = "widening-grace"

    def teardown_grace_s(self, env: DispatchEnv) -> float | None:
        del env
        return _TEARDOWN_GRACE_S + 300.0


class _TighteningScenario(_DefaultScenario):
    """Overrides the grace with a value TIGHTER than the baseline.

    The driver must clamp this back up to the baseline — a scenario may
    only widen the gate, never tighten it (a too-short gate would
    false-fail a healthy-but-slow drain, the very thing the knob exists
    to prevent).
    """

    name = "tightening-grace"

    def teardown_grace_s(self, env: DispatchEnv) -> float | None:
        del env
        return 1.0


def test_default_scenario_uses_baseline() -> None:
    assert _teardown_grace_for(_DefaultScenario(), _env()) == _TEARDOWN_GRACE_S


def test_widening_override_is_honoured() -> None:
    assert _teardown_grace_for(_WideningScenario(), _env()) == (
        _TEARDOWN_GRACE_S + 300.0
    )


def test_tightening_override_is_clamped_to_baseline() -> None:
    """A scenario can only widen the gate, never tighten it."""
    assert _teardown_grace_for(_TighteningScenario(), _env()) == _TEARDOWN_GRACE_S


def test_failover_scenario_scales_grace_with_ledger() -> None:
    """The #510 fix: the promoted-primary teardown grace tracks the ledger.

    With the historical 12-task shape the scenario returns exactly the
    driver baseline (so existing runs are unchanged). With a large ledger
    it widens in step — mirroring the scenario's post-kill convergence
    budget — so a healthy-but-slow post-failover drain isn't flagged as a
    leftover-job regression.
    """
    import tests.e2e.scenarios.primary_death_failover as failover

    grace = _teardown_grace_for(PrimaryDeathFailoverScenario(), _env())
    # The scenario's grace is ledger-scaled at import time off
    # ``_NUM_TASKS_PER_PHASE``; it must never resolve below the baseline.
    assert grace >= _TEARDOWN_GRACE_S
    # And it must equal the scenario's own ledger-scaled computation
    # (clamped up to the baseline), proving the override is wired through.
    assert grace == max(_TEARDOWN_GRACE_S, failover._teardown_grace_s())


def test_failover_grace_matches_convergence_scaling_shape() -> None:
    """Teardown grace scales on the SAME +0.5s/task slope as convergence.

    The drain volume grows with the ledger the same way the post-kill
    convergence tail does, so the two budgets share a slope. This pins
    that the teardown grace isn't accidentally flattened back to a
    constant (the #510 root cause: convergence scaled, teardown did not).
    """
    import tests.e2e.scenarios.primary_death_failover as failover

    base = 90.0
    extra = max(0, failover._NUM_TASKS_PER_PHASE - 12)
    assert failover._teardown_grace_s() == base + 0.5 * extra
