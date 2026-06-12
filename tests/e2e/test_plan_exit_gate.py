"""Unit tests for the scenario verdict's plan-exit gate.

Single concern: ``plan_exit_failures`` — the pure function that turns
plan exit codes into verdict-gating failure messages. The gate exists
because ``assert_outputs`` alone produced a false PASS on a failed
reverse-mode bring-up (run_20260612_084041: the plan exited 1, but
stale output artifacts of an earlier green run satisfied the file
assertion and the harness printed PASS).

Why under ``tests/e2e/`` rather than ``python/dynamic_runner/tests/``:
this code is e2e-driver infrastructure, not framework code (mirrors
``test_worker_leak_gate.py``).
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

from tests.e2e.run_e2e import plan_exit_failures  # noqa: E402
from tests.e2e.scenarios._base import (  # noqa: E402
    DispatchPaths,
    ScenarioPlan,
    ScenarioResult,
)


def _result(
    exit_code: int, *, allows_nonzero_exit: bool = False, label: str = ""
) -> ScenarioResult:
    paths = DispatchPaths(
        source=Path("/nonexistent/src"),
        output=Path("/nonexistent/out"),
        publish_src=Path("/nonexistent/publish-src"),
        publish_dst=Path("/nonexistent/publish"),
    )
    plan = ScenarioPlan(
        argv=["true"],
        paths=paths,
        label=label,
        allows_nonzero_exit=allows_nonzero_exit,
    )
    return ScenarioResult(
        plan=plan,
        exit_code=exit_code,
        log_file=Path("/nonexistent/log"),
        duration_s=0.0,
    )


def test_all_zero_exits_pass_the_gate() -> None:
    assert plan_exit_failures([_result(0), _result(0)]) == []


def test_nonzero_exit_fails_the_gate() -> None:
    """The reverse-mode false-PASS replay: one plan exited 1."""
    failures = plan_exit_failures([_result(1)])
    assert len(failures) == 1
    assert "exited non-zero (1)" in failures[0]


def test_declared_nonzero_exit_passes_the_gate() -> None:
    """primary-death-failover SIGKILLs its dispatcher by design."""
    assert plan_exit_failures([_result(-9, allows_nonzero_exit=True)]) == []


def test_mixed_plans_report_only_undeclared_failures() -> None:
    failures = plan_exit_failures(
        [
            _result(0, label="initial"),
            _result(2, label="rerun"),
            _result(-9, allows_nonzero_exit=True, label="killed"),
        ]
    )
    assert len(failures) == 1
    assert "plan-rerun" in failures[0]
    assert "exited non-zero (2)" in failures[0]
