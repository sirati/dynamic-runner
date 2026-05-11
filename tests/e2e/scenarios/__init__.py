"""Scenario registry for the e2e suite.

Single concern: enumerate the scenarios the driver knows how to run.

Each scenario is a small Python module that subclasses :class:`Scenario`
(see :mod:`tests.e2e.scenarios._base`) and exports a single
module-level ``SCENARIO`` instance. The registry is just the dict
mapping name → instance, populated by importing each scenario module
and reading its ``SCENARIO`` attribute.

The driver (:mod:`tests.e2e.run_e2e`) talks to scenarios exclusively
through the :class:`Scenario` API — it never imports a concrete
scenario module directly. New scenarios slot in by adding a module
here and registering it in :data:`_SCENARIO_NAMES`; the driver
needs no edits.
"""

from __future__ import annotations

from importlib import import_module

from ._base import Scenario, ScenarioPlan, ScenarioResult


# Order is informational only (used by --scenario all to drive
# scenarios in a sensible-to-read order). The driver does not depend
# on the order — each scenario is self-contained.
_SCENARIO_NAMES: tuple[str, ...] = (
    "phase-deps",
    "task-deps-intra",
    "task-deps-cross",
    "publish-atomic",
    "already-done",
    "parallel-4-workers",
    "worker-death-failover",
    "heartbeat-keepalive",
    "reverse-mode",
    "cleanup-teardown",
    "distributed-single-process",
    "distributed-local-subprocess",
    "primary-death-failover",
    "escape-hatch-external-master",
)


def _module_name(scenario_name: str) -> str:
    """Map ``phase-deps`` → ``tests.e2e.scenarios.phase_deps``."""
    return __name__ + "." + scenario_name.replace("-", "_")


def all_scenarios() -> dict[str, Scenario]:
    """Lazy-load every registered scenario.

    Imports happen on first call only; raises ``ImportError`` if any
    scenario module is missing or fails to expose ``SCENARIO``. The
    error message mentions the offending name so the operator does
    not have to grep.
    """
    out: dict[str, Scenario] = {}
    for name in _SCENARIO_NAMES:
        mod = import_module(_module_name(name))
        scn = getattr(mod, "SCENARIO", None)
        if not isinstance(scn, Scenario):
            raise ImportError(
                f"scenario module {mod.__name__!r} does not export a "
                "module-level SCENARIO of type Scenario; got "
                f"{type(scn).__name__!r}"
            )
        if scn.name != name:
            raise ImportError(
                f"scenario module {mod.__name__!r} declares name="
                f"{scn.name!r}, expected {name!r} (registry mismatch)"
            )
        out[name] = scn
    return out


def scenario_names() -> tuple[str, ...]:
    """Names in registry order (no import side-effects)."""
    return _SCENARIO_NAMES


__all__ = [
    "Scenario",
    "ScenarioPlan",
    "ScenarioResult",
    "all_scenarios",
    "scenario_names",
]
