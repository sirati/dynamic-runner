"""Unit tests for the escape-hatch-external-master scenario.

Single concern: pin the scenario's API-surface contract so a future
refactor cannot silently break:

  - the env-var name (``DYNRUNNER_SSH_CONTROL_PATH``) the dispatch
    extra_env carries;
  - the slurm-mode guard (the env-var hatch is only reached on the
    SSH-connect path);
  - the master-teardown invariant (``stop_ssh_master`` must be called
    on assert_outputs regardless of pass/fail).

Why a unit test in addition to the live e2e run:
the live run requires a working SLURM cluster + a real SSH master,
which is the authoritative gate but expensive to invoke. A unit test
that drives ``prepare`` and ``assert_outputs`` with the master spawn
helpers mocked pins the scenario's intent at the API surface — so
even when the live run is skipped (laptop, CI without slurm-test-env)
the regression sensitivity stays under version control.

Why under ``tests/e2e/`` rather than ``python/dynamic_runner/tests/``:
this is e2e-driver-side scenario coverage, not framework code; mirrors
the placement of ``test_distributed_local_subprocess_scenario.py``.
"""

from __future__ import annotations

import sys
import tempfile
from pathlib import Path
from unittest import mock


# Bring repo root onto sys.path so the absolute import works whether
# pytest is invoked from the repo root or from this dir. Mirrors the
# dance ``run_e2e.py`` itself does.
_REPO_ROOT = Path(__file__).resolve().parents[2]
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

import pytest  # noqa: E402

from tests.e2e.scenarios import escape_hatch_external_master as scenario_mod  # noqa: E402
from tests.e2e.scenarios._base import (  # noqa: E402
    DispatchEnv,
    ScenarioResult,
)


SCENARIO = scenario_mod.SCENARIO


def _env(
    mode: str = "slurm",
    *,
    ssh_config_path: Path | None = None,
) -> DispatchEnv:
    """Minimal DispatchEnv mirroring what ``run_e2e.main`` builds in slurm mode.

    ``ssh_config_path`` defaults to a sentinel non-None Path because
    every test that runs ``prepare`` past the mode guard needs one;
    tests asserting the None-guard explicitly pass ``None``.
    """
    return DispatchEnv(
        instance_id="e2e",
        ssh_port=2222,
        slurm_root_folder="/home/e2e-user/dynrunner-e2e",
        workers=4,
        mode=mode,
        ssh_user="e2e-user",
        ssh_config_path=ssh_config_path,
        gateway_host_alias="slurm-gateway",
    )


def _run_prepare_with_mocked_master(
    env: DispatchEnv, tmp_root: Path, *, control_path: Path
):
    """Drive ``prepare`` with both _ssh_state helpers mocked.

    Returns ``(plans, spawn_mock, stop_mock)`` so individual tests
    can assert on the spawn arguments and the eventual stop call.

    Patches happen on the SCENARIO MODULE's namespace because the
    module imported the symbols by name (``from .._ssh_state import
    spawn_ssh_master, stop_ssh_master``), so the live references the
    scenario uses live in ``scenario_mod``, not in ``_ssh_state``.
    """
    with mock.patch.object(
        scenario_mod, "spawn_ssh_master", return_value=control_path
    ) as spawn_mock, mock.patch.object(
        scenario_mod, "stop_ssh_master"
    ) as stop_mock:
        # Reset per-instance state; the module-level SCENARIO is a
        # singleton so a previous test's leftover ``_master_state``
        # would otherwise leak across tests.
        SCENARIO._master_state = None
        plans = SCENARIO.prepare(env, tmp_root)
        return plans, spawn_mock, stop_mock


def test_scenario_name_and_registry_match() -> None:
    """Module's ``SCENARIO`` declares the expected name."""
    assert SCENARIO.name == "escape-hatch-external-master"


def test_prepare_threads_control_path_into_extra_env() -> None:
    """``prepare`` exports ``DYNRUNNER_SSH_CONTROL_PATH`` on extra_env.

    The whole point of the scenario is to point the dispatcher at a
    pre-spawned master via this env var. If the env var is missing
    (or carries a different name), the framework's ssh.rs branch is
    never reached and the scenario gives a meaningless green run.
    """
    cp = Path("/tmp/dynrunner-ssh-master-test.sock")
    cfg = Path("/tmp/test-ssh-config")
    with tempfile.TemporaryDirectory() as raw_tmp:
        tmp_root = Path(raw_tmp)
        plans, spawn_mock, _ = _run_prepare_with_mocked_master(
            _env(ssh_config_path=cfg),
            tmp_root,
            control_path=cp,
        )
    assert len(plans) == 1, (
        f"expected exactly one plan, got {len(plans)}"
    )
    plan = plans[0]
    assert plan.extra_env.get("DYNRUNNER_SSH_CONTROL_PATH") == str(cp), (
        f"expected DYNRUNNER_SSH_CONTROL_PATH={str(cp)!r} in extra_env, "
        f"got {plan.extra_env!r}"
    )
    # Verify spawn was called with the env's ssh_config_path /
    # gateway_host_alias — the scenario must not invent its own
    # cluster connection state.
    spawn_mock.assert_called_once()
    _, kwargs = spawn_mock.call_args
    assert kwargs.get("ssh_config_path") == cfg, (
        f"spawn_ssh_master got ssh_config_path={kwargs.get('ssh_config_path')!r}, "
        f"expected {cfg!r}"
    )
    assert kwargs.get("host_alias") == "slurm-gateway", (
        f"spawn_ssh_master got host_alias="
        f"{kwargs.get('host_alias')!r}, expected 'slurm-gateway'"
    )


def test_prepare_argv_carries_canonical_dispatch_shape() -> None:
    """The plan's argv runs the canonical consumer in slurm mode.

    Pins the contract that the scenario does NOT override the
    dispatch mode (unlike distributed-local-subprocess, which forces
    ``--multi-computer local``): the env-var hatch is only reachable
    via the slurm SSH-connect path, so a mode override would defeat
    the whole purpose.
    """
    cp = Path("/tmp/dynrunner-ssh-master-test.sock")
    cfg = Path("/tmp/test-ssh-config")
    with tempfile.TemporaryDirectory() as raw_tmp:
        tmp_root = Path(raw_tmp)
        plans, _, _ = _run_prepare_with_mocked_master(
            _env(ssh_config_path=cfg),
            tmp_root,
            control_path=cp,
        )
        plan = plans[0]

    joined = " ".join(plan.argv)
    assert "tests.e2e.test_consumer" in joined, (
        f"expected canonical consumer module in argv, got {plan.argv!r}"
    )
    assert "--multi-computer" in plan.argv, (
        f"missing --multi-computer flag in argv: {plan.argv!r}"
    )
    mc_idx = plan.argv.index("--multi-computer")
    assert plan.argv[mc_idx + 1] == "slurm", (
        f"expected --multi-computer slurm (env-var hatch is only "
        f"reached on the SSH-connect path), got "
        f"{plan.argv[mc_idx + 1]!r} in argv: {plan.argv!r}"
    )


def test_prepare_inputs_are_staged_under_tmp_root() -> None:
    """The scenario's plan paths point inside the per-scenario tmp root.

    Pins the contract that the scenario does not reach for absolute
    paths outside the driver-allocated tmpdir (the driver owns
    cleanup; a scenario writing elsewhere would leak).
    """
    cp = Path("/tmp/dynrunner-ssh-master-test.sock")
    cfg = Path("/tmp/test-ssh-config")
    with tempfile.TemporaryDirectory() as raw_tmp:
        tmp_root = Path(raw_tmp)
        plans, _, _ = _run_prepare_with_mocked_master(
            _env(ssh_config_path=cfg),
            tmp_root,
            control_path=cp,
        )
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


@pytest.mark.parametrize("non_slurm_mode", ["single-process", "in-process", "local"])
def test_prepare_rejects_non_slurm_mode(non_slurm_mode: str) -> None:
    """The scenario hard-fails when the harness mode is not slurm.

    The env-var hatch lives in ssh.rs::connect(); only the slurm
    dispatch path reaches it. Other modes would silently produce a
    green run that proves nothing — so the scenario raises with a
    clear message instead of emitting a meaningless plan.

    No master spawn must happen in this rejection path: a partial
    spawn followed by a raise would leak a master.
    """
    with tempfile.TemporaryDirectory() as raw_tmp:
        tmp_root = Path(raw_tmp)
        with mock.patch.object(
            scenario_mod, "spawn_ssh_master"
        ) as spawn_mock, mock.patch.object(
            scenario_mod, "stop_ssh_master"
        ):
            SCENARIO._master_state = None
            with pytest.raises(RuntimeError, match="--mode slurm"):
                SCENARIO.prepare(_env(mode=non_slurm_mode), tmp_root)
            spawn_mock.assert_not_called()


def test_prepare_rejects_missing_ssh_config() -> None:
    """The scenario hard-fails when env.ssh_config_path is None.

    Without a per-cluster ssh_config the master spawn helper has
    nothing to feed ``ssh -F``; the scenario refuses rather than
    inventing a default that would dial against ``$HOME/.ssh``
    (which the slurm-test-env contract forbids).
    """
    with tempfile.TemporaryDirectory() as raw_tmp:
        tmp_root = Path(raw_tmp)
        with mock.patch.object(
            scenario_mod, "spawn_ssh_master"
        ) as spawn_mock, mock.patch.object(
            scenario_mod, "stop_ssh_master"
        ):
            SCENARIO._master_state = None
            with pytest.raises(RuntimeError, match="ssh_config"):
                SCENARIO.prepare(_env(ssh_config_path=None), tmp_root)
            spawn_mock.assert_not_called()


def test_assert_outputs_tears_down_master_even_on_failure() -> None:
    """``assert_outputs`` calls ``stop_ssh_master`` whether the
    assertions pass or fail.

    The dispatch result here is intentionally bogus (rc=1, no log
    file with the hatch line, no published outputs) so the
    assertion's failure path is taken. The teardown invariant must
    still hold — a leak-on-failure would defeat the cleanup
    contract documented in the scenario's docstring.
    """
    cp = Path("/tmp/dynrunner-ssh-master-test.sock")
    cfg = Path("/tmp/test-ssh-config")
    with tempfile.TemporaryDirectory() as raw_tmp:
        tmp_root = Path(raw_tmp)
        with mock.patch.object(
            scenario_mod, "spawn_ssh_master", return_value=cp
        ), mock.patch.object(
            scenario_mod, "stop_ssh_master"
        ) as stop_mock:
            SCENARIO._master_state = None
            env = _env(ssh_config_path=cfg)
            plans = SCENARIO.prepare(env, tmp_root)
            plan = plans[0]
            # Construct a deliberately failing result: rc=1 plus a
            # log file that does NOT contain the hatch-taken line.
            log_file = Path(tmp_root) / "fake-dispatch.log"
            log_file.write_text("nothing relevant here\n")
            results = [
                ScenarioResult(
                    plan=plan,
                    exit_code=1,
                    log_file=log_file,
                    duration_s=0.0,
                )
            ]
            ok, failures = SCENARIO.assert_outputs(env, results)
            assert not ok, (
                "expected assertion to fail (rc=1 + no hatch line + "
                "no outputs)"
            )
            # Failure messages should mention BOTH the non-zero exit
            # AND the missing log line, so future regressions in
            # either signal surface in the diagnostic output.
            joined = "\n".join(failures)
            assert "exited non-zero" in joined, (
                f"expected non-zero-exit message in failures, got {failures!r}"
            )
            assert "escape-hatch log line not found" in joined, (
                f"expected hatch-line-missing message in failures, "
                f"got {failures!r}"
            )
            stop_mock.assert_called_once()
            _, kwargs = stop_mock.call_args
            assert kwargs.get("control_path") == cp, (
                f"stop_ssh_master got control_path="
                f"{kwargs.get('control_path')!r}, expected {cp!r}"
            )
