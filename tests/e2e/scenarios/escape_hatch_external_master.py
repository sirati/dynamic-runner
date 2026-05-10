"""Scenario: ``DYNRUNNER_SSH_CONTROL_PATH`` external-master escape hatch.

Single concern: pin the contract that when the env-var hatch is set
to a live SSH control socket, the framework's
``crates/dynrunner-gateway/src/ssh.rs::connect()`` skips its own
master spawn and reuses the externally-managed master.

Mechanic
--------

Pre-spawn an SSH master via :func:`tests.e2e._ssh_state.spawn_ssh_master`
(the helper kept around for exactly this opt-in path) and point the
dispatcher at the resulting control socket through the
``DYNRUNNER_SSH_CONTROL_PATH`` env var. Dispatch a minimal phase-deps-
equivalent run (canonical produce/consume consumer with
``--num-tasks 2``); on success, grep the dispatch log for the exact
``tracing::info!`` line ssh.rs emits when the hatch is taken.

Why grep, not just rely on dispatch success
-------------------------------------------

A dispatch can succeed even when the framework SILENTLY bypasses the
hatch (e.g. a refactor that drops the env-var check would simply
spawn its own master and the run would still pass). The whole point
of this scenario is to detect that silent bypass — so the assertion
must observe the framework's positive declaration that it took the
hatch.

The exact log substring is a structured-tracing message:

    "using external SSH master (DYNRUNNER_SSH_CONTROL_PATH); "
    "skipping our own spawn"

(see ``crates/dynrunner-gateway/src/ssh.rs`` near the
``DYNRUNNER_SSH_CONTROL_PATH`` env-var branch). If a future refactor
renames the message, this scenario fails loudly — at which point the
operator updates the constant below to match the new wording.

Why slurm-only
--------------

The escape hatch lives in ``ssh.rs::connect()``, which only runs in
slurm dispatch mode (``--multi-computer slurm``). The other dispatch
modes (``single-process``, ``in-process``, ``local``) never connect
via SSH, so the hatch is never reached. Running the scenario under a
non-slurm harness mode would silently produce a green run that
proves nothing — so we hard-fail with a clear message instead.

Cleanup
-------

The pre-spawned master must be torn down whether assertions pass or
fail. ``assert_outputs`` runs the teardown in a ``try/finally``;
``prepare`` additionally registers an ``atexit`` hook so a
timeout-aborted scenario (where ``assert_outputs`` is never called by
the driver — see ``run_e2e._run_one_scenario`` exit-on-rc=124 path)
still releases the master at process exit. Both teardown paths use
the idempotent :func:`tests.e2e._ssh_state.stop_ssh_master`.
"""

from __future__ import annotations

import atexit
from dataclasses import dataclass
from pathlib import Path

from .._ssh_state import spawn_ssh_master, stop_ssh_master
from ._assertions import assert_files_present, expected_canonical_outputs
from ._base import DispatchEnv, Scenario, ScenarioPlan, ScenarioResult
from ._dispatch import build_dispatch_argv
from ._staging import stage_inputs


_NUM_TASKS_PER_PHASE = 2

# Exact substring the framework's ssh.rs emits when the env-var
# hatch is taken (see ``tracing::info!`` near the
# ``DYNRUNNER_SSH_CONTROL_PATH`` branch in
# ``crates/dynrunner-gateway/src/ssh.rs``). We grep for the substring,
# not the full structured-log line, so a tracing-format change
# (e.g. switching from `key=value` field rendering to JSON) does not
# break the assertion as long as the human-readable message text is
# preserved.
_HATCH_TAKEN_LOG_SUBSTRING = (
    "using external SSH master (DYNRUNNER_SSH_CONTROL_PATH); "
    "skipping our own spawn"
)


@dataclass(frozen=True)
class _MasterState:
    """What ``prepare`` stashed for ``assert_outputs`` to tear down.

    Frozen so a typo in ``assert_outputs`` cannot mutate the values
    the atexit hook captured by reference.
    """

    ssh_config_path: Path
    control_path: Path
    host_alias: str


class EscapeHatchExternalMasterScenario(Scenario):
    name = "escape-hatch-external-master"
    description = (
        "Pre-spawns an external SSH master and sets "
        "DYNRUNNER_SSH_CONTROL_PATH; asserts ssh.rs takes the env-var "
        "hatch (greps for the framework's 'using external SSH master' "
        "log line, proving the hatch was actually taken)."
    )
    requires = ()

    # Per-instance stash: ``prepare`` writes, ``assert_outputs`` reads
    # and tears down. ``None`` outside an active run.
    _master_state: _MasterState | None = None

    def prepare(
        self, env: DispatchEnv, tmp_root: Path
    ) -> list[ScenarioPlan]:
        # The escape hatch lives in ssh.rs::connect(), which is only
        # reached when the dispatch goes through SLURM mode. In any
        # other mode the framework never connects via SSH, so the
        # hatch is never exercised. Hard-fail rather than emit a
        # silently-meaningless plan.
        if env.mode != "slurm":
            raise RuntimeError(
                f"escape-hatch-external-master requires --mode slurm "
                f"(got --mode {env.mode!r}); the env-var hatch is only "
                f"reached on the SSH-connect path"
            )
        if env.ssh_config_path is None:
            raise RuntimeError(
                "escape-hatch-external-master needs a per-cluster ssh_config "
                "(env.ssh_config_path is None); run via run_e2e in slurm "
                "mode so the driver provisions the dispatcher user"
            )

        # The driver writes ssh_config to the per-instance state dir
        # (see _ssh_state.generate_ssh_config). Recover that dir from
        # the config path so the master's control socket lives next
        # to the same instance's other state.
        instance_state_dir = env.ssh_config_path.parent

        control_path = spawn_ssh_master(
            instance_state_dir,
            ssh_config_path=env.ssh_config_path,
            host_alias=env.gateway_host_alias,
        )
        self._master_state = _MasterState(
            ssh_config_path=env.ssh_config_path,
            control_path=control_path,
            host_alias=env.gateway_host_alias,
        )
        # Safety net for the timeout path: _run_one_scenario in run_e2e
        # returns early on rc=124 and never calls assert_outputs, so the
        # try/finally there cannot fire. atexit covers driver-clean-exit
        # timeouts; for SIGKILL'd drivers the master is bounded by its
        # ServerAliveInterval × ServerAliveCountMax TTL. stop_ssh_master
        # is idempotent so a double-call (atexit + assert_outputs) is
        # harmless.
        atexit.register(self._teardown_master)

        paths = stage_inputs(tmp_root, _NUM_TASKS_PER_PHASE)
        argv = build_dispatch_argv(
            env=env,
            source=paths.source,
            output=paths.output,
            num_tasks=_NUM_TASKS_PER_PHASE,
        )
        return [
            ScenarioPlan(
                argv=argv,
                paths=paths,
                extra_env={"DYNRUNNER_SSH_CONTROL_PATH": str(control_path)},
            )
        ]

    def assert_outputs(
        self, env: DispatchEnv, results: list[ScenarioResult]
    ) -> tuple[bool, list[str]]:
        del env
        try:
            result = results[0]
            failures: list[str] = []

            if result.exit_code != 0:
                failures.append(
                    f"dispatch exited non-zero: {result.exit_code} "
                    f"(see {result.log_file})"
                )

            # Canonical outputs check — same as phase-deps. A green
            # run with the hatch-taken log line is the dual condition
            # this scenario asserts on.
            ok_files, missing = assert_files_present(
                result.plan.paths.publish_dst,
                expected_canonical_outputs(_NUM_TASKS_PER_PHASE),
            )
            if not ok_files:
                failures.extend(missing)

            # The load-bearing assertion: the framework's ssh.rs
            # MUST log that it took the env-var hatch. A silent
            # bypass (refactor drops the env-var check, framework
            # spawns its own master, run still succeeds) would pass
            # the file-presence check but is exactly what this
            # scenario guards against.
            log_text = result.log_file.read_text(errors="replace")
            if _HATCH_TAKEN_LOG_SUBSTRING not in log_text:
                failures.append(
                    f"escape-hatch log line not found in {result.log_file}: "
                    f"expected substring "
                    f"{_HATCH_TAKEN_LOG_SUBSTRING!r}. "
                    f"The framework either did not take the "
                    f"DYNRUNNER_SSH_CONTROL_PATH branch (silent bypass — "
                    f"see crates/dynrunner-gateway/src/ssh.rs near the "
                    f"env-var check) or the log message wording changed; "
                    f"update _HATCH_TAKEN_LOG_SUBSTRING in this scenario "
                    f"to match the new wording."
                )

            return (not failures, failures)
        finally:
            self._teardown_master()

    def _teardown_master(self) -> None:
        """Idempotent master teardown.

        Safe to call from both ``assert_outputs``' finally clause and
        the ``atexit`` hook registered in ``prepare``: the first call
        clears ``_master_state`` so the second is a no-op. The
        underlying :func:`stop_ssh_master` is itself idempotent (no-op
        when the socket is already gone), so even if the state weren't
        cleared the cleanup would be safe.
        """
        state = self._master_state
        if state is None:
            return
        self._master_state = None
        stop_ssh_master(
            ssh_config_path=state.ssh_config_path,
            control_path=state.control_path,
            host_alias=state.host_alias,
        )


SCENARIO = EscapeHatchExternalMasterScenario()
