"""Scenario: reverse-tunnel dispatch (GatewayPorts no).

Single concern: assert the framework's reverse-tunnel branch parses
the URI-form connection-info correctly.

Background
----------

When the cluster's sshd has ``GatewayPorts no`` (the OpenSSH
default; what the slurm-test-env's ``modules/common.nix`` ships
with), the framework's reverse-tunnel branch is used: workers
connect back to the primary through a reverse port forwarding.

Post-L4.1 (URI-form connection-info), the wrapper script writes
``tcp://<host>:<port>\\n`` to the connection-info file and the
preparation parser expects URI form. This scenario asserts the run
completes against the default (GatewayPorts off) cluster — i.e.
exercises the URI parser end-to-end.

Dependency on L4.1
------------------

Pre-L4.1 the connection-info was ``hostname=...\\ntunnel_port=...``
two-line shape. After L4.1 the wrapper emits URI-form. This
scenario is meaningful ONLY post-L4.1; on a pre-L4.1 build the
assertion still passes (the cluster works either way) but the
URI-parser code path isn't exercised. The scenario's ``requires``
tag flags this so the operator can read the dispatch log to confirm
``Primary URL: tcp://...`` appears (post-L4.1) vs the legacy
two-line shape (pre-L4.1).

The actual pass/fail is identical to phase-deps; the value is in
the manual log inspection the operator does after the run, not in
an automatable assertion (we'd need to grep the log for "tcp://"
to confirm — added below as a soft check).
"""

from __future__ import annotations

from pathlib import Path

from ._assertions import assert_files_present, expected_canonical_outputs
from ._base import DispatchEnv, Scenario, ScenarioPlan, ScenarioResult
from ._dispatch import build_dispatch_argv
from ._staging import stage_inputs


_NUM_TASKS_PER_PHASE = 4


class ReverseModeScenario(Scenario):
    name = "reverse-mode"
    description = (
        "Dispatch against the default (GatewayPorts no) cluster; "
        "exercises the reverse-tunnel branch and (post-L4.1) the "
        "URI-form connection-info parser."
    )
    requires = ("L4.1-uri-rollover",)

    def prepare(
        self, env: DispatchEnv, tmp_root: Path
    ) -> list[ScenarioPlan]:
        paths = stage_inputs(tmp_root, _NUM_TASKS_PER_PHASE)
        argv = build_dispatch_argv(
            env=env,
            source=paths.source,
            output=paths.output,
            num_tasks=_NUM_TASKS_PER_PHASE,
        )
        return [ScenarioPlan(argv=argv, paths=paths)]

    def assert_outputs(
        self, env: DispatchEnv, results: list[ScenarioResult]
    ) -> tuple[bool, list[str]]:
        del env
        result = results[0]
        ok_present, missing = assert_files_present(
            result.plan.paths.publish_dst,
            expected_canonical_outputs(_NUM_TASKS_PER_PHASE),
        )
        if not ok_present:
            return (False, missing)

        try:
            log_text = result.log_file.read_text(
                encoding="utf-8", errors="replace"
            )
        except OSError as e:
            return (False, [f"could not read dispatch log: {e}"])

        # Hard assertion: the framework must announce the SSH
        # ProxyJump topology for SLURM dispatch. Reverse mode is now
        # the SLURM-dispatch default (safe everywhere; gateway-direct
        # outbound requires explicit opt-in via
        # ``gateway_ports_enabled is True`` on a non-LMU cluster
        # where ``-R *:port`` is known-good). Without this assertion
        # the scenario would silently pass even if the default
        # regressed back to gateway-direct outbound: on a
        # GatewayPorts=no cluster the workload still happens to
        # complete because the cluster refuses the public bind and
        # the reverse-forward path falls back to localhost, so
        # outputs match either way.
        proxy_jump_marker = (
            "SLURM connection topology: SSH ProxyJump "
            "(primary tunnels to each secondary via gateway)"
        )
        if proxy_jump_marker not in log_text:
            return (
                False,
                [
                    "ProxyJump topology message missing — framework did "
                    "NOT report SSH ProxyJump as the SLURM connection "
                    "topology. Expected log line: "
                    f'"{proxy_jump_marker}"'
                ],
            )

        # Soft check for the URI-form parser code path. Logs the
        # "Primary URL: tcp://" line on the L4.1+ path. Absence is
        # NOT a failure (a pre-L4.1 build still works) — we just
        # surface a warning.
        if "tcp://" not in log_text:
            print(
                "[reverse-mode] WARNING: log contains no 'tcp://' "
                "marker — pre-L4.1 build, URI parser path not "
                "exercised by this run",
                flush=True,
            )
        return (True, [])


SCENARIO = ReverseModeScenario()
