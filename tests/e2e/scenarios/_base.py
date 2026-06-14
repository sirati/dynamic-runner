"""Scenario protocol — what every scenario must look like to the driver.

Single concern: the boundary between the orchestration driver
(:mod:`tests.e2e.run_e2e`) and an individual scenario.

The driver knows a scenario through three operations:

1. :meth:`Scenario.prepare` — stage source/output/publish dirs and
   declare the dispatch argv. Returns a :class:`ScenarioPlan`.
2. :meth:`Scenario.run_hook` — optional pre/post-dispatch action
   (e.g. kill-mid-write, scancel a worker). Default is a no-op.
3. :meth:`Scenario.assert_outputs` — verify what landed under the
   publish destination matches the scenario's expectations.

Notes on what is NOT in this API
--------------------------------

- The driver owns cluster lifecycle, heartbeat, timeout. Scenarios
  must never touch the cluster directly.
- The driver passes a :class:`DispatchEnv` so every scenario sees the
  same configured ssh port, slurm root folder, instance id, etc.
  Per-scenario customization happens through :class:`ScenarioPlan`,
  not through the env.
- Scenario "extras" (post-run ssh checks, sacct queries) are
  expressed as :meth:`Scenario.assert_outputs` — that method gets the
  plan back so it can re-derive the publish dir and any ssh hostnames.
"""

from __future__ import annotations

import abc
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any


@dataclass(frozen=True)
class DispatchEnv:
    """Cluster-wide configuration shared by every scenario.

    Frozen so the driver cannot accidentally mutate it across
    scenarios. The fields mirror the operator-facing flags on
    ``run_e2e.py`` plus the slurm-test-env naming knobs.
    """

    instance_id: str
    ssh_port: int
    slurm_root_folder: str
    workers: int
    """Cluster size — informational; scenarios that assert on
    distribution (parallel-4-workers) read it to decide the
    expected min/max per-worker counts."""
    mode: str
    """One of ``slurm``, ``single-process``, ``in-process``. Same
    semantics as ``run_e2e.py --mode``."""
    ssh_user: str = "e2e-user"
    """Cluster-side username the dispatcher SSHs in as. Provisioned
    by the driver before any scenario runs (idempotent re-run of
    ``nix run .#provision-user``). Not the operator's identity —
    fresh per-cluster, no relation to ``$USER``."""
    ssh_config_path: Path | None = None
    """Path to a generated ssh_config file pinning identity, port,
    user, ``IdentitiesOnly=yes``, ``IdentityAgent=none``,
    ``StrictHostKeyChecking=no``, ``UserKnownHostsFile=/dev/null``
    for this cluster. Threaded into the dispatcher via the framework's
    ``--ssh-config`` flag (the canonical escape hatch the framework
    surfaced in commit 178a3af). ``None`` in non-slurm modes."""
    ssh_identity_path: Path | None = None
    """Path to the dispatcher's per-cluster private key. Lives under
    the driver's state dir (``tests/e2e/state/<instance_id>/keys/``),
    NOT under ``~/.ssh``. ``None`` in non-slurm modes."""
    slurm_partition: str = "debug"
    """SLURM partition name to submit against. The slurm-test-env
    cluster only ships a ``debug`` partition; the framework's default
    is ``All`` (suitable for production multi-partition clusters but
    rejected by the test env's slurmctld). Threaded into the dispatcher
    via ``--slurm-partition`` in slurm mode."""
    slurm_cpus_per_task: int = 2
    """SLURM ``--cpus-per-task`` per secondary. The framework defaults
    to 14 (sized for production HPC nodes); the test env's workers
    only have 2 cores each, so requesting 14 yields
    ``CPU count per node can not be satisfied``. Threaded through
    ``--slurm-cpus-per-task`` in slurm mode."""
    gateway_host_alias: str = "slurm-gateway"
    """Cluster-internal hostname for the gateway. The framework
    propagates ``self.gateway.host`` verbatim from the
    ``--gateway`` URL into the worker wrapper's
    ``--secondary tcp://<host>:<port>`` URL (see
    ``packaging/preparation.py::_determine_gateway_host``). Workers
    sit in their own netns on the cluster's podman bridge network,
    so they cannot reach the operator host's ``localhost``; they
    DNS-resolve the gateway via its ``--network-alias`` registered
    in ``slurm-test-env/deploy/lib.sh``. Combined with an SSH
    ``Host`` block whose ``HostName`` is ``localhost``, the
    operator host's SSH client still dials the forwarded port
    while the cluster sees the alias."""


@dataclass(frozen=True)
class DispatchPaths:
    """Filesystem paths a scenario hands back to the driver.

    All paths are tmpdir-style and the driver decides cleanup. The
    scenario must NOT remove them itself.
    """

    source: Path
    output: Path
    publish_src: Path
    publish_dst: Path


@dataclass(frozen=True)
class ScenarioPlan:
    """Everything the driver needs to dispatch ONE scenario run.

    A single scenario may produce several plans (e.g. already-done
    runs the same dispatch twice). The driver dispatches one plan at
    a time and feeds the results back through ``assert_outputs``.

    ``argv`` is the full ``[python, -m, ...]`` list — scenarios that
    need to point at a non-default consumer module embed that here.
    Putting it in the plan instead of synthesising it in the driver
    keeps the driver scenario-agnostic.
    """

    argv: list[str]
    paths: DispatchPaths
    extra_env: dict[str, str] = field(default_factory=dict)
    """Scenario-specific env vars overlaid on the driver's base env
    (which always carries DYNRUNNER_PUBLISH_{SRC,DST}_ROOT pointing
    at ``paths``)."""
    timeout_s: int | None = None
    """Per-plan timeout cap. If None, the driver uses its own
    overall budget. Scenarios with short expected runtimes (e.g.
    smoke checks) can shorten this; long-running ones cannot
    extend past the driver's wallclock."""
    label: str = ""
    """Sub-label distinguishing multiple plans within one scenario
    (e.g. ``initial`` / ``rerun`` for already-done). Empty when
    a scenario emits exactly one plan."""
    allows_nonzero_exit: bool = False
    """Declare that this plan's dispatch may legitimately exit
    non-zero (e.g. primary-death-failover SIGKILLs the dispatcher and
    judges convergence out-of-band). The driver FAILS the scenario on
    any undeclared non-zero plan exit, regardless of what
    ``assert_outputs`` concludes — output files alone can be satisfied
    by stale artifacts from an earlier run, so a failed plan must
    never produce a PASS verdict."""


@dataclass
class ScenarioResult:
    """What the driver hands back to ``assert_outputs``.

    Contains the dispatch's exit code plus everything an assertion
    might need to consult — the captured log file, the heartbeat
    timeline, etc. Extending this struct is the way to give
    scenarios access to new driver-collected data.
    """

    plan: ScenarioPlan
    exit_code: int
    log_file: Path
    duration_s: float
    extra: dict[str, Any] = field(default_factory=dict)
    """Driver-collected side-channel data (e.g. ``sacct`` output for
    worker-distribution scenarios). Keys are documented per-scenario
    in the scenario module's docstring."""


class Scenario(abc.ABC):
    """ABC every scenario module must subclass.

    Concrete scenarios live as one-file modules under
    :mod:`tests.e2e.scenarios`; the registry imports them lazily.
    """

    name: str = ""
    """Scenario id (matches the ``--scenario`` flag value).

    Subclasses MUST override with a non-empty string; the registry
    cross-checks against the module name."""

    description: str = ""
    """One-line human description, surfaced in ``--help``."""

    requires: tuple[str, ...] = ()
    """Tags naming framework features this scenario depends on
    having merged (e.g. ``L4.1-uri``, ``L4.9-cleanup``). Not enforced
    at runtime — these are documentation that surface in the
    scenario list so the operator can skip pre-requisite-missing
    scenarios with eyes open."""

    # ── Lifecycle ──────────────────────────────────────────────────

    @abc.abstractmethod
    def prepare(self, env: DispatchEnv, tmp_root: Path) -> list[ScenarioPlan]:
        """Build one or more dispatch plans for this scenario.

        ``tmp_root`` is a per-scenario tmpdir the driver allocates;
        the scenario is free to create subdirectories under it. The
        driver cleans up ``tmp_root`` after the scenario runs unless
        ``--keep-tmp`` is set.
        """

    def teardown_grace_s(self, env: DispatchEnv) -> float | None:
        """Per-scenario override of the driver's post-run teardown grace.

        The driver's teardown gate (``run_e2e.py``) polls the gateway's
        SLURM queue and the compute nodes for leftover framework state
        for up to a grace window after the scenario's assertions pass —
        time for the framework's ``RunComplete`` terminal to propagate
        through the peer mesh and every wrapper to finish its cleanup.

        The driver's baseline grace is sized for a CLEAN run: one primary
        broadcasts ``RunComplete`` and a fixed handful of wrappers drain.
        A scenario whose terminal path is heavier than that — e.g. a
        promoted primary that, post-failover, first inherits and finalizes
        a large ledger before it can even originate ``RunComplete``, then
        fans the terminal out to a larger surviving fleet — legitimately
        drains later. Such a scenario returns a wider grace here so the
        gate doesn't false-positive on a still-converging-but-healthy
        teardown.

        Mirrors :attr:`ScenarioPlan.timeout_s`: ``None`` means "use the
        driver's baseline"; a number overrides it. The driver clamps the
        return to be no smaller than its baseline so a scenario can only
        widen, never tighten, the gate (a tighter gate would risk
        false-failing a healthy slow drain).
        """
        del env
        return None

    def run_hook(
        self, env: DispatchEnv, plan: ScenarioPlan, dispatch_pid: int
    ) -> None:
        """Optional pre/post-dispatch hook fired in a side thread.

        The default is a no-op. Scenarios that need to interact with
        a running dispatch (kill mid-write, scancel a worker) override
        this. The hook is called as soon as the dispatch process is
        spawned.

        Implementation note: the hook itself MUST return quickly (so
        the spawn callback in the dispatch runner doesn't block). If
        the hook needs a delay (wait N seconds before scancel), spawn
        a background thread inside the override; the driver doesn't
        track those threads, so the override is responsible for them
        being daemonic so they don't outlive the process.
        """
        del env, plan, dispatch_pid

    @abc.abstractmethod
    def assert_outputs(
        self, env: DispatchEnv, results: list[ScenarioResult]
    ) -> tuple[bool, list[str]]:
        """Verify the dispatch produced what the scenario expects.

        Returns ``(ok, failures)``: ``ok=True`` when every check
        passed; ``failures`` carries human-readable strings for the
        driver to print on a False result.
        """


__all__ = [
    "DispatchEnv",
    "DispatchPaths",
    "Scenario",
    "ScenarioPlan",
    "ScenarioResult",
]
