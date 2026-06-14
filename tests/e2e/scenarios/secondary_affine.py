"""Scenario: SecondaryAffine per-secondary run-once import gate (#497).

Single concern: PROVE the #497 distributed affine chain end-to-end on a
real cluster — the gate #503 the consumers wait on. The full primary→
secondary distributed flow (import_action reconstructed + run in the
SECONDARY process, the AffineReady originator, the run-once latch) had
only ever been UNIT-tested; the live-blockers each passed green unit tests
yet deadlocked live, so this scenario drives the REAL flow.

Two scenarios, one per AffineReady firing surface — because #503's e2e run
FOUND that the two surfaces are NOT both wired in the distributed seed flow:

* ``secondary-affine`` (NO-DEP) — a gate born ``AffineReady`` at spawn. This
  PASSES: the seed ledger scan resolves it immediately and its dependents
  unblock. Proves assertion 4 + that the affine path works for a no-dep gate.

* ``secondary-affine-withdep`` (WITH-DEP) — a gate depending on a no-op
  upload stand-in ``U`` (the CANONICAL upload→import→build production shape
  asm-dataset uses). The seed fans every task out as ``TaskAdded`` so the gate
  is born CRDT ``Pending`` (never ``Blocked``); #506 added the post-seed firing
  surface that ORIGINATES ``AffineReady`` when the gate's dep ``U`` completes,
  so the gate resolves the moment ``U`` finishes and its dependents unblock
  (per-secondary run-once import first). This scenario is EXPECT-PASS: it
  asserts the SAME full #497 invariants as the no-dep variant (the run
  converges, import-once-per-secondary, all builds done, assertion 4 via the
  post-U-completion transition). A failure is a real #497/#506 regression.

The proof (no-dep, the passing gate)
------------------------------------

The consumer's ``import_action`` runs ONCE per secondary inside the
secondary process and appends ``socket.gethostname()`` (the per-node
identity — the wrapper sets ``--hostname`` to the SLURM worker node's
FQDN) to a shared-NFS marker file. The build worker appends its own node
identity to a sibling marker. The assertions read both markers back
(via gateway ssh in SLURM mode, the publish_dst tmpdir in local mode):

  1. ``import_action`` ran EXACTLY ONCE per secondary that received ≥1
     build: distinct import-marker nodes == distinct build-marker nodes,
     and NO node appears in the import marker more than once.
  2. ALL k builds completed (none stranded/deadlocked behind the gate).
  3. Multi-dependent-same-secondary: at least one node ran ≥2 builds yet
     imported exactly once (non-vacuous run-once-under-concurrency).
  4. No-dep affine: the builds (gated on the no-dep gate) completed — proof
     the gate reached ``AffineReady`` at spawn and unblocked its dependents.

A failure in the no-dep scenario is a REAL #497 bug (RCA it, don't paper over).
"""

from __future__ import annotations

import time
from pathlib import Path

from ._assertions import assert_files_present
from ._base import DispatchEnv, Scenario, ScenarioPlan, ScenarioResult
from ._dispatch import build_dispatch_argv
from ._ssh import gateway_ssh
from ._staging import stage_inputs

# Public naming/marker contract owned by the consumer — re-derived here so a
# rename touches exactly one place (the consumer task module).
from ..test_consumer.task import (
    AFFINE_BUILD_MARKER,
    AFFINE_IMPORT_MARKER,
    affine_input_file_count,
    expected_affine_outputs,
)


_VARIANT_NODEP = "nodep"
_VARIANT_WITHDEP = "withdep"

# Prefix the no-dep gate's builds carry (assertion 4 isolates them). Mirrors
# the consumer's build_task_id(_AFFINE_NODEP_ID, j) shape.
_NODEP_BUILD_PREFIX = "build-affine-import-nodep-"

# Make each task slow enough that the work actually spreads across secondaries
# instead of one fast secondary draining the whole build queue before its peers
# come online — the same rationale as parallel-4-workers. Multi-secondary
# distribution is what makes assertions 1+3 meaningful.
_TASK_SLEEP_S = "0.4"

# Post-dispatch convergence budget for the markers + outputs to land on the
# shared mount. The SLURM bring-up (image build + submit + queue + setup) plus
# the affine import gate + k builds fit comfortably; sized like the failover
# scenario's deadline.
_CONVERGE_DEADLINE_S = 120.0
_POLL_INTERVAL_S = 2.0


def _gateway_out_dir(env: DispatchEnv) -> str:
    """The gateway-side shared NFS dir SLURM workers publish to
    (``<slurm_root_folder>/out``, bind-mounted as ``/app/out-network``).
    Mirrors primary_death_failover._gateway_out_dir."""
    return f"{env.slurm_root_folder}/out"


def _prepare_variant(
    env: DispatchEnv, tmp_root: Path, variant: str
) -> list[ScenarioPlan]:
    """Shared prepare: stage one input file per emitted task and build the
    ``--secondary-affine --secondary-affine-variant <variant>`` dispatch.

    Both the no-dep and with-dep variants run under the driver's default
    ``--timeout`` and are expected to converge + exit 0 (the with-dep #506
    deadlock is fixed), so neither sets a bounded plan timeout nor declares a
    non-zero exit. A hang now surfaces honestly as the driver's overall-timeout
    (exit 124), the real failure signal."""
    n_files = affine_input_file_count(variant)
    paths = stage_inputs(tmp_root, n_files)
    # The gateway out dir is SHARED across runs; clear it so the markers +
    # outputs this run reads back are ONLY from this run (a stale import
    # marker would inflate the per-secondary count and fail assertion 1).
    if env.mode == "slurm":
        _clear_gateway_out_dir(env)
    argv = build_dispatch_argv(
        env=env,
        source=paths.source,
        output=paths.output,
        num_tasks=n_files,
        extra_args=("--secondary-affine", "--secondary-affine-variant", variant),
    )
    return [
        ScenarioPlan(
            argv=argv,
            paths=paths,
            # Slow tasks force multi-secondary distribution so the
            # per-secondary run-once proof is non-vacuous.
            extra_env={"DYNRUNNER_E2E_TASK_SLEEP_S": _TASK_SLEEP_S},
        )
    ]


def _read_marker(
    env: DispatchEnv, publish_dst: Path, marker_name: str
) -> list[str] | None:
    """Read a marker file's non-empty lines, mode-aware. ``None`` when the
    marker does not exist / is unreadable (a hard failure for the caller)."""
    if env.mode == "slurm":
        cmd = f"cat {_gateway_out_dir(env)}/{marker_name} 2>/dev/null"
        try:
            proc = gateway_ssh(env, cmd, timeout_s=15)
        except Exception:
            return None
        if proc.returncode != 0:
            return None
        return [ln for ln in proc.stdout.splitlines() if ln.strip()]
    marker = publish_dst / marker_name
    if not marker.exists():
        return None
    return [ln for ln in marker.read_text().splitlines() if ln.strip()]


def _poll_outputs(
    env: DispatchEnv,
    publish_dst: Path,
    expected: list[str],
    deadline_s: float,
) -> tuple[bool, list[str]]:
    """Poll until every expected output filename is present (mode-aware) or
    the deadline elapses. Returns (ok, missing)."""
    deadline = time.monotonic() + deadline_s
    last_missing: list[str] = list(expected)
    expected_set = set(expected)
    while time.monotonic() < deadline:
        if env.mode == "slurm":
            cmd = f"ls -1 {_gateway_out_dir(env)} 2>/dev/null"
            try:
                proc = gateway_ssh(env, cmd, timeout_s=10)
            except Exception:
                time.sleep(_POLL_INTERVAL_S)
                continue
            if proc.returncode != 0:
                time.sleep(_POLL_INTERVAL_S)
                continue
            present = {
                ln.strip() for ln in proc.stdout.splitlines() if ln.strip()
            }
            missing = sorted(expected_set - present)
        else:
            ok, miss = assert_files_present(publish_dst, expected)
            missing = [] if ok else miss
        if not missing:
            return (True, [])
        last_missing = missing
        time.sleep(_POLL_INTERVAL_S)
    return (False, last_missing)


def _clear_gateway_out_dir(env: DispatchEnv) -> None:
    """Delete every regular file under the gateway out dir so this run's
    markers + outputs aren't polluted by prior runs. Best-effort (a clear
    failure surfaces later as a convergence timeout). Mirrors
    primary_death_failover._clear_gateway_out_dir."""
    out_dir = _gateway_out_dir(env)
    cmd = f"find {out_dir!s} -mindepth 1 -maxdepth 8 -type f -delete 2>/dev/null; true"
    try:
        gateway_ssh(env, cmd, timeout_s=15)
    except Exception:
        pass


def _assert_affine_invariants(
    env: DispatchEnv,
    result: ScenarioResult,
    variant: str,
) -> tuple[bool, list[str]]:
    """The full #497 owner-spec invariant check for an affine run of
    ``variant``. Returns ``(converged, failures)``: ``converged`` is whether
    ALL builds completed (assertion 2) AND every marker invariant held;
    ``failures`` carries the diagnostics. Shared verbatim by both the no-dep
    and with-dep scenarios — both are expect-pass (the #506 fix proven)."""
    publish_dst = result.plan.paths.publish_dst
    failures: list[str] = []

    # --- ASSERTION 2: ALL builds completed (none stranded) ---
    expected = expected_affine_outputs(variant)
    ok_outputs, missing = _poll_outputs(
        env, publish_dst, expected, _CONVERGE_DEADLINE_S
    )
    if not ok_outputs:
        return (
            False,
            [
                f"ASSERTION 2: NOT all builds completed within "
                f"{_CONVERGE_DEADLINE_S:.0f}s — gated builds stranded behind "
                f"the affine import (the deadlock symptom). missing: "
                f"{missing[:8]}{'...' if len(missing) > 8 else ''}. "
                f"mode={env.mode}, variant={variant}."
            ],
        )

    # --- read both markers back ---
    import_lines = _read_marker(env, publish_dst, AFFINE_IMPORT_MARKER)
    build_lines = _read_marker(env, publish_dst, AFFINE_BUILD_MARKER)
    if import_lines is None:
        return (False, ["import marker unreadable — import_action never ran?"])
    if build_lines is None:
        return (False, ["build marker unreadable — no build recorded a node?"])

    import_by_node: dict[str, int] = {}
    for line in import_lines:
        node = line.split("\t", 1)[0]
        import_by_node[node] = import_by_node.get(node, 0) + 1
    builds_by_node: dict[str, int] = {}
    for line in build_lines:
        node = line.split("\t", 1)[0]
        builds_by_node[node] = builds_by_node.get(node, 0) + 1

    importing_nodes = set(import_by_node)
    building_nodes = set(builds_by_node)

    # --- ASSERTION 1: import ran EXACTLY ONCE per building secondary ---
    over_imported = {n: c for n, c in import_by_node.items() if c > 1}
    if over_imported:
        failures.append(
            f"ASSERTION 1 (run-once): node(s) ran import_action MORE THAN "
            f"ONCE: {over_imported}. The node-local run-once latch did not "
            "gate concurrent dependents."
        )
    if importing_nodes != building_nodes:
        failures.append(
            f"ASSERTION 1: importing nodes {sorted(importing_nodes)} != "
            f"building nodes {sorted(building_nodes)}. A building node with no "
            "import means a build ran without its gate; an importing node with "
            "no build means a spurious import."
        )
    if len(import_lines) != len(building_nodes):
        failures.append(
            f"ASSERTION 1: total import invocations {len(import_lines)} != "
            f"distinct building secondaries {len(building_nodes)}. Expected "
            "exactly one import per building secondary."
        )

    # --- ASSERTION 3: multi-dependent-same-secondary imports ONCE ---
    multi_build_nodes = {n: c for n, c in builds_by_node.items() if c >= 2}
    if not multi_build_nodes:
        failures.append(
            "ASSERTION 3 VACUOUS: no secondary ran ≥2 builds, so the "
            "import-once-under-multi-dependent invariant was not exercised "
            f"(builds per node: {builds_by_node}). Increase task spread / k, "
            "or reduce secondaries."
        )
    else:
        bad = {
            n: import_by_node.get(n, 0)
            for n in multi_build_nodes
            if import_by_node.get(n, 0) != 1
        }
        if bad:
            failures.append(
                f"ASSERTION 3: node(s) ran ≥2 builds but did NOT import "
                f"exactly once: imports={bad}, builds="
                f"{ {n: multi_build_nodes[n] for n in bad} }."
            )

    # --- ASSERTION 4: the gate unblocked its dependents ---
    # For the no-dep variant this is the AffineReady-at-spawn proof; for the
    # with-dep variant it confirms the post-U-completion AffineReady fired.
    if variant == _VARIANT_NODEP:
        nodep_build_lines = [
            ln for ln in build_lines if _NODEP_BUILD_PREFIX in ln
        ]
        if not nodep_build_lines:
            failures.append(
                "ASSERTION 4: no no-dep-gate build ran — the no-dep affine "
                "gate did not reach AffineReady at spawn so its dependents "
                "never unblocked."
            )

    if failures:
        return (False, failures)

    print(
        f"[secondary-affine:{variant}] PROVEN: {len(import_lines)} import(s) "
        f"across {len(importing_nodes)} secondaries == {len(building_nodes)} "
        f"building secondaries, exactly once each. builds/node="
        f"{builds_by_node}. multi-build nodes (import-once): "
        f"{ {n: import_by_node[n] for n in multi_build_nodes} }.",
        flush=True,
    )
    return (True, [])


class SecondaryAffineNoDepScenario(Scenario):
    """The PROVEN no-dep affine gate (#497) — AffineReady at spawn."""

    name = "secondary-affine"
    description = (
        "SecondaryAffine no-dep import gate (#497): a gate born AffineReady at "
        "spawn + k builds depending on it. Asserts import_action ran EXACTLY "
        "ONCE per building secondary (marker count == distinct building nodes), "
        "ALL k builds completed, multi-dependent-same-secondary imports once, "
        "and the no-dep gate unblocked its dependents at spawn."
    )
    requires = ("affine-497",)

    def prepare(
        self, env: DispatchEnv, tmp_root: Path
    ) -> list[ScenarioPlan]:
        return _prepare_variant(env, tmp_root, _VARIANT_NODEP)

    def assert_outputs(
        self, env: DispatchEnv, results: list[ScenarioResult]
    ) -> tuple[bool, list[str]]:
        converged, failures = _assert_affine_invariants(
            env, results[0], _VARIANT_NODEP
        )
        return (converged, failures)


class SecondaryAffineWithDepScenario(Scenario):
    """The with-dep affine gate (#497) — the canonical upload→import→build shape.

    A with-dep SecondaryAffine gate ``I_dep`` depends on a no-op upload
    stand-in ``U`` (the production shape: the affine import sits between the
    toolchain upload and the builds). The seed fans every task out as
    ``TaskAdded`` so the gate is born CRDT ``Pending`` (never ``Blocked``);
    #506 added the post-seed firing surface that originates ``AffineReady``
    when the gate's dep ``U`` completes, so the gate resolves the moment ``U``
    finishes and its k builds dispatch (per-secondary run-once import first).

    Expect-pass (#506 fix proven). Asserts the SAME full #497 owner-spec
    invariants as the no-dep scenario via :func:`_assert_affine_invariants`:
    the run CONVERGES (no deadlock — assertion 2 all builds completed),
    ``import_action`` ran EXACTLY ONCE per building secondary (assertion 1),
    multi-dependent-same-secondary imports once (assertion 3), and the gate
    unblocked its dependents (assertion 4 — here the post-U-completion
    AffineReady transition). A failure here is a real #497/#506 regression
    (RCA it, don't paper over).
    """

    name = "secondary-affine-withdep"
    description = (
        "SecondaryAffine WITH-DEP import gate (#497) — the canonical "
        "upload→import→build shape. The gate becomes AffineReady when its "
        "upload-stand-in dep completes (#506 fix). Asserts the run CONVERGES "
        "(all k builds done, none stranded), import_action ran EXACTLY ONCE "
        "per building secondary, multi-dependent imports once, and the gate "
        "unblocked its dependents after the dep completed."
    )
    requires = ("affine-497",)

    def prepare(
        self, env: DispatchEnv, tmp_root: Path
    ) -> list[ScenarioPlan]:
        return _prepare_variant(env, tmp_root, _VARIANT_WITHDEP)

    def assert_outputs(
        self, env: DispatchEnv, results: list[ScenarioResult]
    ) -> tuple[bool, list[str]]:
        converged, failures = _assert_affine_invariants(
            env, results[0], _VARIANT_WITHDEP
        )
        return (converged, failures)


SCENARIO = SecondaryAffineNoDepScenario()
SCENARIO_WITHDEP = SecondaryAffineWithDepScenario()
