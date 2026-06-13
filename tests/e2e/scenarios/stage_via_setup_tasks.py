"""Scenario: ``--stage-via-setup-tasks`` routes file-staging through the
setup-task model (#489 P3/P4 — the #488-free path).

Single concern: pin the ``--stage-via-setup-tasks`` flag end-to-end —
when the flag is ON, the framework's file-staging is the setup-task
model (each file-backed work task gains a per-file PRE-SUCCEEDED setup
task in the replicated ledger + a ``TaskDep`` gating the work task on
it), the legacy ``StageFile`` fan-out is SUPPRESSED, and the dependent
work tasks dispatch + complete only once their staging dep resolves
from the ledger's ``SetupCompleted`` entries. A converged output set is
the end-to-end proof that the dep machine resolved the injected setup
tasks.

Why ``--source-already-staged`` is paired with the flag here
-----------------------------------------------------------

The flag selects the staging SELECTOR; it does NOT itself transfer
bytes. It seeds the per-file setup task PRE-SUCCEEDED in the ledger so
the dependent work task's ``TaskDep`` resolves, but the worker still
needs the file physically resolvable. The setup-task model is therefore
exercised on the corpus-pre-staged path (``--source-already-staged``):
the file is already present on the cluster (mode-2 bind-mount; here, the
single-host pipeline's local dir), so the worker resolves it by
existence (``pre_staged_mode``) while the setup-task ledger gating
drives the dependency readiness. This is exactly the #488-relevant
combination — a pre-staged file becomes a pre-SUCCEEDED setup task in
the REPLICATED ledger, so any primary (original / relocated / promoted)
resolves the dependent's dep from the ledger rather than a per-node
``pre_staged_mode`` flag it could mis-stamp.

(The cold-seed mode-1 combination — flag ON, files on the submitter, NO
``--source-already-staged`` — is NOT exercised here: with the legacy
``StageFile`` fan-out suppressed and no pre-staged corpus, the worker
has no physical-resolution path and the work task fails NonRecoverable.
That combination is left to owner adjudication; the headline feature is
the pre-staged / relocate-safe path this scenario pins.)

What this scenario asserts
--------------------------

For each mode plan (``single-process`` AND ``local``):

1. The dispatch exits zero — the run completed.
2. The published outputs match what the canonical consumer emits for
   ``num_tasks`` items. This is the END-TO-END proof of the setup-task
   gating: each work task carries a ``TaskDep`` on its injected per-file
   setup task; if the pre-succeeded ``SetupCompleted`` ledger entry did
   NOT resolve that dep, the work task would stay ``Blocked``, never
   dispatch, and its output would be missing.
3. The flag flipped the staging SELECTOR and SUPPRESSED the legacy
   ``StageFile`` walk: the framework's
   ``auto-stage skipped: staging-via-setup-tasks is on`` line
   (``crates/dynrunner-manager-distributed/src/primary/staging.rs``,
   ``maybe_auto_stage_initial``) is present — the load-bearing marker
   that distinguishes the setup-task path from the legacy path. It is a
   ``debug``-level line, so the plan adds ``--debug`` (the framework's
   sole verbosity knob — there is no ``RUST_LOG`` read; see
   ``crates/dynrunner-pyo3/src/logging/mod.rs``).
4. The run-complete summary reports ``setup_succeeded=N`` with ``N > 0``
   — the framework's own count of setup tasks that reached
   ``SetupCompleted`` (the run-narrator summary, info level; see
   ``crates/dynrunner-manager-distributed/src/run_narrator.rs``). This
   is the positive evidence that the per-file setup tasks were SEEDED
   and resolved (not merely that the legacy path was skipped): with the
   flag off there are no framework setup tasks, so ``setup_succeeded``
   would be 0.

Cross-mode parametrization
--------------------------

The scenario emits TWO plans by default — ``single-process`` (in-process
distributed pipeline) and ``local`` (network primary + local-subprocess
secondaries). The framework's flagged-staging path is identical across
the dispatch-helper variations, so this pair is the smallest set that
proves the path works without ``slurm-test-env``-heavy infrastructure
(the SLURM path shares the same ``PrimaryConfig.staging_strategy``
plumbing — the marker assertion would be identical).

#488 relocate-safe coverage
---------------------------

This scenario IS the relocate-safe path, not merely a static-primary
one: pairing the flag with ``--source-already-staged`` puts the run on
the mesh-always relocation path — the submitter originates a relocated
seed (``SeedSource::RelocatedSeed``, ``DiscoveryDebt=Owed``) and hands
the primary role to a compute peer, whose ``discover_on_promotion``
discovers the corpus AND applies the setup-task augmentation. So the
per-file setup tasks are SEEDED BY THE RELOCATED PRIMARY and the
dependent work tasks resolve their deps from the replicated ledger — no
``pre_staged_mode`` per-node flag is mis-stamped across the relocation
(the #488 defect of the legacy path). The seed-augmentation thus runs
on the ``discover_on_promotion`` originator live in this scenario (the
``originate_cold_seed`` originator carries the same transform for the
non-relocated path). The relocate-through-failover variant is pinned at
the unit level by ``relocated_primary_reads_ledger_and_dep_is_satisfied``
(``crates/dynrunner-manager-distributed/src/primary/tests/setup_staging.rs``).
"""

from __future__ import annotations

import dataclasses
import re
from pathlib import Path

from ._assertions import assert_files_present, expected_canonical_outputs
from ._base import DispatchEnv, Scenario, ScenarioPlan, ScenarioResult
from ._dispatch import build_dispatch_argv
from ._staging import stage_inputs


_NUM_TASKS_PER_PHASE = 2
_JOBS = 2


# Log markers the scenario greps for. Centralised so the assertion block
# stays declarative and a framework log rename surfaces here, not buried
# inside a string literal.
#
# ``_SETUP_TASK_STAGING_MARKER`` is the framework's own narration that the
# ``--stage-via-setup-tasks`` selector flipped staging to the setup-task
# model AND suppressed the legacy StageFile fan-out — the load-bearing
# behaviour this scenario pins (``maybe_auto_stage_initial``, debug level).
# ``_SETUP_SUCCEEDED_REGEX`` captures the run-narrator's count of setup
# tasks that reached SetupCompleted — positive proof the per-file setup
# tasks were seeded and resolved (0 when the flag is off). It matches the
# CONTIGUOUS, ANSI-free message body of the ``run complete:`` summary (the
# structured ``setup_succeeded=`` field is colorized with interleaved ANSI
# escapes, so the human-readable ``/ N setup /`` clause is the stable
# match target).
_SETUP_TASK_STAGING_MARKER = "auto-stage skipped: staging-via-setup-tasks is on"
_SETUP_SUCCEEDED_REGEX = re.compile(
    r"run complete: \d+ succeeded / (\d+) setup /"
)


class StageViaSetupTasksScenario(Scenario):
    name = "stage-via-setup-tasks"
    description = (
        "Pre-stage the corpus and dispatch with --stage-via-setup-tasks "
        "so file-staging routes through the setup-task model (per-file "
        "pre-succeeded setup tasks + TaskDep gating; the #488-free path); "
        "assert the run completes, outputs land, the legacy StageFile walk "
        "is suppressed, and setup tasks were seeded. Single-process AND local."
    )
    requires = ("setup-task-staging-489",)

    def prepare(
        self, env: DispatchEnv, tmp_root: Path
    ) -> list[ScenarioPlan]:
        # Pre-staged corpus: the file is already present (single-host
        # pipeline's local dir IS the secondary's view), so the worker
        # resolves it by existence while the setup-task ledger gating
        # drives readiness — see the module docstring "Why
        # --source-already-staged is paired with the flag here".
        paths = stage_inputs(tmp_root, _NUM_TASKS_PER_PHASE)
        plans: list[ScenarioPlan] = []
        for mode in ("single-process", "local"):
            mode_env = dataclasses.replace(env, mode=mode)
            argv = build_dispatch_argv(
                env=mode_env,
                source=paths.source,
                output=paths.output,
                num_tasks=_NUM_TASKS_PER_PHASE,
                jobs=_JOBS,
            )
            # `build_dispatch_argv` only appends `--jobs` for slurm/local
            # modes; single-process needs it explicit so the setup-task
            # path has ≥1 secondary to dispatch the (unblocked) work
            # tasks to (mirrors `source_already_staged`).
            if mode == "single-process":
                argv += ["--jobs", str(_JOBS)]
            # The load-bearing flag for this scenario: route file-staging
            # through the setup-task model. Paired with
            # `--source-already-staged` (same dir; single-host pipeline)
            # so the file is physically resolvable while the setup-task
            # ledger gating drives dependency readiness. `--debug` raises
            # the framework's verbosity ceiling so the debug-level
            # `auto-stage skipped: staging-via-setup-tasks is on` marker
            # reaches the captured log (the framework reads NO RUST_LOG —
            # `--debug` is the only knob).
            argv += [
                "--stage-via-setup-tasks",
                "--source-already-staged",
                str(paths.source),
                "--debug",
            ]
            plans.append(
                ScenarioPlan(argv=argv, paths=paths, label=mode)
            )
        return plans

    def assert_outputs(
        self, env: DispatchEnv, results: list[ScenarioResult]
    ) -> tuple[bool, list[str]]:
        del env
        if len(results) < 1:
            return (False, ["no results — scenario emitted no plans"])

        failures: list[str] = []
        expected = expected_canonical_outputs(_NUM_TASKS_PER_PHASE)

        for result in results:
            label = result.plan.label or "<unlabelled>"
            prefix = f"[{label}]"

            if result.exit_code != 0:
                failures.append(
                    f"{prefix} dispatch exited non-zero: {result.exit_code} "
                    f"(see {result.log_file})"
                )
                continue

            # End-to-end gating proof: a complete output set means each
            # work task's staging `TaskDep` resolved from the ledger's
            # pre-succeeded `SetupCompleted` entry and the work task
            # dispatched. A missing output would mean a work task stayed
            # `Blocked` on an unresolved setup-task dep.
            ok_present, missing = assert_files_present(
                result.plan.paths.publish_dst, expected
            )
            if not ok_present:
                failures.extend(f"{prefix} {m}" for m in missing)

            try:
                log_text = result.log_file.read_text(
                    encoding="utf-8", errors="replace"
                )
            except OSError as e:
                failures.append(
                    f"{prefix} could not read dispatch log {result.log_file}: {e}"
                )
                continue

            # The flag flipped the staging selector to the setup-task
            # model AND suppressed the legacy StageFile fan-out — the
            # load-bearing behaviour this scenario pins.
            if _SETUP_TASK_STAGING_MARKER not in log_text:
                failures.append(
                    f"{prefix} expected '{_SETUP_TASK_STAGING_MARKER}' in "
                    f"dispatch log; not found — `--stage-via-setup-tasks` "
                    f"did not select the setup-task staging model (or "
                    f"`--debug` did not surface the marker) "
                    f"(log: {result.log_file})"
                )

            # Positive proof the per-file setup tasks were SEEDED and
            # reached SetupCompleted: the run-narrator summary reports a
            # non-zero `setup_succeeded` count. With the flag off there
            # are no framework setup tasks, so this would be 0.
            hits = _SETUP_SUCCEEDED_REGEX.findall(log_text)
            if not hits:
                failures.append(
                    f"{prefix} no 'run complete: ... / <N> setup /' summary in "
                    f"the dispatch log — the run did not narrate a setup-task "
                    f"count (log: {result.log_file})"
                )
            elif max(int(h) for h in hits) == 0:
                failures.append(
                    f"{prefix} 0 setup tasks in the run-complete summary — "
                    f"`--stage-via-setup-tasks` seeded no per-file setup "
                    f"tasks; the setup-task staging model did not run "
                    f"(log: {result.log_file})"
                )

        return (not failures, failures)


SCENARIO = StageViaSetupTasksScenario()
