"""Scenario: ``--source-already-staged`` defers discovery to a secondary.

Single concern: pin Step 10 (rosy-weaving-cascade) — when the
submitter passes ``--source-already-staged <path>``, discovery moves
off the submitter onto the chosen setup-secondary, which runs
Python ``task.discover_items`` against its locally bind-mounted
source and feeds the result back into the cluster ledger.

Three reasons a secondary becomes primary
-----------------------------------------

The plan distinguishes three causes (legacy bootstrap, setup-promote,
failover) and uses the wire flag ``PromotePrimary.required_setup`` —
NOT ledger emptiness — as the discriminator. This e2e exercises the
setup-promote arm end-to-end through both
``--multi-computer single-process`` and ``--multi-computer local``.

What this scenario asserts
--------------------------

For each mode plan:

1. The dispatch exits zero.
2. The published outputs match what the canonical consumer would have
   emitted for ``num_tasks`` items (so a "discovery returned nothing
   silently" regression would surface as a missing-files assertion).
3. ``--source-already-staged`` flipped discovery off the submitter:
   the framework's ``Pre-staged source mode: deferring task
   discovery`` line (``python/dynamic_runner/run.py``) is present —
   the load-bearing behaviour this scenario pins.
4. Discovery then ran on EXACTLY ONE promoted secondary: the
   consumer's ``discover_items: N produce + N consume = M items``
   line (narrated from inside the ``discover_on_promotion`` closure
   that runs on the chosen setup-secondary) appears exactly once.
   More than one occurrence would mean we promoted more than one
   secondary, breaking the "only the chosen setup-secondary runs
   discovery" contract.
5. Discovery returned a non-empty corpus — that ``discover_items``
   line ends in ``= M items`` with ``M > 0``. A 0-item discovery
   would mean the setup-secondary saw an empty bind-mount and the
   run completed via the "nothing to do" path rather than the
   discovery-then-process path the scenario exists to exercise.

Cross-mode parametrization
--------------------------

The scenario emits TWO plans by default:

* ``single-process`` — the in-process distributed pipeline (primary
  + N secondaries inside one Python process, channel transports).
* ``local`` — network-based primary + local-subprocess secondaries
  (``--multi-computer local``).

A SLURM cross-mode plan would require ``slurm-test-env`` running.
The plan acknowledges that path; it's covered by the
``distributed_local_subprocess`` and ``distributed_single_process``
scenarios' regression-pin semantics for their respective dispatch
helpers — they don't currently parametrize over the
``--source-already-staged`` flag, but the framework path for SLURM
is the SAME as for local (same ``source_pre_staged_root`` plumbing
in the PyO3 wrapper). Adding a SLURM plan here would be
test-infrastructure-heavy for a near-redundant assertion; the
single-process + local pair is the smallest set that proves the
framework path works across the dispatch-helper variations.

Why ``--source`` and ``--source-already-staged`` both point at the
same dir
-------------------------------------------------------------------

``--source`` is the consumer-side path that ``discover_items``
walks; ``--source-already-staged`` is the framework's signal that
the submitter has no local view of the corpus and that the
setup-secondary's bind-mounted ``src_network`` IS that path. In
the single-host pipelines used by ``single-process`` and ``local``
modes the bind-mount IS the submitter's local dir (no SSH, no
container), so the two flag values are the same path — the
discriminator is the FLAG'S PRESENCE, not the path being remote.
For SLURM mode the two would differ (the staged path lives on the
gateway's NFS), but the framework's setup-promote handshake is
identical regardless.
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


# Log markers the scenario greps for. Centralised so the assertion
# block stays declarative and a framework log rename surfaces here,
# not buried inside a regex.
#
# ``_DEFER_MARKER`` is the framework's own narration that
# ``--source-already-staged`` flipped discovery off the submitter
# (``python/dynamic_runner/run.py``) — the load-bearing behaviour this
# scenario pins. ``_DISCOVER_ITEMS_REGEX`` then matches the consumer's
# discovery line that runs on the PROMOTED secondary; its single
# occurrence proves exactly one secondary was promoted to run
# discovery, and the captured count feeds the non-empty check.
_DEFER_MARKER = "Pre-staged source mode: deferring task discovery"
_DISCOVER_ITEMS_REGEX = re.compile(r"discover_items: .*= (\d+) items")


class SourceAlreadyStagedScenario(Scenario):
    name = "source-already-staged"
    description = (
        "Pre-stage the source corpus; dispatch with "
        "--source-already-staged so discovery moves to a setup-secondary. "
        "Runs across --multi-computer single-process AND --multi-computer "
        "local; SLURM cross-mode is covered by sibling distributed-* scenarios."
    )
    requires = ("rosy-weaving-cascade-step-10",)

    def prepare(
        self, env: DispatchEnv, tmp_root: Path
    ) -> list[ScenarioPlan]:
        # Stage one shared corpus. Both plans point ``--source`` AND
        # ``--source-already-staged`` at the SAME dir — see module
        # docstring "Why both flags point at the same dir".
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
            # `build_dispatch_argv` only appends `--jobs` for slurm
            # and local modes; single-process needs it explicit so
            # `_dispatch_single_process` builds ≥1 secondary (the
            # setup-promote path requires AT LEAST one secondary to
            # delegate to — CLI validation enforces this in
            # `cli.py::validate_parsed_args`).
            if mode == "single-process":
                argv += ["--jobs", str(_JOBS)]
            # The load-bearing flag for this scenario. Path is the
            # same as `--source` here (single-host pipeline; no
            # remote bind-mount); the FLAG'S PRESENCE flips the
            # framework into setup-promote mode.
            argv += ["--source-already-staged", str(paths.source)]
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

            # `--source-already-staged` flipped discovery off the
            # submitter — the load-bearing behaviour this scenario
            # pins. The framework narrates the deferral once per plan.
            if _DEFER_MARKER not in log_text:
                failures.append(
                    f"{prefix} expected '{_DEFER_MARKER}' in dispatch "
                    f"log; not found — `--source-already-staged` did "
                    f"not defer discovery off the submitter "
                    f"(log: {result.log_file})"
                )

            # Discovery ran on exactly one promoted secondary. The
            # consumer narrates its discover_items call from inside the
            # discover_on_promotion closure; a single occurrence proves
            # only ONE secondary was promoted to run discovery (more
            # than one would break the "only the chosen setup-secondary
            # runs discovery" contract). The captured count feeds the
            # non-empty check.
            discover_hits = _DISCOVER_ITEMS_REGEX.findall(log_text)
            if len(discover_hits) != 1:
                failures.append(
                    f"{prefix} expected exactly 1 consumer "
                    f"'discover_items: ... = M items' line in dispatch "
                    f"log; got {len(discover_hits)} — discovery did not "
                    f"run on exactly one promoted secondary "
                    f"(log: {result.log_file})"
                )
            elif int(discover_hits[0]) == 0:
                # Discovery returning zero items: the setup-secondary
                # saw an empty bind-mount and the run finished via the
                # "nothing to do" path, not the discovery-then-process
                # path this scenario exercises.
                failures.append(
                    f"{prefix} discovery returned 0 items — the setup-"
                    f"secondary saw an empty bind-mount; check "
                    f"`--source-already-staged` path resolution "
                    f"(log: {result.log_file})"
                )

        return (not failures, failures)


SCENARIO = SourceAlreadyStagedScenario()
