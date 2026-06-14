"""``SyntheticTask`` — the TaskDefinition for the e2e synthetic consumer.

Single concern: declare topology and discover items. The actual per-item
work lives in the worker module so each concern owns one file.

Topology
--------

Two phases with an explicit cross-phase dependency edge::

    produce ──depends_on──▶ consume

``produce`` contains two tasks. The second has an intra-phase
``task_depends_on=("produce-0",)`` so the framework gates its dispatch on
the first finishing — exercising the same code path that variant builds
take in the asm-tokenizer pipeline.

``consume`` contains two tasks. Each declares a cross-phase
``task_depends_on`` naming a specific ``produce`` task; the framework's
phase barrier already enforces "all of produce drained before any of
consume runs", but the per-task edge gives extra coverage of the
PendingPool's blocked-map.

Items
-----

Every TaskInfo's ``path`` points at a real file under ``source_dir``
(which the driver populates with N small input files before running).
The framework needs these files to exist because the SLURM packaging
path uploads ``TaskInfo.path`` to the gateway. In ``--multi-computer
local`` / single-process mode the path is read by the worker but no
upload happens.

The items use stable, readable ``task_id``s so the
``task_depends_on`` edges in the worker can resolve them by name.
"""

from __future__ import annotations

import logging
import os
import socket
from argparse import ArgumentParser, Namespace
from collections.abc import Iterable
from pathlib import Path

from dynamic_runner._shared import BinaryIdentifier, TaskDep, TaskInfo
from dynamic_runner.task_protocol import PhaseSpec, TaskTypeSpec, TypeId
from dynamic_runner.worker.publish import (
    DEFAULT_DST_ROOT,
    ENV_DST_ROOT,
)


_PHASE_PRODUCE = "produce"
_PHASE_CONSUME = "consume"
_TYPE_PRODUCE = "produce-default"
_TYPE_CONSUME = "consume-default"
_WORKER_MODULE = "tests.e2e.test_consumer.worker"
_NUM_TASKS_PER_PHASE = 2

# ── SecondaryAffine (#497) topology constants ───────────────────────────
# A SEPARATE, opt-in topology (selected by ``--secondary-affine``) that
# exercises the per-secondary run-once import gate. Kept entirely
# self-contained: the produce/consume topology above is untouched when the
# flag is off — the only cross-cut is the single discovery-time branch in
# :meth:`SyntheticTask.discover_items`, mirroring ``--keyed-outputs``.
_PHASE_SETUP = "setup"
_PHASE_IMPORT = "import"
_PHASE_BUILD = "build"
_TYPE_SETUP = "setup-default"
_TYPE_IMPORT = "import-default"
_TYPE_BUILD = "build-default"
# The two affine gates: ``I`` is born ready (no deps → AffineReady at spawn,
# the owner's "importantly also if it does not have any deps" case); ``I_dep``
# gates on the no-op upload stand-in ``U`` (→ AffineReady the moment its OWN
# dep is done — the canonical toolchain-upload→import→build production shape).
_AFFINE_NODEP_ID = "affine-import-nodep"
_AFFINE_DEP_ID = "affine-import-withdep"
_SETUP_TASK_ID = "affine-upload-stand-in"
# Which affine variant a run emits, selected by ``--secondary-affine-variant``.
# The two variants exercise the two AffineReady firing surfaces independently
# so a scenario can run the PROVEN no-dep gate on its own and quarantine the
# with-dep gate (which deadlocks until #506 lands — its post-seed
# dep-completion firing surface is missing). ``both`` emits the full topology.
_VARIANT_NODEP = "nodep"
_VARIANT_WITHDEP = "withdep"
_VARIANT_BOTH = "both"
_AFFINE_VARIANTS = (_VARIANT_NODEP, _VARIANT_WITHDEP, _VARIANT_BOTH)
# Builds split into two groups so EACH affine variant gets MULTIPLE
# same-secondary dependents (the run-once-under-multi-dependent proof). Group
# A gates on the no-dep gate, group B on the with-dep gate. The driver runs
# k=2*_BUILDS_PER_GROUP builds against WORKER_COUNT secondaries with
# k >> WORKER_COUNT, so several builds provably co-land per secondary.
_BUILDS_PER_GROUP = 8
# The shared-NFS marker file the per-secondary import_action appends its node
# identity to (one line per import invocation). Lives under the publish dst
# root (= ``/app/out-network`` in-container = the gateway's shared NFS out
# dir), so the e2e driver reads it back via the same gateway path the failover
# scenario ssh-reads. The COUNT of distinct node identities here is the whole
# #497 proof: it must equal the number of distinct secondaries that ran ≥1
# build, exactly once each.
AFFINE_IMPORT_MARKER = "_affine_import.marker"
# The sibling marker each build appends its (node, build_id, gate) line to, so
# the driver can correlate distinct BUILDING secondaries (and per-secondary
# build counts) against the import marker.
AFFINE_BUILD_MARKER = "_affine_build.marker"

_logger = logging.getLogger(__name__)


def _destination_root() -> Path:
    """The shared publish destination root, resolved the SAME way the worker
    does (``DYNRUNNER_PUBLISH_DST_ROOT`` env, default ``/app/out-network``).

    The ``import_action`` runs in the SECONDARY process (reconstructed there
    — the #501-class path), which inherits the wrapper's bind-mount defaults,
    so this resolves to the gateway's shared NFS out dir in SLURM mode and to
    the scenario's tmpdir (env-redirected) in local mode. Centralised so the
    import marker and the build marker land in the same readable place.
    """
    return Path(os.environ.get(ENV_DST_ROOT, DEFAULT_DST_ROOT))


def _produce_task_id(idx: int) -> str:
    return f"produce-{idx}"


def _consume_task_id(idx: int) -> str:
    return f"consume-{idx}"


def _input_filename(idx: int) -> str:
    """Filename for ``produce-{idx}``'s input file under ``source_dir``.

    The same input is reused by ``consume-{idx}`` — there is no need to
    fabricate a second file just to give the consumer phase a path. The
    consumer's actual data dependency lives in the producer's PUBLISHED
    output (under ``out-network``); the source path just drives the
    framework's per-task wire identifier.
    """
    return f"input-{idx}.txt"


class SyntheticTask:
    """TaskDefinition for the e2e synthetic consumer."""

    # ── Topology ────────────────────────────────────────────────────────

    def get_phases(self) -> tuple[PhaseSpec, ...]:
        produce = PhaseSpec(
            phase_id=_PHASE_PRODUCE,
            types=(
                TaskTypeSpec(
                    type_id=_TYPE_PRODUCE,
                    worker_module=_WORKER_MODULE,
                ),
            ),
        )
        consume = PhaseSpec(
            phase_id=_PHASE_CONSUME,
            types=(
                TaskTypeSpec(
                    type_id=_TYPE_CONSUME,
                    worker_module=_WORKER_MODULE,
                ),
            ),
            depends_on=(_PHASE_PRODUCE,),
        )
        # The SecondaryAffine topology phases ride ALONGSIDE produce/consume:
        # declaring them unconditionally is harmless when no affine items are
        # discovered (an empty phase drains immediately), and keeps get_phases
        # free of the discovery-time opt-in branch — the framework's phase
        # state machine tolerates phases with zero items. The "import" phase
        # holds the affine GATE tasks (never worker-assigned — a TaskTypeSpec
        # is still required for the phase to exist, but no build runs there).
        #
        # CRUCIALLY these phases carry NO inter-phase ``depends_on`` edges:
        # the ordering is expressed PURELY through per-task ``TaskDep`` edges
        # (build → its gate; I_dep → U), the canonical production shape ("the
        # affine import sits between the toolchain upload and the build").
        # A phase-level ``build depends_on import depends_on setup`` chain
        # would MASK the no-dep gate's "AffineReady at spawn" property — the
        # group-A no-dep builds would wait for U via the phase barrier even
        # though they have no dep on U. Per-task edges keep the no-dep path
        # un-coupled from setup, so assertion 4 actually tests spawn-readiness.
        setup = PhaseSpec(
            phase_id=_PHASE_SETUP,
            types=(
                TaskTypeSpec(
                    type_id=_TYPE_SETUP,
                    worker_module=_WORKER_MODULE,
                ),
            ),
        )
        affine_import = PhaseSpec(
            phase_id=_PHASE_IMPORT,
            types=(
                TaskTypeSpec(
                    type_id=_TYPE_IMPORT,
                    worker_module=_WORKER_MODULE,
                ),
            ),
        )
        build = PhaseSpec(
            phase_id=_PHASE_BUILD,
            types=(
                TaskTypeSpec(
                    type_id=_TYPE_BUILD,
                    worker_module=_WORKER_MODULE,
                ),
            ),
        )
        return (produce, consume, setup, affine_import, build)

    # ── Item discovery ─────────────────────────────────────────────────

    def discover_items(
        self, source_dir: Path, args: Namespace
    ) -> Iterable[TaskInfo]:
        """Emit produce + consume items.

        Reads ``--num-tasks`` from ``args`` (added by
        :meth:`add_task_arguments`) so the driver / a manual test run can
        scale the workload without touching this file.
        """
        n = getattr(args, "num_tasks", _NUM_TASKS_PER_PHASE)
        # Opt-in: the SecondaryAffine (#497) topology is a SEPARATE run shape.
        # When ``--secondary-affine`` is set, this run is dedicated to the
        # affine proof (no produce/consume items) — the flag picks the whole
        # topology, exactly the run-wide opt-in pattern ``--keyed-outputs``
        # uses, just for the discovered item SET rather than per-item payload.
        if bool(getattr(args, "secondary_affine", False)):
            variant = getattr(args, "secondary_affine_variant", _VARIANT_BOTH)
            return self._discover_affine_items(source_dir, variant)
        # Optional payload flag opting tasks into the keyed-outputs API
        # exercise (Task.publish_string on produce, Task.predecessor_outputs
        # read on consume). The flag rides on every task's payload so
        # the worker can branch on it per-task without re-parsing CLI;
        # discovery is the single place that knows the run-wide opt-in.
        keyed_outputs = bool(getattr(args, "keyed_outputs", False))
        items: list[TaskInfo] = []

        for idx in range(n):
            # Intra-phase dep: produce-i waits for produce-(i-1).
            # Strictly unnecessary — they could run in parallel — but
            # exercising the intra-phase edge is the whole point.
            prev: tuple[str, ...] = (
                (_produce_task_id(idx - 1),) if idx > 0 else ()
            )
            items.append(
                _build_task(
                    source_dir=source_dir,
                    idx=idx,
                    phase_id=_PHASE_PRODUCE,
                    type_id=_TYPE_PRODUCE,
                    task_id=_produce_task_id(idx),
                    task_depends_on=prev,
                    payload={
                        "kind": _PHASE_PRODUCE,
                        "idx": idx,
                        "keyed_outputs": keyed_outputs,
                    },
                )
            )

        for idx in range(n):
            items.append(
                _build_task(
                    source_dir=source_dir,
                    idx=idx,
                    phase_id=_PHASE_CONSUME,
                    type_id=_TYPE_CONSUME,
                    task_id=_consume_task_id(idx),
                    # Cross-phase dep: consume-i depends on produce-i.
                    # The phase barrier already gates this; the per-task
                    # edge additionally exercises PendingPool's
                    # blocked-map shape. A dependency's full identity is
                    # ``(phase_id, task_id)`` — a bare string resolves to
                    # the ENCLOSING phase (here: consume), where no
                    # ``produce-i`` exists, so the seed would classify the
                    # consume tasks ``InvalidTask { missing dep }``. The
                    # cross-phase edge MUST name the prerequisite's phase
                    # via the ``TaskDep`` dataclass (the documented
                    # consumer contract; see
                    # ``dynamic_runner._shared.task_info.TaskDep``).
                    task_depends_on=(
                        TaskDep(
                            task_id=_produce_task_id(idx),
                            phase_id=_PHASE_PRODUCE,
                        ),
                    ),
                    payload={
                        "kind": _PHASE_CONSUME,
                        "idx": idx,
                        "expects_output": _produce_output_filename(idx),
                        "keyed_outputs": keyed_outputs,
                    },
                )
            )

        _logger.info(
            "discover_items: %d produce + %d consume = %d items",
            n,
            n,
            len(items),
        )
        return items

    # ── SecondaryAffine (#497) discovery ───────────────────────────────

    def _discover_affine_items(
        self, source_dir: Path, variant: str
    ) -> list[TaskInfo]:
        """Emit the SecondaryAffine topology for ``variant``.

        Gates (emitted per ``variant``):
        * ``I`` (import phase, no deps) — a SecondaryAffine gate born
          ``AffineReady`` at spawn (the owner's "importantly also if it does
          not have any deps" case; assertion 4). Emitted for ``nodep``/``both``.
        * ``I_dep`` (import phase, TaskDep ``U``) — a SecondaryAffine gate that
          becomes ``AffineReady`` the MOMENT ``U`` completes (the canonical
          upload→import→build production shape). Emitted for ``withdep``/
          ``both``; pulls in the no-op upload stand-in ``U`` (setup phase).

        Each emitted gate gets _BUILDS_PER_GROUP builds depending on it
        (cross-phase ``TaskDep``), with k ≫ worker count so several builds
        co-land per secondary — making the run-once-under-multi-dependent
        invariant (assertion 3) non-vacuous.

        The affine GATE tasks are NEVER worker-assigned; their per-secondary
        import runs via :meth:`import_action` in the secondary process. Their
        ``path`` still points at a staged input only to satisfy the wire
        identifier — the gate is never opened by a worker.

        EVERY task gets a DISTINCT ``input-{idx}.txt`` path. The framework's
        content-hash recipe is ``{phase_id, path, identifier}`` (NOT task_id /
        payload — see ``crates/dynrunner-core/src/task_hash.rs``), so two
        tasks sharing a phase, path AND identifier collapse to ONE ledger
        entry. The builds all share ``phase_id="build"``, so a shared path
        would dedup all k builds to one — distinct paths keep them distinct,
        exactly as the produce/consume topology uses ``input-{i}.txt`` per
        task. The driver stages ``affine_input_file_count(variant)`` files.
        """
        want_nodep = variant in (_VARIANT_NODEP, _VARIANT_BOTH)
        want_withdep = variant in (_VARIANT_WITHDEP, _VARIANT_BOTH)
        items: list[TaskInfo] = []
        idx = 0  # running index → a distinct input-{idx}.txt per task

        if want_withdep:
            # U: the no-op upload stand-in (an ordinary work task in the setup
            # phase). It just publishes its canonical output so the with-dep
            # affine gate's AffineReady-on-completion transition has a real
            # terminal to fire on. Only the with-dep variant needs it.
            items.append(
                _build_task(
                    source_dir=source_dir,
                    idx=idx,
                    phase_id=_PHASE_SETUP,
                    type_id=_TYPE_SETUP,
                    task_id=_SETUP_TASK_ID,
                    task_depends_on=(),
                    payload={"kind": _PHASE_SETUP, "idx": idx},
                )
            )
            idx += 1

        # (gate_id, gate_deps) for each emitted gate.
        gates: list[tuple[str, tuple]] = []
        if want_nodep:
            gates.append((_AFFINE_NODEP_ID, ()))
        if want_withdep:
            gates.append(
                (
                    _AFFINE_DEP_ID,
                    (TaskDep(task_id=_SETUP_TASK_ID, phase_id=_PHASE_SETUP),),
                )
            )

        for gate_id, gate_deps in gates:
            items.append(
                _build_task(
                    source_dir=source_dir,
                    idx=idx,
                    phase_id=_PHASE_IMPORT,
                    type_id=_TYPE_IMPORT,
                    task_id=gate_id,
                    task_depends_on=gate_deps,
                    payload={"kind": _PHASE_IMPORT, "idx": idx},
                    is_secondary_affine=True,
                )
            )
            idx += 1

        # Builds: _BUILDS_PER_GROUP per emitted gate. Each build gates on its
        # gate via a cross-phase TaskDep (the gate lives in the "import" phase).
        for gate_id, _ in gates:
            for j in range(_BUILDS_PER_GROUP):
                build_id = build_task_id(gate_id, j)
                items.append(
                    _build_task(
                        source_dir=source_dir,
                        idx=idx,
                        phase_id=_PHASE_BUILD,
                        type_id=_TYPE_BUILD,
                        task_id=build_id,
                        task_depends_on=(
                            TaskDep(task_id=gate_id, phase_id=_PHASE_IMPORT),
                        ),
                        payload={
                            "kind": _PHASE_BUILD,
                            "idx": idx,
                            "build_id": build_id,
                            "gate": gate_id,
                        },
                    )
                )
                idx += 1
        _logger.info(
            "discover_items (affine variant=%s): %d gate(s) + %d builds "
            "= %d items (distinct paths)",
            variant,
            len(gates),
            len(gates) * _BUILDS_PER_GROUP,
            len(items),
        )
        return items

    def import_action(self, task_id: str, payload_json: str) -> None:
        """Per-secondary SecondaryAffine import callback (#497).

        Reconstructed FRESH in each secondary process (the #501-class path:
        the framework reads ``getattr(task, "import_action", None)`` off the
        ``SyntheticTask`` the secondary builds in ITS OWN process, then runs
        this callable under the GIL inside the run-once affine executor — it
        is NOT pickled across from the primary). RECORDS one line per
        invocation under the shared publish-dst marker so the e2e driver can
        COUNT distinct importing secondaries: the proof that the import ran
        EXACTLY ONCE per secondary (never once globally, never k× per
        secondary) is the marker carrying one line per distinct node identity.

        A clean return ⇒ ``Ok`` (the gate releases this node's queued builds);
        a raise would classify per the bridge (OSError=Transient, else
        NonRecoverable). We never raise — recording is infallible by design,
        and any failure here is a real wiring bug worth surfacing loudly.
        """
        node = socket.gethostname()
        line = f"{node}\t{task_id}\n"
        dst = _destination_root()
        dst.mkdir(parents=True, exist_ok=True)
        marker = dst / AFFINE_IMPORT_MARKER
        # Append is atomic enough for the marker's purpose: each secondary's
        # import runs once and writes one short line; the driver counts
        # distinct node identities, not exact interleaving. Open in append
        # mode so concurrent secondaries on the shared NFS never truncate
        # each other's lines.
        with marker.open("a", encoding="utf-8") as fh:
            fh.write(line)
        _logger.info(
            "import_action: node=%s ran SecondaryAffine import for %s "
            "(marker=%s)",
            node,
            task_id,
            marker,
        )

    # ── Per-type plumbing ──────────────────────────────────────────────

    def estimate_memory(self, item: TaskInfo) -> int:
        """One MiB per item — small enough that the framework's memory
        scheduler never throttles, large enough that a misconfigured
        ``--max-memory`` still spawns >1 worker.
        """
        return 1024 * 1024

    def add_task_arguments(self, parser: ArgumentParser) -> None:
        parser.add_argument(
            "--num-tasks",
            type=int,
            default=_NUM_TASKS_PER_PHASE,
            help=(
                "Number of tasks per phase (produce + consume). Default 2. "
                "The driver also creates exactly this many input files."
            ),
        )
        # Opt-in flag for the keyed-outputs API exercise. When set,
        # discovered tasks carry ``payload["keyed_outputs"] = True``
        # and the worker calls ``task.publish_string`` on produce
        # and reads ``task.predecessor_outputs`` on consume. Default
        # off so existing scenarios using this consumer are unaffected.
        parser.add_argument(
            "--keyed-outputs",
            action="store_true",
            help=(
                "Exercise the keyed-outputs API: produce tasks call "
                "Task.publish_string('nonce', ...) and consume tasks "
                "assert Task.predecessor_outputs carries the value. "
                "Failure mode: worker raises NonRecoverableError."
            ),
        )
        # Opt-in flag for the SecondaryAffine (#497) topology exercise. When
        # set, discovery emits the U → {I, I_dep} → k-builds gate topology
        # instead of produce/consume, and the framework reads
        # ``SyntheticTask.import_action`` (the per-secondary run-once import
        # callback). Default off so existing scenarios are unaffected.
        parser.add_argument(
            "--secondary-affine",
            action="store_true",
            help=(
                "Exercise the SecondaryAffine per-secondary run-once import "
                "gate (#497): emit affine import gate(s) and k builds depending "
                "on them. import_action records each invocation under the "
                "shared publish-dst marker so the import-once-per-secondary "
                "invariant can be COUNTED. See --secondary-affine-variant."
            ),
        )
        parser.add_argument(
            "--secondary-affine-variant",
            choices=_AFFINE_VARIANTS,
            default=_VARIANT_BOTH,
            help=(
                "Which affine gate(s) to emit (with --secondary-affine): "
                "'nodep' = a single no-dep gate born AffineReady at spawn "
                "(the PROVEN path); 'withdep' = a gate depending on an upload "
                "stand-in (the canonical upload→import→build shape, which "
                "DEADLOCKS until #506 lands); 'both' = the full topology. "
                "Default 'both'."
            ),
        )

    def build_worker_command_args(
        self,
        type_id: TypeId,
        args: Namespace,
        source_dir: Path,
        output_dir: Path,
        skip_existing: bool,
    ) -> list[str]:
        # The worker reads --source / --output / --skip_existing already
        # (framework-injected). Forward --num-tasks just so workers can
        # emit a startup line that mentions the configured size if they
        # ever need to debug a mismatch. Forward --keyed-outputs so
        # the worker-side argparser accepts the flag even though the
        # discovery side already injected it into payloads (the worker
        # branches on payload, not argv — see worker.handle).
        argv = ["--num-tasks", str(args.num_tasks)]
        if getattr(args, "keyed_outputs", False):
            argv.append("--keyed-outputs")
        # Forward --secondary-affine (+ its variant) so the worker-side
        # argparser tolerates them (the worker branches on payload["kind"], not
        # argv — see worker.handle — but the flags must be accepted to avoid an
        # unknown-argument error on the spawned build/setup workers).
        if getattr(args, "secondary_affine", False):
            argv.append("--secondary-affine")
            argv += [
                "--secondary-affine-variant",
                getattr(args, "secondary_affine_variant", _VARIANT_BOTH),
            ]
        return argv

    def get_output_filename_pattern(
        self, type_id: TypeId, item: TaskInfo
    ) -> str:
        """Final output filename — used by ``--skip-existing`` checks
        and by the e2e driver's "already-done detection" assertion.

        The worker publishes under exactly this name relative to
        ``out-network`` / the configured publish destination root.
        """
        idx = item.payload["idx"]
        if type_id == _TYPE_PRODUCE:
            return _produce_output_filename(idx)
        if type_id == _TYPE_CONSUME:
            return _consume_output_filename(idx)
        if type_id == _TYPE_SETUP:
            return setup_output_filename()
        if type_id == _TYPE_BUILD:
            # The build worker publishes under its build_id so the driver can
            # assert ALL k builds completed (none stranded/deadlocked) by name.
            return build_output_filename(item.payload["build_id"])
        # The import gate (_TYPE_IMPORT) is a SecondaryAffine task — NEVER
        # worker-assigned, so this is never reached for it; raise loudly if
        # the framework ever asks (a real kind-routing regression).
        raise ValueError(f"unknown/unassignable type_id: {type_id}")

    # ── Lifecycle hooks ────────────────────────────────────────────────

    def on_run_start(
        self, source_dir: Path, output_dir: Path, args: Namespace
    ) -> None:
        _logger.info(
            "on_run_start: source=%s output=%s num_tasks=%d",
            source_dir,
            output_dir,
            getattr(args, "num_tasks", _NUM_TASKS_PER_PHASE),
        )

    def on_run_end(self, success: bool) -> None:
        _logger.info("on_run_end: success=%s", success)

    def on_phase_start(self, phase_id: str) -> None:
        _logger.info("on_phase_start: %s", phase_id)

    def on_phase_end(
        self, phase_id: str, completed: int, failed: int
    ) -> None:
        _logger.info(
            "on_phase_end: %s completed=%d failed=%d",
            phase_id,
            completed,
            failed,
        )


def _produce_output_filename(idx: int) -> str:
    return f"produce-{idx}.out"


def _consume_output_filename(idx: int) -> str:
    return f"consume-{idx}.out"


def setup_output_filename() -> str:
    """The no-op upload stand-in's published output. The build phase's
    cross-phase barrier and the I_dep AffineReady-on-completion transition
    both fire on this terminal. Public: the worker (publish) and the scenario
    (assert) share this single naming source."""
    return f"{_SETUP_TASK_ID}.out"


def build_task_id(gate_id: str, j: int) -> str:
    """Stable, readable build id naming its gate group and index. Public so
    the scenario can re-derive the full expected build set."""
    return f"build-{gate_id}-{j}"


def build_output_filename(build_id: str) -> str:
    """A build's published output — named by build_id so the driver can
    assert ALL k builds completed by name (none stranded/deadlocked). Public:
    shared naming source for worker (publish) and scenario (assert)."""
    return f"{build_id}.out"


def _affine_gates_for(variant: str) -> tuple[str, ...]:
    """The gate ids a ``variant`` emits — the single owner of the
    variant→gates mapping (discovery, expected-outputs, and the input-file
    count all read it, so they cannot drift)."""
    gates: list[str] = []
    if variant in (_VARIANT_NODEP, _VARIANT_BOTH):
        gates.append(_AFFINE_NODEP_ID)
    if variant in (_VARIANT_WITHDEP, _VARIANT_BOTH):
        gates.append(_AFFINE_DEP_ID)
    return tuple(gates)


def expected_affine_outputs(variant: str = _VARIANT_BOTH) -> list[str]:
    """The full set of published output filenames a successful affine run of
    ``variant`` produces: the upload stand-in (with-dep only) plus every
    build's output. The single source of truth the scenario asserts against
    (assertion 2: ALL k builds completed). The affine GATE tasks publish
    nothing (never worker-run)."""
    gates = _affine_gates_for(variant)
    out: list[str] = []
    if _AFFINE_DEP_ID in gates:
        out.append(setup_output_filename())
    for gate_id in gates:
        for j in range(_BUILDS_PER_GROUP):
            out.append(build_output_filename(build_task_id(gate_id, j)))
    return out


def affine_input_file_count(variant: str = _VARIANT_BOTH) -> int:
    """How many distinct ``input-{idx}.txt`` files a ``variant`` run needs
    staged — one per emitted task (the upload stand-in for with-dep, one per
    gate, and _BUILDS_PER_GROUP builds per gate), because the framework
    content-hash recipe ``{phase_id, path, identifier}`` would dedup same-phase
    same-path tasks. Mirrors :meth:`SyntheticTask._discover_affine_items`'s
    emit set exactly so a rename of _BUILDS_PER_GROUP touches only this
    module."""
    gates = _affine_gates_for(variant)
    setup = 1 if _AFFINE_DEP_ID in gates else 0
    return setup + len(gates) + len(gates) * _BUILDS_PER_GROUP


def _build_task(
    *,
    source_dir: Path,
    idx: int,
    phase_id: str,
    type_id: str,
    task_id: str,
    task_depends_on: tuple[str | TaskDep, ...],
    payload: dict,
    is_secondary_affine: bool = False,
) -> TaskInfo:
    """One TaskInfo for ``input-{idx}.txt``.

    Both phases reuse the same source file, so the only fields that
    vary across phases are the topology tags (phase/type/task ids,
    depends_on, payload). Centralising the rest here keeps the
    discovery loop readable and prevents the BinaryIdentifier shape
    drifting between the two phase loops.

    ``is_secondary_affine`` (default False) marks the task as a #497
    SecondaryAffine GATE — never worker-assigned; its per-secondary import
    runs via :meth:`SyntheticTask.import_action`.
    """
    input_name = _input_filename(idx)
    return TaskInfo(
        path=Path(input_name),
        size=_probe_size(source_dir / input_name),
        identifier=BinaryIdentifier(
            binary_name=input_name,
            platform="synthetic",
            compiler="none",
            version="0",
            opt_level="O0",
        ),
        phase_id=phase_id,
        type_id=type_id,
        payload=payload,
        task_id=task_id,
        task_depends_on=task_depends_on,
        is_secondary_affine=is_secondary_affine,
    )


def _probe_size(path: Path) -> int:
    """Best-effort ``os.stat`` of an input file. Returns 1 when the file
    is missing — the framework only needs ``size`` for memory-aware
    scheduling, and the worker never reads ``TaskInfo.size`` itself.
    """
    try:
        return max(1, path.stat().st_size)
    except OSError:
        return 1
