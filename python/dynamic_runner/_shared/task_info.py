"""Framework-generic task primitive.

The asm-binary specific filename parsing (`parse_binary_filename`,
`build_binary_filename_format`, `build_field_regexes`,
`format_binary_info`, `BinaryFilenameFormat`, `FieldRegexes`) used to
live here too — that's been moved out of the framework into consumer
packages because filename-format conventions are task concerns, not
framework primitives. See the asm-tokenizer / asm-dataset-nix
packages for the canonical asm-binary parsing.

The `BinaryIdentifier` shape is currently still here because TaskInfo
references it; decoupling TaskInfo's identifier into a fully generic
slot is a separate, deeper refactor.
"""

from dataclasses import dataclass, field
from pathlib import Path


@dataclass(frozen=True)
class BinaryIdentifier:
    binary_name: str
    platform: str
    compiler: str
    version: str
    opt_level: str


@dataclass(frozen=True)
class TaskDep:
    """One edge in a task's prerequisite-dependency graph.

    Mirrors the Rust-side ``TaskDep`` in
    ``crates/dynrunner-core/src/types/task.rs``. Consumer code uses
    this dataclass on the declarer side (``TaskInfo.task_depends_on``)
    to name a prerequisite and to opt into the framework's
    transitive-ancestry output read.

    A dependency's full identity is ``(phase_id, task_id)``. ``phase_id``
    defaults to the empty string, which the PyO3 bridge resolves to the
    ENCLOSING task's phase — the common INTRA-PHASE case. Set
    ``phase_id`` explicitly only to name a CROSS-PHASE prerequisite (a
    task declared in a different phase). The same ``task_id`` in two
    different phases is a distinct prerequisite.

    Wire-equivalent shapes accepted by the PyO3 bridge:

    * Bare ``str`` — names a prerequisite by id in the SAME phase as the
      declaring task. The extractor lifts it into
      ``TaskDep(task_id=<str>, phase_id=<enclosing>, inherit_outputs=False)``.
    * ``TaskDep(task_id, inherit_outputs=True)`` — opts the dependent
      task into receiving its predecessor's transitive ancestors'
      published outputs in addition to the direct predecessor's.
    * ``TaskDep(task_id, phase_id="other-phase")`` — a cross-phase
      prerequisite.

    The Rust→Python read direction (``TaskInfo.task_depends_on``
    surfaced from a round-trip through the runtime) keeps the legacy
    ``tuple[str, ...]`` projection — the ``inherit_outputs`` /
    ``phase_id`` fields are declarer-side concerns and the runtime stops
    carrying them past the primary's dispatcher.
    """

    task_id: str
    inherit_outputs: bool = False
    # Phase of the prerequisite. Empty == "same phase as the declaring
    # task" (resolved at the PyO3 boundary). Set explicitly only for a
    # cross-phase dependency.
    phase_id: str = ""


@dataclass
class TaskInfo:
    path: Path
    size: int
    identifier: BinaryIdentifier
    phase_id: str = ""
    type_id: str = ""
    affinity_id: str | None = None
    payload: dict = field(default_factory=dict)
    # Stable consumer-supplied task identifier. REQUIRED — every
    # task carries a non-empty id. The framework rejects ``None`` /
    # empty strings at the Python→Rust boundary
    # (``crate::pytypes::extract_binaries``) so producer-side
    # mistakes surface as a loud ``ValueError`` rather than as
    # opaque "feature doesn't work" symptoms later. The empty-string
    # default here lets dataclass-positional construction stay
    # backward-source-compatible (no field reorder); the Rust
    # extractor's rejection of ``""`` is what enforces the contract.
    #
    # Other tasks reference this from their ``task_depends_on`` to
    # express a "wait for that task to complete before dispatching
    # me" ordering constraint. Used by the memprofile sampler for
    # per-task file naming, by the retry tracker for attempt-
    # counting, and by the failure reporter to group results by
    # task identity. Pick stable, readable ids (e.g.
    # ``"toolchain__aarch64__clang15"``) so dependent tasks can
    # reference them without re-deriving a hash.
    task_id: str = ""
    # Task ids of prerequisite tasks that must terminate (success or
    # permanent failure) before this task is eligible for dispatch.
    # Default `()` means "no per-task ordering constraint; eligibility
    # is governed solely by the phase state machine". Common use case:
    # variant builds depending on their corresponding toolchain build,
    # both in the same phase, lets the scheduler dispatch variants
    # continuously as toolchains drain instead of barriering on the
    # whole phase. Validated for unknown ids and cycles at run start;
    # mismatch fails loud with the offending ids in the error.
    #
    # Entries may be bare ``str`` task ids (legacy shape, equivalent
    # to ``TaskDep(task_id=<str>, inherit_outputs=False)``) or
    # :class:`TaskDep` instances. Bare strings keep the pre-feature
    # contract — the PyO3 bridge lifts each into a Rust ``TaskDep``
    # with ``inherit_outputs=False``. Setting
    # ``TaskDep(..., inherit_outputs=True)`` opts the dependent task
    # into the framework's transitive-ancestry output read: it sees
    # not only the direct predecessor's published outputs but also
    # those predecessor's predecessors' outputs (and so on, transitively).
    task_depends_on: tuple["TaskDep | str", ...] = field(default_factory=tuple)
    # Discovery-time "this item's outputs already exist" marker. When a
    # producer determines (during ``discover_items``) that a task's work
    # was already done by a prior run, it sets this to ``True`` instead
    # of dropping the item. The framework then materialises the item
    # DIRECTLY as a terminal ``SkippedAlreadyDone`` ledger entry — never
    # dispatched, never re-running the already-done work, but still
    # counted as a real task of the phase (so a 100%-already-done phase
    # is a phase WITH tasks, not an empty phase that would fail loud).
    #
    # Default ``False`` ⇒ today's behaviour (the item is a normal
    # ``Pending`` task). This is a discovery-boundary routing signal, NOT
    # a property of the scheduling unit: it rides alongside the task at
    # the extract boundary and is consumed by the ingest seam to choose
    # the initial ledger state; it is deliberately NOT folded into the
    # task's content hash. See ``PhaseSpec.may_be_empty`` for the related
    # empty-phase proceed-or-fail contract.
    skipped_already_done: bool = False
    # First-class task-KIND marker. ``False`` (default) ⇒ an ordinary
    # WORKER task (every existing consumer). ``True`` ⇒ a framework SETUP
    # task: it is NEVER dispatched to a worker (it is executed in-process
    # by its source-owning member), is NON-reassignable if that member
    # dies (death → terminal unrecoverable, dependents cascade), can be
    # DEPENDED ON by other tasks via ``task_depends_on`` (a build task
    # gates on a setup task, scheduling overlapping once the setup
    # succeeds), and on success is counted in a SEPARATE setup bucket —
    # never the success count.
    #
    # The setup-task primitive is UNCONDITIONAL: a consumer can declare a
    # setup task regardless of any run/CLI configuration. The bit is
    # carried through the PyO3 boundary
    # (``crate::pytypes::extract_binaries``) onto the core Rust
    # ``TaskInfo.kind`` (``TaskKind::Setup``); unlike
    # ``skipped_already_done`` it is a property of the scheduling unit,
    # not a discovery-time routing signal.
    is_setup: bool = False
    # EXECUTOR-affinity member for a setup task (``is_setup=True``): the
    # peer id of the member that runs this setup task IN-PROCESS (its
    # source-owning member). A consumer setup task names its member here
    # (e.g. a compute node id); ``None`` defaults the executor to the
    # primary itself. Ignored for an ordinary work task. Carried through
    # the PyO3 boundary onto the core Rust ``TaskInfo.setup_affinity``; the
    # primary's setup selector targets exactly this member. A routing
    # concern (like ``affinity_id``), NOT folded into the task's content
    # hash.
    setup_affinity: str | None = None

    @property
    def binary_name(self) -> str:
        return self.identifier.binary_name

    @property
    def platform(self) -> str:
        return self.identifier.platform

    @property
    def compiler(self) -> str:
        return self.identifier.compiler

    @property
    def version(self) -> str:
        return self.identifier.version

    @property
    def opt_level(self) -> str:
        return self.identifier.opt_level

    def to_dict(self) -> dict:
        """Convert TaskInfo to dictionary representation."""
        return {
            "path": str(self.path),
            "size": self.size,
            "binary_name": self.identifier.binary_name,
            "platform": self.identifier.platform,
            "compiler": self.identifier.compiler,
            "version": self.identifier.version,
            "opt_level": self.identifier.opt_level,
            "phase_id": self.phase_id,
            "type_id": self.type_id,
            "affinity_id": self.affinity_id,
            "payload": self.payload,
            "task_id": self.task_id,
            "is_setup": self.is_setup,
            "setup_affinity": self.setup_affinity,
            # Normalise each dep to a JSON-friendly shape: bare-strings
            # stay strings (legacy wire), ``TaskDep`` instances render as
            # ``{"task_id": ..., "inherit_outputs": ...}``. Matches the
            # untagged ``TaskDepWire`` decoder on the Rust side
            # (``crates/dynrunner-core/src/types/task.rs``) so the dict
            # form round-trips through serde without ambiguity.
            "task_depends_on": [_dep_to_jsonable(dep) for dep in self.task_depends_on],
        }


def _dep_to_jsonable(dep: "TaskDep | str") -> "str | dict":
    """Coerce one ``task_depends_on`` entry to its JSON-canonical shape.

    Single concern: be the one place that knows about both legal entry
    shapes (bare-string vs ``TaskDep`` dataclass) for the on-disk
    ``to_dict`` projection. The PyO3 extractor has its own boundary
    (attribute-based, not JSON-based); the two paths share the same
    legal-shape set but not the same wire encoder.
    """
    if isinstance(dep, TaskDep):
        return {
            "task_id": dep.task_id,
            "phase_id": dep.phase_id,
            "inherit_outputs": dep.inherit_outputs,
        }
    return dep


def format_size(size: int) -> str:
    """Format file size in human-readable format (B, KiB, MiB, GiB)."""
    if size < 1024:
        return f"{size}B"
    elif size < 1024 * 1024:
        return f"{size / 1024:.1f}KiB"
    elif size < 1024 * 1024 * 1024:
        return f"{size / (1024 * 1024):.1f}MiB"
    else:
        return f"{size / (1024 * 1024 * 1024):.1f}GiB"
