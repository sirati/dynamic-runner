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


@dataclass
class TaskInfo:
    path: Path
    size: int
    identifier: BinaryIdentifier
    phase_id: str = ""
    type_id: str = ""
    affinity_id: str | None = None
    payload: dict = field(default_factory=dict)
    # Optional consumer-supplied task identifier. Other tasks reference
    # this from their `task_depends_on` to express a "wait for that
    # task to complete before dispatching me" ordering constraint.
    # `None` means the task cannot itself be referenced as a
    # prerequisite (anonymous task); it can still have its own
    # `task_depends_on` entries pointing at named tasks. Pick stable,
    # readable ids (e.g. ``"toolchain__aarch64__clang15"``) so
    # dependent tasks can reference them without re-deriving a hash.
    task_id: str | None = None
    # Task ids of prerequisite tasks that must terminate (success or
    # permanent failure) before this task is eligible for dispatch.
    # Default `()` means "no per-task ordering constraint; eligibility
    # is governed solely by the phase state machine". Common use case:
    # variant builds depending on their corresponding toolchain build,
    # both in the same phase, lets the scheduler dispatch variants
    # continuously as toolchains drain instead of barriering on the
    # whole phase. Validated for unknown ids and cycles at run start;
    # mismatch fails loud with the offending ids in the error.
    task_depends_on: tuple[str, ...] = field(default_factory=tuple)

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
            "task_depends_on": list(self.task_depends_on),
        }


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
