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
