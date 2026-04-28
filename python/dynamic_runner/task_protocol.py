"""Structural protocol for task definitions.

A task definition is any object whose attributes match this protocol;
subclassing is *not* required (Protocol uses structural typing). A
companion ABC is kept available for backward compatibility — existing
TaskDefinition subclasses continue to work because their method
signatures match the protocol.
"""

from __future__ import annotations

from argparse import ArgumentParser, Namespace
from dataclasses import dataclass
from enum import Enum
from pathlib import Path
from typing import Protocol, runtime_checkable

from ._shared import BinaryInfo


class Phase(Enum):
    """Base class for task-specific phase enums."""

    pass


@dataclass
class StageDefinition:
    phase: Phase
    timeout_seconds: float | None = None


@runtime_checkable
class TaskDefinition(Protocol):
    """The duck-typed contract a task package implements.

    Any object with the right attributes satisfies this protocol —
    there is no required base class. (Pre-migration callers subclassed
    a `TaskDefinition` ABC; that base class is gone, but those
    subclasses still satisfy the structural protocol unchanged.)
    """

    def get_stages(self) -> list[StageDefinition]: ...

    def organize_and_sort_items(self, items: list[BinaryInfo]) -> list[BinaryInfo]: ...

    def estimate_memory(self, binary_size: int) -> int: ...

    def get_worker_module(self) -> str: ...

    def add_task_arguments(self, parser: ArgumentParser) -> None: ...

    def build_worker_command_args(
        self,
        args: Namespace,
        source_dir: Path,
        output_dir: Path,
        skip_existing: bool,
    ) -> list[str]: ...

    def get_output_filename_pattern(self, input_filename: str) -> str: ...

    def get_reserved_memory_per_worker(self) -> int: ...
