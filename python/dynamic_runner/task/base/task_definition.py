from abc import ABC, abstractmethod
from argparse import ArgumentParser, Namespace
from dataclasses import dataclass
from enum import Enum
from pathlib import Path

from shared import BinaryInfo


class Phase(Enum):
    """Base class for task-specific phase enums."""

    pass


@dataclass
class StageDefinition:
    phase: Phase
    timeout_seconds: float | None = None


class TaskDefinition(ABC):
    """Abstract base class for defining task-specific behavior in dynamic batch processing."""

    @abstractmethod
    def get_stages(self) -> list[StageDefinition]:
        """Return the list of processing stages with their names and timeouts.

        Returns:
            List of StageDefinition objects defining each processing stage.
        """
        pass

    @abstractmethod
    def organize_and_sort_items(self, items: list[BinaryInfo]) -> list[BinaryInfo]:
        """Define the strategy for ordering items before processing.

        This determines the order in which binaries are processed to optimize
        resource utilization.

        Args:
            items: List of BinaryInfo objects to be ordered

        Returns:
            Reordered list of BinaryInfo objects
        """
        pass

    @abstractmethod
    def estimate_memory(self, binary_size: int) -> int:
        """Estimate memory consumption for processing a binary.

        Args:
            binary_size: File size in bytes

        Returns:
            Estimated memory usage in bytes
        """
        pass

    @abstractmethod
    def get_worker_module(self) -> str:
        """Return the module name to invoke for worker processes.

        Returns:
            Module name as a string (e.g., "tokenizer")
        """
        pass

    @abstractmethod
    def add_task_arguments(self, parser: ArgumentParser) -> None:
        """Add task-specific arguments to the argument parser.

        This should not include file discovery or skip_existing logic,
        only task-specific parameters that should be passed to workers.

        Args:
            parser: ArgumentParser to add arguments to
        """
        pass

    @abstractmethod
    def build_worker_command_args(
        self,
        args: Namespace,
        source_dir: Path,
        output_dir: Path,
        skip_existing: bool,
    ) -> list[str]:
        """Build command line arguments for worker processes.

        This should return a list of command-line arguments to pass to the worker,
        excluding the standard arguments (source, output, skip_existing, etc.).

        Args:
            args: Parsed command-line arguments
            source_dir: Source directory path
            output_dir: Output directory path
            skip_existing: Whether to skip existing outputs

        Returns:
            List of command-line argument strings (e.g., ["--arg1", "value1", "--arg2", "value2"])
        """
        pass

    @abstractmethod
    def get_output_filename_pattern(self, input_filename: str) -> str:
        """Generate the output filename pattern for a given input file.

        Used for determining if output already exists when skip_existing is enabled.

        Args:
            input_filename: Name of the input binary file

        Returns:
            Expected output filename
        """
        pass

    def get_reserved_memory_per_worker(self) -> int:
        """Return reserved memory per worker in bytes.

        Returns:
            Reserved memory amount (default: 650MB)
        """
        return 650 * 1024 * 1024
