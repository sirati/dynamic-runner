import math
from argparse import ArgumentParser, Namespace
from collections import defaultdict
from pathlib import Path

from shared import BinaryInfo

from ..base import Phase, StageDefinition, TaskDefinition


class TokenizerPhase(Phase):
    ANGR_1 = "angr-1"
    ANGR_2 = "angr-2"
    TOKENIZATION = "tokenization"


class TokenizerTask(TaskDefinition):
    """Task definition for binary tokenization."""

    def get_stages(self) -> list[StageDefinition]:
        """Return tokenizer processing stages.

        Stage 1 and 2: No timeout (long-running disassembly and tokenization)
        Stage 3: 10 second timeout (CSV writing with keepalive)
        """
        return [
            StageDefinition(phase=TokenizerPhase.ANGR_1, timeout_seconds=None),
            StageDefinition(phase=TokenizerPhase.ANGR_2, timeout_seconds=None),
            StageDefinition(phase=TokenizerPhase.TOKENIZATION, timeout_seconds=10.0),
        ]

    def organize_and_sort_items(self, items: list[BinaryInfo]) -> list[BinaryInfo]:
        """Group by binary_name, calculate average size, sort by average (largest first),
        then sort within each group by size (largest first).

        This optimizes resource utilization by processing similar binaries together
        and prioritizing larger files.
        """
        groups: dict[str, list[BinaryInfo]] = defaultdict(list)
        for binary in items:
            groups[binary.binary_name].append(binary)

        group_averages: list[tuple[str, float, list[BinaryInfo]]] = []
        for binary_name, group in groups.items():
            avg_size = sum(b.size for b in group) / len(group)
            group.sort(key=lambda b: b.size, reverse=True)
            group_averages.append((binary_name, avg_size, group))

        group_averages.sort(key=lambda x: x[1], reverse=True)

        result: list[BinaryInfo] = []
        for _, _, group in group_averages:
            result.extend(group)

        return result

    def estimate_memory(self, binary_size: int) -> int:
        """Estimate memory consumption using a power law model.

        Model: RAM (MiB) = 430.870 × size^1.051 + 260.15

        R² = 0.9866, RMSE = 203.66 MiB

        Args:
            binary_size: File size in bytes

        Returns:
            Estimated RAM usage in bytes (rounded up)
        """
        mb = binary_size / 1024 / 1024  # Convert to MiB

        # Power law coefficients (MiB units)
        a = 430.870
        b = 1.051
        c = 260.15
        rmse = 203.66

        # Add the RMSE as we want to rather overestimate
        ram_mb = a * (mb**b) + c + rmse

        return math.ceil(ram_mb * 1024 * 1024)

    def get_worker_module(self) -> str:
        """Return the tokenizer module name."""
        return "tokenizer"

    def add_task_arguments(self, parser: ArgumentParser) -> None:
        """Add tokenizer-specific arguments to the argument parser.

        Currently, tokenizer has no additional task-specific arguments
        beyond the standard file discovery parameters.
        """
        pass

    def build_worker_command_args(
        self,
        args: Namespace,
        source_dir: Path,
        output_dir: Path,
        skip_existing: bool,
    ) -> list[str]:
        """Build command line arguments for tokenizer workers.

        Args:
            args: Parsed command-line arguments
            source_dir: Source directory path
            output_dir: Output directory path
            skip_existing: Whether to skip existing outputs

        Returns:
            List of command-line arguments for the worker
        """
        # Tokenizer requires --platform argument
        # The value "auto" tells tokenizer to extract platform from filename
        cmd_args = ["--platform", "auto"]

        # Add simulate-crash if provided
        if hasattr(args, "simulate_crash") and args.simulate_crash is not None:
            cmd_args.extend(["--simulate-crash", str(args.simulate_crash)])

        return cmd_args

    def get_output_filename_pattern(self, input_filename: str) -> str:
        """Generate output filename for tokenizer.

        Args:
            input_filename: Name of the input binary file

        Returns:
            Expected output filename with _output.csv suffix
        """
        return f"{input_filename}_output.csv"
