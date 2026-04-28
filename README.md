# dynamic_runner

A multi-process / multi-host Python task runner with a Rust backend. The
user supplies a `TaskDefinition` (a duck-typed Protocol describing the
work, worker subprocess, and resource estimates); the runner schedules
the work across local processes or a pool of remote secondaries, with
memory-aware admission control, retry/failure handling, and pluggable
transports (QUIC, Unix sockets, in-process channels). The Python frontend
in `python/dynamic_runner/` is intentionally thin — argparse, logging,
binary discovery, and dispatch — and delegates execution to a native
extension compiled from a 14-crate Rust workspace.

## Status

Pre-1.0; API may change. License: Apache-2.0.

## Quickstart (FHS)

`dynamic_runner` is not yet published to PyPI. Until v0.1.0 ships:

```bash
pip install git+https://github.com/sirati/dynamic-runner@v0.1.0
```

The wheel is built with maturin in mixed layout, so the import name is
`dynamic_runner` and the native extension lives at
`dynamic_runner._native`.

Once v0.1.0 is on PyPI:

```bash
pip install dynamic-runner
```

## Quickstart (Nix)

Add the flake as an input and apply its overlay:

```nix
{
  inputs.nixpkgs.url = "github:NixOS/nixpkgs/nixos-unstable";
  inputs.dynamic-runner.url = "github:sirati/dynamic-runner";

  outputs = { self, nixpkgs, dynamic-runner, ... }: let
    pkgs = import nixpkgs {
      system = "x86_64-linux";
      overlays = [ dynamic-runner.overlays.default ];
    };
  in {
    # `pkgs.python3Packages.dynamic-runner` is now available.
    devShells.x86_64-linux.default = pkgs.mkShell {
      packages = [ (pkgs.python3.withPackages (ps: [ ps.dynamic-runner ])) ];
    };
  };
}
```

## Minimal task example

A task is any object whose attributes match the `TaskDefinition`
protocol. No subclassing is required.

```python
from argparse import ArgumentParser, Namespace
from pathlib import Path
from enum import Enum

import dynamic_runner
from dynamic_runner import StageDefinition, Phase


class MyPhase(Phase):
    ANALYZE = "analyze"


class MyTask:
    def get_stages(self):
        return [StageDefinition(phase=MyPhase.ANALYZE, timeout_seconds=300.0)]

    def organize_and_sort_items(self, items):
        return sorted(items, key=lambda b: b.size)

    def estimate_memory(self, binary_size: int) -> int:
        return 4 * binary_size + 256 * 1024 * 1024  # 4x + 256 MB headroom

    def get_reserved_memory_per_worker(self) -> int:
        return 128 * 1024 * 1024

    def get_worker_module(self) -> str:
        return "my_task.worker"  # python -m my_task.worker ...

    def add_task_arguments(self, parser: ArgumentParser) -> None:
        parser.add_argument("--my-flag", action="store_true")

    def build_worker_command_args(
        self, args: Namespace, source_dir: Path, output_dir: Path, skip_existing: bool
    ) -> list[str]:
        cmd = [str(source_dir), str(output_dir)]
        if skip_existing:
            cmd.append("--skip-existing")
        if args.my_flag:
            cmd.append("--my-flag")
        return cmd

    def get_output_filename_pattern(self, input_filename: str) -> str:
        return f"{input_filename}.out"


if __name__ == "__main__":
    dynamic_runner.run(MyTask(), description="My analysis task")
```

## Architecture

- **Pure-Python frontend** in `python/dynamic_runner/` — CLI parsing,
  logging, binary discovery, secondary spawning, and dispatch into the
  native runtime via `run()`.
- **Native runtime** in `dynamic_runner._native` — a cdylib built from
  `crates/dynrunner-pyo3` that re-exports manager classes, config
  dataclasses, and the `run_local` / `run_distributed` /
  `run_primary` / `run_secondary` entry points.
- **14 internal Rust crates** under `crates/dynrunner-*`. The most
  relevant: `dynrunner-core` (traits), `dynrunner-scheduler`,
  `dynrunner-manager-local`, `dynrunner-manager-distributed`,
  `dynrunner-transport-quic` / `-socket` / `-channel`,
  `dynrunner-slurm`, `dynrunner-gateway`. See [`crates/`](crates/) for
  the full set.

## Development

The development environment is provided by the flake — there is no
`requirements.txt` or venv.

```bash
nix develop
cargo test --workspace
maturin develop --release   # installs dynamic_runner into the nix env
pytest
```

## Releases

Tagging `vX.Y.Z` triggers `wheels.yml`, which builds wheels for the
supported targets and publishes them to PyPI via trusted publishing
(configured separately on PyPI; no token lives in the repo).

## License

[Apache-2.0](LICENSE).
