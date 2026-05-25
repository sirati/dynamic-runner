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
protocol. No subclassing is required. Topology is declared as
**phases** of **task types**; each item carries its `phase_id`,
`type_id`, and (optionally) an `affinity_id` for soft worker
pinning. See [`docs/PHASES.md`](docs/PHASES.md) for the full
migration guide and a deeper walk-through of the model.

```python
from argparse import ArgumentParser, Namespace
from pathlib import Path

import dynamic_runner
from dynamic_runner._shared import TaskInfo
from dynamic_runner.task_protocol import PhaseSpec, TaskTypeSpec


class MyTask:
    def get_phases(self):
        return (
            PhaseSpec(
                phase_id="analyze",
                types=(
                    TaskTypeSpec(
                        type_id="analyze",
                        worker_module="my_task.worker",
                        timeout_seconds=300.0,
                        reserved_memory_per_worker=128 * 1024 * 1024,
                    ),
                ),
            ),
        )

    def discover_items(self, source_dir: Path, args: Namespace):
        for path in self._walk(source_dir, args):
            yield TaskInfo(
                path=path,
                size=path.stat().st_size,
                identifier=self._parse(path),
                phase_id="analyze",
                type_id="analyze",
            )

    def estimate_memory(self, item: TaskInfo) -> int:
        return 4 * item.size + 256 * 1024 * 1024  # 4x + 256 MB headroom

    def add_task_arguments(self, parser: ArgumentParser) -> None:
        parser.add_argument("--my-flag", action="store_true")

    def build_worker_command_args(
        self,
        type_id: str,
        args: Namespace,
        source_dir: Path,
        output_dir: Path,
        skip_existing: bool,
    ) -> list[str]:
        cmd = [str(source_dir), str(output_dir)]
        if skip_existing:
            cmd.append("--skip-existing")
        if args.my_flag:
            cmd.append("--my-flag")
        return cmd

    def get_output_filename_pattern(
        self, type_id: str, item: TaskInfo
    ) -> str:
        return f"{item.path.name}.out"

    # Lifecycle hooks default to no-ops; implement only as needed:
    # on_run_start, on_run_end, on_phase_start, on_phase_end.


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

## Per-task memory profiling (`--memprofile`)

Opt-in 1Hz per-task memory profiler. Each worker writes one zstd-framed
JSONL file per task it processes, capturing the worker's cgroup-v2
`memory.current`, `memory.swap.current`, and full `memory.stat` once per
second. Frame-per-sample makes the file `zstd -dc`-recoverable after a
hard manager death (the last in-flight sample is the most the consumer
can lose).

```bash
python -m <your-task> --multi-computer local --memprofile \
    --output-dir /tmp/run-out --jobs 12
```

Output lands at `{--output-dir}/memprofile/{task_id}.worker-{N}.memprofile.jsonl.zst`.
`task_id` is treated as a relative path, so a slash-bearing identifier
like `nping/x86/clang/9/Os` becomes
`memprofile/nping/x86/clang/9/Os.worker-3.memprofile.jsonl.zst` (the
writer `mkdir -p`s the parents).

**SLURM mode:** the same flag works on the SLURM secondary. The
secondary's container has `/app/out-network` bind-mounted to the
gateway-shared output filesystem; when `--memprofile` is set the
secondary writes there by default. Post-run, profiles land alongside
the rest of the run's artifacts on the dispatcher's gateway output
directory. Operator checklist after a `--memprofile` SLURM run:

- `ls {gateway.output_path}/memprofile/` should list one
  `.jsonl.zst` per completed task.
- `zstd -dc {gateway.output_path}/memprofile/<task>.worker-0.memprofile.jsonl.zst | head -1 | jq`
  to spot-check a sample.
- If the directory is empty, check the secondary's logs for
  `--memprofile set but /app/out-network is not present` (operator
  ran the secondary outside the wrapper) or a cgroup-v2 fallback warn
  (`cgroup-v2 leaf not found` etc.) — both downgrade to a no-op with
  a single warn line.

**Requires** delegated cgroup-v2 on the host (rootless podman with
`--cgroup-manager=cgroupfs`, or `systemd-run --user` with `Delegate=yes`).
Without delegation the sampler exists but produces no files; one warn
line surfaces the reason at startup.

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
