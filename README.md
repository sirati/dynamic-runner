# dynamic_runner

A multi-process / multi-host Python task runner with a Rust backend. You
supply a `TaskDefinition` (a duck-typed Protocol describing the work,
worker subprocess, and resource estimates); the runner schedules it
across local processes or a pool of remote secondaries, with
memory-aware admission control, retry/failure handling, and pluggable
transports (QUIC, Unix sockets, in-process channels). The Python frontend
in `python/dynamic_runner/` is intentionally thin — argparse, logging,
config, binary discovery, dispatch — and delegates all runtime,
lifecycle, and scheduling to a native extension compiled from a Rust
workspace.

## Status

Pre-1.0; API may change. License: Apache-2.0.

## Install

```bash
pip install dynamic-runner          # once published to PyPI
pip install git+https://github.com/sirati/dynamic-runner@v0.4.0   # from a tag
```

The wheel is built with maturin in mixed layout: the import name is
`dynamic_runner` and the native extension lives at
`dynamic_runner._native`.

### Nix

Add the flake as an input and apply its overlay; this exposes
`pkgs.python3Packages.dynamic-runner`:

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
    devShells.x86_64-linux.default = pkgs.mkShell {
      packages = [ (pkgs.python3.withPackages (ps: [ ps.dynamic-runner ])) ];
    };
  };
}
```

## Minimal task example

A task is any object whose attributes match the `TaskDefinition`
protocol — no subclassing. Topology is declared as **phases** of
**task types**; each discovered item carries its `phase_id`, `type_id`,
and (optionally) an `affinity_id` for soft worker pinning. See
[`docs/PHASES.md`](docs/PHASES.md) for the full model.

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
            cmd.append("--skip_existing")  # underscore: matches the framework's own worker-arg injection
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

Run it with `--source`/`--output` for the I/O directories and `--cores`
to size the local worker pool (`0` = all detected cores):

```bash
python -m my_task --source ./in --output ./out --cores 8
```

Plain local mode (no `--multi-computer`) runs everything in-process.
The four `--multi-computer` modes (`slurm`, `local`, `single-process`,
`remote-podman`) enable distributed dispatch; the SLURM modes also take
a `--gateway`, `--packaging`, `--slurm-root-folder`, and `--jobs`
(number of secondary nodes to spawn).

## Architecture

- **Python frontend** (`python/dynamic_runner/`) — CLI parsing, logging,
  binary discovery, secondary spawning, and dispatch into the native
  runtime via `run()`.
- **Native runtime** (`dynamic_runner._native`) — extension module built
  from `crates/dynrunner-pyo3`, re-exporting manager classes, config
  dataclasses, and the `run_local` / `run_distributed` / `run_primary` /
  `run_secondary` / `run_observer_late_joiner` entry points.
- **18 internal Rust crates** under `crates/dynrunner-*`. Notable ones:
  `dynrunner-core` (traits), `dynrunner-scheduler`(`-api`),
  `dynrunner-manager-local`, `dynrunner-manager-distributed`,
  `dynrunner-transport-quic` / `-socket` / `-channel` / `-tunnel`,
  `dynrunner-worker`, `dynrunner-gateway`, `dynrunner-driver`,
  `dynrunner-discovery`, `dynrunner-slurm`, `dynrunner-publish`, and the
  two protocol crates. See [`crates/`](crates/) for the full set.

## Per-task memory profiling (`--memprofile`)

Opt-in 1 Hz per-task memory profiler. Each worker writes one
zstd-framed JSONL file per task it processes, capturing the worker's
cgroup-v2 `memory.current`, `memory.swap.current`, and full
`memory.stat` once per second. Frame-per-sample keeps the file
`zstd -dc`-recoverable after a hard manager death (the last in-flight
sample is the most a consumer can lose).

```bash
python -m my_task --multi-computer local --memprofile \
    --output /tmp/run-out --cores 12
```

Output lands at
`{--output}/memprofile/{task_id}.worker-{N}.memprofile.jsonl.zst`.
`task_id` is treated as a relative path, so a slash-bearing identifier
like `nping/x86/clang/9/Os` becomes
`memprofile/nping/x86/clang/9/Os.worker-3.memprofile.jsonl.zst` (the
writer `mkdir -p`s the parents).

**SLURM mode:** the same flag works on the SLURM secondary, whose
container has `/app/out-network` bind-mounted to the gateway-shared
output filesystem; the secondary writes there by default so profiles
survive job teardown and land alongside the run's other artifacts.
Operator checklist after a `--memprofile` SLURM run:

- `ls {gateway.output_path}/memprofile/` should list one `.jsonl.zst`
  per completed task.
- `zstd -dc {gateway.output_path}/memprofile/<task>.worker-0.memprofile.jsonl.zst | head -1 | jq`
  to spot-check a sample.
- If the directory is empty, check the secondary's logs for
  `--memprofile set but /app/out-network is not present` (ran outside
  the wrapper) or a cgroup-v2 fallback warn (`cgroup-v2 leaf not found`
  etc.) — both downgrade to a no-op with a single warn line.

**Requires** delegated cgroup-v2 on the host (rootless podman with
`--cgroup-manager=cgroupfs`, or `systemd-run --user` with
`Delegate=yes`). Without delegation the sampler produces no files and
surfaces one warn line at startup.

## Development

The dev environment is provided by the flake — no `requirements.txt` or
venv. Requires Python ≥ 3.12.

```bash
nix develop
cargo test --workspace
maturin develop --release   # installs dynamic_runner into the nix env
pytest
```

## Releases

Pushing a `v*` tag triggers `wheels.yml`, which builds wheels for the
supported targets and uploads them as workflow artifacts. Publishing a
GitHub Release against that tag triggers `publish.yml`, which fetches
those wheels and uploads them to PyPI via Trusted Publishing (OIDC; no
token lives in the repo).

## License

[Apache-2.0](LICENSE).
