# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Fixed
- **SLURM wrapper now defaults `--ulimit nproc=32768:32768`** on every
  `podman run` invocation. Without it, podman propagates the SLURM
  job's inherited per-user `RLIMIT_NPROC` (pam_limits) — or the
  `containers.conf` default — into the container, and fork-heavy
  in-container workloads (autotools `./configure`, parallel
  gcc/clang, JVM thread spawn) fail with
  `fork: Resource temporarily unavailable` (`EAGAIN`) whenever that
  inherited cap lands below the workload's peak fork count. This is
  a *different* counter from `--pids-limit` (which caps the
  container cgroup's `pids.max`); both must be permissive for the
  workload to make progress. Field signal: asm-dataset-nix's
  workload-sized nix builds wedged 10/10 attempts on a cluster
  whose user shells inherited a 4096 nproc cap. Override via
  `TaskDeploymentSpec.extra_run_args=("--ulimit=nproc=<N>:<N>",)`;
  podman last-wins parsing applies the consumer's value. See
  `docs/MIGRATION_2026_05_PYTHON_TO_RUST.md` and the rationale
  comment in `crates/dynrunner-slurm/src/wrapper_script.rs`.
- `nix/wheel.nix` `cargoDeps.hash` was the v0.2.0 value
  (`MscydA1Kui...`) on v0.3.0 / v0.3.1. The workspace version bump
  0.1.0 → 0.3.0 in Cargo.lock invalidates the recorded SRI hash even
  with no transitive-dep changes — flake consumers hit a fixed-output-
  derivation mismatch. Recalibrated to
  `sha256-T1d1IprrdayGG+gwnNON2bx+h6mauhH/zkETedHetms=` (verified by
  two independent local builds).

### Changed (BREAKING)
- **`_native.find_items(task_definition, root)` no longer accepts the
  `gateway_url`, `ssh_config_path`, or `ssh_identity_file` kwargs.**
  Discovery is always local-filesystem. In `--source-already-staged`
  mode the framework now schedules the call on the cluster secondary
  that has the staged path bind-mounted, not on the submitter — so a
  submitter-side SSH walk is no longer needed (or possible). Consumers
  calling `_native.find_items(self, root, gateway_url=...)` to walk a
  staged corpus from the submitter must delete the `gateway_url`
  argument (and the SSH-side kwargs if they were forwarded). Known
  caller: asm-tokenizer's `_iter_gateway_pairs`.
  Concurrently, `dynrunner_gateway::Filesystem` (trait) +
  `FsError` + `DirEntry` are removed from the public surface, along
  with the `Filesystem` impls on `LocalGateway` and `SshGateway`.
  `LocalGateway` and `SshGateway` themselves remain (upload,
  command execution, and port-forwarding are unchanged).
- **`run()` API**: the optional `spawn_secondary_factory` parameter is
  gone; replaced with `deployment: TaskDeploymentSpec | None`. The spec
  carries the secondary Python module name, container image identity
  (name, tag), the nix build target, and an optional SLURM job-name
  prefix override. The framework derives the local-subprocess spawn
  closure from `deployment.secondary_module`, so consumers no longer
  construct it themselves. `--multi-computer slurm|local` now requires
  `deployment` and exits early with an actionable error if it is None.
- **`dynamic_runner.cli.run` deprecated alias removed.** v0.2.0 was its
  one-release grace window; v0.3.0 drops it. Use
  `from dynamic_runner import run` (or
  `from dynamic_runner.run import run`).
- **`make_subprocess_spawn_factory` removed** from the public surface.
  Replace
  `make_subprocess_spawn_factory("dynrunner.tokenize")` plumbed via
  `spawn_secondary_factory=` with
  `TaskDeploymentSpec(secondary_module="dynrunner.tokenize",
  image_name="<your-image>")` plumbed via `deployment=`.

### Fixed
- **SLURM wrapper invoked the obsolete `dynamic_batch` module name** at
  `packaging/job_manager.py:223,239` regardless of which task package
  was running, so every SLURM secondary crashed at startup with
  `ModuleNotFoundError`. The module name is now sourced from
  `TaskDeploymentSpec.secondary_module`.
- **SLURM test wrapper had two broken commands** at
  `packaging/job_manager.py:316,320`. With `Entrypoint=["python","-m"]`
  on the consumer image, `... run image:tag python --version` became
  `python -m python --version` (ModuleNotFoundError) and
  `... dynamic_batch --help` became `python -m dynamic_batch --help`
  (also ModuleNotFoundError); both failed silently because the surrounding
  `set -e` only fires on tracked exit codes. Replaced with a single
  positive check: `... run image:tag {secondary_module} --help` through
  the entrypoint.
- **Hardcoded `asm-tokenizer` task identity** removed from
  `packaging/podman.py` (image name, image tag, image tar basename,
  sha256 marker basename, layered-upload log label, nix build target),
  `packaging/preparation.py` (image tar basename, SLURM job-name
  prefix), and `packaging/layered_transfer.py` (docstring). All
  read from `TaskDeploymentSpec`.

### Added
- `dynamic_runner.TaskDeploymentSpec`: frozen dataclass capturing the
  consumer-supplied deployment metadata the SLURM packaging path needs.
  See `python/dynamic_runner/deployment_spec.py`.

## [0.2.0] - 2026-04-29

### Changed (BREAKING)
- **`TaskDefinition` Protocol redesign**: first-class phases / task types
  / affinity classes. The old `get_stages` / `Phase` enum /
  `StageDefinition` / `organize_and_sort_items` / `get_worker_module`
  / `get_reserved_memory_per_worker` / `dispatch_binary` surface is
  gone. Consumers declare topology via `get_phases()` returning
  `PhaseSpec` + `TaskTypeSpec`; classify items via per-item
  `phase_id` / `type_id` / `affinity_id` set in `discover_items()`;
  hook lifecycle via `on_run_start` / `on_run_end` /
  `on_phase_start` / `on_phase_end`. See `docs/PHASES.md`.
- **`BinaryInfo` → `TaskInfo`**: the type now carries scheduling
  metadata (phase / type / affinity / payload), not just binary
  metadata. `BinaryIdentifier` (the inner identifier shape) is
  unchanged. Python module path `dynamic_runner._shared.binary_info`
  preserved; class `BinaryInfo` renamed to `TaskInfo`.
- **`ResourceEstimator` is now generic over `<I>`**: estimators
  receive the full `TaskInfo<I>` instead of just `binary_size: u64`.
  This unlocks per-`type_id` memory estimation in the pyo3 bridge.
- **wire format**: `protocol-primary-secondary`'s `TaskInfo` was
  renamed `TaskListEntry` to free the name for `dynrunner-core`'s
  scheduling unit. `DistributedBinaryInfo` will gain
  phase_id / type_id / affinity_id / payload fields in a follow-up
  (Phase 4B); for now, defaults are used on receive with a TODO
  marker.

### Added
- `PhaseId`, `TypeId`, `AffinityId` newtypes in `dynrunner-core`
  (`Arc<str>`-backed; same shape as `ResourceKind`).
- `PendingPool<I>` in `dynrunner-scheduler-api`: per-(phase, type,
  affinity) bucketed pool with soft worker pinning, phase state
  machine (Blocked → Active → Draining → Drained → Done), and
  drain-transition events. Replaces the `pending_binaries:
  Vec<BinaryInfo<I>>` field in both managers.
- Per-type memory-estimator bridge in pyo3: each `TaskTypeSpec`'s
  `estimator_attr` is probed once at run start and cached as
  `Linear` / `Constant` / `PyCallable` (last is a fallback with a
  warning).
- `on_run_start` / `on_run_end` / `on_phase_start` / `on_phase_end`
  lifecycle hooks called via FFI at the right boundaries.
- `docs/PHASES.md` migration guide.

### Removed
- `Phase` enum, `StageDefinition`, `get_stages`,
  `organize_and_sort_items`, `get_worker_module`,
  `get_reserved_memory_per_worker`, `find_binaries`,
  `dispatch_binary`. No legacy shims, aliases, or compatibility
  paths — downstream consumers must migrate. See `docs/PHASES.md`.

## [0.1.1] - 2026-04-29

### Fixed
- `nix/wheel.nix` `cargoDeps.hash` was a `lib.fakeHash` placeholder in
  v0.1.0, breaking any `nix build` of the wheel via the flake overlay.
  Pinned to the actual SRI hash so consumers can build dynamic-runner
  through `dynamic-runner.overlays.default` without manual hash
  calibration.

## [0.1.0] - 2026-04-29

### Added
- Initial release. Extracted from `asm-tokenizer` (commit history preserved
  via `git filter-repo`).
- Python frontend `dynamic_runner` (mixed-layout maturin wheel).
- 14 internal Rust crates under `crates/dynrunner-*`.
- Local + distributed manager implementations.
- QUIC, Unix-socket, and in-process channel transports.
- Slurm gateway integration.

[Unreleased]: https://github.com/sirati/dynamic-runner/compare/v0.2.0...HEAD
[0.2.0]: https://github.com/sirati/dynamic-runner/releases/tag/v0.2.0
[0.1.1]: https://github.com/sirati/dynamic-runner/releases/tag/v0.1.1
[0.1.0]: https://github.com/sirati/dynamic-runner/releases/tag/v0.1.0
