# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

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

[Unreleased]: https://github.com/sirati/dynamic-runner/compare/v0.1.1...HEAD
[0.1.1]: https://github.com/sirati/dynamic-runner/releases/tag/v0.1.1
[0.1.0]: https://github.com/sirati/dynamic-runner/releases/tag/v0.1.0
