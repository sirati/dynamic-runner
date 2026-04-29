# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Changed
- **rename**: `BinaryInfo` -> `TaskInfo`. The type carries
  phase/type/affinity/payload now, not just binary metadata.
  `BinaryIdentifier` is unchanged. The wire-format
  `dynrunner_protocol_primary_secondary::TaskInfo` was renamed to
  `TaskListEntry` to disambiguate from the (renamed) core `TaskInfo`;
  `DistributedBinaryInfo` is unchanged and tracked by Phase 4B.

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
