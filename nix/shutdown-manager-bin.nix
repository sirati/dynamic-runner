{ pkgsCross }:

# Musl-static rustPlatform for the shutdown-manager binary.
# Kept distinct from the framework wheel's rustPlatform
# because that one uses the glibc default (matches the PyO3
# extension's `python3 -c "import _native"` target), while the
# shutdown manager runs out-of-cgroup as a systemd-user-scope
# subprocess on whatever compute node the SLURM job lands on
# and must not link against the dispatcher's libc.
#
# We use ``pkgsCross.musl64.rustPlatform`` rather than
# ``makeRustPlatform`` over a rust-overlay toolchain because
# the nixpkgs ``cargoBuildHook`` derives ``--target`` from
# ``stdenv.hostPlatform.rust.rustcTarget``, NOT from the
# toolchain's installed targets or the ``CARGO_BUILD_TARGET``
# env var (the hook prepends an explicit ``--target=`` to
# ``cargo build`` arguments that overrides both). Switching
# the entire ``rustPlatform``'s stdenv to a musl-host one is
# the only way to make ``rustcTarget`` resolve to
# ``x86_64-unknown-linux-musl`` end-to-end.
#
# The standalone ``shutdown-manager/flake.nix`` keeps using
# crane (where target selection is direct, not stdenv-driven).
# We deliberately use nixpkgs' ``rustPlatform.buildRustPackage``
# here so the framework flake stays free of the crane input.
# ``pkgsCross.musl64.pkgsStatic`` (not plain ``pkgsCross.musl64``)
# because nixpkgs' rustc-wrapper injects
# ``-C target-feature=-crt-static`` for musl host platforms that
# are not also ``isStatic``, which would override any RUSTFLAGS
# we pass at the derivation level. The ``pkgsStatic`` overlay
# sets ``hostPlatform.isStatic`` and the wrapper leaves
# ``+crt-static`` alone — matching the standalone crane build
# in ``shutdown-manager/flake.nix`` (which produces a
# ``static-pie linked`` artefact). See
# ``pkgs/build-support/rust/rustc-wrapper/default.nix`` in
# nixpkgs for the override rule.
pkgsCross.musl64.pkgsStatic.rustPlatform.buildRustPackage {
  pname = "dynrunner-slurm-shutdown";
  # Mirrors `shutdown-manager/Cargo.toml`'s `[package].version`.
  # Bump together with that file on shutdown-manager releases.
  version = "0.1.0";
  src = ../shutdown-manager;
  cargoLock.lockFile = ../shutdown-manager/Cargo.lock;
  # ``+crt-static`` forces full static linking against musl,
  # mirroring the standalone flake's commonArgs (see
  # ``shutdown-manager/flake.nix``). Without this flag the
  # cross-musl stdenv still produces a dynamically-linked
  # musl binary, which would not survive an off-store
  # invocation on the compute node.
  CARGO_BUILD_RUSTFLAGS = "-C target-feature=+crt-static";
  # Tests link against the host glibc; we only ship the
  # musl-static release artefact from this derivation. The
  # standalone `shutdown-manager/flake.nix` mirrors the
  # `doCheck = false` discipline for the same reason.
  doCheck = false;
}
