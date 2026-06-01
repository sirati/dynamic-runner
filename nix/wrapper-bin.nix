{ pkgsCross }:

# Musl-static rustPlatform for the slurm-wrapper binary.
# Kept distinct from the framework wheel's rustPlatform
# because that one uses the glibc default (matches the PyO3
# extension's `python3 -c "import _native"` target), while the
# wrapper runs as the SLURM-job entrypoint on whatever compute
# node the job lands on and must not link against the
# dispatcher's libc.
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
# We deliberately use nixpkgs' ``rustPlatform.buildRustPackage``
# here so the framework flake stays free of any crane input.
# ``pkgsCross.musl64.pkgsStatic`` (not plain ``pkgsCross.musl64``)
# because nixpkgs' rustc-wrapper injects
# ``-C target-feature=-crt-static`` for musl host platforms that
# are not also ``isStatic``, which would override any RUSTFLAGS
# we pass at the derivation level. The ``pkgsStatic`` overlay
# sets ``hostPlatform.isStatic`` and the wrapper leaves
# ``+crt-static`` alone. See
# ``pkgs/build-support/rust/rustc-wrapper/default.nix`` in
# nixpkgs for the override rule.
pkgsCross.musl64.pkgsStatic.rustPlatform.buildRustPackage {
  pname = "dynrunner-slurm-wrapper";
  # Mirrors `slurm-wrapper/Cargo.toml`'s `[workspace.package].version`.
  # Bump together with that file on slurm-wrapper releases.
  version = "0.1.0";
  # The standalone 2-member workspace (`config` + `wrapper`). The
  # `wrapper` member depends on `config` with the `cli` feature
  # (so clap is pulled in by a normal workspace build); no extra
  # feature flag is required here.
  src = ../slurm-wrapper;
  cargoLock.lockFile = ../slurm-wrapper/Cargo.lock;
  # Build only the wrapper member's binary. The `config` member is
  # a pure lib (also consumed by the root workspace as a path-dep),
  # so naming the bin keeps the artefact scope unambiguous.
  cargoBuildFlags = [
    "--bin"
    "dynrunner-slurm-wrapper"
  ];
  # ``+crt-static`` forces full static linking against musl. Without
  # this flag the cross-musl stdenv still produces a dynamically-linked
  # musl binary, which would not survive an off-store invocation on
  # the compute node.
  CARGO_BUILD_RUSTFLAGS = "-C target-feature=+crt-static";
  # Tests link against the host glibc; we only ship the
  # musl-static release artefact from this derivation.
  doCheck = false;

  meta = {
    description = "Musl-static SLURM secondary wrapper binary (dynrunner-slurm-wrapper).";
  };
}
