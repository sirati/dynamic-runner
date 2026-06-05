{ pkgsCross, lib }:

# The standalone `slurm-wrapper` workspace now path-depends on
# `crates/dynrunner-reap` (the shared reap state-machine, also used by the
# shutdown-manager), which lives OUTSIDE the `slurm-wrapper/` subtree. So
# `src` can no longer be `../slurm-wrapper` alone — it must also include
# `crates/dynrunner-reap`. We therefore root `src` at the repo and filter
# to exactly the two trees the wrapper workspace reads, then point
# `cargoRoot` at the `slurm-wrapper` manifest. The filter keeps the
# rebuild trigger tight (edits elsewhere in the repo don't invalidate this
# derivation) — only `slurm-wrapper/**` and `crates/dynrunner-reap/**`
# changes do.
let
  # The wrapper workspace path-depends on `crates/dynrunner-reap`, so the
  # src must include BOTH trees with their RELATIVE layout preserved (the
  # `../../crates/dynrunner-reap` path-dep must resolve). `lib.fileset` is
  # the robust API for "union these subtrees into one source": it keeps the
  # repo-relative paths intact and includes ancestor dirs automatically, so
  # no hand-rolled prefix/ancestor filter is needed.
  wrapperSrc = lib.fileset.toSource {
    root = ../.;
    fileset = lib.fileset.unions [
      ../slurm-wrapper
      ../crates/dynrunner-reap
    ];
  };
in

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
  # Repo-root-filtered src (see the `let` above): includes both the
  # `slurm-wrapper` workspace AND the `crates/dynrunner-reap` path-dep it
  # now references. `buildAndTestSubdir` runs the cargo build/check/install
  # hooks INSIDE `slurm-wrapper/` (where the workspace manifest + Cargo.lock
  # live) while keeping the full filtered tree as the source — so the
  # wrapper's `../../crates/dynrunner-reap` path-dep resolves to
  # `<src>/crates/dynrunner-reap`, present in the tree. (Plain `cargoRoot`
  # only relocates vendoring, not the build cwd, so the build cargo ran at
  # the src root and could not find the manifest.)
  src = wrapperSrc;
  # `cargoRoot` tells the vendor + lockfile-consistency hooks the Cargo.lock
  # lives in `slurm-wrapper/`; `buildAndTestSubdir` runs the build/check
  # hooks there too. Both are needed in this nixpkgs: `cargoRoot` alone
  # left the build cwd at the src root (no manifest), `buildAndTestSubdir`
  # alone made the lock hook look for Cargo.lock at the src root.
  cargoRoot = "slurm-wrapper";
  buildAndTestSubdir = "slurm-wrapper";
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
