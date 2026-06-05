{
  description = "dynrunner-slurm-shutdown: dev shell for the static container teardown binary";

  # No build package here. Since the manager started path-depending on the
  # repo-root `crates/dynrunner-reap` crate (the shared reap state-machine,
  # also used by the slurm-wrapper), a standalone `crane` build rooted at
  # `shutdown-manager/` can no longer resolve that out-of-subtree sibling
  # without rooting `src` at the repo and running cargo in a subdir — which
  # fights crane's root-`Cargo.toml`/root-`Cargo.lock` assumptions for both
  # the deps layer and the install hook's `cargo metadata`.
  #
  # The CANONICAL musl-static build is the framework flake's
  # `nix build .#dynrunner-slurm-shutdown` target (its
  # `nix/shutdown-manager-bin.nix` roots the build src at the repo and
  # unions both trees via `lib.fileset`, using nixpkgs' `rustPlatform`).
  # That is the exact artefact the wheel bundles. This standalone flake now
  # provides ONLY the musl dev toolchain (a `crane` build target here would
  # be a second, redundant build path to keep green for no benefit — and a
  # broken one is worse than none). See `README.md` → Building.
  inputs = {
    nixpkgs.url = "github:NixOS/nixpkgs/nixos-unstable";
    flake-utils.url = "github:numtide/flake-utils";
    rust-overlay = {
      url = "github:oxalica/rust-overlay";
      inputs.nixpkgs.follows = "nixpkgs";
    };
  };

  outputs =
    {
      self,
      nixpkgs,
      flake-utils,
      rust-overlay,
      ...
    }:
    flake-utils.lib.eachDefaultSystem (
      system:
      let
        pkgs = import nixpkgs {
          inherit system;
          overlays = [ (import rust-overlay) ];
        };

        # Minimal toolchain with the musl target, mirroring the framework
        # flake's musl build. No clippy / rustfmt / rust-src — those are
        # the repo-root devShell's concern.
        rustToolchain = pkgs.rust-bin.stable.latest.minimal.override {
          targets = [ "x86_64-unknown-linux-musl" ];
        };
      in
      {
        # Dev shell mirroring the build toolchain. Use
        # `cargo build/test` from this directory for native (glibc)
        # development; build the shipped musl artefact from the repo root
        # with `nix build .#dynrunner-slurm-shutdown`.
        devShells.default = pkgs.mkShell {
          name = "dynrunner-slurm-shutdown-dev";
          nativeBuildInputs = [ rustToolchain ];
        };
      }
    );
}
