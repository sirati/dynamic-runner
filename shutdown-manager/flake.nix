{
  description = "dynrunner-slurm-shutdown: static container teardown binary";

  inputs = {
    nixpkgs.url = "github:NixOS/nixpkgs/nixos-unstable";
    flake-utils.url = "github:numtide/flake-utils";
    rust-overlay = {
      url = "github:oxalica/rust-overlay";
      inputs.nixpkgs.follows = "nixpkgs";
    };
    crane.url = "github:ipetkov/crane";
  };

  outputs =
    {
      self,
      nixpkgs,
      flake-utils,
      rust-overlay,
      crane,
      ...
    }:
    flake-utils.lib.eachDefaultSystem (
      system:
      let
        pkgs = import nixpkgs {
          inherit system;
          overlays = [ (import rust-overlay) ];
        };

        # Minimal toolchain with the musl target. We don't need clippy /
        # rustfmt / rust-src here — those are devShell concerns.
        rustToolchain = pkgs.rust-bin.stable.latest.minimal.override {
          targets = [ "x86_64-unknown-linux-musl" ];
        };

        craneLib = (crane.mkLib pkgs).overrideToolchain rustToolchain;

        src = craneLib.cleanCargoSource ./.;

        commonArgs = {
          inherit src;
          strictDeps = true;

          CARGO_BUILD_TARGET = "x86_64-unknown-linux-musl";
          # +crt-static forces full static linking against musl.
          CARGO_BUILD_RUSTFLAGS = "-C target-feature=+crt-static";

          # Tests run against the host triple (glibc) so we cargo-test
          # without the musl flags via a separate derivation; the
          # release artefact is musl-static.
          doCheck = false;
        };

        # Pre-built dependency layer (cached separately so source-only
        # edits don't trigger a dep rebuild).
        cargoArtifacts = craneLib.buildDepsOnly commonArgs;

        shutdownManager = craneLib.buildPackage (
          commonArgs
          // {
            inherit cargoArtifacts;
            pname = "dynrunner-slurm-shutdown";
          }
        );
      in
      {
        packages = {
          default = shutdownManager;
          dynrunner-slurm-shutdown = shutdownManager;
        };

        # Optional dev shell mirroring the build toolchain.
        devShells.default = pkgs.mkShell {
          name = "dynrunner-slurm-shutdown-dev";
          nativeBuildInputs = [ rustToolchain ];
        };
      }
    );
}
