{
  description = "dynamic_runner - generic Rust runner backend with Python bindings";

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
    }:
    # System-independent outputs (overlays, lib, etc.).
    {
      # Consumer-facing overlay: adds `dynamic-runner` into every
      # Python package set of the consuming nixpkgs instance via
      # `pythonPackagesExtensions`. After applying this overlay a
      # consumer flake can do `pkgs.python3Packages.dynamic-runner`.
      overlays.default = import ./nix/overlay.nix;
    }
    //
      # Per-system outputs (packages, devShells).
      flake-utils.lib.eachDefaultSystem (
        system:
        let
          pkgs = import nixpkgs {
            inherit system;
            overlays = [ rust-overlay.overlays.default ];
          };

          # Developer-facing toolchain (used in the devShell only).
          # The wheel derivation uses nixpkgs' default rustPlatform so
          # consumers do not need to layer rust-overlay to build it.
          rustToolchain = pkgs.rust-bin.stable.latest.default.override {
            extensions = [
              "rust-src"
              "rust-analyzer"
              "clippy"
            ];
          };

          # The wheel/Python-package derivation.
          dynamic-runner = pkgs.python3Packages.callPackage ./nix/wheel.nix { };

          # E2E test_consumer container. Built with `nix build .#dockerImage`
          # (default `TaskDeploymentSpec.nix_build_target` per
          # `python/dynamic_runner/deployment_spec.py`). Consumed by the
          # framework's `--packaging podman` SLURM dispatch in
          # `tests/e2e/run_e2e.py`. See `nix/test-consumer-image.nix`
          # for the derivation shape and the references it draws on.
          dockerImage = pkgs.callPackage ./nix/test-consumer-image.nix {
            inherit dynamic-runner;
            testsSrc = ./tests;
          };
        in
        {
          packages = {
            inherit dynamic-runner dockerImage;
            default = dynamic-runner;
          };

          devShells.default = pkgs.mkShell {
            name = "dynamic-runner-dev";

            nativeBuildInputs = [
              rustToolchain
              pkgs.maturin
              pkgs.pkg-config
            ];

            buildInputs = [
              pkgs.openssl
              # python3 + pyo3 build prerequisites
              pkgs.python3
              pkgs.python3Packages.pip
              pkgs.python3Packages.setuptools
            ];

            # Help PyO3 find the right interpreter when running maturin
            # outside a buildPythonPackage context.
            env = {
              PYO3_PYTHON = "${pkgs.python3}/bin/python3";
            };
          };
        }
      );
}
