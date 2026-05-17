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
          shutdown-manager-bin = pkgs.pkgsCross.musl64.pkgsStatic.rustPlatform.buildRustPackage {
            pname = "dynrunner-slurm-shutdown";
            # Mirrors `shutdown-manager/Cargo.toml`'s `[package].version`.
            # Bump together with that file on shutdown-manager releases.
            version = "0.1.0";
            src = ./shutdown-manager;
            cargoLock.lockFile = ./shutdown-manager/Cargo.lock;
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
          };

          # The wheel/Python-package derivation. Pass the built
          # shutdown-manager binary so the wheel's `postInstall` can
          # drop it into `dynamic_runner/_shutdown_manager/` inside
          # the site-packages tree (see `nix/wheel.nix`).
          dynamic-runner = pkgs.python3Packages.callPackage ./nix/wheel.nix {
            shutdownManagerBin = shutdown-manager-bin;
          };

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
            # Sibling output for the bundled shutdown-manager binary —
            # exposed so consumers (CI smoke tests, debugging) can
            # `nix build .#dynrunner-slurm-shutdown` and inspect the
            # exact artefact the wheel embeds without unpacking the
            # site-packages tree.
            dynrunner-slurm-shutdown = shutdown-manager-bin;
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
