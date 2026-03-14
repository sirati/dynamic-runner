{
  description = "dynamic_batch_rs - Rust backend for dynamic_batch";

  inputs = {
    nixpkgs.url = "github:NixOS/nixpkgs/nixos-unstable";
    rust-overlay = {
      url = "github:oxalica/rust-overlay";
      inputs.nixpkgs.follows = "nixpkgs";
    };
  };

  outputs =
    {
      self,
      nixpkgs,
      rust-overlay,
    }:
    let
      systems = [
        "x86_64-linux"
        "aarch64-linux"
        "x86_64-darwin"
        "aarch64-darwin"
      ];
      forAllSystems = nixpkgs.lib.genAttrs systems;
    in
    {
      packages = forAllSystems (
        system:
        let
          pkgs = import nixpkgs {
            inherit system;
            overlays = [ rust-overlay.overlays.default ];
          };

          rustToolchain = pkgs.rust-bin.stable.latest.default.override {
            extensions = [
              "rust-src"
              "rust-analyzer"
              "clippy"
            ];
          };

          rustPlatform = pkgs.makeRustPlatform {
            cargo = rustToolchain;
            rustc = rustToolchain;
          };

          python-package = pkgs.python314Packages.callPackage ./package.nix {
            inherit rustPlatform;
          };
        in
        {
          inherit python-package;
          rust-toolchain = rustToolchain;
          default = python-package;
        }
      );

      overlays.default = final: prev: {
        pythonPackagesExtensions = (prev.pythonPackagesExtensions or [ ]) ++ [
          (
            python-final: python-prev: {
              dynamic-batch-rs = python-final.callPackage ./package.nix {
                rustPlatform =
                  let
                    toolchain = final.rust-bin.stable.latest.default;
                  in
                  final.makeRustPlatform {
                    cargo = toolchain;
                    rustc = toolchain;
                  };
              };
            }
          )
        ];
      };
    };
}
