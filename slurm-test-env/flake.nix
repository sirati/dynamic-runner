{
  description = "slurm-test-env: podman-based local slurm cluster (1 gateway + N workers)";

  inputs = {
    nixpkgs.url = "github:NixOS/nixpkgs/nixos-unstable";
    flake-utils.url = "github:numtide/flake-utils";
    nixos-generators = {
      url = "github:nix-community/nixos-generators";
      inputs.nixpkgs.follows = "nixpkgs";
    };
  };

  outputs =
    {
      self,
      nixpkgs,
      flake-utils,
      nixos-generators,
    }:
    flake-utils.lib.eachSystem [ "x86_64-linux" "aarch64-linux" ] (
      system:
      let
        pkgs = import nixpkgs { inherit system; };

        # OCI tarball produced from a NixOS module set. The result is a
        # single tarball file; the deploy script handles podman load + tag
        # extraction.
        mkImage =
          extraModules:
          nixos-generators.nixosGenerate {
            inherit system;
            format = "docker";
            modules = [
              ./modules/common.nix
              ./modules/slurm-cluster.nix
            ] ++ extraModules;
          };

        gatewayImage = mkImage [ ./modules/gateway.nix ];
        workerImage = mkImage [ ./modules/worker.nix ];

        # Bundle the host-side scripts (deploy lifecycle + user provisioner)
        # into a single package, with $PATH and image-tarball locations
        # baked in via wrappers so `nix run .#up` works without env wiring.
        deploy = pkgs.runCommand "slurm-test-env-deploy" {
          nativeBuildInputs = [ pkgs.makeWrapper ];
        } ''
          mkdir -p $out/bin $out/share/slurm-test-env

          install -m 0644 ${./deploy/env.sh}            $out/share/slurm-test-env/env.sh
          install -m 0755 ${./deploy/up.sh}             $out/bin/slurm-test-env-up
          install -m 0755 ${./deploy/down.sh}           $out/bin/slurm-test-env-down
          install -m 0755 ${./scripts/provision-user.sh} $out/bin/slurm-test-env-provision-user

          for bin in $out/bin/*; do
            wrapProgram "$bin" \
              --set SLURM_TEST_ENV_GATEWAY_IMAGE ${gatewayImage} \
              --set SLURM_TEST_ENV_WORKER_IMAGE  ${workerImage} \
              --set SLURM_TEST_ENV_ENV_FILE      $out/share/slurm-test-env/env.sh \
              --prefix PATH : ${
                pkgs.lib.makeBinPath [
                  pkgs.podman
                  pkgs.coreutils
                  pkgs.gawk
                  pkgs.gnused
                  pkgs.gnugrep
                  pkgs.findutils
                  pkgs.util-linux
                ]
              }
          done
        '';
      in
      {
        packages = {
          gateway-image = gatewayImage;
          worker-image = workerImage;
          inherit deploy;
          default = deploy;
        };

        apps = {
          up = {
            type = "app";
            program = "${deploy}/bin/slurm-test-env-up";
          };
          down = {
            type = "app";
            program = "${deploy}/bin/slurm-test-env-down";
          };
          provision-user = {
            type = "app";
            program = "${deploy}/bin/slurm-test-env-provision-user";
          };
        };
      }
    );
}
