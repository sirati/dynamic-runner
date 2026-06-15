{
  description = "slurm-test-env: podman-based local slurm cluster (1 gateway + N workers)";

  inputs = {
    nixpkgs.url = "github:NixOS/nixpkgs/nixos-unstable";
    flake-utils.url = "github:numtide/flake-utils";
  };

  outputs =
    {
      self,
      nixpkgs,
      flake-utils,
    }:
    flake-utils.lib.eachSystem [ "x86_64-linux" "aarch64-linux" ] (
      system:
      let
        pkgs = import nixpkgs { inherit system; };

        # NixOS rootfs tarball at <out>/tarball/nixos-system-*.tar.xz,
        # produced by the in-tree `virtualisation/docker-image.nix` module
        # (upstreamed from nixos-generators in NixOS 25.05). The deploy
        # script imports it via `podman import` — it is NOT a layered OCI
        # image; see deploy/up.sh::locate_tarball.
        mkImage =
          extraModules:
          (nixpkgs.lib.nixosSystem {
            inherit system;
            modules = [
              (
                { modulesPath, ... }:
                {
                  imports = [ "${modulesPath}/virtualisation/docker-image.nix" ];
                }
              )
              ./modules/common.nix
              ./modules/slurm-cluster.nix
            ] ++ extraModules;
          }).config.system.build.tarball;

        gatewayImage = mkImage [ ./modules/gateway.nix ];
        workerImage = mkImage [ ./modules/worker.nix ];

        # Bundle the host-side scripts (deploy lifecycle + user provisioner)
        # into a single package, with $PATH and image-tarball locations
        # baked in via wrappers so `nix run .#up` works without env wiring.
        deploy = pkgs.runCommand "slurm-test-env-deploy" {
          nativeBuildInputs = [ pkgs.makeWrapper ];
        } ''
          mkdir -p $out/bin $out/share/slurm-test-env

          install -m 0644 ${./deploy/env.sh}             $out/share/slurm-test-env/env.sh
          install -m 0644 ${./deploy/lib.sh}             $out/share/slurm-test-env/lib.sh
          install -m 0755 ${./deploy/up.sh}              $out/bin/slurm-test-env-up
          install -m 0755 ${./deploy/down.sh}            $out/bin/slurm-test-env-down
          install -m 0755 ${./deploy/reset.sh}           $out/bin/slurm-test-env-reset
          install -m 0755 ${./deploy/reboot-node.sh}     $out/bin/slurm-test-env-reboot-node
          install -m 0755 ${./scripts/provision-user.sh} $out/bin/slurm-test-env-provision-user
          install -m 0755 ${./scripts/smoke-test.sh}     $out/bin/slurm-test-env-smoke-test
          install -m 0755 ${./scripts/test-543-no-scancel.sh} $out/bin/slurm-test-env-test-543-no-scancel
          install -m 0755 ${./scripts/test-547-chunking.sh} $out/bin/slurm-test-env-test-547-chunking
          install -m 0755 ${./scripts/test-565-572-pending-resources.sh} \
            $out/bin/slurm-test-env-test-565-572-pending-resources
          install -m 0755 ${./scripts/test-571-tunnel-deadline.sh} $out/bin/slurm-test-env-test-571-tunnel-deadline
          install -m 0755 ${./scripts/test-574-stats-skip.sh} $out/bin/slurm-test-env-test-574-stats-skip
          install -m 0755 ${./scripts/test-575-resource-stats.sh} $out/bin/slurm-test-env-test-575-resource-stats

          # Ship the #547 chunking-test workload package alongside the
          # assertion script. The script wires the driver via
          # DYNRUNNER_CMD which the operator points at the package
          # location at execution time (the dynamic_runner wheel is
          # deployed into the cluster out-of-band).
          mkdir -p $out/share/slurm-test-env/test_547_workload
          install -m 0644 ${./scripts/test_547_workload/__init__.py} \
            $out/share/slurm-test-env/test_547_workload/__init__.py
          install -m 0644 ${./scripts/test_547_workload/driver.py} \
            $out/share/slurm-test-env/test_547_workload/driver.py
          install -m 0644 ${./scripts/test_547_workload/worker.py} \
            $out/share/slurm-test-env/test_547_workload/worker.py


          # PATH wrapping: include the system deps each script needs, plus
          # $out/bin itself so e.g. smoke-test can call the wrapped
          # provision-user (and any future sibling) by short name.
          # SLURM_TEST_ENV_LIB_SH points at the shared helper sourced by
          # up.sh, reboot-node.sh, and down.sh — env-var-with-fallback so
          # the same script works under `nix run` and `bash deploy/...sh`.
          for bin in $out/bin/*; do
            wrapProgram "$bin" \
              --set SLURM_TEST_ENV_GATEWAY_IMAGE ${gatewayImage} \
              --set SLURM_TEST_ENV_WORKER_IMAGE  ${workerImage} \
              --set SLURM_TEST_ENV_ENV_FILE      $out/share/slurm-test-env/env.sh \
              --set SLURM_TEST_ENV_LIB_SH        $out/share/slurm-test-env/lib.sh \
              --prefix PATH : "$out/bin" \
              --prefix PATH : ${
                pkgs.lib.makeBinPath [
                  pkgs.bash
                  pkgs.podman
                  pkgs.openssh
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
          reset = {
            type = "app";
            program = "${deploy}/bin/slurm-test-env-reset";
          };
          reboot-node = {
            type = "app";
            program = "${deploy}/bin/slurm-test-env-reboot-node";
          };
          provision-user = {
            type = "app";
            program = "${deploy}/bin/slurm-test-env-provision-user";
          };
          smoke-test = {
            type = "app";
            program = "${deploy}/bin/slurm-test-env-smoke-test";
          };
          test-543-no-scancel = {
            type = "app";
            program = "${deploy}/bin/slurm-test-env-test-543-no-scancel";
          };
          test-547-chunking = {
            type = "app";
            program = "${deploy}/bin/slurm-test-env-test-547-chunking";
          };
          test-565-572-pending-resources = {
            type = "app";
            program = "${deploy}/bin/slurm-test-env-test-565-572-pending-resources";
          };
          test-571-tunnel-deadline = {
            type = "app";
            program = "${deploy}/bin/slurm-test-env-test-571-tunnel-deadline";
          };
          test-574-stats-skip = {
            type = "app";
            program = "${deploy}/bin/slurm-test-env-test-574-stats-skip";
          };
          test-575-resource-stats = {
            type = "app";
            program = "${deploy}/bin/slurm-test-env-test-575-resource-stats";
          };
        };
      }
    );
}
