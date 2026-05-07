{ config, lib, pkgs, ... }:
{
  # Empty hostName keeps NixOS's hostname-activation a no-op; the actual
  # hostname is whatever podman set via `--hostname=slurm-workerN` before
  # init started. This is what lets a single image serve all N workers.
  networking.hostName = lib.mkForce "";

  # slurmd reads the same slurm.conf the gateway produced (via
  # slurm-cluster.nix), so as long as the kernel hostname matches one of the
  # NodeName entries, slurmctld will recognize the worker.
  services.slurm.client.enable = true;

  # Per-requirement: workers have podman installed.
  virtualisation.podman = {
    enable = true;
    dockerCompat = false;
  };
}
