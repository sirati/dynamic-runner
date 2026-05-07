{ config, lib, pkgs, ... }:
{
  networking.hostName = "slurm-gateway";

  # The gateway runs slurmctld and is the user-facing submission host.
  # Slurm CLI tools (sbatch/srun/sinfo/scontrol) come from slurm-cluster.nix.
  services.slurm.server.enable = true;
}
