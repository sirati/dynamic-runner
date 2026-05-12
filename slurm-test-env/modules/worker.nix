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

  # Workers receive a host-side bind mount at /tmp from up.sh (so the host's
  # disk, not a small in-container tmpfs, holds large image tarballs and
  # other scratch). Letting NixOS mount tmpfs over it would mask the bind.
  boot.tmp.useTmpfs = false;

  # Defensive belt-and-braces against the pthread_create EAGAIN class that
  # killed slurmd on 2026-05-11 (ds-test, journalctl -u slurmd: "fatal:
  # _try_service_msg: pthread_create error Resource temporarily
  # unavailable" at 09:53:28 UTC, slurmd[419] uptime 22h34m). The
  # TaskPlugin=task/cgroup + proctrack/cgroup change in
  # slurm-cluster.nix is the architectural fix: batch-job processes get
  # their own cgroup and can no longer exhaust the shared accounting.
  # These two unit-level lifts ensure that even if a future cgroup-
  # isolation regression slips through (or operators run a workload
  # that probes a different limit class), slurmd's own thread/fork
  # accounting can't be throttled by anything we control here.
  #
  # slurmd peaks at ~9.5 MiB RSS / ~266ms CPU over 22h+ uptime, so
  # unbounding NPROC/TasksMax is genuinely cost-free — slurmd has no
  # appetite for either.
  systemd.services.slurmd.serviceConfig = {
    LimitNPROC = "infinity";
    TasksMax = "infinity";
    # TEMPORARY DIAGNOSTIC (2026-05-12, revert post-investigation).
    # asm-dataset-nix needs DYNRUNNER_DISABLE_TEARDOWN_WATCHDOG=1
    # to propagate into the framework wrapper bash for definitive
    # watchdog rule-out. The propagation path
    # consumer-pytest-env → ssh sbatch → slurmctld → slurmd is
    # blocked because:
    #   - sshd has PermitUserEnvironment=no (no ~/.ssh/environment)
    #   - framework doesn't expose sbatch --export surface
    #   - systemd units don't read /etc/environment
    # Setting it directly on slurmd's unit injects into slurmd's env,
    # which inherits to slurmstepd → wrapper bash. This is a per-
    # cluster knob until the framework grows an --export equivalent;
    # remove once the bilateral-SIGTERM root cause is identified and
    # closed.
    Environment = [ "DYNRUNNER_DISABLE_TEARDOWN_WATCHDOG=1" ];
  };
}
