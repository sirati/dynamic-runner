{ config, lib, pkgs, ... }:
let
  # Static slot ceiling baked into slurm.conf as `slurm-worker[1-16]`.
  # The number of containers actually started is governed independently
  # by $WORKER_COUNT in deploy/env.sh (default 4). Slots above the
  # started count appear as DOWN in `sinfo`; that's fine — slurm only
  # schedules onto live nodes. The ceiling is generous on purpose: it
  # lets operators scale workers up/down without rebuilding the image.
  workerCount = 16;
  workerNodeSpec = "slurm-worker[1-${toString workerCount}]";

  # Insecure fixed dev key. Both gateway and worker images embed the
  # *same* derivation — same nix store hash — so they share a key without
  # any host-side secret distribution. Acceptable here because:
  #   - this environment is explicitly a local test harness (.rules);
  #   - it is never reachable beyond the operator's host;
  #   - a real cluster would source this from /etc/cluster-secrets or vault.
  #
  # `head -c 1024 /dev/zero | tr '\0' 'Z'` produces a deterministic 1024-
  # byte file (the minimum size munge accepts for a key). We avoid
  # `yes Z | head -c 1024` because head closes the pipe early, `yes` then
  # exits on SIGPIPE, and runCommand runs the builder under set -o pipefail
  # — so the whole derivation would fail. Reading /dev/zero (a regular
  # device) lets head exit cleanly before tr finishes, no SIGPIPE.
  insecureMungeKey = pkgs.runCommand "slurm-test-env-insecure-munge.key" { } ''
    head -c 1024 /dev/zero | tr '\0' 'Z' > $out
  '';
in
{
  environment.etc."slurm-test-env/workers".text =
    lib.concatMapStringsSep "\n"
      (i: "slurm-worker${toString i}")
      (lib.range 1 workerCount)
    + "\n";
  environment.etc."slurm-test-env/worker-count".text = toString workerCount + "\n";

  # Real-file install of the munge key (mode != "symlink" forces a copy
  # with the requested perms/owner, which is what munged needs — a symlink
  # into the world-readable nix store would fail munge's ownership check).
  environment.etc."munge/munge.key" = {
    source = insecureMungeKey;
    mode = "0400";
    user = "munge";
    group = "munge";
  };

  services.munge.enable = true;
  # services.munge.password defaults to /etc/munge/munge.key; the entry
  # above provides exactly that path, so no override needed.

  services.slurm = {
    clusterName = "test";
    controlMachine = "slurm-gateway";

    # RealMemory hardcoded: container /proc/meminfo reflects host RAM, not
    # the cgroup cap, so slurmd would otherwise advertise too much. 3500
    # leaves ~500 MiB headroom under the 4 GiB cgroup limit.
    nodeName = [
      "${workerNodeSpec} CPUs=2 RealMemory=3500 State=UNKNOWN"
    ];

    partitionName = [
      "debug Nodes=${workerNodeSpec} Default=YES MaxTime=INFINITE State=UP"
    ];

    # proctrack/cgroup pairs with TaskPlugin=task/cgroup (below) to put
    # batch-job processes in a per-job cgroup, distinct from slurmd's own
    # cgroup. Without this, a fork-bomby batch job (e.g. a nix-daemon
    # substitution storm with many parallel build workers) exhausts the
    # shared cgroup's pids/threads accounting and slurmd's next
    # pthread_create fails with EAGAIN — observed end-to-end on
    # ds-test 2026-05-11 at 09:53:28 UTC, slurmd died fatally after a
    # 102s task/none job. cgroup-isolating jobs from slurmd fixes the
    # whole class.
    procTrackType = "proctrack/cgroup";
    # extraCgroupConfig deliberately left at "" — slurm's defaults
    # (ConstrainCores/RAMSpace/SwapSpace=no, CgroupAutomount=no,
    # CgroupPlugin=autodetect) are exactly what we want here: per-job
    # cgroup membership for process tracking, NO new resource
    # constraints. The existing RealMemory accounting via select/cons_tres
    # is untouched; adding ConstrainRAMSpace=yes would silently tighten
    # the per-job memory ceiling to the slurm-allocated --mem value,
    # which is a behavioural change downstream consumers haven't asked
    # for. Additive in a future iteration if needed.

    extraConfig = ''
      TaskPlugin=task/cgroup
      SchedulerType=sched/backfill
      SelectType=select/cons_tres
      SelectTypeParameters=CR_Core_Memory
      MpiDefault=none
      SlurmctldDebug=info
      # SlurmdDebug=debug2 was bumped from info on 2026-05-12 to give
      # consumer diagnostic agents visibility into proctrack/cgroup's
      # step-kill calls. asm-dataset-nix has a reproducible bilateral
      # SIGTERM landing at ~T+39s into ds-test T3 runs that nothing in
      # slurmd's info-level journal explains; debug2 logs slurmstepd's
      # per-PID kill iteration so we can identify whether the kill
      # originates from slurmstepd at all and, if so, what triggered it.
      # Verbose but cheap on a test harness; revert to info once the
      # diagnostic phase is over.
      SlurmdDebug=debug2
    '';
  };

  # Slurm CLI tools (sbatch/srun/sinfo/scontrol) are installed by the
  # NixOS slurm module itself when client.enable or server.enable is set
  # — and crucially, it installs the *wrapped* derivation that pre-sets
  # SLURM_CONF. Listing `pkgs.slurm` here would shadow that wrapper with
  # the bare package and break user invocations (no SLURM_CONF → "Could
  # not establish a configuration source"). So we deliberately do NOT
  # add slurm to systemPackages from this module.

  systemd.tmpfiles.rules = [
    "d /var/log/slurm 0755 slurm slurm - -"
  ];
}
