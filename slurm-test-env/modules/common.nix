{ config, lib, pkgs, ... }:
{
  system.stateVersion = "24.11";

  # The hostname is set by `podman --hostname=...`; leaving it empty here makes
  # NixOS skip the activation-time hostname write so the kernel value (set by
  # podman before init runs) sticks. Per-role modules may override.
  networking.hostName = lib.mkDefault "";

  # Containers get DNS + addressing from podman; turn off NixOS networking
  # bring-up so it does not fight podman's CNI setup.
  networking.useDHCP = lib.mkDefault false;
  networking.useNetworkd = lib.mkDefault false;
  services.resolved.enable = lib.mkDefault false;
  networking.firewall.enable = lib.mkDefault false;

  time.timeZone = lib.mkDefault "UTC";

  # /tmp on tmpfs (per requirement: writable, ephemeral). Per-role modules
  # may override (e.g. workers bind-mount a host dir at /tmp instead, so
  # large image tarballs don't blow the tmpfs size cap).
  boot.tmp.useTmpfs = lib.mkDefault true;

  # SSH policy:
  #   - Root login disabled. The cluster operator does not ssh to root anywhere.
  #     User provisioning happens host-side via `podman exec`.
  #   - Password auth disabled. Login is exclusively pubkey.
  #   - The user's authorized_keys file lives at $HOME/.ssh/authorized_keys.
  #     $HOME is on the shared /home bind mount, so writing the pubkey from
  #     any one node makes it valid on every node — no per-node distribution
  #     required.
  services.openssh = {
    enable = true;
    settings = {
      PermitRootLogin = "no";
      PasswordAuthentication = false;
      KbdInteractiveAuthentication = false;
      PubkeyAuthentication = true;

      # `GatewayPorts no` (OpenSSH default) binds every -R reverse
      # forward to the gateway's loopback only. Workers in this
      # cluster live in separate netns on the same podman bridge
      # network — they can reach the gateway by hostname/IP but not
      # by loopback, so a loopback-bound reverse forward is invisible
      # to them. `clientspecified` lets a caller opt in per-forward
      # via `ssh -R *:port:host:port` (or an explicit bind_addr) to
      # bind on all interfaces, while a plain `ssh -R port:host:port`
      # still binds loopback-only — no behavior change for callers
      # that don't need cross-container reach.
      GatewayPorts = "clientspecified";
    };
  };

  # Provisioned users are created by `podman exec useradd ...` from the host;
  # mutableUsers must be true for runtime useradd to be respected.
  users.mutableUsers = true;

  # Subuid/subgid layout for nested rootless podman.
  #
  # The whole container only has 65536 uids in its user namespace (the
  # rootless host podman gives us exactly one /etc/subuid block of
  # 65536, and we cannot ask for more without sudo on the host). We
  # carve that fixed budget into:
  #
  #   0      .. 9999     system / OS users (default NixOS layout)
  #   10000  .. 19999    cluster users (UID_BASE..UID_CEILING in
  #                       provision-user.sh)
  #   20000  .. 65535    shared subuid pool (~45k uids)
  #
  # The pool is *shared*: provision-user.sh writes the same
  # `<user>:20000:45536` line into /etc/subuid (and /etc/subgid) on
  # every container, for every cluster user. Two reasons:
  #
  #   1. The whole container fleet runs under one host /etc/subuid
  #      block; outer (host-side) uids are identical regardless of
  #      which container wrote a file. Sharing the pool keeps each
  #      cluster user's budget at the full 45k uids — partitioning
  #      would shrink each user's budget without buying isolation
  #      that the host can already see through.
  #   2. Files written by a cluster user's nested rootless podman map
  #      to the same host subuid on every container's view of the
  #      shared /home, so user state stays coherent across nodes.
  #
  # SUB_UID_COUNT=0 disables useradd's auto-subuid: provision-user.sh
  # writes the entries directly, and auto-allocation would otherwise
  # pick the default range (100000+) which falls outside this
  # container's 65536-uid user namespace and breaks newuidmap.
  security.loginDefs.settings = {
    SUB_UID_COUNT = 0;
    SUB_GID_COUNT = 0;
  };

  environment.systemPackages = with pkgs; [
    bashInteractive
    python3
    coreutils
    util-linux
    iproute2
    iputils
    openssh
    shadow
    less
    nano
    gawk
    gnugrep
    gnused
    procps
    # General-purpose diagnostic; useful for any peer attaching to
    # in-container processes (conmon, slurmstepd, the framework's
    # wrapper bash, secondary container PID 1) when the failure mode
    # is a signal arriving from an unexpected sender. Added 2026-05-12
    # for asm-dataset-nix's bilateral-SIGTERM investigation; kept
    # permanently since the diagnostic value generalizes — no test
    # harness should require an image rebuild to attach an strace.
    strace
  ];
}
