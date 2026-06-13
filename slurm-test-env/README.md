# slurm-test-env

Local podman-backed slurm cluster (1 gateway + N workers) for
end-to-end testing.

> **Bring-up takes several minutes** (it builds two NixOS image
> tarballs). Keep the cluster up for the whole session; tear down with
> `down` when you're finished.

The API is the flake apps; `INSTANCE_ID` scopes every host-visible
resource (containers, network, image tags, state dir) so multiple
clusters can coexist. It is required and has no default — pick anything
matching `[a-zA-Z0-9_-]+`.

## Bring up

```sh
INSTANCE_ID=<tag> nix run .#up
```

For a second concurrent instance also set `SSH_PORT=<unused-port>`
(default `2222`). Other knobs (all overridable via env): `WORKER_COUNT`
(4), `WORKER_MEMORY` (4g), `WORKER_CPUS` (2), `GATEWAY_MEMORY` (1g),
`GATEWAY_CPUS` (2), `STATE_BASE_DIR`. See `deploy/env.sh`.

## Add a user

```sh
INSTANCE_ID=<tag> nix run .#provision-user -- <username> <pubkey-file>
```

Allocates a stable cluster-wide UID/GID and authorizes the pubkey on
every node. Username must match `[a-z_][a-z0-9_-]{0,31}`. Re-run with
more pubkey files to authorize additional keys; the pubkey is appended.

## Use it

It's a slurm cluster reachable over ssh; only the gateway publishes a
host port:

```sh
ssh -p <SSH_PORT> <username>@localhost
srun --partition=debug -N1 hostname
sinfo
```

Shared `/home` is bind-mounted on every node (host path printed by
`up`). Each container also has its own writable `/tmp` (per-container,
not shared).

## Smoke test

```sh
INSTANCE_ID=<tag> nix run .#smoke-test
```

Provisions a `testuser` (keypair persisted under the state dir) and
drives the control plane end-to-end (partitions, sbatch + srun attach,
multi-node distribution, inter-node networking). Exit 0 = all stages
pass; 70 = cluster not running.

## Reboot a node

Restart one node in place by its cluster-internal hostname (what
`sinfo` prints, e.g. `slurm-worker1`, `slurm-gateway`):

```sh
INSTANCE_ID=<tag> nix run .#reboot-node -- slurm-worker1
```

## Environment parity (vs. LMU Krater)

- **I9 — polkit**: LMU runs an active polkit whose stock
  `org.freedesktop.login1.set-self-linger` policy lets an unprivileged
  user run plain `loginctl enable-linger` for themselves; the node
  image matches (polkit active, default policy, no custom rules). To
  test the polkit-absent deny branch, take polkit down on one node at
  runtime. `systemctl mask` does not work here (the unit file is a
  read-only `/etc` symlink, which also shadows a `/run`-level mask), so
  the kill-switch is a `/run` drop-in that replaces `ExecStart` with
  `false` — named `zz-*` so it sorts after the image's nix-store
  `overrides.conf` drop-in (drop-ins apply in filename order, last
  writer wins):

  ```sh
  podman exec <container> /bin/sh -c '
    export PATH=/run/current-system/sw/bin
    mkdir -p /run/systemd/system/polkit.service.d
    printf "[Service]\nExecStart=\nExecStart=/run/current-system/sw/bin/false\n" \
      > /run/systemd/system/polkit.service.d/zz-deny.conf
    systemctl daemon-reload && systemctl stop polkit'
  # ... deny path: unprivileged `loginctl enable-linger` on that node
  #     now fails with "Access denied" (logind fails closed) ...
  podman exec <container> /bin/sh -c '
    export PATH=/run/current-system/sw/bin
    rm /run/systemd/system/polkit.service.d/zz-deny.conf
    rmdir /run/systemd/system/polkit.service.d
    systemctl daemon-reload && systemctl restart polkit'
  ```

## Reset

Clear run state to a fresh baseline **without** tearing the cluster
down (no slow image rebuild on the next run):

```sh
INSTANCE_ID=<tag> nix run .#reset
```

Empties the two bind-mounted scratch surfaces: every user's `/home`
contents (job output, logs, caches, framework state) are removed,
leaving only each user's home directory plus their SSH access
(`.ssh/`) and stable cluster-UID marker (`.cluster_uid`) — so the
user can still log in and keeps their UID, but starts from an empty
home, no re-provision needed. Each worker's `/tmp` is cleared too.
Safe to run whether the cluster is up or down.

## Tear down

```sh
INSTANCE_ID=<tag> nix run .#down
```

Removes all containers, the network, and instance-scoped image tags.
The simulated `/home` is preserved on the host for post-test
inspection and so user provisioning carries across runs.

---

The scripts also run directly (`bash deploy/up.sh`, etc.) when not
invoked through the flake; the apps just wrap them with the image
tarballs and `$PATH` baked in.
