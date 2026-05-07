# slurm-test-env

Local podman-backed slurm cluster for end-to-end testing.

> **Bring-up takes several minutes.** Keep the cluster up for the whole
> coding session — only run `down.sh` when you're done.

## Bring up

```sh
INSTANCE_ID=<pick-any-tag> ./deploy/up.sh
```

`INSTANCE_ID` scopes every host-visible resource so multiple clusters
can coexist; pick anything alphanumeric (e.g. `dev`, `ci-42`).

For a second concurrent instance also set `SSH_PORT=<unused-port>`.

## Add your user

```sh
INSTANCE_ID=<same-tag> ./scripts/provision-user.sh <username> <pubkey-file>
```

Re-run with additional `<pubkey-file>` paths to authorize more keys.

## Use it

It's a slurm cluster reachable via ssh:

```
ssh -p 2222 <username>@localhost
```

Shared `/home` is bind-mounted on every node; the host path is printed
by `up.sh`. Each container additionally has its own writable `/tmp`
(per-container, not shared).

## Tear down

```sh
INSTANCE_ID=<same-tag> ./deploy/down.sh
```

The simulated `/home` is preserved on the host for post-test inspection.
