# SLURM Environment Guide

## Overview

This document details the findings from testing containerized workloads in the LMU SLURM cluster environment, including the specific challenges encountered and the solutions implemented.

## Network Architecture

### Gateway Access

The SLURM cluster uses a gateway architecture:
- **Gateway Host**: `lmu` (configured in SSH config as `remote.cip.ifi.lmu.de`)
- **Compute Nodes**: Internal cluster nodes (e.g., `bentonit.cip.ifi.lmu.de`, `essexit.cip.ifi.lmu.de`)

### SSH Connection Patterns

#### Gateway Connection
```bash
ssh lmu
```

#### Direct Compute Node Access (via Jump Host)
Compute nodes are NOT directly accessible from outside - they require the gateway as a jump host:

```bash
# This FAILS (no direct route):
ssh kruppb@bentonit.cip.ifi.lmu.de

# This WORKS (via jump host):
ssh -J lmu kruppb@bentonit.cip.ifi.lmu.de
```

#### Persistent SSH Connection (Recommended)
To avoid repeated connections and ensure all operations hit the same gateway node:

```bash
# Establish master connection
ssh -M -N -f -o ControlPath=/tmp/ssh-control -o ControlMaster=auto -o ControlPersist=yes lmu

# All subsequent commands reuse this connection:
ssh -o ControlPath=/tmp/ssh-control lmu 'command'
scp -o ControlPath=/tmp/ssh-control file.tar lmu:/path/

# Close master connection
ssh -O exit -o ControlPath=/tmp/ssh-control lmu
```

**Critical**: This is implemented in `gateway/ssh_gateway.py` to ensure all operations use a single connection to the same gateway node.

## Storage Architecture

### Network vs Local Storage

**Network Storage** (accessible from gateway and compute nodes):
- User home: `~/` or `/home/k/kruppb/`
- Shared directories: `~/BIG/`, `~/tmp/`
- SLURM job output must be written here

**Local Storage** (per-node, NOT visible to gateway):
- `/tmp/` on each compute node
- Faster for temporary operations
- NOT accessible from gateway or other nodes

### Best Practices

1. **Job Output Files**: MUST be on network storage
   ```bash
   # WRONG - won't be visible from gateway:
   sbatch --output=/tmp/job_%j.out script.sh
   
   # CORRECT - visible from gateway:
   sbatch --output=$HOME/tmp/job_%j.out script.sh
   ```

2. **Large File Transfers**: Copy to local `/tmp` first
   ```bash
   cp /home/k/kruppb/BIG/slurm-test/image_bin/image.tar /tmp/image-$SLURM_JOB_ID.tar
   # Use /tmp/image-$SLURM_JOB_ID.tar for operations
   ```

## Container Runtime Environment

### The Problem: Podman in SLURM Jobs

When running Podman inside SLURM jobs, the default configuration fails with:
```
Error: default OCI runtime "crun" not found: invalid argument
time="..." level=warning msg="RunRoot is pointing to a path (/run/user/24563/containers) which is not writable. Most likely podman will fail."
```

### Root Cause Analysis

The issue stems from differences between interactive SSH sessions and SLURM job environments:

| Aspect | SSH Session | SLURM Job |
|--------|-------------|-----------|
| `/run/user/{uid}/` | Created by systemd logind | Does NOT exist |
| `XDG_RUNTIME_DIR` | Set and accessible | Set but directory missing |
| Podman storage | Can use default locations | Fails to initialize |
| User session | Full systemd user session | No user session |

#### Key Finding via Testing

**SSH to compute node**:
```bash
ssh -J lmu kruppb@bentonit.cip.ifi.lmu.de
$ ls /run/user/24563/
# Directory exists and is accessible
$ podman run --rm hello-world
# Works perfectly!
```

**SLURM job on same node**:
```bash
srun --nodes=1 hostname  # Returns: bentonit.cip.ifi.lmu.de
# Inside job:
$ ls /run/user/24563/
# ls: cannot access '/run/user/24563': No such file or directory
$ podman run --rm hello-world
# Error: default OCI runtime "crun" not found
```

**Conclusion**: The `/run/user/{uid}/` directory is created by systemd's logind during interactive login but does NOT exist in SLURM job contexts.

### The Solution: Explicit Podman Configuration

Podman works in SLURM jobs when provided with explicit storage paths in writable locations:

```bash
#!/bin/bash
# SLURM job script

# Create temporary storage directories
JOB_TMP="/tmp/podman-job-$SLURM_JOB_ID"
PODMAN_STORAGE="$JOB_TMP/storage"
PODMAN_RUN="$JOB_TMP/run"

mkdir -p "$PODMAN_STORAGE" "$PODMAN_RUN"
chmod 700 "$PODMAN_STORAGE" "$PODMAN_RUN"

# Set runtime directory
export XDG_RUNTIME_DIR="$PODMAN_RUN"

# Use podman with explicit paths
podman --root "$PODMAN_STORAGE" \
       --runroot "$PODMAN_RUN" \
       --runtime /usr/bin/crun \
       run --rm hello-world
```

**Required Flags**:
- `--root`: Graph root for image storage (use `/tmp/...`)
- `--runroot`: Runtime root for container state (use `/tmp/...`)
- `--runtime`: Explicit path to OCI runtime (`/usr/bin/crun`)

### Verified Configuration (Job 71741)

This configuration was tested and confirmed working:
```
Hostname: bentonit.cip.ifi.lmu.de
XDG_RUNTIME_DIR: /tmp/podman-job-71741/run

Testing podman with explicit storage...
# Successfully showed podman info

Testing simple container...
# Successfully ran hello-world container
```

## Environment Variables in SLURM Jobs

From testing job 71738, SLURM jobs have these key environment variables:

```bash
SLURM_JOB_ID=71738
SLURM_JOBID=71738
SLURM_JOB_NODELIST=bentonit
SLURM_NODELIST=bentonit
HOME=/home/k/kruppb
USER=kruppb
TMPDIR=/tmp
XDG_RUNTIME_DIR=/run/user/24563  # Set but directory doesn't exist!
DBUS_SESSION_BUS_ADDRESS=unix:path=/run/user/24563/bus
```

**Important**: `XDG_RUNTIME_DIR` is SET but the directory does NOT EXIST. This causes Podman to fail unless overridden.

## Implementation in Code

### Gateway SSH Connection

File: `dynamic_batch/gateway/ssh_gateway.py`

Uses SSH ControlMaster to maintain a single persistent connection:
1. Establishes master connection at `connect()`
2. All subsequent operations reuse via `ControlPath`
3. Properly closes with `disconnect()`

### Podman Wrapper Scripts

File: `dynamic_batch/slurm/job_manager.py`

Generated wrapper scripts include:
```bash
# Setup Podman environment for SLURM
PODMAN_STORAGE="$RNDTMP/podman-storage"
PODMAN_RUN="$RNDTMP/podman-run"
mkdir -p "$PODMAN_STORAGE" "$PODMAN_RUN"
chmod 700 "$PODMAN_STORAGE" "$PODMAN_RUN"
export XDG_RUNTIME_DIR="$PODMAN_RUN"

# Load image with explicit paths
podman --root "$PODMAN_STORAGE" \
       --runroot "$PODMAN_RUN" \
       --runtime /usr/bin/crun \
       load < image.tar

# Run container with explicit paths
podman --root "$PODMAN_STORAGE" \
       --runroot "$PODMAN_RUN" \
       --runtime /usr/bin/crun \
       run --rm [options] image:tag
```

### Docker Packaging Module

File: `dynamic_batch/docker/docker_packaging.py`

Methods accept optional `storage_root` and `run_root` parameters:
- When provided: generates Podman commands with explicit paths
- When `None`: generates standard Docker commands

## Testing and Validation

### Test Job Submission

Create and submit test job:
```bash
ssh lmu << 'EOF'
mkdir -p ~/tmp
cat > ~/tmp/test.sh << 'JOBEOF'
#!/bin/bash
JOB_TMP="/tmp/podman-test-$SLURM_JOB_ID"
mkdir -p "$JOB_TMP/storage" "$JOB_TMP/run"
export XDG_RUNTIME_DIR="$JOB_TMP/run"

podman --root "$JOB_TMP/storage" \
       --runroot "$JOB_TMP/run" \
       --runtime /usr/bin/crun \
       run --rm hello-world

rm -rf "$JOB_TMP"
JOBEOF
chmod +x ~/tmp/test.sh
sbatch --partition=All --nodes=1 --output=$HOME/tmp/test_%j.out ~/tmp/test.sh
EOF
```

### Checking Job Output

```bash
# Find job ID
ssh lmu 'ls -lt ~/tmp/test_*.out | head -1'

# View output
ssh lmu 'cat ~/tmp/test_71741.out'
```

### Direct Compute Node Testing

To test directly on a compute node:
```bash
# Get available node from sinfo
ssh lmu 'sinfo -N -h -p All -t idle | head -1'

# SSH to node via jump host
ssh -J lmu kruppb@NODE.cip.ifi.lmu.de 'podman run --rm hello-world'
```

## Cluster-Specific Details (LMU)

### Partition Information
```bash
ssh lmu 'sinfo'
```

Available partitions:
- `All`: General partition (38 allocated, 93 idle nodes)
- `NvidiaAll`: Nodes with NVIDIA GPUs
- `AMD`: Nodes with AMD GPUs
- `Krater`: Specific node group
- `Abaki`: Specific node group

### Default Partition

The cluster requires explicit partition specification:
```bash
# FAILS:
srun hostname
# Error: No partition specified or system default partition

# WORKS:
srun --partition=All hostname
```

Always specify `--partition=All` (or appropriate partition) in sbatch/srun commands.

## Common Pitfalls

### ❌ Attempting Multiple SSH Connections
**Problem**: Each new SSH connection might go to a different gateway node.
**Solution**: Use persistent SSH connection with ControlMaster.

### ❌ Writing SLURM Output to /tmp
**Problem**: `/tmp` is local to each node, not visible from gateway.
**Solution**: Write to `~/tmp/` or other network-mounted path.

### ❌ Using Default Podman Configuration in SLURM
**Problem**: Podman tries to use `/run/user/{uid}/` which doesn't exist.
**Solution**: Provide explicit `--root`, `--runroot`, and `--runtime` flags.

### ❌ Forgetting to Specify Partition
**Problem**: Job submission fails with "No partition specified".
**Solution**: Always include `--partition=All` (or appropriate partition).

### ❌ Expecting SSH Keys to Work for Compute Nodes
**Problem**: Direct SSH to compute nodes requires password or different keys.
**Solution**: Use jump host (`-J lmu`) for compute node access.

## Summary

The key insights for working with containers in SLURM environments:

1. **Persistent Connections**: Use SSH ControlMaster to maintain a single gateway connection
2. **Storage Awareness**: Network storage for persistent data, local `/tmp` for speed
3. **Podman Configuration**: Explicit paths required in SLURM jobs
4. **Jump Host Access**: Compute nodes require gateway as jump host
5. **Partition Specification**: Always specify partition in job submissions

These solutions are implemented in the `dynamic_batch` SLURM integration and enable containerized distributed processing across the cluster.