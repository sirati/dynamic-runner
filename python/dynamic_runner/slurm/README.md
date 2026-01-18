# SLURM Distributed Processing Architecture

## Overview

This document describes the distributed processing system for binary tokenization using SLURM clusters. The architecture supports dynamic task distribution across multiple compute nodes with automatic failover, peer-to-peer communication, and efficient binary transfer mechanisms.

## Command Line Interface

### Basic Usage

```bash
python -m dynamic_batch --gateway <gateway> --slurm [options]
```

### Gateway Configuration

The `--gateway` parameter specifies where the SLURM manager process runs:

- **SSH Gateway**: `--gateway ssh://user@hostname:port` - Connect to a remote SLURM controller
- **Local Gateway**: `--gateway local` - Run directly on the SLURM controller

### SLURM Parameters

#### Required Parameters

- `--slurm`: Enable SLURM mode
- `--slurm-root-folder <path>`: Root directory for SLURM operations (suggested: `~/slurm` or `~/BIG/slurm`)
- `--packaging docker`: Specify packaging method (currently only Docker is supported)

#### Optional Parameters

- `--slurm-notify-email <email>`: Email address for SLURM job notifications
- `--slurm-image-subfolder <name>`: Subdirectory for Docker images (default: `image_bin`)
- `--slurm-output-subfolder <name>`: Subdirectory for output files (default: `out`)
- `--slurm-log-subfolder <name>`: Subdirectory for log files (default: `log`)

### Example

```bash
python -m dynamic_batch \
  --gateway ssh://user@cluster.example.com \
  --slurm \
  --packaging docker \
  --slurm-root-folder ~/BIG/slurm \
  --slurm-notify-email user@example.com
```

## Architecture Overview

### Component Structure

The system consists of three main components:

1. **Primary**: Runs on the local machine, orchestrates initial distribution
2. **Gateway Manager**: Runs on the SLURM controller, manages job submission
3. **Secondaries**: Run on SLURM compute nodes, execute processing tasks

### Directory Structure

```
<slurm-root-folder>/
├── image_bin/           # Docker images and source binaries
│   └── srcbins/         # Zipped source binaries
├── out/                 # Completed output files
└── log/                 # Worker log files
    └── worker_<S>_<W>.*.log
```

## Processing Workflow

### Phase 1: Initialization

1. **File Collection**: Primary collects list of binaries to process
2. **Gateway Connection**: Establish connection to SLURM controller
3. **Image Building**: Build Docker image using Nix on gateway
4. **Image Transfer**: Transfer Docker image to `<slurm-root-folder>/image_bin/`

### Phase 2: Job Submission

The SLURM job performs the following setup:

1. **Temporary Directory Creation**: Create random directory in `/tmp` (rndtmp)
2. **Volume Mapping**:
   - `rndtmp/src` → `/app/src-tmp` (temporary source files)
   - `rndtmp/out` → `/app/out-tmp` (temporary output files)
   - `rndtmp/log` → `/app/log-tmp` (temporary log files)
   - `<slurm-image-subfolder>/srcbins/` → `/app/src-network` (network source files)
   - `<slurm-output-subfolder>` → `/app/out-network` (network output files)
   - `<slurm-log-subfolder>` → `/app/log-network` (network log files)

3. **Socket Communication**: Create Unix named socket for container-to-host command execution
4. **Network Setup**: Expose port for incoming QUIC connections
5. **Secondary Start**: Launch Docker container with `--secondary {communication_mode}`

### Phase 3: Network Establishment

1. **Welcome**: Secondary sends capabilities (RAM, worker count) to primary
2. **Entropy Exchange**: Primary sends entropy; secondary generates QUIC certificates
3. **Certificate Exchange**: Secondary sends public certificate and IP addresses (IPv4/IPv6)
4. **Peer Discovery**: Primary relays connection information to all secondaries
5. **QUIC Mesh**: Secondaries establish authenticated peer-to-peer QUIC connections
6. **Worker Ready**: Each secondary reports worker readiness with memory budgets

### Phase 4: Initial Distribution

1. **Wait for Workers**: Primary waits for all secondary workers to report ready
2. **Preliminary Assignment**: Primary assigns initial tasks per secondary based on memory estimates
3. **Source Discovery**: First secondary scans `/app/src-network` for ZIP files:
   - Opens ZIPs matching `.hash` files
   - Extracts binary metadata and hashes
   - Sends `(zip_name, local_path, binary_info, hash)` tuples to primary

4. **Intelligent Zipping**: For each secondary's assigned files:
   - Check if hash matches first secondary's discovered binaries
   - Mark matching binaries as "already sent" (skip in ZIP)
   - Stream non-duplicate files into `srcbins/{unique_name}_{random}.zip`
   - Send initial assignment with ZIP locations to secondary

5. **Extraction**: Secondary extracts from `/app/src-network` to `/app/src-tmp`
6. **Worker Assignment**: Secondary assigns tasks to workers with paths:
   - Source: `/app/src-tmp/<file>`
   - Output: `/app/out-tmp/<file>`
   - Log: `/app/log-tmp/worker_{S}_{W}.0.log` (S=secondary ID, W=worker ID)

### Phase 5: Continuous Distribution

1. **Progress Tracking**: All secondaries maintain peer-to-peer task completion updates
2. **Keepalive Protocol**: 
   - Secondaries send keepalive every 1 second
   - Timeout threshold: 2 minutes
   - On timeout: query peers for last keepalive
   - Mark dead if all peers report >1 minute staleness

3. **Batched Transfer**: Primary creates ZIPs of pending binaries:
   - Target size: 20MB minimum
   - No compression (storage-only ZIP)
   - Skip already-sent binaries (including first secondary's duplicates)
   - Single-binary ZIPs for files >20MB

4. **Dynamic Assignment**: When workers complete early:
   - Secondary requests new task from primary
   - Primary assigns next batch

5. **Transfer Completion**: When all files sent:
   - Primary notifies all secondaries
   - Primary promotes random secondary to "SLURM-primary"
   - Primary sends complete task list to all secondaries
   - Primary prints "Safe to close with Ctrl+C"

### Phase 6: Autonomous Operation

After primary disconnection, secondaries operate autonomously:

1. **SLURM-Primary Role**:
   - Assigns tasks to other secondaries
   - Does NOT track overall progress (peer-to-peer responsibility)
   - Timeout: 30 seconds

2. **Failover Protocol**:
   - Any secondary detecting SLURM-primary timeout notifies all peers
   - Peers relay notification and send confirmations
   - Relayed messages also generate confirmations
   - After confirmation convergence: secondary without timeouts becomes new SLURM-primary
   - Workers idle during election process

3. **Task Completion**:
   - Worker finishes: move files from `/app/out-tmp` to `/app/out-network`
   - Log rotation: if ≥1 minute elapsed since last increment:
     - Send increment command to worker
     - Worker switches to new log file: `worker_{S}_{W}.{N+1}.log`
     - Move old log from `/app/log-tmp` to `/app/log-network`
   - On error/crash/OOM: always increment and move log

## File Naming Conventions

### Worker Logs

```
worker_{secondary_id}_{worker_id}.{increment}.log
```

- **secondary_id**: Unique identifier for SLURM node
- **worker_id**: Worker number within secondary
- **increment**: Starts at 0, incremented on rotation or failure

### Source Binary ZIPs

```
srcbins/{descriptive_name}_{random_suffix}.zip
```

- Stored in `<slurm-root-folder>/<slurm-image-subfolder>/srcbins/`
- Uncompressed (store-only) for fast extraction

## Network Protocol

### QUIC Communication

- **Authentication**: Mutual certificate verification using exchanged public certificates
- **Transport**: IPv4 and IPv6 support
- **Purpose**: Peer-to-peer task status updates and keepalive messages

### Message Types

1. **secondary-welcome**: Capabilities announcement (RAM, workers)
2. **entropy**: Certificate generation seed
3. **cert-exchange**: Public certificate and IP addresses
4. **peer-info**: Relayed connection information
5. **task-complete**: Binary processing completion
6. **task-failed**: Binary processing failure
7. **keepalive**: Liveness signal (1 second interval)
8. **timeout-detected**: Peer timeout notification
9. **promotion**: New SLURM-primary election
10. **assignment**: Task assignment from SLURM-primary

## Container-Host Communication

The SLURM wrapper script creates a Unix named socket (in `/tmp`) for the containerized secondary to request host command execution. This enables:

- File system operations outside container
- Network diagnostics
- Resource monitoring

Communication protocol:
1. Secondary writes command to socket
2. Host wrapper executes command
3. Host relays output back through socket
4. All sockets reside in `/tmp` for ephemeral lifecycle

## Failure Handling

### Secondary Failure

- Detected via keepalive timeout (2 minutes)
- Cross-validated with peer reports (>1 minute consensus)
- Tasks redistributed by SLURM-primary
- Logs preserved in `/app/log-network`

### SLURM-Primary Failure

- Detected via 30-second timeout
- Democratic election protocol with confirmation convergence
- No task loss (all secondaries maintain full task state)
- Brief idle period during election

### Worker Failure

- Crash/OOM detected by secondary
- Log immediately rotated and moved to network storage
- Task marked as failed
- Secondary requests new task from SLURM-primary

## Performance Considerations

### Memory Management

- Initial assignments respect per-secondary memory budgets
- Workers report available memory after each task
- SLURM-primary assigns based on current availability

### Network Efficiency

- First secondary's discovered binaries avoid duplicate transfers
- 20MB batching reduces network overhead
- Uncompressed ZIPs enable fast extraction
- Peer-to-peer status updates reduce primary bottleneck

### Storage Strategy

- Temporary files in `/tmp` (fast local storage)
- Network-shared storage only for completed work
- Log rotation minimizes network I/O during processing
- Incremental log movement (≥1 minute interval)

## Implementation Details

### Code Organization

```
dynamic_batch/
├── docker/          # Docker image building and management
├── slurm/           # SLURM job submission and wrapper scripts
├── gateway/         # SSH gateway and local gateway implementations
└── slurm.md         # This documentation
```

### Extension Points

Future packaging methods can be added by implementing the packaging interface:
- `build_image()`: Create deployable image
- `transfer_image()`: Move image to gateway
- `get_run_command()`: Generate container invocation

## Troubleshooting

### Common Issues

1. **Gateway connection fails**: Verify SSH credentials and SLURM controller access
2. **Image build fails**: Check Nix installation and flake configuration on gateway
3. **Secondaries not connecting**: Verify firewall rules for QUIC port
4. **Timeout storms**: Check network latency and increase timeout thresholds
5. **Log files missing**: Verify write permissions to `<slurm-log-subfolder>`

### Debug Mode

Enable verbose logging with additional flags (to be implemented):
- `--debug-network`: Log all QUIC messages
- `--debug-tasks`: Log task assignment decisions
- `--debug-keepalive`: Log all keepalive exchanges