# SLURM Distributed Processing - Implementation Status

## Overview

This document tracks the implementation status of the SLURM distributed processing feature for the `dynamic_batch` system.

## ✅ Completed Components

### 1. Documentation
- **File**: `slurm.md`
- Comprehensive documentation of architecture, workflow, and protocols
- Command-line interface specification
- Detailed phase descriptions
- Troubleshooting guide

### 2. Gateway Infrastructure
- **Directory**: `gateway/`
- **Files**:
  - `__init__.py` - Gateway interface and factory
  - `local_gateway.py` - Local SLURM controller implementation
  - `ssh_gateway.py` - SSH-based remote gateway implementation
- Gateway URL parsing (`local`, `ssh://user@host:port`)
- Command execution, file transfer, directory management

### 3. Docker Packaging
- **Directory**: `docker/`
- **Files**:
  - `__init__.py` - Packaging interface and factory
  - `docker_packaging.py` - Docker/Nix-based packaging
- Docker image building using Nix on gateway
- Image transfer and loading commands
- Container run command generation with volume mounts and ports

### 4. SLURM Job Management
- **Directory**: `slurm/`
- **Files**:
  - `__init__.py` - SLURM configuration dataclass
  - `job_manager.py` - Job submission and lifecycle management
  - `protocol.py` - Complete network protocol message definitions
- Directory structure management (`image_bin/`, `out/`, `log/`, `srcbins/`)
- SLURM job submission with sbatch
- Wrapper script generation with volume mappings
- Job status monitoring and cancellation

### 5. Network Protocol
- **File**: `slurm/protocol.py`
- **Message Types** (18 total):
  - Primary ↔ Secondary: WELCOME, ENTROPY, CERT_EXCHANGE, PEER_INFO, etc.
  - Secondary ↔ Secondary: TASK_COMPLETE, TASK_FAILED, KEEPALIVE, etc.
  - Host ↔ Container: EXECUTE_COMMAND, COMMAND_RESULT
- JSON serialization/deserialization
- Type-safe message classes with dataclasses

### 6. Command-Line Interface
- **File**: `__main__.py` (updated)
- Added arguments:
  - `--secondary <url>` - Run in secondary mode
  - `--gateway <url>` - Gateway specification
  - `--slurm` - Enable SLURM mode
  - `--packaging docker` - Packaging method
  - `--slurm-root-folder` - Root directory on gateway
  - `--slurm-notify-email` - Email notifications
  - `--slurm-image-subfolder` - Image subdirectory (default: image_bin)
  - `--slurm-output-subfolder` - Output subdirectory (default: out)
  - `--slurm-log-subfolder` - Log subdirectory (default: log)
- Argument validation
- Mode routing (local, SLURM primary, secondary)

### 7. Secondary Mode Structure
- **File**: `slurm/secondary_mode.py`
- Secondary node execution framework
- Phase structure:
  1. Connect to primary
  2. Send welcome with capabilities
  3. Certificate exchange
  4. Connect to peers via QUIC
  5. Start workers
  6. Main processing loop
- Keepalive management (1 second interval)
- Timeout detection (2 minute threshold)
- Worker lifecycle hooks
- File movement (tmp → network storage)
- Log rotation logic

### 8. Primary Coordinator Structure
- **File**: `slurm/coordinator.py`
- Primary orchestration framework
- Phase structure:
  1. Submit SLURM jobs
  2. Wait for secondaries
  3. Certificate exchange
  4. Wait for workers
  5. Preliminary assignment
  6. Source discovery
  7. File distribution
  8. Transfer complete notification
  9. SLURM-primary promotion
  10. Full task list distribution
  11. Monitor mode
- Task assignment tracking
- Task hash computation
- Completion/failure handling

### 9. Nix Flake Integration
- **File**: `flake.nix` (updated)
- Separated deployment vs development packages
- Docker-specific packages (bash, coreutils)
- Dynamic `.gitignore` filtering using `gitignore.nix`
- Docker image generation with proper entrypoint
- Source file inclusion in container

## 🚧 Not Yet Implemented (TODOs in Code)

### 1. QUIC Communication Layer
- Actual QUIC connection establishment
- Certificate generation with entropy mixing
- Authenticated peer-to-peer connections
- Message send/receive over QUIC
- Connection pooling and management

### 2. Message Handling
- Primary listening for secondary connections
- Message dispatch and routing
- Request-response correlation
- Broadcast to multiple peers
- Message queue management

### 3. File Operations
#### Source Discovery (Phase 6)
- First secondary scanning `/app/src-network`
- ZIP file opening and hash verification
- Binary metadata extraction
- Hash reporting to primary

#### File Distribution (Phase 7)
- Duplicate detection using first secondary's hashes
- Streaming ZIP creation (uncompressed)
- 20MB batching logic
- ZIP transfer to `srcbins/` directory
- Assignment message with ZIP locations

#### File Extraction (Secondary)
- ZIP extraction from `/app/src-network` to `/app/src-tmp`
- Worker assignment with proper paths
- Completed file movement (tmp → network)

### 4. Worker Integration
- Worker process creation in secondary mode
- Task assignment to workers
- Progress monitoring
- Memory budget tracking
- Worker restart on completion (if configured)

### 5. Log Management
- Log file naming: `worker_{S}_{W}.{N}.log`
- Time-based rotation (≥1 minute interval)
- Error/crash-triggered rotation
- Log movement from tmp to network storage

### 6. Failover and Election
- Timeout consensus protocol
- SLURM-primary election algorithm
- Confirmation convergence
- Task redistribution on node failure
- Worker idle during election

### 7. Unix Socket Communication
- Host-side command relay service
- Container-to-host command execution
- Socket protocol implementation
- Result relaying back to container

### 8. Project Synchronization
- Project source transfer to gateway
- Selective file synchronization
- Version tracking

### 9. Testing Infrastructure
- Unit tests for protocol messages
- Gateway implementation tests
- Integration tests for coordinator
- Mock SLURM environment for testing
- End-to-end workflow tests

## 📊 Recent Progress (2026-01-18)

### ✅ Latest Achievement: Persistent SSH Connection
**Successfully implemented SSH ControlMaster for connection reuse**:
- Single SSH master connection established at start
- All subsequent commands (execute, scp, etc.) reuse the same connection via ControlPath
- Properly closes connection and cleans up control socket on exit
- Eliminates connection overhead for repeated operations
- Ensures all operations go to the same gateway node (critical for SLURM clusters)
- Debug logging shows connection lifecycle clearly

### ✅ Completed Today
1. **Persistent SSH Connection** - Implemented ControlMaster for single reusable connection across all operations
2. **Docker Image Build & Transfer** - Successfully builds locally with Nix and transfers to gateway via SCP
3. **Gateway Integration** - SSH gateway connects, creates directories, transfers 367MB image in ~26 seconds
4. **Path Expansion** - Properly expands `~` paths for remote execution using detected remote home
5. **SLURM Job Submission** - Successfully submits test jobs with proper wrapper scripts
6. **Test Mode** - Added `--slurm-test-job` flag to validate Docker image loading on compute nodes
7. **Debug Logging** - Added `--debug` flag for comprehensive logging throughout the pipeline

### 🚧 Current Blocker
**Container Runtime on Compute Nodes**: SLURM compute nodes have Podman in rootless mode, but:
- `/run/user/{uid}/containers` is not available/writable
- Default OCI runtime "crun" not found
- Error code 125 from Podman

**Possible Solutions**:
1. Use Singularity/Apptainer instead of Docker (common on HPC clusters)
2. Configure Podman with alternative storage/runtime directories
3. Pre-configure compute node environment via SLURM prolog scripts
4. Use system-wide container runtime if available

**Test Results**:
- Job 71732: Image copied to /tmp successfully (367MB)
- Docker load command fails with runtime error
- Need to investigate cluster's container capabilities

## 📋 Implementation Priority

### High Priority (Core Functionality)
1. **Container Runtime Setup** - BLOCKER: Fix Podman/Docker runtime on compute nodes or switch to Singularity
2. **QUIC Communication** - Required for all distributed operations
3. **File Distribution** - Core data transfer mechanism
4. **Worker Integration** - Actual task execution
5. **Message Handling** - Enable coordinator-secondary communication

### Medium Priority (Robustness)
6. **Failover Protocol** - Ensure reliability
7. **Log Management** - Proper debugging and monitoring
8. **Unix Socket Commands** - Container-host interaction

### Low Priority (Polish)
9. **Testing Infrastructure** - Quality assurance
10. **Performance Optimization** - Tuning and benchmarking
11. **Monitoring Dashboard** - Real-time status visibility

## 🧪 Testing Strategy

### Unit Tests
- Protocol message serialization
- Gateway operations
- Job script generation
- Hash computation

### Integration Tests
- Gateway + packaging workflow
- Primary + single secondary
- Multiple secondaries coordination
- Failover scenarios

### System Tests
- Full SLURM cluster deployment
- Large-scale file distribution
- Network partition handling
- Resource exhaustion scenarios

## 📊 Estimated Completion

- **Documentation & Architecture**: 100% ✅
- **Infrastructure & CLI**: 100% ✅
- **Build & Transfer Pipeline**: 90% ✅ (works locally, blocked on compute nodes)
- **Core Components**: 40% 🚧
- **QUIC & Networking**: 0% ⏸️
- **File Operations**: 10% 🚧
- **Failover & Robustness**: 0% ⏸️
- **Testing**: 5% 🚧 (test job submission works, runtime blocked)

**Overall Progress**: ~40% complete

## 🚀 Next Steps

### Immediate (Unblock Progress)
1. **Investigate container runtime on LMU cluster**:
   - Check if Singularity/Apptainer is available
   - Test if system-wide Docker/Podman works
   - Check cluster documentation for container best practices
2. **Alternative: Implement Singularity packaging** alongside Docker
3. **Test container execution** on actual compute node with working runtime

### After Unblocking
4. Implement QUIC communication layer (use `aioquic` or similar)
5. Complete message handling in coordinator and secondary
6. Implement file distribution with deduplication
7. Integrate with existing worker manager
8. Add comprehensive error handling
9. Develop testing infrastructure
10. Performance profiling and optimization
11. Production deployment testing

## 📝 Notes

- All message types are defined and type-safe
- Gateway abstraction allows easy addition of new transport methods
- Packaging interface supports future container runtimes (Podman, Singularity)
- Protocol designed for extensibility (easy to add new message types)
- Clean separation between coordinator, gateway, and secondary concerns
- Persistent SSH connection ensures reliability and consistency

### Working Components (Tested)
- Persistent SSH ControlMaster connection: ✅ (single connection reused for all operations)
- Nix builds Docker image locally: ✅ (367MB, ~30 seconds)
- SSH gateway connection: ✅ (auto-detects remote home)
- Directory creation on gateway: ✅ (all operations via same connection)
- SCP file transfer: ✅ (367MB in ~26 seconds, via persistent connection)
- SLURM job submission: ✅ (via persistent connection)
- Path expansion (~/ → /home/k/kruppb): ✅
- Image copy to /tmp on compute node: ✅
- Debug logging throughout: ✅ (use --debug flag)

### Known Issues
- Podman rootless mode on compute nodes requires runtime directories that don't exist
- Need to investigate cluster-specific container configuration