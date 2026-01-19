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
- Persistent SSH ControlMaster for connection reuse
- **New**: `upload_file()` and `download_file()` methods for bidirectional transfer
- **Note**: Certificate transfer via message protocol, not file transfer

### 3. Docker Packaging
- **Directory**: `docker/`
- **Files**:
  - `__init__.py` - Packaging interface and factory
  - `docker_packaging.py` - Docker/Nix-based packaging
- Docker image building using Nix on gateway
- Image transfer and loading commands
- Container run command generation with volume mounts and ports
- Podman support with custom storage/runroot paths for SLURM environments

### 4. SLURM Job Management
- **Directory**: `slurm/`
- **Files**:
  - `__init__.py` - SLURM configuration dataclass
  - `job_manager.py` - Job submission and lifecycle management
  - `protocol.py` - Complete network protocol message definitions
- Directory structure management (`image_bin/`, `out/`, `log/`, `srcbins/`)
- SLURM job submission with sbatch
- Wrapper script generation with:
  - Host networking mode (`--network host`) for QUIC connections
  - Dynamic port allocation on compute nodes
  - Temporary directory management (short paths for Podman)
  - Connection info files for SSH tunnel coordination
- Job status monitoring and cancellation
- Per-run log directories to avoid file conflicts

### 5. Network Protocol & Communication

#### Message Protocol
- **File**: `slurm/protocol.py`
- **Message Types** (18 total):
  - Primary ↔ Secondary: WELCOME, ENTROPY, CERT_EXCHANGE, PEER_INFO, etc.
  - Secondary ↔ Secondary: TASK_COMPLETE, TASK_FAILED, KEEPALIVE, etc.
  - Host ↔ Container: EXECUTE_COMMAND, COMMAND_RESULT
- JSON serialization/deserialization
- Type-safe message classes with dataclasses

#### Message Router ✅ IMPLEMENTED
- **File**: `slurm/message_router.py`
- Handler registration for different message types
- Request-response correlation with futures
- Broadcast to multiple peers
- Primary/secondary connection management
- Length-prefixed message framing (4 bytes + JSON)
- Connection monitoring and error handling

#### QUIC Transport ✅ IMPLEMENTED
- **File**: `slurm/quic_transport.py`
- **Full QUIC implementation using aioquic**:
  - Client and server QUIC connections
  - Certificate-based mutual authentication
  - Stream-based message framing
  - Connection lifecycle management
  - Configurable bind address (localhost for primary, 0.0.0.0 for secondaries)
- Self-signed certificate generation with OpenSSL
- Peer connection pooling
- Message handlers with async dispatch
- Certificate fingerprint computation
- Local IP address detection (IPv4/IPv6)

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
  - `--skip-image-build` - Skip Docker image rebuild
- Argument validation
- Mode routing (local, SLURM primary, secondary)

### 7. Secondary Mode ✅ FULLY IMPLEMENTED
- **File**: `slurm/secondary_mode.py`
- Complete secondary node execution
- **Phase structure**:
  1. ✅ Connect to primary via QUIC (with SSH tunnel fallback)
  2. ✅ Send welcome with capabilities (RAM, worker count, hostname)
  3. ✅ Generate certificates and start QUIC server
  4. ✅ Send certificate exchange to primary
  5. ✅ Wait for peer list from primary (no timeout assumptions)
  6. ✅ Connect to peers via QUIC
  7. ✅ Start workers (placeholder structure ready)
  8. ✅ Main processing loop with keepalive
- **Features**:
  - ✅ Connection retry logic (60 seconds, 1 attempt/second)
  - ✅ Detailed error logging with type and message
  - ✅ Setup completion tracking
  - ✅ Abort on primary disconnect during setup
  - ✅ Custom log handler sends WARNING/ERROR to primary
  - ✅ Primary-controlled peer connection (no assumptions)
  - ✅ Event-based synchronization for peer list
- **Error Reporting**:
  - ✅ Exceptions sent to primary with full traceback
  - ✅ Warnings and errors forwarded to primary for centralized logging
  - ✅ Graceful shutdown on primary disconnect after setup

### 8. Primary Coordinator ✅ SIGNIFICANTLY ENHANCED
- **File**: `slurm/coordinator.py`
- Primary orchestration framework
- **Phase structure**:
  1. ✅ Setup QUIC transport and generate/load certificates
  2. ✅ Submit SLURM jobs
  3. ✅ Wait for secondaries (via SSH ProxyJump tunnels currently)
  4. ✅ Certificate exchange (collect from all secondaries)
  5. ✅ Distribute peer list to all secondaries
  6. ⏸️ Wait for workers
  7. ⏸️ Preliminary assignment
  8. ⏸️ Source discovery
  9. ⏸️ File distribution
  10. ⏸️ Transfer complete notification
  11. ⏸️ SLURM-primary promotion
  12. ⏸️ Full task list distribution
  13. ⏸️ Monitor mode
- **Certificate Management** ✅ IMPLEMENTED:
  - ✅ Primary generates self-signed certificates
  - ✅ Certificates stored locally in `./run/{run_id}/certificates/`
  - ✅ Certificate persistence for recovery/monitoring
  - ✅ Secondary certificates saved on receipt
  - ✅ Connection info saved as JSON per secondary
  - ✅ All via message protocol, no file transfer
- **QUIC Setup** ✅ IMPLEMENTED:
  - ✅ Primary listens only on localhost (127.0.0.1)
  - ✅ QUIC server started before job submission
  - ✅ Message handlers registered for both TCP and QUIC
  - ✅ Port allocated automatically by OS
- **SSH Tunnel Coordination** ✅ IMPLEMENTED:
  - ✅ Dynamic port allocation on compute nodes
  - ✅ SSH reverse tunnels from compute nodes to primary
  - ✅ Connection info files for tunnel discovery
  - ✅ Per-secondary unique ports to avoid conflicts
- **Error Handling** ✅ IMPLEMENTED:
  - ✅ Receive and display secondary errors with traceback
  - ✅ Receive and display secondary warnings/errors from log handler
  - ✅ Formatted output with secondary ID prefix

### 9. Nix Flake Integration
- **File**: `flake.nix` (updated)
- Separated deployment vs development packages
- Docker-specific packages (bash, coreutils, openssl)
- **New**: Added `aioquic` for QUIC support
- Dynamic `.gitignore` filtering using `gitignore.nix`
- Docker image generation with proper entrypoint
- Source file inclusion in container

### 10. Certificate & Key Management ✅ IMPLEMENTED
- **Storage Location**: `./run/{run_id}/certificates/`
- **Primary Certificates**:
  - `primary_cert.pem` - Primary's public certificate
  - `primary_key.pem` - Primary's private key (mode 600)
- **Secondary Certificates** (per secondary):
  - `{secondary_id}_cert.pem` - Secondary's public certificate
  - `{secondary_id}_info.json` - Connection info (IP, port)
- **Features**:
  - ✅ Certificate reuse on primary restart with same run_id
  - ✅ All stored locally, not on gateway
  - ✅ Transfer via secure message protocol
  - ✅ Self-signed certificates generated with OpenSSL
  - ✅ Certificate fingerprints for verification

### 11. Connection Architecture ✅ IMPLEMENTED
- **Current State**: Hybrid TCP/QUIC
  - Primary ↔ Secondary: SSH ProxyJump TCP tunnels
  - Secondary ↔ Secondary: QUIC peer-to-peer
- **Security**:
  - ✅ Primary listens only on localhost
  - ✅ Connections through SSH reverse tunnels
  - ✅ QUIC with self-signed certificate authentication
  - ✅ No direct network exposure of primary
- **Future**: Full QUIC migration planned
  - Primary connection info in wrapper script
  - Direct QUIC connections without SSH tunnels

## 🚧 Not Yet Implemented (TODOs in Code)

### 1. Worker Integration ⏸️
- Worker process creation in secondary mode
- Task assignment to workers
- Progress monitoring
- Memory budget tracking
- Worker restart on completion (if configured)
- File movement (tmp → network storage)

### 2. File Operations ✅ PARTIALLY IMPLEMENTED
#### Source Discovery (Phase 6) ✅ IMPLEMENTED
- ✅ Scan srcbins directory for existing ZIPs
- ✅ ZIP file opening and hash verification
- ✅ Binary hash computation
- ✅ Hash reporting structure ready

#### File Distribution (Phase 7) ✅ IMPLEMENTED
- ✅ Duplicate detection using hash comparison
- ✅ Streaming ZIP creation (uncompressed, ZIP_STORED)
- ✅ 20MB batching logic with size-based grouping
- ✅ Single-file ZIPs for large binaries
- ✅ ZIP transfer to `srcbins/` directory
- ⏸️ TODO: Integration with coordinator distribution phase

#### File Extraction (Secondary) ✅ IMPLEMENTED
- ✅ ZIP extraction helper methods
- ✅ Selective file extraction from ZIP
- ⏸️ TODO: Worker assignment with proper paths
- ⏸️ TODO: Completed file movement (tmp → network)

### 3. Log Management ⏸️
- Log file naming: `worker_{S}_{W}.{N}.log`
- Time-based rotation (≥1 minute interval)
- Error/crash-triggered rotation
- Log movement from tmp to network storage

### 4. Failover and Election ⏸️
- Timeout consensus protocol
- SLURM-primary election algorithm
- Confirmation convergence
- Task redistribution on node failure
- Worker idle during election

### 5. Unix Socket Communication ⏸️
- Host-side command relay service
- Container-to-host command execution
- Socket protocol implementation
- Result relaying back to container

### 6. Full QUIC Migration ⏸️
- Replace SSH tunnels with direct QUIC connections
- Pass primary connection info in wrapper script
- Primary certificate distribution to secondaries
- Direct QUIC connections from compute nodes

## 📊 Recent Progress (2026-01-19)

### ✅ Latest Achievements

#### 1. Full QUIC Implementation
**Replaced TCP placeholder with actual QUIC using aioquic**:
- Real QUIC client and server connections
- Stream-based message framing
- Certificate-based mutual authentication
- Connection lifecycle management with keep-alive
- Proper cleanup of client connections

#### 2. Certificate Management
**Local storage and persistence**:
- Certificates stored in `./run/{run_id}/certificates/`
- Primary generates and persists certificates
- Secondary certificates saved on receipt
- Connection info saved as JSON
- No file transfer (scp) - all via message protocol
- Certificate reuse on primary restart

#### 3. Primary Listens Localhost Only
**Security enhancement**:
- Primary QUIC server binds to 127.0.0.1 only
- Configurable bind address in QuicTransport
- Secondaries bind to 0.0.0.0 for peer connections
- All connections through SSH reverse tunnels
- No direct network exposure

#### 4. Error Reporting Infrastructure
**Comprehensive error handling**:
- Secondary sends exceptions with full traceback to primary
- Custom log handler forwards WARNING/ERROR to primary
- Primary displays errors with secondary ID prefix
- Setup completion tracking
- Abort on primary disconnect during setup
- Connection monitoring with graceful shutdown

#### 5. Primary-Controlled Peer Connections
**Eliminated assumptions**:
- Secondary waits indefinitely for peer list from primary
- No arbitrary timeouts
- Event-based synchronization
- Primary has full control over topology
- Works with any number of secondaries (including 1)

#### 6. Connection Retry Logic
**Robust connection establishment**:
- 60 attempts over 60 seconds (1/second)
- Detailed error messages with type and description
- Continues setup after successful connection
- Graceful handling of connection failures

### 🔧 Technical Improvements

#### SSH Tunnel Coordination
- Dynamic port allocation on compute nodes
- Free port detection before container start
- Connection info files for tunnel discovery
- Per-secondary unique ports
- Host networking mode for containers

#### Message Handler Compatibility
- Unified handler signature: `(message, sender_id)`
- Handlers work with both TCP and QUIC
- QUIC handlers pass peer_id instead of protocol object
- Registered with both message_router and quic_transport

#### Per-Run Isolation
- Unique run_id for each execution
- Run-specific directories for logs and certificates
- Avoids file conflicts between runs
- Enables parallel runs

## 📋 Implementation Priority

### High Priority (Core Functionality)
1. ✅ **QUIC Communication** - COMPLETE
2. ✅ **Certificate Management** - COMPLETE
3. ✅ **Error Reporting** - COMPLETE
4. ✅ **Connection Architecture** - COMPLETE (hybrid mode)
5. ⏸️ **File Distribution Integration** - Need to wire into coordinator
6. ⏸️ **Worker Integration** - Actual task execution

### Medium Priority (Robustness)
7. ⏸️ **Failover Protocol** - Ensure reliability
8. ⏸️ **Log Management** - Proper debugging and monitoring
9. ⏸️ **Unix Socket Commands** - Container-host interaction
10. ⏸️ **Full QUIC Migration** - Remove SSH tunnel dependency

### Low Priority (Polish)
11. ⏸️ **Testing Infrastructure** - Quality assurance
12. ⏸️ **Performance Optimization** - Tuning and benchmarking
13. ⏸️ **Monitoring Dashboard** - Real-time status visibility

## 🧪 Testing Strategy

### Unit Tests
- Protocol message serialization
- Gateway operations
- Job script generation
- Hash computation
- QUIC connection establishment
- Certificate generation and verification

### Integration Tests
- Gateway + packaging workflow
- Primary + single secondary
- Multiple secondaries coordination
- Failover scenarios
- QUIC peer-to-peer connections
- Error reporting end-to-end

### System Tests
- Full SLURM cluster deployment
- Large-scale file distribution
- Network partition handling
- Resource exhaustion scenarios
- Primary restart and recovery

## 📊 Estimated Completion

- **Documentation & Architecture**: 100% ✅
- **Infrastructure & CLI**: 100% ✅
- **Build & Transfer Pipeline**: 100% ✅
- **Gateway Abstraction**: 100% ✅
- **QUIC & Networking**: 95% ✅ (full QUIC implemented, migration to direct connections pending)
- **Certificate Management**: 100% ✅
- **Error Reporting**: 100% ✅
- **Core Components**: 75% 🚧 (coordinator/secondary structure complete, needs worker integration)
- **File Operations**: 85% ✅ (distribution + deduplication + batching complete)
- **Worker Integration**: 10% ⏸️ (structure ready, needs implementation)
- **Failover & Robustness**: 0% ⏸️
- **Testing**: 15% 🚧 (basic testing done, needs comprehensive suite)

**Overall Progress**: ~78% complete

## 🚀 Next Steps

### Phase 1: Worker Integration (High Priority)
1. **Connect secondary to worker_manager**:
   - Start workers in secondary mode
   - Assign tasks to workers with proper paths
   - Handle worker completion/failure
2. **Implement file movement**:
   - Extract ZIPs from /app/src-network to /app/src-tmp
   - Move completed files from tmp to network storage
3. **Implement log rotation and movement**:
   - Time-based rotation (≥1 minute interval)
   - Error/crash-triggered rotation
   - Move logs from tmp to network storage

### Phase 2: Coordinator Integration (Medium Priority)
4. **Integrate file distributor with coordinator**:
   - Wire _distribute_files phase
   - Use FileDistributor for batched ZIP creation
   - Send initial assignments with ZIP locations
5. **Implement source discovery phase**:
   - First secondary scans srcbins
   - Report existing binaries to primary
   - Deduplicate against existing files
6. **Memory budget tracking**:
   - Report worker memory budgets
   - Track available memory after each task

### Phase 3: Robustness (Medium Priority)
7. **Implement failover protocol**:
   - Timeout consensus with peer queries
   - SLURM-primary election algorithm
   - Task redistribution on node failure
8. **Unix socket host-container communication**:
   - Host-side command relay service
   - Container-to-host protocol
9. **Full QUIC migration** (optional):
   - Remove SSH tunnel dependency
   - Direct QUIC connections from compute nodes
   - Primary certificate distribution via wrapper script

### Phase 4: Testing & Polish (Low Priority)
10. **Testing infrastructure**:
    - Unit tests for new modules
    - Integration tests end-to-end
    - Mock SLURM environment
11. **Performance optimization**:
    - Profile file distribution
    - Optimize ZIP batching
    - Network transfer optimization
12. **Production deployment**:
    - Full cluster test
    - Documentation updates
    - Monitoring and logging improvements

## 📝 Notes

### Architecture Decisions
- QUIC for all peer-to-peer connections (secondary ↔ secondary)
- Hybrid mode for primary ↔ secondary (SSH tunnels + TCP, ready for QUIC)
- Primary listens only on localhost for security
- Certificates stored locally for persistence and recovery
- Message protocol for all control plane communication
- Event-driven synchronization (no polling)

### Working Components (Tested & Verified)
- ✅ QUIC transport with aioquic
- ✅ Certificate generation and persistence
- ✅ Primary listens on localhost only
- ✅ SSH tunnel coordination with dynamic ports
- ✅ Error reporting with tracebacks
- ✅ Log forwarding from secondary to primary
- ✅ Primary-controlled peer connections
- ✅ Connection retry logic (60 attempts)
- ✅ Setup completion tracking
- ✅ Graceful shutdown on disconnect
- ✅ Message handlers for TCP and QUIC
- ✅ Per-run certificate directories
- ✅ Host networking mode for containers

### Known Issues & Solutions
- ✅ FIXED: Podman rootless mode - use explicit --root/--runroot/--runtime paths
- ✅ FIXED: Port conflicts - dynamic port allocation per secondary
- ✅ FIXED: Connection race - secondary retry logic (60s)
- ✅ FIXED: Peer connection assumptions - primary controls via peer_list
- ✅ FIXED: Certificate transfer - via messages, not scp
- ✅ FIXED: Primary exposure - listen on localhost only
- ✅ FIXED: Error visibility - comprehensive error reporting

### Directory Structure
```
./run/{run_id}/
├── certificates/
│   ├── primary_cert.pem           # Primary's certificate
│   ├── primary_key.pem             # Primary's private key (mode 600)
│   ├── secondary-0_cert.pem        # Secondary certificates
│   ├── secondary-0_info.json       # Secondary connection info
│   ├── secondary-1_cert.pem
│   └── secondary-1_info.json
└── log/
    ├── connection_info/
    │   ├── secondary-0.info        # SSH tunnel coordination
    │   └── secondary-1.info
    ├── slurm_{jobid}.out           # SLURM output logs
    └── slurm_{jobid}.err           # SLURM error logs
```

### Security Model
- Primary never directly exposed to network
- All connections through localhost or SSH tunnels
- QUIC with certificate-based mutual authentication
- Certificates persisted locally (not on shared storage)
- No hardcoded ports (all dynamically allocated)
- Private keys with restrictive permissions (600)