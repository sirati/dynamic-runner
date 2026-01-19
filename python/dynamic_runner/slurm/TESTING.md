# Testing SLURM Distributed Processing

This guide explains how to test the SLURM distributed processing implementation.

## Prerequisites

1. SSH access to the SLURM gateway (e.g., `lmu`)
2. SLURM partition access
3. Docker/Podman image built and transferred to gateway
4. Source binaries to process (optional for connection testing)

## Test Levels

### Level 1: Local Connection Test (No SLURM)

Tests basic primary-secondary communication without SLURM.

```bash
# Simple connection test
python test_connection.py primary &
sleep 2
python test_connection.py secondary localhost 5000
```

### Level 2: SSH Port Forwarding Test

Tests SSH port forwarding from local machine to gateway.

```bash
# Terminal 1: Start primary
python test_connection.py primary

# Terminal 2: Setup SSH forwarding and test
ssh -R 6000:localhost:5000 lmu 'python3 -c "
import asyncio
import sys

async def test():
    reader, writer = await asyncio.open_connection(\"localhost\", 6000)
    msg = b\"test\"
    writer.write(len(msg).to_bytes(4, \"big\"))
    writer.write(msg)
    await writer.drain()
    print(\"SUCCESS\")
    writer.close()
    await writer.wait_closed()

asyncio.run(test())
"'
```

### Level 3: Integration Test (Mock SLURM)

Tests full coordinator and secondary interaction without actual SLURM submission.

```bash
# Local mode (no SSH)
python test_integration.py local

# SSH mode (requires manual SSH forwarding in another terminal)
python test_integration.py ssh
```

### Level 4: SLURM Test Mode (Real Container, No Processing)

Tests actual SLURM job submission with container startup.

```bash
# Build and transfer image (if not already done)
python -m dynamic_batch \
  --gateway ssh://lmu \
  --slurm \
  --packaging podman \
  --slurm-root-folder '~/BIG/slurm-test'

# Submit test job
python -m dynamic_batch \
  --gateway ssh://lmu \
  --slurm \
  --packaging podman \
  --slurm-root-folder '~/BIG/slurm-test' \
  --slurm-test-job
```

Check job output:
```bash
ssh lmu 'ls -lt ~/BIG/slurm-test/log/*.out | head -5'
ssh lmu 'cat ~/BIG/slurm-test/log/test_*.out'
```

### Level 5: Full SLURM Execution (Production)

Runs full distributed processing with actual binaries.

```bash
python -m dynamic_batch \
  --source ./src \
  --output ./out \
  --gateway ssh://lmu \
  --slurm \
  --packaging podman \
  --slurm-root-folder '~/BIG/slurm-test' \
  --num-secondaries 2 \
  --platform x86 x64 \
  --compiler gcc clang \
  --opt O0 O1 O2 O3
```

**What this does:**
1. Collects binaries matching criteria from `./src`
2. Builds Docker image locally with Nix
3. Transfers image to gateway `~/BIG/slurm-test/image_bin/`
4. Establishes SSH port forwarding: gateway:6000 → localhost:5000
5. Starts primary coordinator listening on localhost:5000
6. Submits 2 SLURM jobs that start secondary containers
7. Secondaries connect to gateway:6000 (forwarded to primary)
8. Files distributed, processed, results saved to `./out`

## Monitoring

### Check Primary Status

The primary coordinator logs show:
- Connection establishment
- Secondary registrations
- File distribution progress
- Task completion

### Check SLURM Jobs

```bash
# List jobs
ssh lmu 'squeue -u $USER'

# Check job details
ssh lmu 'scontrol show job JOBID'

# Check job output
ssh lmu 'cat ~/BIG/slurm-test/log/asm-tokenizer-secondary-*.out'
```

### Check Secondary Logs

```bash
# Inside container logs go to /app/log-tmp/
# After completion, moved to log network storage
ssh lmu 'ls -lh ~/BIG/slurm-test/log/'
```

## Debugging

### Primary Won't Start

**Error:** Port already in use
```bash
# Check what's using port 5000
lsof -i :5000
# Kill if needed
kill <PID>
```

### Secondaries Can't Connect

**Check SSH forwarding:**
```bash
# Verify forwarding is active
ssh lmu 'netstat -ln | grep 6000'
```

**Check firewall:**
```bash
# On gateway, ensure port 6000 is accessible
ssh lmu 'nc -l 6000' &
ssh lmu 'nc -zv localhost 6000'
```

### SLURM Job Fails

**Check job output:**
```bash
ssh lmu 'tail -100 ~/BIG/slurm-test/log/*.out'
```

**Common issues:**
- Image not found: Verify `~/BIG/slurm-test/image_bin/asm-tokenizer-docker.tar` exists
- Podman errors: Check `/tmp` has space, runroot path is short
- Connection refused: Verify SSH port forwarding is active

### Container Won't Start

**Test Podman manually:**
```bash
ssh lmu 'srun --partition=All bash -c "
  podman --root /tmp/test/storage --runroot /tmp/test/run --runtime /usr/bin/crun images
"'
```

## Test Scenarios

### Scenario 1: Connection Only (No Binaries)

Test that primary and secondary can communicate without processing files.

```bash
# Create empty source directory
mkdir -p test-src
python -m dynamic_batch \
  --source test-src \
  --output test-out \
  --gateway ssh://lmu \
  --slurm \
  --packaging podman \
  --slurm-root-folder '~/BIG/slurm-test' \
  --num-secondaries 1
```

**Expected:** 
- Primary starts
- 1 secondary connects
- No files to distribute
- Clean shutdown

### Scenario 2: Single File Processing

Test with one small binary.

```bash
# Prepare test binary
mkdir -p test-src/x86/gcc/O0
cp /bin/ls test-src/x86/gcc/O0/test_binary

python -m dynamic_batch \
  --source test-src \
  --output test-out \
  --gateway ssh://lmu \
  --slurm \
  --packaging podman \
  --slurm-root-folder '~/BIG/slurm-test' \
  --num-secondaries 1
```

**Expected:**
- File transferred to secondary
- Processing attempted
- Output saved to test-out/

### Scenario 3: Multiple Secondaries

Test load balancing across secondaries.

```bash
python -m dynamic_batch \
  --source ./src \
  --output ./out \
  --gateway ssh://lmu \
  --slurm \
  --packaging podman \
  --slurm-root-folder '~/BIG/slurm-test' \
  --num-secondaries 3
```

**Expected:**
- 3 secondaries connect
- Files distributed across all
- Parallel processing
- Keepalive messages exchanged

## Success Criteria

### Connection Success
- Primary reports "Primary listening on 0.0.0.0:5000"
- Secondary reports "Connected to primary successfully"
- Secondary sends welcome message
- Primary receives welcome and registers secondary

### Processing Success
- Files transferred to secondaries
- Workers start processing
- Output files appear in output directory
- Logs show no errors

### Shutdown Success
- Primary can disconnect without breaking secondaries
- Secondaries continue processing autonomously
- All output files saved correctly
- Clean exit codes

## Troubleshooting Checklist

- [ ] SSH access to gateway working
- [ ] Docker image built and transferred
- [ ] Port 5000 available locally
- [ ] SSH port forwarding active (gateway:6000)
- [ ] SLURM partition accessible
- [ ] Compute nodes have Podman/crun
- [ ] `/tmp` writable on compute nodes
- [ ] Source directory contains binaries
- [ ] Output directory writable

## Known Limitations

1. **Worker Integration:** Workers are placeholders, not fully integrated
2. **File Distribution:** Core logic implemented, needs coordinator integration
3. **Failover:** Election protocol not yet implemented
4. **Logs:** Rotation logic implemented, not connected to workers
5. **QUIC:** Using TCP placeholder, upgrade to QUIC pending

## Next Steps

After validating connection:
1. Integrate file distributor with coordinator
2. Connect worker_manager for actual processing
3. Implement log rotation
4. Add failover and election
5. Performance testing and optimization