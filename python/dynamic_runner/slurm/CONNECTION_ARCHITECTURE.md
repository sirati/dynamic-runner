# Primary-Secondary Connection Architecture

## Overview

This document describes how the primary coordinator (running locally) connects to secondary workers (running on SLURM compute nodes behind a gateway).

## Network Topology

```
[Local Machine]  <--SSH-->  [Gateway]  <--Internal Network-->  [Compute Nodes]
   Primary                     lmu                              bentonit, essexit, etc.
   (public)                 (jump host)                         (private network)
```

## Connection Flow

### 1. SSH Port Forwarding Setup

The primary establishes SSH port forwarding **before** connecting:

```python
gateway.setup_port_forwarding(local_port=5000, remote_port=6000)
gateway.connect()  # Establishes SSH with -R 6000:localhost:5000
```

This creates a tunnel:
- Connections to `gateway:6000` are forwarded to `localhost:5000`
- Compute nodes can reach the primary by connecting to the gateway

### 2. Primary Starts Listening

The primary coordinator listens on `localhost:5000`:

```python
server = await asyncio.start_server(handle_connection, "0.0.0.0", 5000)
```

### 3. SLURM Job Submission

Primary submits SLURM jobs that start secondaries:

```bash
podman run dynamic_batch \
  --secondary tcp://gateway_hostname:6000 \
  --secondary-id secondary-0
```

### 4. Secondary Connects

Secondary (running on compute node) connects to gateway:

```python
reader, writer = await asyncio.open_connection("gateway_hostname", 6000)
```

The gateway forwards this connection to the primary's `localhost:5000`.

### 5. Message Exchange

Once connected, primary and secondary exchange messages:

1. **Secondary → Primary**: Welcome message with capabilities
2. **Primary → Secondary**: Entropy for certificate generation
3. **Primary ↔ Secondary**: Certificate exchange
4. **Primary → Secondary**: Peer information
5. **Secondary ↔ Secondary**: Direct QUIC connections
6. **Ongoing**: Task assignments, status updates, keepalives

## Implementation Details

### Gateway Interface

```python
class Gateway(Protocol):
    def setup_port_forwarding(self, local_port: int, remote_port: int) -> None:
        """Must be called before connect()"""
        ...
```

### SSH Gateway

Port forwarding is added to the SSH master connection:

```bash
ssh -M -N -f \
  -o ControlPath=/tmp/control \
  -R 6000:localhost:5000 \
  gateway
```

### Primary Coordinator

```python
# Setup
await self._setup_server()  # Start listening + setup forwarding

# Submit jobs
self._submit_slurm_jobs(num_secondaries)

# Wait for connections
await self._wait_for_secondaries(num_secondaries)
```

### Secondary Mode

```python
# Connect
await self._connect_to_primary()  # Connects to gateway:6000

# Send welcome
await self._send_welcome()

# Main loop
await self._main_loop()  # Keepalives, task processing
```

## Message Protocol

All messages use length-prefixed JSON:

```
[4 bytes: message length][N bytes: JSON message]
```

Example welcome message:

```json
{
  "type": "secondary_welcome",
  "secondary_id": "secondary-0",
  "ram_bytes": 8589934592,
  "worker_count": 4,
  "hostname": "bentonit.cip.ifi.lmu.de"
}
```

## Testing

Use the test script to validate the connection:

```bash
# Terminal 1 (local): Start primary
python test_connection.py primary

# Terminal 2: Setup SSH forwarding
ssh -R 6000:localhost:5000 lmu

# Terminal 3 (on gateway): Test secondary
python test_connection.py secondary localhost 6000
```

## Advantages of This Approach

1. **No Public Exposure**: Compute nodes remain on private network
2. **Single SSH Connection**: Reuses existing persistent SSH connection
3. **Firewall Friendly**: No inbound connections to local machine needed
4. **Scalable**: Multiple secondaries can connect through same forwarded port
5. **Simple**: Standard SSH feature, no custom tunneling needed

## Port Allocation

- **Local**: Primary listens on `5000`
- **Gateway**: Forwarded port `6000`
- **Compute Nodes**: Connect to `gateway:6000`

These can be configured but must match between primary and SLURM job scripts.
