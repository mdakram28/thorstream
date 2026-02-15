# Thorstream Architecture

## High-level components
- `broker`: topic/partition ownership, produce/fetch routing, offsets.
- `storage`: append-only partition logs and segment files.
- `server`: custom wire protocol and Kafka wire protocol endpoints.
- `protocol`: request/response codecs and Kafka binary protocol handlers.
- `cluster`: leader election loop, peer ping, replication RPC, quorum ack logic.

## Data flow (write)
1. Client sends `Produce` (custom protocol or Kafka).
2. Request reaches server dispatcher.
3. Leader check (`Broker::is_leader`).
4. Leader appends locally (`Broker::produce`).
5. Record is replicated to peers via cluster RPC (`cluster::replicate_to_quorum`).
6. Quorum ack returns success.

## Data flow (read)
1. Client sends `Fetch`.
2. Server reads from leader local log (`Broker::fetch`).
3. Response includes records and high-water-mark.

## Leader election
- Control plane performs periodic peer pings.
- Leader is elected from reachable nodes with deterministic lowest node id rule.
- On node failure, remaining nodes re-elect.

## Recovery model
- Broker scans data directory at startup to rebuild topics and partitions.
- Segment offsets are recovered from on-disk record stream.
- Replicated local data is loaded similarly on restart.

## Current limitations
- Election is deterministic heartbeat-based, not full Raft-log consensus yet.
- Membership changes are static (from startup config/env).
- Snapshot shipping and log compaction for cluster replication are not yet implemented.
