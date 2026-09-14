# ADR 016: Synchronous All-Replica Replication and Cluster Authority Model

## Status
Accepted (Phase 17)

## Context
As the Distributed Search Engine evolved from single-node partitioning (Phase 10) to networked remote nodes (Phase 12) and failure-resilient communication (Phases 13–15), the system required data redundancy across multiple nodes to ensure high availability and prevent data loss during individual node crashes.

A fundamental design choice was needed: how should document mutations and reads establish authoritative cluster state across shard replicas, and how does this relate to asynchronous event pipelines like Apache Kafka?

## Decision

We chose a **Synchronous All-Replica Replication / Cluster Authority Model** managed directly by the `ShardCoordinator`.

1. **Synchronous Replication as the Sole Authority:**
   - For every document mutation (`ingest`, `update`, `remove`), the `ShardCoordinator` maps the document ID to a logical shard and identifies its configured deterministic replica set (primary plus backup replicas, e.g., $R=2$).
   - The coordinator executes parallel write requests to **all** configured replicas in the replica set via `std::async`.
   - The write is considered authoritative and successful if and only if **all configured replica writes succeed**. Document mutations succeed only when the configured synchronous replica writes required by ShardCoordinator succeed.
   - If any replica fails or times out, the mutation fails, returning an error response to the client.

2. **Kafka is an Asynchronous Side Channel, Not an Authority:**
   - Kafka is strictly an auxiliary event dissemination channel for background notifications and remote event processing.
   - Kafka does **not** sit on the synchronous write path.
   - Client document mutations are never blocked on, nor authorized by, Kafka publication.

3. **Explicit Non-Use of Raft or Paxos:**
   - This design deliberately avoids distributed consensus protocols such as Raft or Paxos.
   - There is no dynamic leader election or distributed consensus log. Primary/replica assignments are deterministic static mappings computed by `make_deterministic_replica_sets(S, N, R)`.

## Consequences

### Positive
- **Deterministic Read-Your-Writes Consistency:** Once a write is acknowledged with HTTP 201/200, every configured replica holds the document in its local store and inverted index. Subsequent searches to any replica yield consistent results.
- **Failover Simplicity:** If a primary node fails during a search, the `ShardCoordinator` immediately fails over to an in-sync backup replica without reconciliation or state recovery.
- **Independence from Message Infrastructure:** Cluster state remains fully authoritative and functional even if Kafka is completely unavailable or offline.

### Negative
- **Write Availability Dependent on All Configured Replicas:** If any replica for a shard is offline, write operations to that shard are rejected until the replica recovers.
- **Network Latency Bound:** The write latency is bounded by the slowest replica write response time.
