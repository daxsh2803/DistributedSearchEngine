# Phase 20 Learning: Multi-Node Resilience and Chaos Verification

## 1. Objective

Phase 20 validated the resilience, fault-tolerance, and consistency invariants of the Distributed Search Engine under realistic distributed failure scenarios across a multi-node cluster ($N=3, S=3, R=2$) with native Apache Kafka integration.

---

## 2. Multi-Node Topology Under Test

The resilience test harness evaluated a 3-node topology with overlapping replica placement:
- **Node 0 (Primary for Shard 0, Replica for Shard 2):** Public port 8080, RPC port 9081.
- **Node 1 (Primary for Shard 1, Replica for Shard 0):** Public port 8081, RPC port 9082.
- **Node 2 (Primary for Shard 2, Replica for Shard 1):** Public port 8082, RPC port 9083.
- **Kafka Cluster:** Apache Kafka 4.3.1 KRaft broker serving topic `documents.mutations` (3 partitions).

```
   Node 0 (Host: 8080, RPC: 9081)
       Primary: Shard 0 ──── Sync Write ────> Replica: Shard 0 on Node 1
       Replica: Shard 2 <─── Sync Write ───── Primary: Shard 2 on Node 2

   Node 1 (Host: 8081, RPC: 9082)
       Primary: Shard 1 ──── Sync Write ────> Replica: Shard 1 on Node 2
       Replica: Shard 0 <─── Sync Write ───── Primary: Shard 0 on Node 0

   Node 2 (Host: 8082, RPC: 9083)
       Primary: Shard 2 ──── Sync Write ────> Replica: Shard 2 on Node 0
       Replica: Shard 1 <─── Sync Write ───── Primary: Shard 1 on Node 1
```

---

## 3. Resilience Scenarios Evaluated

### Scenario 1: Clean Synchronous Replica Failover
- **Fault Injection:** Primary node for Shard 0 (Node 0) stopped while search queries are active.
- **Observation:** `ShardCoordinator` on surviving nodes detected node failure, recorded a circuit breaker strike, and seamlessly redirected queries for Shard 0 to Node 1 (its backup replica).
- **Result:** Read requests completed with 100% data consistency; search response marked `complete: true`.

### Scenario 2: Synchronous Write Rejection Under Partition
- **Fault Injection:** Node 1 killed while write requests targeting Shard 0 ($R=2$) were submitted to Node 0.
- **Observation:** Node 0 attempted synchronous writes to both Node 0 (local) and Node 1 (remote RPC). Because Node 1 was down, the remote write failed.
- **Result:** In accordance with the Synchronous All-Replica authority model, the mutation was rejected (`is_error: true`). Crucially, **no domain event was recorded in the outbox**, preserving consistency between synchronous state and downstream event streams.

### Scenario 3: Node Crash and State Recovery
- **Fault Injection:** An active node hosting local shard data was killed via `SIGKILL` and subsequently restarted with its persistent data directory.
- **Observation:**
  1. On restart, the node reloaded its JSONL segments from `data/nodeX/shard_*.jsonl`, rebuilding its in-memory inverted index without remote synchronization.
  2. `PersistentEventStore` parsed `events.jsonl` and reset in-flight `DISPATCHING` records to `PENDING`.
  3. The local `EventDispatcher` resumed and successfully republished pending events.
  4. Remote peer circuit breakers automatically probed the recovered RPC port and transitioned from `OPEN` to `CLOSED`.

### Scenario 4: Kafka Broker Disconnection and Reconnection
- **Fault Injection:** The external Kafka container was stopped while clients executed sustained document writes.
- **Observation:**
  - Client writes succeeded without interruption (HTTP 201 Created), as Tier 1 synchronous replication remained fully operational.
  - Events accumulated safely in `PersistentEventStore` on disk.
  - The `EventDispatcher` worker logged delivery timeouts and retried.
- **Recovery:** Upon Kafka container restart, the dispatcher successfully drained queued events and resumed normal publishing without data loss.

---

## 4. Key Architectural Lessons

1. **Decoupling Prevents Cascading Outages:** Because Kafka is an asynchronous side channel rather than a write-path authority, message broker instability has zero blast radius on client search and indexing availability.
2. **Deterministic Partitioning Guarantees In-Order Recovery:** Keying Kafka events by `document_id` ensures that document mutations are processed sequentially per document partition, preventing race conditions during consumer catch-up.
3. **Loop Prevention is Essential in Replicated Topologies:** Without `source_node_id` filtering in `RemoteEventProcessor`, replicated nodes consuming from a shared Kafka topic would re-apply mutations that they already applied locally during synchronous replication.
