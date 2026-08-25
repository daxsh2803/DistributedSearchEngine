# Phase 17 — Shard Replication & Failover

## Overview

Phase 17 introduces shard replication and replica failover to the distributed search engine. Every shard can now be replicated across multiple nodes, ensuring that write operations are durable across replicas and read operations can fail over from a primary replica to secondary replicas when the primary is unavailable.

Phase 17 builds directly on:

- **Phase 10** — Shard architecture (shard routing, placement)
- **Phase 11** — Node abstraction (NodeClient interface)
- **Phase 12** — Remote node HTTP transport
- **Phase 13** — Failure semantics (partial results, `complete=false`)
- **Phase 14** — Bounded retry with exponential backoff
- **Phase 15** — Circuit breaker failure isolation
- **Phase 16** — Observability (MetricsCollector)

The phase consists of four sub-phases:

| Sub-phase | Commit | Purpose |
|-----------|--------|---------|
| 17A | `1f413f3` | ShardReplicaPlacement — immutable placement abstraction |
| 17B | `9009de5` | Coordinator write replication (write-all semantics) |
| 17C | `1b9e47c` | Replica-aware read failover |
| 17D | `cabfdb2` | Replicated persistence and end-to-end integration |

## Architecture

```
              HttpServer
                  |
                  v
          ShardCoordinator
           /      |       \
          /       |        \
         v        v         v
    RemoteNode  RemoteNode  RemoteNode
        |           |           |
        v           v           v
    NodeServer  NodeServer  NodeServer
        |           |           |
        v           v           v
    LocalNode    LocalNode   LocalNode
        |           |           |
   Shard 0,1    Shard 0,1   Shard 0,1
   (repl set)   (repl set)  (repl set)
```

Each shard exists on `R` physical nodes. The `ShardReplicaPlacement` maps each logical `shard_id` to an ordered set of `node_id` values. Writes fan out to ALL replicas. Reads try replicas in order until one succeeds.

## Phase 17A — ShardReplicaPlacement

### Data Model

```cpp
struct ShardReplicaSet {
    std::size_t shard_id;
    std::vector<std::size_t> node_ids;  // [0]=primary, [1..]=secondaries
};

class ShardReplicaPlacement {
public:
    ShardReplicaPlacement(shard_count, node_count, replication_factor, replica_sets);
    const std::vector<std::size_t>& replicas_of(shard_id) const;
    std::size_t primary_of(shard_id) const;
    std::size_t shard_count() const;
    std::size_t node_count() const;
    std::size_t replication_factor() const;
};
```

### Invariants

The constructor validates:

1. `shard_count > 0`
2. `node_count > 0`
3. `replication_factor > 0`
4. `replication_factor <= node_count`
5. Exactly `shard_count` replica sets provided
6. Every `shard_id` in `[0, shard_count)` appears exactly once
7. Each replica set contains exactly `replication_factor` nodes
8. All `node_ids < node_count`
9. No duplicate `node_ids` within a single replica set

### Thread Safety

The placement is **immutable after construction**. All members return `const` data. Safe for concurrent `const` reads without synchronization. No mutexes needed.

### R=1 Backward Compatibility

`ShardReplicaPlacement` fully supports replication factor 1:

```cpp
ShardReplicaSet{0, {2}}  // shard 0 → node 2, primary = node 2
```

This represents the existing single-node placement model and is used by the legacy `ShardPlacement` constructor overload in `ShardCoordinator`.

## Phase 17B — Coordinator Write Replication

### Write-All Semantics

Every write operation (`ingest`, `update`, `remove`) sends the operation to **ALL** replicas of the target shard:

```
coordinator.ingest(doc_id, content)
    → shard_id = router.route(doc_id)
    → replicas = placement.replicas_of(shard_id)
    → fan out add_document to ALL replicas in parallel
    → collect results
    → ALL must succeed for overall success
```

If **any** replica fails, the overall operation returns `is_error = true`.

| Scenario | Result |
|----------|--------|
| R=1, primary succeeds | ✅ success |
| R=2, both succeed | ✅ success |
| R=2, primary succeeds, secondary fails | ❌ error |
| R=2, primary fails, secondary succeeds | ❌ error |
| R=2, both fail | ❌ error |
| R=3, all succeed | ✅ success |
| R=3, one fails | ❌ error |

### Parallel Fan-Out

Writes use `std::async(std::launch::async, ...)` to send all replica writes concurrently. The coordinator collects all futures before returning. This keeps wall-clock latency close to a single write.

### Partial Failure Semantics

When one replica succeeds and another fails, the data may exist on the successful replica. This is an intentional Phase 17 limitation. No automatic repair, hinted handoff, or rollback is implemented. The coordinator reports the failure using the existing error model.

### Metrics

A single user write increments `coordinator_writes_total` exactly **once**, regardless of replication factor. Individual replica operations are counted separately by RemoteNode-level metrics.

```
One user ingest with R=2:
    coordinator_writes_total += 1    (once, at the end)
    RemoteNode 0: writes_total += 1  (node-level)
    RemoteNode 1: writes_total += 1  (node-level)
```

### Backward Compatibility

R=1 continues to behave identically to the pre-replication system. The legacy `ShardCoordinator(router, ShardPlacement, nodes)` constructor converts `ShardPlacement` to R=1 `ShardReplicaPlacement` automatically.

## Phase 17C — Replica-Aware Read Failover

### Failover Algorithm

For each read operation (search, get_document, document_count), the coordinator tries replicas in placement order:

```
for node_id in replicas_for_shard(shard_id):
    response = node.operation(shard_id)
    if response indicates success:
        return response
    // else: try next replica
// All replicas exhausted:
    report failure using existing error semantics
```

### Search Failover

The search path (`collect_postings`) tries replicas per-shard independently. Only **one** replica per shard contributes results — no duplicate documents are returned.

```
shard 0 replicas = [node0, node2]

    node0 succeeds:
        use node0 results only → NO secondary query
    node0 fails:
        node2 succeeds:
            use node2 results only
```

### ShardGetResponse Three-Way Distinction

`ShardGetResponse` distinguishes three states:

| `is_error` | `found` | Meaning | Failover? |
|------------|---------|---------|-----------|
| `false` | `true` | Document present | No — return document |
| `false` | `false` | Document genuinely absent | No — return not-found |
| `true` | (any) | Actual node/operation failure | **Yes** — try next replica |

This distinction was introduced specifically for Phase 17C. Previously, `ShardGetResponse` lacked `is_error`, making it impossible to distinguish "document not found" from "node unavailable."

The `is_error` field survives the full wire path:

```
LocalNode → NodeServer (JSON serialization) → RemoteNode (JSON deserialization) → ShardCoordinator
```

The wire format uses `j.value("is_error", false)` for backward compatibility with servers that don't include the field.

### Document Count Failover

`compute_global_n()` tries replicas per-shard for `document_count`. Only one replica per shard contributes the count — counts are NOT summed across replicas.

### Circuit Breaker Interaction

The coordinator does not directly manipulate remote circuit breakers. If a RemoteNode's circuit is OPEN, it returns `is_error = true`, which triggers failover to the next replica. The existing RemoteNode retry/circuit-breaker logic is preserved intact.

### Metrics

A coordinator search with failover increments `coordinator_searches_total` exactly **once**. RemoteNode-level metrics may record each attempted replica operation separately.

## Phase 17D — Replicated Persistence

### save_all() / load_all()

`save_all()` persists **every replica** of every shard:

```
for each shard_id:
    for each node_id in replicas_of(shard_id):
        nodes[node_id].save_shard(shard_id)
```

`load_all()` follows the same pattern. Each replica's persisted state is independent.

### Persistence Model

Each `Shard` has its own persistence file path (e.g., `data/shard-0/documents.jsonl`). Since replicas are on different `LocalNode` instances with separate `Shard` objects, they naturally persist to independent files. No special replica-awareness is needed in the persistence layer.

### Restart Behavior

After restart:

1. `load_all()` loads every replica from its persistence file
2. Each replica independently recovers its DocumentStore and InvertedIndex
3. Search/read failover operates on the loaded data normally
4. No automatic data repair between replicas

## Concurrency Model

### Write Concurrency

- `std::async` fans out writes to all replicas in parallel
- Each `LocalNode` shard has its own `mutation_mutex_` serializing same-shard mutations
- `RemoteNode` creates fresh `httplib::Client` per request (thread-safe)
- `MetricsCollector` is thread-safe (atomic counters + mutex-protected latency buffers)
- No shared mutable per-request state in `ShardCoordinator`

### Read Concurrency

- Cross-shard search fans out via `std::async` (one future per shard)
- Per-shard failover is sequential within the async task
- `ShardReplicaPlacement` is immutable — concurrent `const` reads are safe
- No deadlocks: no nested locking, no lock ordering dependencies

### Tested Concurrency Scenarios

- Concurrent replicated writes (4 threads × 20 documents)
- Concurrent replicated reads (8 threads)
- Concurrent failover searches (8 threads with failing primary)
- Concurrent HTTP ingest + search (4 writers + 4 readers)
- Concurrent `/metrics` requests

## Observability

### MetricsCollector Integration

Replication integrates with Phase 16 MetricsCollector at two levels:

**Coordinator level** (user-initiated operations):
- `coordinator_searches_total` — one per user search, regardless of failover
- `coordinator_writes_total` — one per user write, regardless of R
- `coordinator_search_latency` — end-to-end distributed search latency
- `coordinator_write_latency` — end-to-end write latency

**Node level** (individual operations):
- `searches_total`, `writes_total` — one per actual node operation
- `retries_total` — per retry attempt
- `circuit_open_events`, `circuit_close_events` — per state transition
- `per_node` metrics — per-node breakdown

### No Double-Counting

A single user search across 3 shards with R=2 produces:

```
coordinator_searches_total = 1        (one user request)
searches_total = 1 to 6              (1-3 successful node ops, depending on failover)
```

A single user write with R=2 produces:

```
coordinator_writes_total = 1          (one user request)
writes_total = 1 to 2               (1-2 successful node ops)
```

### /metrics Endpoint

`GET /metrics` returns the full `MetricsSnapshot` as JSON, including both coordinator and node-level metrics. It does NOT reset counters.

## Failure Semantics

### Primary Node Failure

| Operation | Behavior |
|-----------|----------|
| Search | Try secondary replicas; return first successful result |
| Get document | Try secondary replicas; `is_error` triggers failover |
| Document count | Try secondary replicas; return first successful count |
| Ingest/Update/Remove | **ALL replicas must succeed** — partial failure = error |

### All Replicas Failure

| Operation | Behavior |
|-----------|----------|
| Search | Returns Phase 13 semantics: `complete=false`, `errors[]` populated |
| Get document | Returns `std::nullopt` |
| Document count | Returns 0, with incomplete flag |
| Ingest/Update/Remove | Returns `is_error=true` |

### Phase 13 Semantics Preserved

The existing Phase 13 failure model is unchanged:
- Search failures produce `complete=false` with `errors[]`, NOT `is_error=true`
- Partial shard availability degrades results gracefully
- Error information is propagated to the client

## Testing Strategy

### Test Suites

| Test File | Tests | Coverage |
|-----------|-------|----------|
| `replica_placement_test.cpp` | 27 | Placement construction, validation, accessors, R=1 compat, concurrent reads |
| `shard_coordinator_replication_test.cpp` | 25 | Write fan-out, R=1/R=2/R=3, concurrent writes, metrics, persistence, partial failures |
| `shard_coordinator_failover_test.cpp` | 18 | Search/get_document/document_count failover, R=1 compat, metrics, concurrent failover |
| `replication_integration_test.cpp` | 22 | End-to-end writes, persistence, failover, metrics, HTTP API, concurrency |

### Total: 92 replication-specific tests

### Test Design Principles

- **Deterministic**: No arbitrary sleeps or timing assumptions
- **Focused**: Each test validates one specific behavior
- **Independent**: Tests use fresh coordinators; no shared state between tests
- **Comprehensive**: Covers R=1, R=2, R=3, partial failure, all-fail, concurrent access

## Design Decisions

1. **Write-all semantics**: All replicas must succeed. This is the simplest correct model and avoids silent data divergence. Quorum writes are deferred to future phases.

2. **Read-first-primary**: Reads always try the primary first. This is the simplest failover model. Replica health tracking or load-based selection are deferred.

3. **No result duplication**: Each shard is queried on exactly ONE replica. Replicas contain identical data, so querying multiple replicas would return duplicate results.

4. **ShardGetResponse is_error**: The three-way distinction (found, not-found, error) was added specifically to enable correct failover. Without it, "document not found" and "node unavailable" were indistinguishable.

5. **Immutable placement**: `ShardReplicaPlacement` is immutable after construction. This eliminates synchronization for concurrent reads and makes the placement a pure data structure.

6. **Metrics are observational only**: Metrics recording never alters control flow, retry behavior, or failure semantics.

7. **Separate coordinator/node metrics**: Coordinator metrics represent logical user operations; node metrics represent physical network operations. This prevents conceptual double-counting.

## Non-Goals (Explicitly NOT Implemented)

The following capabilities are explicitly deferred to future phases:

- **Kafka / Redis / message queues** — no external dependencies
- **Automatic replica repair / anti-entropy** — no read-repair, no hinted handoff
- **Consensus protocols** — no Raft, Paxos, or leader election
- **Quorum reads/writes** — write-all and first-successful-read only
- **Consistent hashing** — existing `std::hash % shard_count` routing unchanged
- **Shard rebalancing** — static placement only
- **Dynamic cluster membership** — no add/remove nodes at runtime
- **Version vectors / vector clocks** — no causal consistency tracking
- **Cross-region replication** — single数据中心 only
- **Replica promotion** — primary is always `node_ids[0]`
- **Automatic failover for writes** — partial write failure is reported, not retried
- **Service discovery** — static node configuration

## Known Limitations

1. **Partial write divergence**: If a write succeeds on some replicas but fails on others, data may exist on a subset of replicas. No automatic repair is implemented.

2. **Static placement**: Replica placement cannot be changed at runtime. Rebalancing requires application restart with a new placement configuration.

3. **No replica health tracking**: The coordinator does not track replica health proactively. Failover is reactive — the next replica is tried only when the current one fails.

4. **No write deduplication on retry**: If a write succeeds on the coordinator level after all replicas succeed, but the client retries, the second write may fail with "already exists" on all replicas. This is handled by the existing duplicate detection.

5. **Persistence is per-replica**: Each replica persists independently. There is no mechanism to ensure all replicas have identical persistent state.

## Future Extensions

Potential future work built on the Phase 17 foundation:

- **Consistent hashing** for dynamic shard placement
- **Shard rebalancing** when nodes are added/removed
- **Read repair** to detect and fix stale replicas
- **Anti-entropy** for background replica synchronization
- **Quorum reads/writes** for tunable consistency
- **Replica health tracking** for proactive failover
- **Dynamic membership** for cluster scaling
- **Cross-datacenter replication** for geographic distribution
