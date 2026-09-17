# Distributed Search Engine — System Architecture Blueprint

## 1. System Overview

The Distributed Search Engine (DSE) is an in-process, distributed information retrieval system built from first principles in modern C++20. It provides partitioned full-text indexing, BM25 / TF-IDF ranking, document store persistence, fault-tolerant synchronous replication across cluster nodes, a durable outbox event pipeline, and an asynchronous Kafka side channel for decoupled cross-node event dissemination.

### Key Architectural Invariants

1. **Two-Tier Authority Model:**
   - **Tier 1 (Authoritative):** Synchronous All-Replica Replication / Cluster Authority Model (`ShardCoordinator`). Document mutations succeed only when the configured synchronous replica writes required by ShardCoordinator succeed. This is NOT Raft, Paxos, or another distributed consensus protocol. Search operations query replicas across shards synchronously.
   - **Tier 2 (Asynchronous Side Channel):** Kafka Event Pipeline (`EventStore`, `EventDispatcher`, `KafkaMessageBroker`). Successful domain mutations generate asynchronous events delivered to Kafka at-least-once. Kafka is an auxiliary event dissemination channel and **never** substitutes for synchronous replication.
2. **First Principles Implementation:** Zero dependencies on external search platforms (no Lucene, Solr, Elasticsearch, or OpenSearch).
3. **Clear Component Ownership & RAII:** All concurrency, memory, network connections, file handles, and worker threads are strictly lifecycle-managed.

---

## 2. Component Architecture

```
                       ┌─────────────────────────────────────────┐
                       │               HTTP Client               │
                       └────────────────────┬────────────────────┘
                                            │ REST (JSON)
                                            v
                       ┌─────────────────────────────────────────┐
                       │            HttpServer (8080)            │
                       │    /search, /documents, /health, /metrics │
                       └────────────────────┬────────────────────┘
                                            │
                                            v
                       ┌─────────────────────────────────────────┐
                       │            ShardCoordinator             │
                       │     (Cluster Authority & Routing)       │
                       └──────┬───────────────────────────┬──────┘
                              │                           │
           Tier 1: Synchronous Writes / Reads             │ Tier 2: Durable Event Outbox
                              │                           │
         ┌────────────────────┴──────────────┐            v
         │                                   │    ┌───────────────────────────────┐
         v                                   v    │     PersistentEventStore      │
┌─────────────────┐                 ┌─────────────────┐   │  (events.jsonl append log)    │
│   NodeClient    │                 │   NodeClient    │   └───────────────┬───────────────┘
│  (Primary Node) │                 │ (Replica Node)  │                   │
└────────┬────────┘                 └────────┬────────┘                   v
         │                                   │            ┌───────────────────────────────┐
         v                                   v            │        EventDispatcher        │
┌─────────────────┐                 ┌─────────────────┐   │    (Bounded queue, worker)    │
│ Local / Remote  │                 │ Local / Remote  │   └───────────────┬───────────────┘
│   Node Server   │                 │   Node Server   │                   │
└────────┬────────┘                 └────────┬────────┘                   v
         │                                   │            ┌───────────────────────────────┐
         v                                   v            │         MessageBroker         │
┌─────────────────────────────────────────────────────┐   │   (KafkaMessageBroker / InMem)│
│                   Shard Engine                      │   └───────────────┬───────────────┘
│   - DocumentStore (JSONL document store)           │                   │
│   - InvertedIndex (Postings lists, vocabulary)      │                   v
└─────────────────────────────────────────────────────┘   ┌───────────────────────────────┐
                                                          │   Kafka Cluster (Mutations)   │
                                                          └───────────────┬───────────────┘
                                                                          │
                                                                          v
                                                          ┌───────────────────────────────┐
                                                          │     RemoteEventProcessor      │
                                                          │   (Applies mutation locally,  │
                                                          │    skips own source_node_id)  │
                                                          └───────────────────────────────┘
```

---

## 3. Node Architecture & API Separation

Every physical process running `DistributedSearchEngine.exe` acts as an autonomous cluster node identified by an integer `node_id`.

### Public HTTP API vs. Private RPC API

Each node separates public client traffic from internal cluster communication across distinct ports:

1. **Public HTTP Server (`HttpServer` — e.g. Port 8080):**
   - Serves external client requests.
   - Routes user queries (`GET /search`), document CRUD (`POST`, `PUT`, `GET`, `DELETE /documents`), health checks (`GET /health`), and operational metrics (`GET /metrics`).
   - Delegates business logic to the node's internal `ShardCoordinator`.
2. **Private Node RPC Server (`NodeServer` — e.g. Port 9081):**
   - Serves peer node remote procedure calls via lightweight HTTP REST endpoints (`/rpc/add`, `/rpc/update`, `/rpc/delete`, `/rpc/get`, `/rpc/search`, `/rpc/count`, `/rpc/save`, `/rpc/load`).
   - Interacts directly with the local node's `LocalNode` and hosted shards without public overhead or client exposure.
   - Bounded socket timeouts ensure predictable failover detection.

---

## 4. Shard and Replica Placement

The cluster partitions the global document keyspace across $S$ logical shards, distributed across $N$ physical nodes with replication factor $R$.

- **Routing:** Deterministic hash routing via `ShardRouter`:
  $$\text{shard\_id} = \text{MurmurHash64A}(\text{doc\_id}) \pmod S$$
- **Replica Sets:** Deterministic assignment generated by `make_deterministic_replica_sets(S, N, R)`:
  For shard `sid`, replica $r \in [0, R)$:
  $$\text{node\_id} = (sid + r) \pmod N$$
  - The first node in the replica set is the **primary** for that shard.
  - The remaining nodes are **backup replicas**.

---

## 5. Synchronous Write Path (Tier 1 Authority)

Write mutations (`ingest`, `update`, `remove`) are strictly authoritative and synchronous:

```
Client             HttpServer         ShardCoordinator       Primary NodeClient     Replica NodeClient
  │                    │                      │                      │                      │
  │── POST /documents ─>                      │                      │                      │
  │                    │── ingest(req) ───────>                      │                      │
  │                    │                      │── route(doc_id)      │                      │
  │                    │                      │                      │                      │
  │                    │                      │── add_document() ────> (async std::future)  │
  │                    │                      │── add_document() ───────────────────────────> (async std::future)
  │                    │                      │                      │                      │
  │                    │                      │<── OK (terms=12) ────┤                      │
  │                    │                      │<── OK (terms=12) ───────────────────────────┤
  │                    │                      │                      │                      │
  │                    │                      │ [All Replicas OK]    │                      │
  │                    │                      │── create_event() ──> [PersistentEventStore] │
  │                    │                      │── enqueue() ───────> [EventDispatcher]      │
  │                    │<── Success ──────────┤                      │                      │
  │<── 201 Created ────┤                      │                      │                      │
```

### Invariants:
1. **Synchronous All-Replica Replication:** Document mutations succeed only when the configured synchronous replica writes required by ShardCoordinator succeed. The coordinator fans out writes to all replicas in parallel via `std::async`. This is NOT Raft, Paxos, or another distributed consensus protocol.
2. **Atomic Rollback on Rejection:** If any replica fails or times out, `is_error = true` is returned to the client.
3. **Decoupled Outbox:** Domain events are created in the `PersistentEventStore` **only after** all synchronous replicas succeed.

---

## 6. Search & Ranking Path

Distributed search implements cross-shard scatter-gather with global corpus statistics:

```
Client             HttpServer         ShardCoordinator        Shard 0 (NodeClient)   Shard 1 (NodeClient)
  │                    │                      │                       │                      │
  │── GET /search ────>│                      │                       │                      │
  │                    │── search(query) ─────>                       │                      │
  │                    │                      │── collect_postings() ─> (Try Primary/Replica)│
  │                    │                      │── collect_postings() ────────────────────────> (Try Primary/Replica)
  │                    │                      │                       │                      │
  │                    │                      │<── Postings (term A) ─┤                      │
  │                    │                      │<── Postings (term A) ────────────────────────┤
  │                    │                      │                       │                      │
  │                    │                      │── compute_global_n()  │                      │
  │                    │                      │── TF-IDF / BM25 score │                      │
  │                    │                      │── Top-K heap sort     │                      │
  │                    │<── Ranked Results ───┤                       │                      │
  │<── 200 OK (JSON) ──┤                      │                       │                      │
```

### Search Logic:
1. **Replica Failover:** For each shard, the coordinator attempts the primary node first. If the primary is down or circuit-broken, it falls back to backup replicas transparently.
2. **Partial Search Degradation:** If all replicas for a shard fail, the search completes partially with available shards. The response sets `complete = false` and appends detailed `NodeFailureInfo`.
3. **Global Scoring:** Postings from all participating shards are aggregated to compute exact document frequencies ($df$) and corpus document totals ($N$), calculating mathematically exact global IDF:
   $$\text{IDF}(t) = \ln\left(\frac{N}{\text{df}(t)}\right)$$

---

## 7. Asynchronous Event Pipeline (Tier 2 Side Channel)

### Durable Outbox Event Pipeline (`EventStore`)
The outbox decouples document mutations from message broker availability. It guarantees **at-least-once** event dispatch across process crashes without database transactions.

- **Append-Only Log:** `PersistentEventStore` persists event lifecycles to disk (`events.jsonl`) with metadata checkpoints (`events.meta`).
- **Lifecycle Transitions:**
  - `PENDING`: Event generated by coordinator following confirmed mutation.
  - `DISPATCHING`: Dequeued by `EventDispatcher` worker; broker delivery in progress.
  - `PUBLISHED`: Delivery report confirmed by message broker.
  - `FAILED`: Delivery attempts exhausted or broker permanently rejected.
- **Crash Recovery:** Upon node restart, `PersistentEventStore` reloads the log and automatically shifts any lingering `DISPATCHING` records back to `PENDING` for redelivery.

### Bounded In-Process Dispatcher (`EventDispatcher`)
- Bounded internal circular queue (default 4096 capacity) with non-blocking or timed enqueue.
- Backpressure: If the queue is saturated, `enqueue()` times out and increments `dispatcher_rejected`. The underlying synchronous document mutation is **not rolled back**.
- Dedicated background worker thread continuously drains the queue and publishes to the `MessageBroker`.

---

## 8. Kafka Integration & Consumer Processing

```
EventDispatcher ──> KafkaMessageBroker ──> librdkafka Producer ──> Kafka Cluster (documents.mutations)
                                                                            │
                                                                   Partition 0, 1, 2
                                                                   (Keyed by doc_id)
                                                                            │
                                                                            v
LocalNode <── RemoteEventProcessor <── MessageHandler <── KafkaConsumer (Manual Commit)
```

### Kafka Producer Architecture (`KafkaMessageBroker`)
- Partitions messages across topic `documents.mutations` by string key `std::to_string(doc_id)`. This guarantees strict per-document ordering across Kafka partitions.
- Utilizes asynchronous delivery callbacks (`set_delivery_report_callback`). When librdkafka acknowledges broker storage, `EventStore::mark_published()` transitions the outbox state.

### Kafka Consumer Architecture (`KafkaConsumer`)
- Assigned to dedicated consumer group per node (`dse-consumer-group-node-<id>`).
- Manual Offset Commits: `enable.auto.commit = false`. Offsets are committed only after the handler returns `true`.
- **Sequential Backoff & Partition Pausing:** If a consumer handler returns `false` (mutation application failure), the consumer pauses partition consumption (`pause_all()`) and enters bounded exponential backoff (100ms to 5000ms), retrying the exact same message. Consumption resumes (`resume_all()`) only upon success.

### Feedback-Loop Prevention (`RemoteEventProcessor`)
When nodes consume from Kafka, an infinite ping-pong loop must be prevented:
1. Every event includes `source_node_id` representing the primary node that originated the mutation.
2. `RemoteEventProcessor` verifies:
   $$\text{source\_node\_id} == \text{own\_node\_id}$$
3. If the event originated locally, it is skipped (`Outcome::Skipped`) and acknowledged immediately.
4. If from a remote node, mutations are applied directly to local shards via `LocalNode::apply_remote_*()`, which mutates the index and document store **without** generating replication calls or secondary events.

---

## 9. Failure Semantics Matrix

| Component Failure Scenario | System Impact | Immediate Client Response | Recovery / State Guarantee |
| :--- | :--- | :--- | :--- |
| **Replica Node Unavailable during Write** | Configured replica write failure on synchronous fanout. | HTTP 500 Internal Server Error (`is_error: true`). | Mutation rejected; no outbox event created; state remains consistent. |
| **Primary Node Down during Search** | Coordinator detects failure / tripped breaker. | HTTP 200 OK (`complete: true`). | Transparent failover to backup replica; client unimpacted. |
| **All Replicas for Shard Down during Search** | Degraded cross-shard search. | HTTP 200 OK (`complete: false`, partial results). | Surviving shards return results; failure details logged in response JSON. |
| **Dispatcher Queue Saturated (Backpressure)** | Event dropped from in-memory queue. | HTTP 201 Created (Write succeeds). | Document write persists; `dispatcher_rejected` increments; mutation is authoritative. |
| **Kafka Cluster Outage** | Asynchronous delivery fails. | HTTP 201 Created (Write succeeds). | Events accumulate in `PersistentEventStore` as `FAILED` or retry in dispatcher; zero client impact. |
| **Consumer Handler Mutation Failure** | Handler returns `false`. | N/A (Internal consumer). | Partition pauses; consumer performs sequential exponential backoff; offset not committed until resolved. |
| **Node Crash / Sudden Power Loss** | In-flight memory lost. | N/A. | Node loads JSONL segments; EventStore reloads `events.jsonl`; `DISPATCHING` reset to `PENDING` for redelivery. |

---

## 10. Metrics & Observability Architecture

Observability is implemented in-process without third-party daemons via `MetricsCollector` and queried at `GET /metrics`.

- **Lock-Free Atomic Counters:** Ultra-low overhead write/search operations, errors, retries, and circuit transitions.
- **Thread-Safe Latency Sampling:** Bounded circular buffers (1000 samples) computing deterministic average and $P_{99}$ latency.
- **Polymorphic Broker Abstraction:** `MessageBroker` defines `virtual std::uint64_t consumer_lag() const { return 0; }`. `KafkaMessageBroker` overrides it with live partition lag from librdkafka; `InMemoryMessageBroker` returns default zero.
- **Kafka Consumer Retry Visibility:** `KafkaMessageBroker::stats()` maps atomic `messages_nacked_` to `messages_retried`, cleanly exposing consumer backoff loops as `consumer_messages_nacked` via `/metrics`.

---

## 11. Threading and Ownership Boundaries

| Component | Thread Model | Synchronization Primitives | Resource Ownership |
| :--- | :--- | :--- | :--- |
| `HttpServer` | Multi-threaded thread pool (cpp-httplib). | Lock-free metrics; coordinator synchronization. | Owns `httplib::Server`; references `ShardCoordinator`, `MetricsCollector`. |
| `ShardCoordinator` | Request thread initiates `std::async` workers. | Futures (`std::future::get()`). | Owns `ShardRouter`, `ShardReplicaPlacement`, vector of `NodeClient`s. |
| `Shard` | Concurrent read, serialized write. | `std::shared_mutex` (RW lock). | Owns `DocumentStore` and `InvertedIndex`. |
| `PersistentEventStore` | Safe for concurrent producers and consumers. | `std::mutex` protecting in-memory map & disk writes. | Owns file stream handles; disk paths. |
| `EventDispatcher` | Single dedicated worker thread. | `std::mutex`, `std::condition_variable` (enqueue/dequeue). | Owns worker thread; references `MessageBroker` & `EventStore`. |
| `KafkaMessageBroker` | 1 Producer poll thread + 1 Consumer thread. | `std::mutex` for handler registry; atomic counters. | Owns `KafkaClient`, `KafkaConsumer`, threads. |
