# Phase 28: Failure Testing & Fault Injection

Phase 28 systematically verifies and characterizes the failure semantics of the Distributed Search Engine under controlled, deterministic fault injection.

## Objectives & Non-Goals

The primary objective of Phase 28 is to audit and test the existing distributed architecture—not to invent or introduce new distributed coordination mechanisms.
In particular:
- No two-phase commit (2PC), distributed transactions, or automatic rollback/compensation are introduced.
- No consensus algorithms (Paxos, Raft), leader election, or quorum models are introduced.
- Phase 17 synchronous replication remains the authoritative source of truth for document state.
- Apache Kafka and the `EventStore` remain an asynchronous side-channel.
- Document mutations succeed or fail according to synchronous replica availability, completely decoupled from message broker availability.
- Search requests gracefully degrade (`complete=false`) when shards are partially offline without claiming false cluster-wide completeness.

---

## Failure Matrix & Verified Scenarios

### Scenario A: One Replica Unavailable During Synchronous Write
* **Architecture Verified:** Phase 17 All-Replica Synchronous Replication & Cluster Authority Model.
* **Test Fixture:** Reduced deterministic R=2 topology (1 shard, replica set `{0, 1}`) explicitly documented as a test fixture.
* **Fault Injected:** Secondary NodeServer (Node 1) is gracefully stopped via `ServerHandle::stop_and_join()`.
* **Observed Behavior:**
  1. A write request issued via `ShardCoordinator::ingest` fans out to all replicas in the replica set.
  2. Because Node 1 is offline, the RPC to Node 1 fails (`connection refused`).
  3. `ShardCoordinator::ingest` returns `response.is_error == true` with an explicit error message to the caller.
  4. **No False Success:** The caller is informed that the synchronous write failed.
  5. **Partial State Verification:** Because there is no distributed 2PC or compensation mechanism, primary Node 0 executed the write locally before the secondary failed. Querying Node 0 directly reveals the document is present on Node 0. This partial state is a fundamental and documented characteristic of the Phase 17 design.

### Scenario B: Primary Read Replica Unavailable
* **Architecture Verified:** Phase 24 Distributed Read Resilience & Query Failover.
* **Test Fixture:** R=2 replica set `{0, 1}` for Shard 0. Node 0 is configured with a deterministic `FailingNode` double; Node 1 is healthy and contains pre-indexed content.
* **Fault Injected:** Primary node RPC fails immediately with simulated node unavailability.
* **Observed Behavior:**
  1. `ShardCoordinator::search` attempts to read from primary Node 0, encounters an error, and seamlessly falls back to secondary Node 1.
  2. `ShardCoordinator::get_document` similarly falls back to Node 1.
  3. The search response has `is_error == false` and `complete == true`, correctly indicating that all required shards were successfully queried.
  4. `read_failovers_total` metric is incremented in `MetricsCollector`.

### Scenario C: All Replicas of One Shard Unavailable
* **Architecture Verified:** Phase 25 Partial-Availability Search Semantics.
* **Test Fixture:** 2-shard cluster (Shard 0 and Shard 1). Shard 0 is placed on a healthy node; Shard 1 has all replicas mapped to a `FailingNode`.
* **Fault Injected:** Complete unavailability of all replicas of Shard 1, while Shard 0 remains operational.
* **Observed Behavior:**
  1. The query executes across all shards.
  2. Shard 0 succeeds and contributes its matching documents.
  3. Shard 1 fails completely.
  4. The coordinator returns partial results with `response.is_error == false`, but crucially sets `response.complete == false`.
  5. `response.errors` records an explicit error identifying `shard_id == 1`.
  6. The system never misrepresents partial results as complete cluster results.

### Scenario D: Kafka Unavailable During Ingestion
* **Architecture Verified:** Phase 18 Durable Event Store & Phase 19 Asynchronous Decoupling.
* **Fault Injected:** Message broker publish operations throw connection exceptions (simulating Kafka broker downtime or network partition).
* **Observed Behavior:**
  1. Synchronous document ingestion through `ShardCoordinator::ingest` succeeds completely independently of Kafka.
  2. The document is immediately indexed on the local shard and queryable via search.
  3. `EventDispatcher` dequeues the domain event and attempts delivery to the broker.
  4. After exhausting configured retries (`config.max_retries`), `EventDispatcher` marks the event in `EventStore` with `EventStatus::FAILED`.
  5. The event remains durably recorded in `EventStore` in the `FAILED` state, retaining its stable `event_id`, `topic`, and `key`.

### Scenario E: Kafka Recovery and Explicit Replay
* **Architecture Verified:** Phase 19F Distributed Reliability & Replay Lifecycle.
* **Fault Injected:** Broker recovers from the outage simulated in Scenario D.
* **Observed Behavior:**
  1. **No Magic Automatic Replay:** Simply restoring broker connectivity does not cause events in `FAILED` status to transition to `PUBLISHED`. They remain in the `FAILED` state to prevent uncontrolled replays.
  2. **Explicit Replay Invocation:** Calling `EventDispatcher::replay_failed()` queries the `EventStore` for all events with status `FAILED`, re-queues them to `PENDING` with their original `event_id`, and re-dispatches them.
  3. **Preserved Identity:** The event is successfully published to the broker. In `EventStore`, its status transitions to `PUBLISHED`, preserving its exact original `event_id` and document `key`.

### Scenario F: Node Shutdown and Restart Persistence
* **Architecture Verified:** Phase 7 Shard Persistence & Node Lifecycle.
* **Fault Injected:** NodeServer is gracefully shut down (`ServerHandle::stop_and_join()`) after persisting shard data to disk, and a new NodeServer is started pointing to the same file.
* **Observed Behavior:**
  1. The existing `NodeServer` successfully writes the shard state to JSONL via `save_shard`.
  2. Graceful termination completes cleanly.
  3. A new `NodeServer` loads the persisted shard via `load_shard`.
  4. All previously ingested documents are restored with full fidelity.
  5. Documented strictly as graceful restart recovery (distinct from ungraceful crash recovery).

### Scenario G: RPC Retry Behavior (Search vs. Mutation)
* **Architecture Verified:** Phase 12 RemoteNode & Phase 14 Retry Policy.
* **Fault Injected:** Connection refused against closed ports vs. application-level 404/not-found responses.
* **Observed Behavior:**
  1. **Read Operations (Search):** When configured with `RetryPolicy{max_attempts = 3}`, transport failures trigger retries up to `max_attempts`. Retries increment `retries_total` and `per_node[id].retries` in `MetricsCollector`.
  2. **Mutation Operations (Add/Update/Delete):** Mutations do NOT retry on transport failure. They perform exactly one attempt, record circuit breaker failure if transport fails, and immediately return `is_error = true` without incrementing retry metrics.
  3. **Application Errors:** Application-level responses (such as querying a non-existent document that returns `found == false`) return a valid response and are never retried.

---

## Verification & Metric Consistency

All tests in `tests/fault_injection_integration_test.cpp` run deterministically:
- `read_failovers_total` precisely accounts for read failover events.
- `retries_total` strictly accounts for retry attempts on read RPCs.
- `load_shed_rejections_total` remains unaffected as edge load shedding is never applied to internal replication RPCs.
- No new unapproved metrics were introduced.
