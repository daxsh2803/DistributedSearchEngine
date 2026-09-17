# Phase 25 Learning: Search Result Correctness & Partial-Availability Semantics

## 1. Objective and Scope

Phase 25 establishes and documents the formal behavioral contract for distributed search query processing under partial-availability conditions in the Distributed Search Engine.

As an explicitly scoped test and documentation phase, Phase 25 introduces **no runtime code changes** to search execution, scoring algorithms, replica pinning, or cluster replication. Instead, Phase 25 codifies, tests, and documents the system's intentional partial-availability semantics, error visibility models, and ranking behaviors when cluster nodes experience partial outages or mid-query degradation.

---

## 2. Distributed Search Execution Overview

Distributed search queries fan out across all cluster shards through `ShardCoordinator` via a two-phase protocol:

1. **Global Document Count Phase (`compute_global_n`):**
   Queries each shard to determine total document count across the cluster ($N = \sum N_{\text{shard}}$). For each shard, a healthy replica is selected and pinned for the duration of the query.
2. **Postings Collection Phase (`collect_postings`):**
   Dispatches term-level search requests to the pinned replica for each shard to retrieve postings lists (document IDs and term frequencies).
3. **Scoring & Aggregation:**
   Aggregates postings, computes inverse document frequency ($IDF = \ln(N / DF)$), scores matching documents ($TF \times IDF$), and intersects (AND) or unions (OR) results.

Under ideal conditions, all shard operations succeed, yielding a complete-cluster search response. Under node failure or network degradation, the coordinator behaves according to the contracts defined below.

---

## 3. Search Result Contracts

### Complete Search (`complete == true`)

When:
```json
{
  "complete": true,
  "errors": []
}
```

- **Definition:** Every required shard operation across both phases (`compute_global_n` and `collect_postings`) completed successfully across the cluster.
- **Data Guarantee:** All indexed documents matching the query criteria across all shards were inspected and evaluated.
- **Ranking Guarantee:** Document scoring ($TF \times IDF$) is computed using complete cluster-wide corpus statistics. The corpus size $N$ represents the exact aggregate document count, and document frequency $DF$ reflects exact term occurrences across all shards.
- **Equivalence:** The result is equivalent to a unified single-node index over the same data.

---

### Partial Search (`complete == false`)

When:
```json
{
  "complete": false,
  "errors": [ ... ]
}
```

- **Definition:** One or more shard operations encountered errors (such as network timeouts, connection rejections, or node unavailability) during either the document count or postings collection phase.
- **Partial Availability:** The coordinator does not fail the entire search request with an unhandled transport or application error. Instead, it returns useful partial results gathered from surviving shards.
- **Data Scope:** Returned results are **correct over the successfully searched/available data, but potentially incomplete relative to the full cluster**.
  - Documents stored exclusively on unavailable shards will be omitted.
  - AND queries evaluate intersection strictly over the available postings. Documents present on available shards that satisfy all query terms within that subset are returned.
  - OR queries evaluate the union across available postings, returning matching documents from healthy shards.
- **Ranking Semantics & Mathematical Approximation:**
  - When all shards that succeed in postings also succeeded in count, but some shards were completely unavailable from the start, $N$ reflects only the available shards and $DF$ reflects term occurrences on those available shards.
  - When a shard succeeds during the initial `compute_global_n()` phase but its pinned replica subsequently fails during `collect_postings()`, the aggregate $N$ retains that shard's document count, whereas $DF$ reflects only surviving shards.
  - In this failure window, the resulting $TF \times IDF$ score is **approximate and mathematically skewed relative to both a complete cluster and an isolated available subset**.
- **Client Responsibilities:**
  - Clients requiring strict complete-cluster ranking consistency must inspect `response.complete`.
  - If `complete == false`, callers should recognize that the ranking and document set may be partial and retry if full consistency is required.

---

## 4. Error Visibility

Failures encountered during shard fan-out are captured in the structured `errors` array of `SearchResponse`. Each failure entry identifies:

- `shard_id`: The identifier of the shard whose operation failed.
- `node_id`: The identifier of the node that encountered the failure.
- `category`: The failure phase or type (e.g., `"count_failure"`, `"search_failure"`).
- `message`: Diagnostic description of the underlying error (e.g., node unavailable, connection failure).

The presence of entries in `errors` correlates directly with `complete == false`. Callers can inspect `errors` to identify specific degraded shards, implement alerting, or route diagnostic metrics.

---

## 5. Relationship to Phase 24 Replica Pinning

Phase 24 introduced query-local replica pinning:

1. **Within-Query Pinning:**
   For each shard, the replica node selected during `compute_global_n()` is pinned for all subsequent `collect_postings()` calls within that query.
   $$\text{Replica}_{\text{Global N}}(\text{shard } s) = \text{Replica}_{\text{Postings}}(\text{shard } s)$$
2. **Prevention of Split Reads:**
   Pinning prevents mid-query failover across different replicas of the same shard between terms or between count and search phases.
3. **Failure Boundary:**
   Query-local pinning ensures that if the pinned replica fails after `compute_global_n()`, the coordinator does **not** fail over mid-query to another replica. Instead, the shard is marked as failed for that query, recording a search failure in `errors` and setting `complete = false`.
4. **Resilience vs. Consistency Boundary:**
   Pinning guarantees replica identity across phases when healthy, but does not prevent a pinned node from crashing between phases.

---

## 6. Preserved Architectural Invariants

Phase 25 strictly adheres to the core system architecture and maintains all established invariants:

1. **Synchronous All-Replica Write Authority (Phase 17):**
   Document mutations (ingest, update, remove) require synchronous success across all configured replicas in `ShardReplicaPlacement`. Synchronous replication remains the sole authority for document state.
2. **Asynchronous Kafka Side Channel (Phase 19):**
   Kafka is strictly an asynchronous publication channel for external consumers. It is not on the synchronous write path and is not authoritative for cluster search state.
3. **Durable Outbox Event Pipeline (Phase 18):**
   `PersistentEventStore` and `EventDispatcher` maintain guaranteed at-least-once asynchronous event delivery with local idempotency tracking.
4. **Query-Local Pinning (Phase 24):**
   Replica selection decisions remain pinned for the lifetime of a single search query, with no mid-query replica hopping.

---

## 7. Explicit Architectural Decision: No Runtime Redesign

Phase 25 explicitly rejects recalculating effective Global $N$ after a postings failure:

- **No Second Count Round:**
  If a shard fails during `collect_postings()`, the coordinator does not re-query surviving shards or subtract the failed shard's document count from $N$.
- **Rationale:**
  1. A second round of document counts would introduce additional network hops, increasing latency on degraded queries.
  2. The underlying state of surviving shards could change concurrently under active ingest, introducing new inconsistencies.
  3. The response is already explicitly flagged with `complete = false`. Clients requiring exact scoring are notified to retry.
  4. Attempting complex dynamic repair during read execution introduces unnecessary runtime complexity without eliminating the fundamental incompleteness of missing postings.

---

## 8. Terminology and System Classification

To maintain architectural precision and avoid misleading claims:

- **No Distributed Consensus Claims:** This system does **not** implement Raft, Paxos, Viewstamped Replication, quorum consensus, or two-phase commit (2PC).
- **No Equivalence Claims:** Partial search results are **not** claimed to be equivalent to complete-cluster queries. Partial results are:
  > *"correct over the successfully searched and available data, but potentially incomplete relative to the full cluster."*
- **Availability Semantics:** Under partial node outages, the read path prioritizes partial availability by returning available documents alongside explicit completeness flags and diagnostic failure lists.

---

## 9. Verification Test Suite

Phase 25 adds test coverage in `tests/shard_coordinator_failover_test.cpp`:

1. `Phase25_AndQueryPartialAvailability`:
   Validates multi-shard AND queries when a shard is unavailable. Confirms `resp.complete == false`, errors identify the failed shard, and AND intersection correctly matches documents across available postings without transport errors.
2. `Phase25_OrQueryPartialAvailability`:
   Validates multi-shard OR queries when a shard is unavailable. Confirms `resp.complete == false`, errors identify the failed shard, and OR union aggregates matching documents across available postings.
3. `Phase25_TfIdfScoringUnderPartialAvailability`:
   Validates the failure window where a shard succeeds in `compute_global_n()` but its pinned replica fails during `collect_postings()`. Confirms `resp.complete == false`, verifies the failure is recorded in `errors`, and asserts that the returned score reflects the exact current implementation formula ($TF \times \ln(N / DF)$ with Global $N$ including the failed shard), proving and locking in the documented mathematical behavior.
