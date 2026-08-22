# Phase 11: Distributed Node Abstraction

## Overview

Phase 11 introduces a transport-independent `NodeClient` interface between `ShardCoordinator` and shards, establishing the architectural boundary for future distributed search.

## What Changed

### Before (Phase 10)
```
HttpServer → ShardCoordinator → vector<unique_ptr<Shard>>
```

### After (Phase 11)
```
HttpServer → ShardCoordinator → ShardPlacement → NodeClient → LocalNode → Shard
```

## Key Concepts

### Shard vs Node

- **Shard**: Logical partition of documents. Owns DocumentStore + InvertedIndex.
- **Node**: Physical/logical host that serves one or more shards. Exposes the `NodeClient` interface.
- **Coordinator**: Routes queries and mutations across nodes.

### NodeClient Interface

A pure virtual interface that abstracts all node operations:
- `search()` — returns local postings for query terms
- `add_document()`, `update_document()`, `remove_document()` — lifecycle
- `get_document()`, `document_count()` — reads
- `save_shard()`, `load_shard()` — persistence

All request/response types are transport-independent.

### LocalNode

Wraps one or more `Shard` instances behind the `NodeClient` interface. Each operation routes to the correct shard by `shard_id`.

### ShardPlacement

Maps `shard_id → node_id`. Supports multiple shards per node:
```
Shard 0 → Node 0
Shard 1 → Node 1
Shard 2 → Node 0
```

### Multiple Shards Per Node

A `LocalNode` can host multiple shards. This is important for:
- Single-machine deployments with many shards
- Reducing physical node count while maintaining logical partitioning

### Global TF-IDF

The coordinator computes global statistics:
- `global_N = Σ local_document_count(shard)` for all shards
- `global_df(term) = Σ |postings(term, shard)|` for all shards
- `tfidf(t,d) = tf(t,d) × ln(global_N / global_df(term))`

This preserves Phase 10 ranking semantics across the new abstraction.

### Parallel Fan-Out

Search uses `std::async` to fan out to all shards concurrently. Each shard search runs independently, and results are collected via `std::future`. This demonstrates concurrent cross-shard queries and prepares for future network-based fan-out.

### Fail-Fast Model

If a node returns an error, the coordinator returns an error. No partial results. No retries.

## Routing Path

```
doc_id
  ↓
ShardRouter (doc_id → shard_id)
  ↓
ShardPlacement (shard_id → node_id)
  ↓
NodeClient (node_id → operations)
```

## Invariants Preserved

- DocumentStore state == InvertedIndex state (per shard)
- DocumentStore::size() == InvertedIndex::document_count()
- Global TF-IDF ranking unchanged
- All HTTP endpoints preserved
- Thread safety preserved
- Persistence per shard preserved

## Concurrency Model

- Mutations on different shards (possibly on different nodes) proceed independently
- Same-shard mutations serialized by shard's internal mutex
- Search is read-only across nodes
- No global mutation lock

## Persistence

- Each shard persists independently via `NodeClient::save_shard()`
- Persistence paths are per-shard
- Changing shard count for existing dataset is not supported

## Test Coverage

| Test Suite | Tests | What It Proves |
|-----------|-------|----------------|
| LocalNodeTest | 27 | Node identity, multi-shard, search, lifecycle, persistence |
| ShardPlacementTest | 10 | Shard→node mapping, validation, edge cases |
| ShardCoordinatorTest | 28 | Routing, search, TF-IDF, lifecycle, persistence (single-node) |
| DistributedCoordinatorTest | 16 | Multi-node, cross-node search, TF-IDF, persistence, lifecycle |

## Phase 12 Preparation

Phase 11 creates the exact interface that Phase 12 will implement:
- `RemoteNode` implementing `NodeClient` over network transport
- Same coordinator, same interface, different backend

The coordinator doesn't need to change — only the `NodeClient` implementation changes.
