# Phase 10 — In-Process Shard Architecture

## Overview

Phase 10 partitions the search engine into N independent in-process shards.
Each shard owns a subset of documents and maintains its own searchable index.
A coordinator routes document operations and performs cross-shard search with
global TF-IDF statistics.

## Why Sharding Comes After Phase 9

The system needed:
- Thread-safe components (Phase 8)
- Complete document lifecycle (Phase 9)
- Clean ownership boundaries (DocumentStore = source of truth, InvertedIndex = derived state)

Before sharding, the single-node system had:
- One global mutation mutex bottleneck
- No path toward distribution
- All documents in one index

## Architecture

```
                    HttpServer
                        |
                        v
                ShardCoordinator
                 /      |       \
                /       |        \
               v        v         v
           Shard 0   Shard 1   Shard 2
             |         |         |
        DocumentStore DocumentStore DocumentStore
        InvertedIndex InvertedIndex InvertedIndex
        mutex         mutex        mutex
        persistence   persistence  persistence
```

## Key Components

### ShardRouter

Deterministic document-to-shard routing:

```cpp
std::hash<doc_id>{}(id) % shard_count
```

- Same ID always maps to same shard
- Uniform distribution
- No mutable state (thread-safe by construction)

### Shard

Self-contained ownership boundary:

```cpp
class Shard {
    DocumentStore store_;
    InvertedIndex index_;
    std::unique_ptr<SharedMutex> mutation_mutex_;
    std::string data_path_;
};
```

Lifecycle operations maintain the invariant:
```
DocumentStore state == InvertedIndex state
```

### ShardCoordinator

Routes writes, performs cross-shard search:

- `ingest()` → route to owning shard → shard.add_document()
- `update()` → route to owning shard → shard.update_document()
- `remove()` → route to owning shard → shard.remove_document()
- `search()` → collect postings from all shards → global TF-IDF → merge → rank

## Global TF-IDF

When searching across shards, TF-IDF statistics must be global:

```
global_N = Σ shard.document_count()
global_df(term) = Σ shard.index().postings(term).size()
tfidf(t,d) = tf(t,d) × ln(global_N / global_df(t))
```

This ensures scores are comparable across shards.

## Concurrency Model

Per-shard mutation serialization:
```
Shard 0: unique lock for writes, shared lock for reads
Shard 1: unique lock for writes, shared lock for reads
Shard 2: unique lock for writes, shared lock for reads
```

Mutations on different shards proceed independently.
Search remains concurrent across all shards.

## Persistence

Per-shard JSONL files:
```
data/shard-0/documents.jsonl
data/shard-1/documents.jsonl
data/shard-2/documents.jsonl
```

Startup recovery loads each shard independently and rebuilds its index.

## Fixed Shard Count

The shard count is fixed at construction time. Changing it for an existing
dataset is NOT supported in Phase 10. Data migration is deferred to a
later phase.

## Single-Shard Compatibility

When `shard_count = 1`, behavior is identical to Phase 9:
- Same lifecycle semantics
- Same search semantics
- Same ranking semantics
- Same persistence semantics
- Same HTTP API behavior

## HTTP Compatibility

All existing endpoints preserved:
- `GET /search` — cross-shard search via ShardCoordinator
- `POST /documents` — write routed to owning shard
- `PUT /documents/:id` — write routed to owning shard
- `DELETE /documents/:id` — write routed to owning shard

HTTP does not know which shard owns a document.

## Tests Added

### ShardRouter (11 tests)
- Deterministic routing
- Distribution balance
- Edge cases (zero shards, large count, max doc_id)
- Thread safety

### Shard (29 tests)
- Add/get/contains/size
- Content validation
- Update lifecycle
- Delete lifecycle
- Local search verification
- Persistence round-trip
- Concurrency (add, read+write)
- Posting list sorted invariant

### ShardCoordinator (28 tests)
- Write routing correctness
- Create/read/update/delete
- Cross-shard OR search
- Cross-shard AND search
- Global TF-IDF ranking
- Limit application
- Tie-breaking by doc_id
- Single-shard compatibility
- Unrelated document preservation
- Persistence round-trip
- Error responses

## Phase 10 Non-Goals

- Networking between shards
- Consistent hashing
- Data migration
- Replication
- Fault tolerance
- Distributed coordination
- Containerization

## Phase 11 Direction

Phase 10 creates clean boundaries for Phase 11:
- Shard → separate process/node
- ShardRouter → consistent hashing
- ShardCoordinator → network query coordinator (scatter-gather)
- Per-shard persistence → per-node persistence
