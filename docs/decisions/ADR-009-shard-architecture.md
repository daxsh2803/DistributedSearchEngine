# ADR-009: In-Process Shard Architecture

## Status

Accepted — Phase 10

## Context

By Phase 9, the search engine is a single-process, in-memory system with local persistence. All documents live in one `InvertedIndex` and one `DocumentStore`. The `IngestionService` uses a single `mutation_mutex_` to serialize all writes globally.

As the document volume grows, a single index becomes a bottleneck:
- Memory pressure on one node
- Write serialization across all documents
- No path toward distribution

The project's long-term goal is a genuinely distributed search engine with sharding, replication, and fault tolerance. Before introducing networking, the architecture needs clean shard boundaries.

## Decision

Introduce an in-process shard architecture with:

1. **Shard** — owns one `DocumentStore` + one `InvertedIndex` + one mutation mutex
2. **ShardRouter** — deterministic `doc_id → shard_index` via `std::hash<doc_id>{}(id) % shard_count`
3. **ShardCoordinator** — routes writes, performs cross-shard search with global TF-IDF

The shard count is fixed at construction time. Changing shard count for an existing dataset is NOT supported in Phase 10 (data migration deferred to later phases).

## Shard Boundary

```
Shard
  ├── DocumentStore   (source of truth)
  ├── InvertedIndex   (derived searchable state)
  └── mutation_mutex_ (per-shard write serialization)
```

Each shard is self-contained. The coordinator never acquires shard-level locks directly.

## Routing Strategy

```
std::hash<doc_id>{}(id) % shard_count
```

- Deterministic: same ID → same shard, always
- Uniform distribution for typical document ID patterns
- Thread-safe by construction (no mutable routing state)
- Simpler than consistent hashing (acceptable for fixed shard count)

## Concurrency Model

```
Shard 0 mutation_mutex_  ← serializes writes within Shard 0
Shard 1 mutation_mutex_  ← serializes writes within Shard 1
Shard 2 mutation_mutex_  ← serializes writes within Shard 2
```

- Mutations on different shards proceed independently
- Mutations on the same shard are serialized
- Search (read-only) is concurrent across all shards
- No global mutation mutex

## Cross-Shard Search

The coordinator performs global TF-IDF scoring:

```
global_N = sum(shard.document_count() for each shard)
global_df(term) = sum(shard.index().postings(term).size() for each shard)

tfidf(t,d) = tf(t,d) × ln(global_N / global_df(t))
```

This ensures scores are comparable across shards.

## Persistence

Each shard owns its persistence path:

```
data/shard-0/documents.jsonl
data/shard-1/documents.jsonl
data/shard-2/documents.jsonl
```

Startup recovery loads each shard independently and rebuilds its index from its DocumentStore.

## HTTP Compatibility

All existing endpoints are preserved:
- `GET /search` — routed through ShardCoordinator
- `POST /documents` — routed through ShardRouter to owning shard
- `PUT /documents/:id` — routed through ShardRouter to owning shard
- `DELETE /documents/:id` — routed through ShardRouter to owning shard

HTTP does not know which shard owns a document.

## Single-Shard Compatibility

When `shard_count = 1`, behavior is equivalent to Phase 9:
- Same lifecycle semantics
- Same search semantics
- Same ranking semantics
- Same persistence semantics

## Consequences

### Positive
- Clean shard boundaries ready for future distribution
- Independent shard mutations improve write throughput
- Global TF-IDF ensures correct cross-shard ranking
- Single-shard backward compatibility preserves existing behavior

### Negative
- Score computation requires collecting postings from all shards (network cost in future)
- Fixed shard count requires data migration for scaling (deferred)
- Per-shard persistence paths multiply with shard count

### Risks
- If shards become unevenly sized, some shards become hotspots (mitigated by good hash distribution)
- Cross-shard search reads all shards even for targeted queries (acceptable for in-process; needs optimization for distributed)

## Alternatives Considered

1. **Consistent hashing** — better for dynamic shard counts, but more complex; deferred to Phase 11
2. **Global mutation mutex** — simpler but serializes all writes; rejected because independent shard mutations are a key benefit
3. **Per-shard TF-IDF (no global stats)** — simpler but scores are incomparable across shards; rejected because correctness matters more

## Phase 11 Direction

Phase 10 creates clean boundaries for Phase 11:
- `Shard` becomes the unit extracted into a separate process
- `ShardRouter` evolves to consistent hashing
- `ShardCoordinator` becomes the network query coordinator
- Per-shard persistence becomes per-node persistence
