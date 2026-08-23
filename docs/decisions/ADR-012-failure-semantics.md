# ADR-012: Distributed Failure Semantics

## Status

Accepted (Phase 13)

## Context

Phase 12 introduced `RemoteNode` for real HTTP transport between the coordinator and nodes. During testing and review, we discovered that the coordinator silently dropped failed shards from search results, making it impossible for clients to distinguish between:

- "There are only 3 matching documents"
- "There may be more documents, but one shard was unavailable"

This is unacceptable for a distributed search system where correctness must be transparent.

## Decision

Introduce explicit degradation metadata in search results:

```json
{
  "results": [...],
  "total": 5,
  "complete": false,
  "errors": [
    {
      "node_id": 1,
      "shard_id": 3,
      "category": "connection_failure",
      "message": "connection refused"
    }
  ]
}
```

### Failure Model

**Search**: Partial results + explicit degradation metadata
- Successful shards contribute normally
- Failed shards are recorded in `errors`
- `complete = false` when any shard fails
- Global TF-IDF computed from available shards only
- No fabricated statistics for missing shards

**Writes**: Strict error propagation
- Failed writes return `is_error = true`
- No silent degradation

### Node Health

- No persistent health state
- No circuit breakers
- No automatic retries
- Failed request → record error → next request may retry
- Node recovery is automatic (just become reachable again)

## Consequences

### Positive
- Clients can make informed decisions about result quality
- Search remains useful even with partial failures
- Simple implementation without complex distributed systems machinery

### Negative
- Search ranking may be skewed when shards are unavailable
- Clients must handle partial results appropriately

### Future Work
- Phase 14+: Circuit breakers, retries, partial result policies
- Health monitoring and alerting
- Consistent hashing for shard rebalancing
