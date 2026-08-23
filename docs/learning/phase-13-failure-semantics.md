# Phase 13: Distributed Failure Semantics & Health

## Overview

Phase 13 makes distributed search explicitly failure-aware. Instead of silently dropping failed shards, the coordinator now reports degradation metadata so clients can distinguish between "no results" and "results may be incomplete."

## Key Concepts

### Complete vs Degraded Search

- **Complete**: All shards participated successfully
- **Degraded**: One or more shards failed, results may be incomplete

### Failure Information

Every search response can include:
- `complete` (boolean): Whether all shards participated
- `errors` (array): Per-shard failure details with node_id, shard_id, category, message

### Global TF-IDF Under Failure

When a shard fails:
- Its document count is excluded from `global_N`
- Its postings are excluded from `global_df`
- Ranking uses available statistics only
- The response is marked as degraded

This means ranking may not be globally optimal, but it's honest about its limitations.

## Implementation Details

### Coordinator Changes

The `ShardCoordinator::search()` method now:
1. Collects postings from all shards concurrently
2. Records failures in `SearchResponse::errors`
3. Sets `SearchResponse::complete = false` when failures occur
4. Computes global TF-IDF from available shards only

### Error Propagation

**Search**: Partial results with degradation metadata
**Writes**: Strict error propagation (failed write = error response)

### Node Health

No persistent health state. Failed requests are recorded but nodes can recover immediately by becoming reachable again.

## Testing

Added comprehensive tests covering:
- All shards available → `complete = true`
- One/multiple shards unavailable → `complete = false`, partial results
- Global TF-IDF with missing shards
- Write failure propagation
- Node recovery after failure
- AND/OR search with degraded shards
- Error message diagnostic information

## Lessons Learned

1. **Silent failures are dangerous**: Clients cannot make informed decisions without knowing about partial failures
2. **Global statistics under failure**: Must be explicit about incomplete data rather than fabricating statistics
3. **Simple health model works**: For Phase 13, no persistent health state is needed—just record failures and let nodes recover naturally
4. **Partial results are useful**: Search remains functional even with unavailable shards, as long as degradation is explicit

## Phase 14+ Considerations

- Circuit breakers for repeated failures
- Retry policies with exponential backoff
- Health monitoring and alerting
- Consistent hashing for shard rebalancing
- Partial result policies (client-configurable)
