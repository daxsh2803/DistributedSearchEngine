# ADR-013: Retry and Resilience

## Status

Accepted (Phase 14)

## Context

Phase 13 introduced explicit failure reporting for distributed search, where failed shards are recorded in `SearchResponse::errors` and `complete=false` is set when any shard fails. However, transient network failures (connection refused, timeout, reset) were immediately surfaced as errors without any retry attempt.

In distributed systems, transient failures are common and often self-resolving. A node that is briefly unreachable may become available again within milliseconds. Without retries, every transient failure causes:
- Degraded search results (partial results)
- Failed writes that could succeed on retry
- Poor user experience for temporary network issues

## Decision

Introduce bounded retries with exponential backoff for transient network failures.

### Retry Policy

```cpp
struct RetryPolicy {
    std::size_t max_attempts = 3;        // Total attempts (1 = no retries)
    std::size_t initial_delay_ms = 100;  // Initial delay before first retry
    std::size_t max_delay_ms = 1000;     // Maximum delay cap
    double backoff_multiplier = 2.0;     // Exponential backoff multiplier
    bool retry_writes = false;           // Whether to retry write operations
};
```

### Error Classification

**Retryable (transient)**:
- Connection refused
- Connection reset
- Timeout
- Transport-level errors

**Not retryable (application-level)**:
- Invalid request / malformed input
- Document not found
- Document already exists
- Server errors (500)
- Unknown errors

### Write Safety

Write operations (ingest, update, delete) are **NOT retried by default** because:
1. The current API does not provide idempotency keys
2. Blind retries can cause duplicate ingestion
3. Non-idempotent operations have side effects

The `retry_writes` flag exists for future use when idempotency is added.

### Search/Read Retries

Read operations (search, document_count, get_document) are retried by default because:
1. They are idempotent (same request → same result)
2. Retries improve availability for search
3. Partial results after retry exhaustion are acceptable

### Backoff Strategy

Exponential backoff with configurable multiplier and cap:
- Attempt 0: no delay (initial request)
- Attempt 1: initial_delay_ms
- Attempt 2: initial_delay_ms × multiplier
- Attempt 3: initial_delay_ms × multiplier² (capped at max_delay_ms)

No jitter is used for deterministic behavior and simpler testing.

## Consequences

### Positive
- Transient failures are automatically recovered
- Bounded retries prevent infinite loops
- Deterministic behavior enables testing
- Configurable policy allows tuning per deployment
- Backward compatible (default: no retries)

### Negative
- Adds latency for failed requests (delay × retries)
- Increases resource usage during transient failures
- May mask persistent failures temporarily

### Future Work
- Idempotency keys for safe write retries
- Circuit breakers for persistent failures
- Adaptive backoff based on failure patterns
- Retry budgets across requests
