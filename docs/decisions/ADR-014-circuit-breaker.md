# ADR-014: Circuit Breaker Pattern

## Status

Accepted (Phase 15)

## Context

Phase 14 introduced bounded retries with exponential backoff for transient network failures. While retries help recover from temporary issues, they don't protect against persistent failures. When a remote node is genuinely unavailable (e.g., crashed, network partition), every request still attempts the full retry sequence before failing, wasting time and resources.

In distributed systems, the circuit breaker pattern prevents cascading failures by:
1. Detecting repeated failures to a dependent service
2. "Opening" the circuit to fail fast without attempting requests
3. Periodically allowing probe requests to check if the service has recovered
4. "Closing" the circuit when the service responds successfully

## Decision

Introduce a circuit breaker as a separate abstraction from the retry policy.

### State Machine

```
CLOSED → OPEN → HALF_OPEN → CLOSED
         ↑         ↓
         └─────────┘
```

**CLOSED (normal operation)**:
- All requests allowed
- Consecutive failures tracked
- Threshold exceeded → transition to OPEN

**OPEN (blocking)**:
- All requests fail fast (no network I/O)
- After recovery timeout → transition to HALF_OPEN

**HALF_OPEN (probing)**:
- Limited probe requests allowed
- Successful probe → transition to CLOSED
- Failed probe → transition to OPEN

### Configuration

```cpp
struct CircuitBreakerConfig {
    std::size_t failure_threshold = 5;      // Failures to trip
    std::size_t recovery_timeout_ms = 5000; // Time before probing
    std::size_t half_open_max_probes = 1;   // Probes in HALF_OPEN
};
```

### Integration with Retry Policy

Circuit breaker and retry policy are separate, composable abstractions:

1. **Check circuit breaker** before making a request
2. If allowed, proceed with the request (retries may apply)
3. **Record success/failure** in the circuit breaker after the operation completes
4. Transport failures count toward the circuit breaker threshold
5. Application errors (e.g., "document not found") do NOT count

### Interaction with Phase 13 Failure Semantics

- Circuit breaker does NOT change the `complete` / `errors` response structure
- When circuit is OPEN, requests fail immediately with a clear error message
- Coordinator records the failure normally in `SearchResponse::errors`
- `complete=false` when any shard's circuit is OPEN

## Consequences

### Positive
- Prevents repeated requests to genuinely unhealthy nodes
- Reduces latency for requests to known-unavailable services
- Allows automatic recovery detection
- Composable with existing retry policy
- Thread-safe by design

### Negative
- Adds complexity to RemoteNode
- Circuit breaker state is per-RemoteNode instance (not shared across coordinator)
- Recovery detection requires actual probe requests

### Future Work
- Shared circuit breaker state across coordinator instances
- Adaptive thresholds based on failure patterns
- Integration with health monitoring
- Metrics and observability for circuit state changes
