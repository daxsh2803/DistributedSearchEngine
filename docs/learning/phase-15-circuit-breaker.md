# Phase 15: Circuit Breaker Pattern

## Overview

Phase 15 adds a circuit breaker to prevent repeated requests to unhealthy remote nodes. This builds on Phase 14's retry policy by adding failure isolation — when a node consistently fails, the circuit breaker "opens" to fail fast without wasting time on retries.

## Key Concepts

### Why Circuit Breakers?

Without a circuit breaker, every request to a failed node goes through the full retry sequence:
- 3 attempts × (connection timeout + backoff delay) = seconds of wasted time
- During this time, the user waits even though the node is known to be unavailable
- Under load, this can cause cascading failures as threads pile up waiting for retries

With a circuit breaker:
- After N consecutive failures, the circuit "opens"
- Subsequent requests fail immediately (milliseconds, not seconds)
- The system periodically probes to detect recovery
- When the node responds successfully, the circuit "closes" again

### State Machine

```
    ┌─────────┐
    │ CLOSED  │──── failures exceed threshold ────┐
    └─────────┘                                   │
         ▲                                        ▼
         │                                  ┌─────────┐
    probe success                           │  OPEN   │
         │                                  └─────────┘
         │                                        │
         │                                  recovery timeout
         ▼                                        │
    ┌───────────┐                                 │
    │ HALF_OPEN │←────────────────────────────────┘
    └───────────┘
         │
    probe failure → back to OPEN
```

### States Explained

**CLOSED** (normal operation):
- All requests are allowed through
- Each failure increments a counter
- When failures reach the threshold, transition to OPEN

**OPEN** (blocking):
- All requests fail immediately without network calls
- After a configured timeout, allow one probe request (transition to HALF_OPEN)

**HALF_OPEN** (probing):
- Allow a limited number of probe requests
- If probes succeed → transition back to CLOSED
- If probes fail → transition back to OPEN

## Configuration

```cpp
CircuitBreakerConfig config;
config.failure_threshold = 5;       // Trip after 5 failures
config.recovery_timeout_ms = 5000;  // Wait 5s before probing
config.half_open_max_probes = 1;    // Allow 1 probe request
```

## Integration with Retry Policy

Circuit breaker and retry policy work together:

```
Request arrives
    ↓
Check circuit breaker
    ↓
OPEN? → Fail fast (no network call)
    ↓
CLOSED or HALF_OPEN? → Proceed with request
    ↓
Apply retry policy (up to max_attempts)
    ↓
Record success/failure in circuit breaker
```

**Important distinction**:
- **Retry policy**: Handles transient failures within a single request
- **Circuit breaker**: Handles persistent failures across multiple requests

## Error Classification

The circuit breaker distinguishes between:

**Transport failures** (count toward threshold):
- Connection refused
- Connection reset
- Timeout
- Network errors

**Application errors** (do NOT count):
- Document not found
- Invalid request
- Server error (500)
- Other deterministic failures

This prevents application-level errors from tripping the circuit breaker.

## Thread Safety

The circuit breaker is thread-safe:
- Internal state protected by mutex
- Safe for concurrent RemoteNode calls
- No shared mutable state across nodes

## Interaction with Phase 13

Circuit breaker integrates with existing failure semantics:
- When circuit is OPEN, request fails immediately
- Coordinator records failure in `SearchResponse::errors`
- `complete=false` when any shard's circuit is OPEN
- User sees which nodes are unavailable

## Testing

Phase 15 tests verify:
- State transitions (CLOSED → OPEN → HALF_OPEN → CLOSED)
- Failure threshold behavior
- Recovery timeout behavior
- Probe limiting in HALF_OPEN
- Thread safety under concurrent access
- Integration with RemoteNode
- Search degradation with open circuits

## Lessons Learned

1. **Separate concerns**: Circuit breaker and retry policy are independent abstractions
2. **Don't trip on application errors**: Only transport failures should open the circuit
3. **Thread safety is essential**: RemoteNode is called concurrently
4. **Probes enable recovery**: Without probes, an OPEN circuit stays open forever
5. **Fail fast saves resources**: Avoiding doomed requests improves overall system health

## Future Considerations

- Shared circuit breaker state across coordinator instances
- Adaptive thresholds based on failure patterns
- Integration with health monitoring systems
- Metrics and alerting for circuit state changes
