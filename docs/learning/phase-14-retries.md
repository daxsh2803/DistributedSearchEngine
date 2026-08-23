# Phase 14: Retry and Resilience

## Overview

Phase 14 adds bounded retries with exponential backoff for transient network failures. This improves availability by automatically recovering from temporary network issues without requiring manual intervention.

## Key Concepts

### Transient vs Permanent Failures

**Transient failures** are temporary and self-resolving:
- Connection refused (server restarting)
- Connection reset (network hiccup)
- Timeout (temporary overload)
- Transport errors (network issues)

**Permanent failures** require application-level fixes:
- Invalid request (client bug)
- Document not found (data issue)
- Document already exists (duplicate)
- Server error (bug)

### Bounded Retries

Retries MUST be bounded to prevent infinite loops:
- Default: 3 total attempts (1 initial + 2 retries)
- Configurable via `RetryPolicy::max_attempts`
- Deterministic behavior (no randomness)

### Exponential Backoff

Delay between retries increases exponentially:
- Attempt 1: 100ms
- Attempt 2: 200ms
- Attempt 3: 400ms (capped at 1000ms)

This gives the failing service time to recover while avoiding thundering herd.

## Implementation Details

### RetryPolicy

Immutable configuration for retry behavior:
```cpp
RetryPolicy policy;
policy.max_attempts = 3;
policy.initial_delay_ms = 100;
policy.backoff_multiplier = 2.0;
policy.retry_writes = false;
```

### Error Classification

`RetryPolicy::classify_error()` uses string matching to categorize errors:
- "Connection failed" → ConnectionRefused
- "timeout" → Timeout
- "not found" → DocumentNotFound
- etc.

### RemoteNode Integration

RemoteNode accepts an optional `RetryPolicy`:
```cpp
RemoteNode node(0, "127.0.0.1", 8080, 30, policy);
```

Default is `RetryPolicy::no_retries()` for backward compatibility.

## Write Safety

Write operations are NOT retried by default because:
1. No idempotency keys in current API
2. Duplicate ingestion possible
3. Side effects not safe to repeat

**Important**: If you enable `retry_writes`, ensure your application handles duplicates.

## Interaction with Phase 13

Phase 14 retries are transparent to Phase 13 failure semantics:
- After retry exhaustion, failure is reported normally
- `complete=false` when any shard fails after retries
- Error metadata includes the final error message
- Global TF-IDF uses only available shard statistics

## Testing

Phase 14 tests verify:
- Retry policy configuration and delay computation
- Error classification (transient vs permanent)
- Bounded retry count
- Successful requests not retried
- Application errors not retried
- Search degradation after retry exhaustion
- Coordinator integration with retry policy

## Lessons Learned

1. **Bounded retries are essential**: Unbounded retries can cause infinite loops and resource exhaustion
2. **Write retries are dangerous**: Without idempotency, retries cause duplicates
3. **Backoff prevents thundering herd**: Exponential backoff spreads retry load
4. **Error classification matters**: Only retry transient failures, not application errors
5. **Default to safe behavior**: No retries by default, opt-in for safety

## Phase 15+ Considerations

- Idempotency keys for safe write retries
- Circuit breakers for persistent failures
- Retry budgets across requests
- Adaptive backoff based on failure patterns
- Health monitoring and alerting
