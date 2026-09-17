# Phase 16 — Observability

## Overview

Phase 16 adds lightweight, in-process observability to the distributed search engine. It introduces a `MetricsCollector` that tracks operations, latency, retries, circuit breaker events, and coordinator-level request behavior, then exposes these metrics through HTTP endpoints.

Phase 16 is strictly **additive** — it does not alter core search, write, ranking, failure, retry, or circuit breaker semantics.

The phase consists of four sub-phases:

| Sub-phase | Commit | Purpose |
|-----------|--------|---------|
| 16A + 16B | `485ef03` | MetricsCollector foundation, unit tests, and RemoteNode integration (committed together) |
| 16C | `96012c6` | ShardCoordinator metrics integration |
| 16D | `bdcf170` | HTTP observability endpoints (`/health`, `/metrics`) |

## Architecture

```
                          MetricsCollector
                         /       |        \
                        /        |         \
                 RemoteNode  ShardCoordinator  HttpServer
                     |             |              |
                     |             |              |
               node metrics   coordinator     /metrics
               retry metrics     metrics       /health
               CB metrics
                        \          |          /
                         \         |         /
                          \        |        /
                           shared collector
```

A single `MetricsCollector` instance is shared across three components:

- **RemoteNode** — records node/network-level operations (individual HTTP calls)
- **ShardCoordinator** — records coordinator-level operations (user-initiated requests)
- **HttpServer** — reads the collector to serve `/metrics`

The wiring in `main.cpp`:

```cpp
dse::MetricsCollector metrics;

auto coordinator = std::make_unique<dse::ShardCoordinator>(...);
coordinator->set_metrics(&metrics);

dse::HttpServer server(*coordinator, &metrics);
```

RemoteNode instances also receive a pointer to the same collector via `set_metrics()`. The collector is stack-allocated in `main()` and outlives all consumers.

## Phase 16A — MetricsCollector Foundation

### Design Principles

- **Lock-free atomic counters** for high-frequency operations (searches, writes, retries)
- **Mutex-protected circular buffer** for latency samples (infrequent writes)
- **Thread-safe `snapshot()`** for consistent reads
- **No external dependencies** — no Prometheus, OpenTelemetry, or logging frameworks
- **Composable** — optional `MetricsCollector*` parameter in consumers
- **Testable** — `reset()` for clean test state

### MetricsSnapshot Fields

`MetricsSnapshot` is the immutable, point-in-time view returned by `snapshot()`. All fields:

#### Node-level search metrics

One count per actual RemoteNode operation.

| Field | Type | Description |
|-------|------|-------------|
| `searches_total` | `uint64_t` | Total search operations across all nodes |
| `search_errors` | `uint64_t` | Failed search operations |
| `search_incomplete` | `uint64_t` | Searches where not all shards participated |
| `search_latency` | `LatencyStats` | Latency statistics for search operations |

#### Node-level write metrics

One count per actual RemoteNode write operation.

| Field | Type | Description |
|-------|------|-------------|
| `writes_total` | `uint64_t` | Total write operations across all nodes |
| `write_errors` | `uint64_t` | Failed write operations |

#### Retry metrics

| Field | Type | Description |
|-------|------|-------------|
| `retries_total` | `uint64_t` | Total retry attempts (excludes initial request) |

#### Circuit breaker metrics

| Field | Type | Description |
|-------|------|-------------|
| `circuit_open_events` | `uint64_t` | Times a circuit breaker transitioned to OPEN |
| `circuit_close_events` | `uint64_t` | Times a circuit breaker left OPEN state |

#### Coordinator-level search metrics

One count per user-initiated distributed search request.

| Field | Type | Description |
|-------|------|-------------|
| `coordinator_searches_total` | `uint64_t` | Total coordinator search requests |
| `coordinator_search_success` | `uint64_t` | Successful coordinator searches |
| `coordinator_search_incomplete` | `uint64_t` | Searches with partial results |
| `coordinator_search_errors` | `uint64_t` | Coordinator searches that returned errors |
| `coordinator_search_latency` | `LatencyStats` | End-to-end search latency |

#### Coordinator-level write metrics

One count per user-initiated write (ingest/update/delete).

| Field | Type | Description |
|-------|------|-------------|
| `coordinator_writes_total` | `uint64_t` | Total coordinator write requests |
| `coordinator_write_success` | `uint64_t` | Successful coordinator writes |
| `coordinator_write_errors` | `uint64_t` | Coordinator writes that returned errors |
| `coordinator_write_latency` | `LatencyStats` | End-to-end write latency |

#### Per-node metrics

| Field | Type | Description |
|-------|------|-------------|
| `per_node` | `map<size_t, NodeMetrics>` | Per-node breakdown keyed by `node_id` |

Each `NodeMetrics` entry contains:

| Field | Type | Description |
|-------|------|-------------|
| `searches` | `uint64_t` | Node search operations |
| `search_errors` | `uint64_t` | Node search errors |
| `search_incomplete` | `uint64_t` | Incomplete node searches |
| `writes` | `uint64_t` | Node write operations |
| `write_errors` | `uint64_t` | Node write errors |
| `retries` | `uint64_t` | Retry attempts against this node |
| `circuit_state` | `CircuitState` | Current breaker state (0=Closed, 1=Open, 2=HalfOpen) |
| `circuit_open_events` | `uint64_t` | Times this node's breaker opened |
| `circuit_close_events` | `uint64_t` | Times this node's breaker left open |

### Latency Statistics

`LatencyStats` is computed from a bounded circular buffer (default 1000 samples):

| Field | Type | Description |
|-------|------|-------------|
| `average_ms` | `double` | Mean latency from stored samples |
| `p99_ms` | `double` | 99th percentile latency (deterministic from samples) |
| `sample_count` | `size_t` | Number of samples in the buffer |

The buffer stores **recent samples only**, not lifetime exact percentiles. This is explicitly documented and intentional.

### Thread Safety

- Counters use `std::atomic<std::uint64_t>` — lock-free for concurrent increments
- Latency buffers and per-node maps are protected by `std::mutex`
- `snapshot()` acquires the mutex once, copies all data, releases the mutex
- No data races between concurrent `record_*()` calls and `snapshot()`

## Phase 16B — RemoteNode Metrics Integration

RemoteNode records **node/network-level** metrics — one observation per actual HTTP call to a remote node.

### What RemoteNode Records

| Operation | Metric | Recorded As |
|-----------|--------|-------------|
| `search()` | operation count | `record_search()` |
| `search()` | latency | `record_search()` |
| `search()` | success/failure | `record_search()` |
| `document_count()` | operation count | `record_search()` (read operation) |
| `get_document()` | operation count | `record_write("get")` |
| `add_document()` | operation count | `record_write("add")` |
| `update_document()` | operation count | `record_write("update")` |
| `remove_document()` | operation count | `record_write("delete")` |
| retry attempts | retry count | `record_retry()` |
| circuit breaker state changes | open/close events | `record_circuit_breaker()` |

### Retry Accounting

For `max_attempts = 3`:

| Attempt | Description | Retry recorded? |
|---------|-------------|-----------------|
| 1 | Initial request | No |
| 2 | Retry #1 | Yes |
| 3 | Retry #2 | Yes |

`retries_total` is 2 for this scenario (not 3).

### Circuit Breaker Observation

Circuit breaker state changes are observed after every operation that calls `circuit_breaker_->record_failure()` or `circuit_breaker_->record_success()`. This includes:

- `search()`
- `add_document()`
- `update_document()`
- `remove_document()`
- `get_document()`
- `document_count()`
- `save_shard()`
- `load_shard()`

The `observe_circuit_breaker()` helper detects state transitions and calls `MetricsCollector::record_circuit_breaker()` only when the state actually changes.

### Backward Compatibility

When `metrics_ == nullptr` (the default), no metrics code executes. All existing RemoteNode behavior is preserved.

### Latency Measurement

Uses `std::chrono::steady_clock` (not `system_clock`). A `start_time` variable is captured at method entry, and `elapsed_ms()` computes duration at each return point.

## Phase 16C — ShardCoordinator Metrics Integration

ShardCoordinator records **coordinator-level** metrics — one observation per user-initiated distributed request.

### Double-Counting Prevention

A single user search may contact multiple nodes. The coordinator and node metrics operate at different semantic levels:

```
User search request
    ↓
ShardCoordinator.search()    → coordinator_searches_total += 1
    ↓
    +---- RemoteNode A       → searches_total += 1
    +---- RemoteNode B       → searches_total += 1
    +---- RemoteNode C       → searches_total += 1
```

| Metric | Value | Meaning |
|--------|-------|---------|
| `coordinator_searches_total` | 1 | One user-initiated request |
| `searches_total` | 3 | Three actual node operations |

These use **different counter names** (`coordinator_*` prefix) to prevent conceptual confusion.

### What the Coordinator Records

| Operation | Counter Incremented |
|-----------|-------------------|
| `search()` success | `coordinator_search_success` |
| `search()` with partial results | `coordinator_search_incomplete` |
| `search()` error | `coordinator_search_errors` |
| `ingest()` | `coordinator_writes_total` |
| `update()` | `coordinator_writes_total` |
| `remove()` | `coordinator_writes_total` |
| Successful write | `coordinator_write_success` |
| Failed write | `coordinator_write_errors` |

### Metrics Are Observational Only

Metrics recording never affects coordinator behavior. It does not:

- change search semantics
- alter retry policy
- modify circuit breaker behavior
- affect error propagation
- change result ordering

### Latency

The coordinator measures end-to-end request latency using `std::chrono::steady_clock`, capturing start time at method entry and computing elapsed time at each return path.

## Phase 16D — HTTP Observability Endpoints

### GET /health

A **liveness endpoint**. Returns immediately without contacting nodes or performing searches.

```
HTTP/1.1 200 OK
Content-Type: application/json

{"status": "ok"}
```

This endpoint:
- does not perform a search
- does not contact remote nodes
- does not inspect shard state
- does not depend on MetricsCollector

It is strictly a process/server liveness check.

### GET /metrics

Returns the current `MetricsSnapshot` as JSON.

**When MetricsCollector is configured:**

```
HTTP/1.1 200 OK
Content-Type: application/json

{
  "searches_total": 150,
  "search_errors": 2,
  "search_incomplete": 0,
  "search_latency": {
    "average_ms": 12.5,
    "p99_ms": 45.2,
    "sample_count": 150
  },
  "writes_total": 50,
  "write_errors": 1,
  "retries_total": 3,
  "circuit_open_events": 1,
  "circuit_close_events": 1,
  "coordinator_searches_total": 45,
  "coordinator_search_success": 43,
  "coordinator_search_incomplete": 2,
  "coordinator_search_errors": 0,
  "coordinator_search_latency": {
    "average_ms": 35.8,
    "p99_ms": 120.5,
    "sample_count": 45
  },
  "coordinator_writes_total": 20,
  "coordinator_write_success": 19,
  "coordinator_write_errors": 1,
  "coordinator_write_latency": {
    "average_ms": 8.3,
    "p99_ms": 22.1,
    "sample_count": 20
  },
  "per_node": {
    "0": {
      "searches": 150,
      "search_errors": 2,
      "search_incomplete": 0,
      "writes": 50,
      "write_errors": 1,
      "retries": 3,
      "circuit_state": 0,
      "circuit_open_events": 1,
      "circuit_close_events": 1
    }
  }
}
```

**When MetricsCollector is NOT configured (nullptr):**

```
HTTP/1.1 503 Service Unavailable
Content-Type: application/json

{"error": "Metrics unavailable"}
```

### Snapshot Semantics

`/metrics` calls `metrics_->snapshot()` and serializes the result. It **never** calls `reset()`. Metrics are cumulative — repeated requests return monotonically increasing counters.

## Metrics JSON Schema

Complete JSON field mapping from `MetricsSnapshot`:

```
{
  "searches_total":                  uint64,
  "search_errors":                   uint64,
  "search_incomplete":               uint64,
  "search_latency": {
    "average_ms":                    double,
    "p99_ms":                        double,
    "sample_count":                  uint64
  },
  "writes_total":                    uint64,
  "write_errors":                    uint64,
  "retries_total":                   uint64,
  "circuit_open_events":             uint64,
  "circuit_close_events":            uint64,
  "coordinator_searches_total":      uint64,
  "coordinator_search_success":      uint64,
  "coordinator_search_incomplete":   uint64,
  "coordinator_search_errors":       uint64,
  "coordinator_search_latency": {
    "average_ms":                    double,
    "p99_ms":                        double,
    "sample_count":                  uint64
  },
  "coordinator_writes_total":        uint64,
  "coordinator_write_success":       uint64,
  "coordinator_write_errors":        uint64,
  "coordinator_write_latency": {
    "average_ms":                    double,
    "p99_ms":                        double,
    "sample_count":                  uint64
  },
  "per_node": {
    "<node_id>": {
      "searches":                    uint64,
      "search_errors":               uint64,
      "search_incomplete":           uint64,
      "writes":                      uint64,
      "write_errors":                uint64,
      "retries":                     uint64,
      "circuit_state":               int,
      "circuit_open_events":         uint64,
      "circuit_close_events":        uint64
    }
  }
}
```

This is JSON — not Prometheus exposition format. Each field name corresponds directly to the `MetricsSnapshot` struct field.

## Optional Metrics Behavior

Metrics integration is optional at every level:

```cpp
// RemoteNode — metrics disabled by default
RemoteNode node(0, "127.0.0.1", 9001);
node.set_metrics(&metrics);  // enable

// ShardCoordinator — metrics disabled by default
coordinator->set_metrics(&metrics);  // enable

// HttpServer — metrics disabled by default
HttpServer server(coordinator);            // /metrics returns 503
HttpServer server(coordinator, &metrics);  // /metrics returns data
```

When `metrics == nullptr`:
- No metrics code executes
- No counters are incremented
- `/metrics` returns HTTP 503
- Existing behavior is completely unchanged

This preserves backward compatibility — code that constructs `HttpServer(coordinator)` without a metrics pointer continues to compile and work identically.

## Thread Safety

### MetricsCollector

| Operation | Synchronization |
|-----------|----------------|
| `record_search()` | `std::atomic` for counters, `std::mutex` for latency buffer |
| `record_write()` | `std::atomic` for counters, `std::mutex` for latency buffer |
| `record_retry()` | `std::atomic` counter |
| `record_circuit_breaker()` | `std::atomic` for counters, `std::mutex` for per-node state |
| `snapshot()` | Acquires mutex once, copies all data, releases mutex |
| `reset()` | Acquires mutex, resets all state |

### HTTP /metrics

The endpoint calls `snapshot()` which produces an immutable copy. No shared mutable state exists between concurrent `/metrics` requests. The serialization (`j.dump()`) operates on the local copy.

### Concurrent Recording

Multiple threads can call `record_*()` simultaneously:
- Atomic counters are lock-free
- Mutex-protected buffers serialize only the buffer writes, not the entire request
- No per-request state is shared

## Testing

### Test Suites

Phase 16 introduces four test files:

| File | Tests | Purpose |
|------|-------|---------|
| `tests/metrics_test.cpp` | 31 | MetricsCollector foundation: counters, latency, snapshot, reset, concurrency |
| `tests/remote_node_metrics_test.cpp` | 19 | RemoteNode metrics: per-operation counting, retry counting, circuit breaker events, concurrency |
| `tests/coordinator_metrics_test.cpp` | 13 | Coordinator metrics: double-counting prevention, incomplete search, concurrent searches |
| `tests/http_metrics_test.cpp` | 13 | HTTP endpoints: health, metrics, JSON validation, 503 behavior, no-reset invariant, concurrent access |

### HTTP Metrics Tests

1. `HealthReturns200` — liveness endpoint returns 200
2. `HealthReturnsCorrectJson` — response is `{"status":"ok"}`
3. `MetricsReturns200WhenConfigured` — `/metrics` returns 200 with collector
4. `MetricsReturnsValidJson` — response parses as JSON object
5. `MetricsExposesCoordinatorSearchMetrics` — coordinator fields are present
6. `SearchReflectedInMetrics` — one search increments `coordinator_searches_total`
7. `MultiShardSearchOneCoordinatorSearch` — multi-shard search counts as 1 coordinator search
8. `NodeAndCoordinatorMetricsSeparate` — node and coordinator counters use different fields
9. `MetricsReturns503WithoutCollector` — no collector → 503 with error JSON
10. `MetricsDoesNotResetCounters` — two consecutive `/metrics` calls return same values
11. `ConcurrentMetricsRequestsAreSafe` — 5 threads request `/metrics` concurrently
12. `ExistingRoutesStillWork` — `/search` and `/health` unaffected
13. `MetricsExposesAllSnapshotFields` — verifies every `MetricsSnapshot` field is present in JSON

## Validation Results

Reported during Phase 16 completion:

```
766/766 tests passed (100%)
  — 690 original tests (no regressions)
  — 31 metrics_test tests
  — 19 remote_node_metrics_test tests
  — 13 coordinator_metrics_test tests
  — 13 http_metrics_test tests

Compiler warnings: 0
git diff --check: CLEAN
```

## Git History

| Commit | Description |
|--------|-------------|
| `485ef03` | `feat: integrate metrics into remote node` — Phase 16A (MetricsCollector foundation + tests) and 16B (RemoteNode integration + tests). 1822 lines added across 7 files. |
| `96012c6` | `feat: add coordinator metrics` — Phase 16C (ShardCoordinator integration + tests). 703 lines added across 6 files. Coordinator-level counters with `coordinator_*` prefix. |
| `bdcf170` | `feat: add HTTP observability endpoints` — Phase 16D (HttpServer `/health` and `/metrics` endpoints + tests). 516 lines added across 5 files. |

## Design Decisions

1. **Observational only** — Metrics never alter control flow, search semantics, error propagation, retry policy, or circuit breaker behavior.

2. **Thread-safe by construction** — Atomic counters for high-frequency operations; mutex-protected buffers for latency and per-node state.

3. **Cumulative metrics** — Counters never reset during normal operation. `reset()` exists only for testing.

4. **Snapshot-based reads** — `snapshot()` produces a consistent, immutable copy. No locking during serialization.

5. **Dual semantic levels** — RemoteNode metrics represent actual network operations; coordinator metrics represent logical user requests. Separate counter names prevent conceptual double-counting.

6. **Optional integration** — `MetricsCollector*` is always nullable. Default `nullptr` disables metrics with zero overhead.

7. **Liveness endpoint** — `/health` is intentionally minimal. It does not perform dependency checks, which would defeat its purpose as a liveness probe.

8. **JSON format** — `/metrics` serves raw `MetricsSnapshot` JSON. No Prometheus format, no separate schema. Each JSON field directly maps to a `MetricsSnapshot` struct field.

9. **Circuit breaker observation** — State transitions are explicitly observed across all RemoteNode operations, not just search. The `observe_circuit_breaker()` helper is called after every `record_failure()` / `record_success()` call.

10. **Latency buffer size** — Default 1000 samples. P99 is computed deterministically from stored samples. This is a recent-sample statistic, not a lifetime exact percentile.

## Non-Goals

Phase 16 deliberately does **not** implement:

- Prometheus exposition format
- Grafana dashboards
- Alerting rules or thresholds
- Persistent metrics storage
- Historical time-series data
- Metrics aggregation across server instances
- Authentication or authorization for observability endpoints
- Request tracing or distributed tracing
- Structured logging framework
- OpenTelemetry integration

## Future Extensions

Potential directions for future phases:

- **Prometheus exposition** — Add a `/metrics/prometheus` endpoint serving text format
- **Grafana dashboards** — Pre-built dashboards for search latency, error rates, circuit breaker state
- **Alert thresholds** — Configurable thresholds for circuit breaker trips, error rate spikes
- **Time-series storage** — Persist metric snapshots for historical analysis
- **Per-shard metrics** — Break down metrics by shard_id in addition to node_id
- **Multi-instance aggregation** — Aggregate metrics across multiple coordinator processes
