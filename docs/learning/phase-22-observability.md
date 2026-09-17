# Phase 22 Learning: Operational Metrics and Observability Integration

## 1. Overview

Phase 22 integrated end-to-end operational observability across all search engine subsystems into the existing `GET /metrics` HTTP endpoint. Prior to Phase 22, metrics collection was confined to coordinator and node-level request counters established in Phase 16. Phase 22 expanded visibility to cover the asynchronous event pipeline, outbox lifecycles, dispatcher queue depth, and Kafka consumer group lag and retries without introducing third-party monitoring dependencies.

---

## 2. Subsystem Metrics Architecture

### 1. In-Process Metrics Collector (`MetricsCollector` — Phase 16 Foundation)
- Lock-free atomic counters for latency-sensitive operations: `searches_total`, `writes_total`, `search_errors`, `write_errors`, `retries_total`, and circuit breaker transitions.
- Mutex-protected circular latency buffers providing deterministic average and $P_{99}$ latency statistics for both coordinator-level user requests and node-level RPC operations.

### 2. Event Outbox Metrics (`EventStore` — Phase 18)
When an `EventStore` is configured on `HttpServer`, `/metrics` exposes outbox lifecycle counts:
- `events_total`: Total domain events recorded.
- `events_pending`: Events awaiting dispatch.
- `events_dispatching`: Events actively in-flight with the dispatcher worker.
- `events_published`: Events confirmed delivered by the broker.
- `events_failed`: Events that permanently exhausted delivery attempts.
- `events_retried`, `events_replayed`: Replay and retry counts.

### 3. Dispatcher Pipeline Metrics (`EventDispatcher` — Phase 22)
When an `EventDispatcher` is attached to `HttpServer`, `/metrics` exposes internal queue health:
- `dispatcher_enqueued`: Cumulative events accepted into the in-memory queue.
- `dispatcher_pending`: Instantaneous in-memory queue depth.
- `dispatcher_rejected`: Events dropped due to queue capacity timeouts (backpressure) or shutdown.
- `dispatcher_broker_errors`: Exceptions or transport rejections during `broker.publish()`.
- `dispatcher_retried`: Retries executed by the dispatcher worker thread.

### 4. Consumer and Message Broker Metrics (`MessageBroker` — Phase 22)
When a `MessageBroker` is attached to `HttpServer`, `/metrics` exposes consumer group progress:
- `consumer_lag`: Estimated partition lag across assigned topics.
- `consumer_messages_consumed`: Cumulative messages passed to consumer handlers.
- `consumer_messages_acked`: Consumer handler executions returning `true`.
- `consumer_messages_nacked`: Consumer handler executions returning `false` / retrying.

---

## 3. Key Design Refactors and Commits

### Commit 1: `905df70`
`feat(metrics): add Phase 22 operational metrics for EventDispatcher and MessageBroker`
- **Polymorphic `consumer_lag()` Abstraction:**
  Introduced `virtual std::uint64_t consumer_lag() const { return 0; }` on the abstract base class `MessageBroker`.
  `KafkaMessageBroker` overrides this to query `KafkaConsumer::estimated_lag()`.
  `InMemoryMessageBroker` inherits the default zero implementation.
- **Decoupled `HttpServer`:**
  Removed all Kafka-specific header inclusions, `#ifdef DSE_KAFKA_ENABLED` compile-time guards, and `dynamic_cast<KafkaMessageBroker*>` checks from `src/http_server.cpp`. `HttpServer` queries `broker_->consumer_lag()` purely through polymorphic dispatch.
- **Wire-up:**
  Updated `src/main.cpp` to pass `&dispatcher` and `&broker` to `HttpServer`. Added unit tests in `tests/http_metrics_test.cpp` verifying metric omission when unconfigured, presence when configured, and concurrent scrape safety.

### Commit 2: `1def09d`
`fix(metrics): expose Kafka consumer retry count`
- **Audit Discovery:** An audit of `KafkaMessageBroker::stats()` revealed that `s.messages_retried` was hardcoded to `0` with a stale comment, even though the consumer retry loop actively incremented an atomic `messages_nacked_` counter. Consequently, `consumer_messages_nacked` reported 0 under Kafka despite active retries.
- **The Fix:** Updated `KafkaMessageBroker::stats()` to report `s.messages_retried = messages_nacked_.load(std::memory_order_relaxed);`.
- **Regression Verification:** Extended `KafkaConsumerTest.SequentialRetryBackoff` in `tests/kafka_message_broker_test.cpp` to assert that 3 failed consumer handler attempts correctly report `messages_retried == 3u`.

---

## 4. Verification and Test Evidence

- **`http_metrics_test`:** 17/17 tests passing, verifying correct JSON field population, optional field suppression, and thread-safe concurrent scrapes.
- **`metrics_test` & `coordinator_metrics_test`:** 44/44 tests passing.
- **Full Repository CTest:** 1051/1051 tests passing across standard and Kafka-enabled builds.

---

## 5. Architectural Lessons

1. **Polymorphic Defaults Preserve In-Memory Testability:** Placing default zero-cost methods (`consumer_lag() { return 0; }`) on the base interface allows high-level presentation layers (`HttpServer`) to remain completely agnostic of whether Kafka or an in-memory double is running.
2. **Unified JSON Without External Daemons:** A well-structured in-process JSON endpoint provides actionable operational telemetry without requiring external scrapers or heavyweight telemetry frameworks during development.
