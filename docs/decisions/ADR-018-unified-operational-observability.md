# ADR 018: Unified End-to-End Operational Observability

## Status
Accepted (Phase 22)

## Context
By Phase 21, the Distributed Search Engine possessed multiple decoupled subsystems:
1. Synchronous query and replica write routing (`ShardCoordinator`),
2. In-memory circuit breakers and latency tracking (`MetricsCollector`),
3. Durable asynchronous outbox storage (`EventStore`),
4. Bounded worker queue dispatching (`EventDispatcher`), and
5. Message broker consumer group processing (`MessageBroker` / `KafkaMessageBroker`).

However, operational visibility across these layers was fragmented. The existing `GET /metrics` endpoint only reported coordinator and node-level search/write metrics from Phase 16. Operators running multi-node clusters had no unified visibility into event queue depths, outbox failures, consumer lag, or Kafka consumer handler retries without introducing third-party monitoring daemons.

## Decision

We unified operational metrics into the existing `GET /metrics` endpoint by extending the existing in-process metrics architecture while preserving clean component boundaries.

1. **Plumbing Optional Observability Pointers to `HttpServer`:**
   - `HttpServer` was extended to optionally accept `EventStore*`, `EventDispatcher*`, and `MessageBroker*`.
   - When any component is absent (`nullptr`), its associated metrics block is cleanly omitted from the output JSON, ensuring full backward compatibility.

2. **Polymorphic `consumer_lag()` Abstraction:**
   - Instead of coupling `HttpServer` to Kafka headers or using conditional preprocessor macros, `MessageBroker` defines a virtual method:
     ```cpp
     virtual std::uint64_t consumer_lag() const { return 0; }
     ```
   - `KafkaMessageBroker` overrides this method by querying `KafkaConsumer::estimated_lag()`.
   - `InMemoryMessageBroker` inherits the default implementation returning `0`.
   - `HttpServer` queries `broker_->consumer_lag()` polymorphically with zero Kafka-specific headers, zero `#ifdef DSE_KAFKA_ENABLED` blocks, and zero `dynamic_cast`.

3. **Kafka Consumer Retry Accounting:**
   - In `KafkaMessageBroker`, sequential exponential backoff retries increment atomic `messages_nacked_`.
   - `KafkaMessageBroker::stats()` explicitly maps `messages_retried = messages_nacked_.load(std::memory_order_relaxed)`.
   - This ensures `consumer_messages_nacked` in `/metrics` accurately reflects active consumer handler retries under Kafka.

4. **No External Monitoring Infrastructure:**
   - All metrics remain in-process and dependency-free. No external scraping daemons, Prometheus format exporters, or OpenTelemetry SDKs were introduced.

## Consequences

### Positive
- **Single-Pane-of-Glass Visibility:** A single HTTP `GET /metrics` request provides complete end-to-end visibility from client write latency and circuit breaker status down to outbox state, dispatcher queue depth, and Kafka consumer lag.
- **Architectural Decoupling:** `HttpServer` remains entirely unaware of whether the underlying messaging implementation is Kafka or an in-memory test double.
- **Strict Backward Compatibility:** All pre-existing metrics fields and schemas remain unchanged.

### Negative
- **Poll-Based Inspection:** Operators must query `/metrics` over HTTP periodically; there is no built-in push alert mechanism.
