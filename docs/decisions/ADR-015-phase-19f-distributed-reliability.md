# ADR 015: Phase 19F Distributed Reliability and Production Readiness

## Status
Accepted

## Context
Phase 19 integration of Kafka required production-readiness improvements to ensure reliable event delivery, ordered processing, and correct failure semantics without regressions in the existing synchronous replication architecture.

## Decisions

1. **Consumer Retry Model**: Implement a strict sequential retry mechanism within the `KafkaMessageBroker`. If a consumer handler fails, it blocks the consumer thread and retries the exact same message with bounded exponential backoff. This ensures ordered processing per partition and avoids data loss, without introducing complex Dead Letter Queue (DLQ) logic.
2. **Single Topic**: Migrate from three separate event topics (`documents.indexed`, `documents.updated`, `documents.removed`) to a single topic `documents.mutations`. Events are partitioned by `document_id` to guarantee ordering of mutations for a single document.
3. **Producer Reliability**: Utilize librdkafka delivery reports. The `EventDispatcher` asynchronously receives delivery success/failure via a callback and updates the `EventStore` accordingly, ensuring AT-LEAST-ONCE delivery from the local outbox.
4. **Durability Invariant**: The `RemoteEventProcessor` synchronously flushes mutations to disk (`Shard::save()`) before returning success, which triggers the Kafka offset commit. If the system crashes after save but before commit, Kafka redelivers the message and the idempotent operations ensure correct state.
5. **Observability**: Expose Kafka consumer lag via `KafkaConsumer::estimated_lag()`.

## Consequences
- **Positive**: Strict ordering is guaranteed per document. No message loss during transient network partitions with Kafka. Clean separation between local synchronous replication and remote asynchronous event dissemination.
- **Negative**: A poisoned message (permanent processing failure) will block its partition indefinitely until operator intervention.
