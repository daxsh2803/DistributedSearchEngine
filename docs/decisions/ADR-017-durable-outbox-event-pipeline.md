# ADR 017: Durable Outbox Event Pipeline

## Status
Accepted (Phase 18)

## Context
When document mutations occur within the search engine, downstream systems and remote nodes need to be notified asynchronously. If the application directly publishes events to an external message broker during the write request, a broker outage or network glitch causes one of two failure modes:
1. The write operation fails even though local shard storage succeeded, or
2. The write succeeds locally, but the event is permanently lost if the message broker rejects it or the process crashes before transmission.

We required an asynchronous event delivery architecture that decouples document mutation success from broker availability while providing at-least-once delivery guarantees across process restarts.

## Decision

We implemented a **Durable Outbox Event Pipeline** consisting of an `EventStore` lifecycle abstraction, a file-backed `PersistentEventStore`, and a bounded asynchronous `EventDispatcher`.

1. **Terminology and Scope:**
   - This architecture is a **Durable Outbox Event Pipeline**, **not** a transactional outbox.
   - The engine does not utilize relational database transactions or two-phase commit protocols across disk mutations and events.
   - Instead, the `ShardCoordinator` commits mutations to all configured synchronous replicas first; upon confirmed replica write success, it records a tracked event in the outbox before returning success to the client.

2. **Explicit Event Lifecycle States:**
   - `PENDING`: Event record created with a monotonically assigned, stable `EventId`.
   - `DISPATCHING`: The event has been claimed by the `EventDispatcher` worker thread; network transmission is actively in progress.
   - `PUBLISHED`: The message broker has acknowledged successful storage.
   - `FAILED`: All delivery retry attempts have been exhausted.

3. **Persistent JSONL Durability & Crash Recovery:**
   - `PersistentEventStore` records event state transitions into an append-only `events.jsonl` file alongside metadata in `events.meta` via atomic write-temp-then-rename flushes.
   - On process startup, `PersistentEventStore` reads `events.jsonl` and executes recovery state transitions: any event found in the `DISPATCHING` state (which was in-flight when the process crashed or terminated) is automatically reverted to `PENDING` to ensure redelivery.

4. **Bounded In-Process Dispatcher (`EventDispatcher`):**
   - Implements a bounded in-memory queue with configurable capacity (default 4096 events) drained by a dedicated worker thread.
   - Backpressure handling: If the queue is saturated, `enqueue()` blocks up to a configured timeout. If space does not become available, `enqueue()` returns `false` and increments `dispatcher_rejected`. The document mutation is **not rolled back**, preserving storage authority.

## Consequences

### Positive
- **At-Least-Once Delivery Across Restarts:** In-flight events survive node crashes and are redelivered upon restart.
- **Broker Independence:** The core search engine continues serving client writes at full speed even during total Kafka broker downtime.
- **Stable Identity:** `EventId` remains constant across retries and crash recovery, enabling idempotent deduplication downstream.

### Negative
- **Duplicate Deliveries on Crash:** An event acknowledged by the broker right before a crash may be redelivered upon restart because the local store did not persist the `PUBLISHED` state transition before termination. Downstream consumers must handle deduplication.
- **Potential Backpressure Drops:** When the in-memory queue remains full beyond the timeout, events are dropped with an error counter, requiring manual administrative replay via `replay_failed()`.
