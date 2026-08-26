# Phase 18 — Asynchronous Event / Message Pipeline

## Overview

Phase 18 introduces an internal asynchronous messaging abstraction into the Distributed Search Engine. This educational implementation demonstrates the core concepts found in systems like Apache Kafka, WITHOUT introducing external dependencies.

The phase progressively builds:
1. Message and broker abstractions (18A-18B)
2. Domain event integration (18C)
3. Asynchronous dispatch (18D)
4. Reliable delivery with retry (18E)
5. Durable persistence (18F)
6. Application integration and observability (18G)

## Architecture

```
                     HTTP Client
                         |
                         v
                     HttpServer
                         |
                         v
                   ShardCoordinator
                    /          \
                   /            \
          EventStore         Metrics
         (tracks)          (observes)
              \                /
               \              /
                v            v
            EventDispatcher
                    |
                    | bounded queue
                    | worker thread
                    |
                    v
            InMemoryMessageBroker
                    |
                    | at-least-once delivery
                    |
                    v
              Consumer Handlers
```

### Data Flow

```
1. Document Mutation (ingest/update/remove)
         |
         v
2. EventStore.create_event()  ← stable event_id assigned
         |
         v
3. EventDispatcher.enqueue_with_event()
         |
         | bounded queue (backpressure)
         v
4. Worker thread drains queue
         |
         v
5. MessageBroker.publish()
         |
         v
6. Consumer handler processes message
         |
         +---> success → mark_published()
         +---> failure → retry (up to max_retries)
                   +---> exhausted → mark_failed()
```

## Sub-Phases

| Sub-phase | Purpose |
|-----------|---------|
| 18A | Message model, InMemoryMessageBroker |
| 18B | Abstract MessageBroker interface |
| 18C | Document mutation domain events |
| 18D | Async EventDispatcher with bounded queue |
| 18E | Reliable EventStore with retry/replay |
| 18F | PersistentEventStore (durable outbox) |
| 18G | Application integration and observability |

## MessageBroker Abstraction

The `MessageBroker` abstract interface defines the contract for all broker implementations:

```cpp
class MessageBroker {
public:
    virtual Offset publish(Message message) = 0;
    virtual std::optional<Offset> publish_with_timeout(
        Message message, std::size_t timeout_ms) = 0;
    virtual void subscribe(const Topic& topic, MessageHandler handler) = 0;
    virtual void start() = 0;
    virtual void stop() = 0;
    virtual std::size_t queue_size(const Topic& topic) const = 0;
    virtual BrokerStats stats() const = 0;
    virtual std::vector<Message> dead_letters(const Topic& topic) const = 0;
    virtual bool was_processed(MessageId id) const = 0;
};
```

### InMemoryMessageBroker

The concrete in-memory implementation provides:

- **Thread-safe publish/subscribe**: Multiple producers, multiple consumers
- **Backpressure**: Bounded queue with configurable size
- **Retry**: Failed messages retried up to `max_delivery_attempts`
- **Dead-letter queue**: Messages exceeding retry limit
- **Idempotent consumers**: Optional deduplication by message ID
- **Graceful shutdown**: Drains queue, joins threads

### Delivery Semantics

**AT-LEAST-ONCE delivery** is guaranteed. A message may be delivered multiple times if:

1. Consumer crashes before acknowledging
2. Network partition occurs (in distributed deployment)
3. Timeout expires during processing

Idempotent consumers must use `Message.id` to deduplicate.

**NOT exactly-once**: Achieving exactly-once requires distributed coordination (transactional commits) which is out of scope.

## Document Events (Phase 18C)

Domain events are published after successful document mutations:

| Operation | Event Type | Topic |
|-----------|-----------|-------|
| `ingest()` | `DocumentIndexedEvent` | `documents.indexed` |
| `update()` | `DocumentUpdatedEvent` | `documents.updated` |
| `remove()` | `DocumentRemovedEvent` | `documents.removed` |

### Event Structure

```cpp
struct DocumentIndexedEvent {
    EventId event_id;      // Stable, unique identifier
    doc_id document_id;    // Document that was indexed
    std::size_t shard_id;  // Shard where document lives
};
```

### Key Properties

- **One event per logical operation**: R=2 ingest produces ONE event, not two
- **Success only**: Failed mutations produce NO events
- **Stable ID**: `event_id` never changes across retries/replay

## EventDispatcher (Phase 18D)

Bounded, lifecycle-managed asynchronous dispatch layer.

### Configuration

```cpp
struct Config {
    std::size_t max_queue_size = 4096;      // Queue capacity
    std::size_t default_timeout_ms = 100;    // Enqueue timeout
    std::size_t max_retries = 3;            // Retry attempts
    std::size_t retry_delay_ms = 0;         // Delay between retries
};
```

### Backpressure

When the queue is full:
- `enqueue()` blocks until space available or timeout
- Returns `false` if timeout expires
- **Document mutation is NOT rolled back**

### Shutdown

```
dispatcher.stop()
    → stops accepting new events
    → drains queued events
    → joins worker thread
```

Destructor calls `stop()` automatically (RAII).

## EventStore (Phase 18E)

Tracks event lifecycle from creation through delivery.

### Lifecycle States

```
PENDING → DISPATCHING → PUBLISHED
                    \→ FAILED
                         ↓
                    (replay → PENDING)
```

### Key Operations

```cpp
EventId create_event(topic, payload);      // PENDING
void mark_dispatching(EventId);             // PENDING → DISPATCHING
void mark_published(EventId);              // DISPATCHING → PUBLISHED
void mark_failed(EventId, error);          // DISPATCHING → FAILED
bool requeue(EventId);                     // FAILED → PENDING
```

### Retry/Replay

Failed events can be replayed via `EventDispatcher::replay_failed()`:

1. Queries EventStore for FAILED events
2. Requeues each (FAILED → PENDING)
3. Re-enqueues into dispatcher
4. Dispatcher retries with same event ID

## PersistentEventStore (Phase 18F)

Durable outbox implementation backed by JSONL files.

### Storage Format

```
data/events/
    events.jsonl    # Event records (one JSON object per line)
    events.meta     # Metadata (next_event_id, stats)
```

### Recovery Semantics

On construction (restart):

| State | After Recovery | Reason |
|-------|---------------|--------|
| PENDING | PENDING | Not yet dispatched |
| DISPATCHING | PENDING | Was in-flight at crash |
| PUBLISHED | PUBLISHED | Don't redeliver |
| FAILED | FAILED | Available for replay |

### Crash Safety

Uses write-temp-then-rename pattern:

1. Write to `events.jsonl.tmp`
2. Flush and close
3. Rename to `events.jsonl`

If crash occurs during write, only the temp file is corrupted.

### Thread Safety

All public methods are thread-safe. Uses `std::mutex` for synchronization.

## Application Integration (Phase 18G)

### Startup Sequence

```cpp
// Create components
PersistentEventStore store("data/events");
InMemoryMessageBroker broker(config);
EventDispatcher dispatcher(broker, store, config);

// Wire to coordinator
coordinator->set_event_store(&store);
coordinator->set_event_dispatcher(&dispatcher);

// Start event system
dispatcher.start();
broker.start();
```

### Shutdown Sequence

```cpp
// Stop in correct order
dispatcher.stop();    // 1. Stop accepting, drain queue
store.flush();        // 2. Persist remaining events
broker.stop();        // 3. Stop consumer threads
```

### Metrics Integration

Event metrics are exposed via `GET /metrics`:

```json
{
  "events_total": 42,
  "events_pending": 3,
  "events_dispatching": 1,
  "events_published": 35,
  "events_failed": 4,
  "events_retried": 2,
  "events_replayed": 1
}
```

Metrics come directly from `EventStore::stats()` — no duplication.

## Threading Model

```
HTTP Thread(s)
    |
    | enqueue()
    v
EventDispatcher Worker Thread
    |
    | broker.publish()
    v
InMemoryMessageBroker Consumer Threads
```

### Concurrency Guarantees

- **EventStore**: Thread-safe (mutex-protected)
- **EventDispatcher**: Thread-safe for producers; single worker thread
- **InMemoryMessageBroker**: Thread-safe; one consumer per topic

### No Deadlocks

- No nested locking
- Lock ordering: EventStore → EventDispatcher → Broker
- All locks acquired/released within single method scope

## Testing Strategy

### Test Suites

| Test File | Tests | Coverage |
|-----------|-------|----------|
| `message_broker_test.cpp` | 42 | Broker lifecycle, backpressure, retry, dead-letter |
| `event_store_test.cpp` | 19 | EventStore operations, concurrency |
| `event_dispatcher_test.cpp` | 30 | Dispatcher lifecycle, retry, replay |
| `persistent_event_store_test.cpp` | 21 | Persistence, recovery, durability |
| `document_event_test.cpp` | 23 | Domain events, serialization |
| `event_integration_test.cpp` | 8 | End-to-end lifecycle |

### Test Design Principles

- **Deterministic**: No arbitrary sleeps or timing assumptions
- **Isolated**: Each test uses fresh components
- **Comprehensive**: Covers R=1, concurrent access, crash recovery

## Non-Goals (Explicitly NOT Implemented)

The following are explicitly deferred to future phases:

- **Apache Kafka**: External message broker
- **Redis**: Distributed cache
- **Exactly-once delivery**: Requires distributed coordination
- **Event sourcing**: Complete event replay for state reconstruction
- **Schema registry**: Event schema versioning
- **Consumer groups**: Distributed consumer coordination
- **Cross-service events**: Inter-service communication
- **Distributed consensus**: Raft, Paxos, etc.
- **Transaction outbox pattern**: Database-level atomicity

## Known Limitations

1. **In-memory broker**: Messages lost on process restart (unless using PersistentEventStore)
2. **Single consumer per topic**: No load balancing across consumers
3. **No consumer groups**: Cannot distribute processing across instances
4. **No event replay**: Cannot replay historical events (only failed ones)
5. **No schema evolution**: Event schemas are fixed at compile time

## Future Extensions

Potential work built on Phase 18 foundation:

- **KafkaMessageBroker**: Replace InMemoryMessageBroker with Kafka
- **Consumer groups**: Distributed processing across instances
- **Event sourcing**: Rebuild state from event history
- **Schema registry**: Version and validate event schemas
- **Dead letter queue handler**: Automatic retry or manual inspection
- **Event archival**: Move old events to cold storage
- **Cross-datacenter replication**: Event replication across regions
