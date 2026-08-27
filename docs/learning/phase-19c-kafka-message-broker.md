# Phase 19C — Kafka Message Broker

## Objective

Implement `KafkaMessageBroker` as a concrete `MessageBroker` implementation backed by Apache Kafka via `KafkaClient`. This allows the `EventDispatcher` to publish document events to Kafka for external consumption while maintaining the same `MessageBroker` interface used by `InMemoryMessageBroker`.

## Motivation

Phase 18 established the `MessageBroker` abstraction and `InMemoryMessageBroker` for in-process event delivery. While useful for testing and development, the in-memory broker does not provide:

- **Durability**: messages are lost when the process crashes.
- **External consumption**: only in-process consumers can receive events.
- **Scalability**: bounded by single-process memory and CPU.

Kafka provides a production-grade message broker that solves all three limitations. By implementing `KafkaMessageBroker` against the existing `MessageBroker` interface, the `EventDispatcher` can switch from in-memory to Kafka delivery without any code changes.

## Architecture

```
EventDispatcher
    |
    | publish(Message)
    v
MessageBroker (abstract)
    |
    +-- InMemoryMessageBroker (in-memory, for tests)
    |
    +-- KafkaMessageBroker (Kafka, for production)
            |
            | produce_async(topic, payload, key)
            v
        KafkaClient
            |
            | librdkafka C++ API
            v
        Kafka broker (Docker, localhost:9094)
```

The `EventDispatcher` programs against the `MessageBroker` interface. It does not know whether it is using the in-memory or Kafka implementation. This is the core design principle: **swappable delivery backend**.

## Publish Semantics

### What `publish()` means

`MessageBroker::publish()` means: **"the broker implementation accepted the message for delivery"**. It does NOT mean: **"the message has been confirmed delivered and consumed"**.

This is consistent across both implementations:

| Implementation | `publish()` returns after... |
|----------------|------------------------------|
| `InMemoryMessageBroker` | Message pushed to internal queue |
| `KafkaMessageBroker` | Message accepted into librdkafka producer queue |

In both cases, the actual delivery (consumer processing / Kafka broker acknowledgement) happens asynchronously afterward.

### Mapping to KafkaClient

```cpp
Offset KafkaMessageBroker::publish(Message message) {
    // 1. Accept into librdkafka's producer queue
    bool accepted = client_->produce_async(
        message.topic, message.payload, /*key=*/"");

    if (!accepted) {
        throw std::runtime_error("produce_async rejected message");
    }

    // 2. Return internal offset (caller ignores it)
    return next_offset_.fetch_add(1);
}
```

The key insight is that `librdkafka::Producer::produce()` is an **asynchronous operation** — it returns immediately after accepting the message into its internal queue. The actual TCP send to Kafka happens in a background thread. This maps directly to the `MessageBroker::publish()` contract.

### Delivery Reports

`librdkafka` provides delivery reports through a callback. These reports indicate whether the message was actually delivered to the Kafka broker. The `KafkaMessageBroker` processes these reports in a dedicated poll thread.

However, **delivery reports do NOT affect the EventStore lifecycle**. The EventStore marks an event as `PUBLISHED` when `publish()` returns successfully. This is consistent with `InMemoryMessageBroker` behavior where `publish()` returns before the consumer processes the message.

## Kafka Message Key

The current implementation uses an empty key for all messages:

```cpp
client_->produce_async(topic, payload, /*key=*/"");
```

This means Kafka will round-robin messages across partitions. For Phase 19C, this is sufficient because:

1. Consumer groups are deferred to Phase 19D.
2. Partition-level ordering is not required yet.
3. The document events already contain their `document_id` in the JSON payload.

**Future enhancement (Phase 19D+):** Use `document_id` as the Kafka message key to ensure all events for the same document are sent to the same partition, enabling partition-level ordering.

## Threading Model

```
EventDispatcher worker thread
    |
    | publish()
    v
KafkaMessageBroker
    |
    +-- publish() thread (EventDispatcher worker)
    |
    +-- poll thread (KafkaMessageBroker owns)
    |       |
    |       | KafkaClient::poll() periodically
    |       | processes delivery reports
    |       v
    +-- librdkafka background threads
            |
            | TCP sends to Kafka broker
            v
        Kafka broker
```

### Poll Thread

`KafkaMessageBroker` owns a dedicated poll thread that calls `KafkaClient::poll()` periodically (default: 50ms interval). This thread processes librdkafka delivery reports.

The poll thread is started **lazily** on the first `publish()` call. This avoids unnecessary thread creation if the broker is constructed but never used.

### Thread Safety

- `publish()` and `publish_with_timeout()` are safe for concurrent calls (EventDispatcher may call from its worker thread).
- `stop()` must be called after all publishers have quiesced.
- The destructor calls `stop()` if not already stopped.

## Shutdown Sequence

```
1. EventDispatcher::stop()
   - Sets stopping_ flag
   - Drains queue (publishes remaining events)
   - Joins worker thread
   - Returns

2. KafkaMessageBroker::stop()
   - Sets poll_running_ = false
   - Joins poll thread
   - Flushes pending messages (client_->flush())
   - Closes Kafka client (client_->close())
   - Returns

3. KafkaMessageBroker destructor
   - Calls stop() if not already stopped
   - Destroys KafkaClient (RAII)
```

**Critical ordering:** The EventDispatcher must stop before the KafkaMessageBroker. This ensures no publish calls are in-flight when the broker is flushed and closed.

In the application lifecycle (`main.cpp`):

```cpp
// Destruction order (reverse of construction):
dispatcher.stop();      // 1. Stop dispatching
broker->stop();         // 2. Stop broker (flush + close)
// dispatcher destroyed  // 3. Destroy dispatcher
// broker destroyed      // 4. Destroy broker
```

## Consumer Methods (Phase 19D)

The `MessageBroker` interface includes consumer-side methods:

- `subscribe()`
- `start()`
- `queue_size()`
- `dead_letters()`
- `was_processed()`

In `KafkaMessageBroker`, these are **minimal stubs**:

- `subscribe()`: No-op (consumer groups deferred to Phase 19D).
- `start()`: Starts the poll thread for delivery reports.
- `queue_size()`: Returns librdkafka's outqueue length (proxy metric).
- `dead_letters()`: Returns empty vector (dead-letter tracking is EventStore's responsibility).
- `was_processed()`: Returns `false` (idempotency checking deferred to Phase 19D).

**Why stubs?** Implementing Kafka consumer groups requires:

- Consumer group coordination
- Partition assignment strategies
- Offset management (commit/seek)
- Rebalance handling
- Consumer recovery

These are substantial architectural concerns that belong in Phase 19D, not 19C.

## Failure Handling

### `publish()` Rejection

If `produce_async()` returns `false` (producer queue full or client closed), `publish()` throws `std::runtime_error`. This allows `EventDispatcher` to apply its existing retry policy:

```cpp
// EventDispatcher::publish_to_broker()
try {
    broker_.publish(std::move(msg));
    return true;  // success
} catch (...) {
    return false;  // failure → retry
}
```

### Asynchronous Delivery Failure

If `produce_async()` succeeds (message accepted into librdkafka queue) but Kafka delivery subsequently fails (broker unavailable, topic doesn't exist, etc.), the delivery report callback fires with an error.

**This does NOT affect the EventStore lifecycle.** The event is already marked `PUBLISHED`. This is consistent with `InMemoryMessageBroker` where `publish()` returns before consumer processing.

**Future enhancement:** Phase 19E+ could introduce a delivery confirmation callback that updates EventStore status based on Kafka delivery reports.

### No Independent Retries

`KafkaMessageBroker` does NOT implement its own retry policy. Retries are handled by:

1. **EventDispatcher/EventStore** (application-level): Retries failed events up to `max_retries`.
2. **librdkafka** (producer-level): Automatically retries message delivery with exponential backoff.

`KafkaMessageBroker` is a thin mapping layer — it does not duplicate retry logic.

## Configuration

```cpp
struct KafkaBrokerConfig {
    std::string bootstrap_servers = "localhost:9094";
    std::string client_id = "dse-kafka-broker";
    int poll_interval_ms = 50;
};
```

The configuration wraps `KafkaClientConfig` without unnecessary duplication. The default bootstrap server (`localhost:9094`) matches the Phase 19A Docker infrastructure.

## Relationship to Phase 18

Phase 19C builds directly on Phase 18:

| Phase 18 Component | Phase 19C Role |
|--------------------|----------------|
| `MessageBroker` | Interface that `KafkaMessageBroker` implements |
| `EventDispatcher` | Calls `publish()` — unchanged |
| `EventStore` | Tracks event lifecycle — unchanged |
| `DocumentEvent` | Defines topics (`documents.indexed`, etc.) — unchanged |
| `InMemoryMessageBroker` | Alternative implementation — unchanged |

**No Phase 18 code was modified.** `KafkaMessageBroker` is purely additive.

## Why KafkaClient Exists

`KafkaClient` (Phase 19B) exists to:

1. **Hide librdkafka headers**: The Pimpl pattern keeps `rdkafkacpp.h` out of project headers.
2. **Simplify lifecycle**: RAII constructor/destructor handles producer creation/destruction.
3. **Provide clean API**: `produce_async()`, `poll()`, `flush()`, `close()`.
4. **Enable testability**: `KafkaClient` can be mocked independently of `MessageBroker`.

`KafkaMessageBroker` uses `KafkaClient` as its single abstraction over librdkafka. It does not directly call librdkafka APIs.

## Development vs Production

| Aspect | Development | Production |
|--------|-------------|------------|
| Default build | `ENABLE_KAFKA=OFF` | `ENABLE_KAFKA=ON` |
| Broker | `InMemoryMessageBroker` | `KafkaMessageBroker` |
| Kafka | Docker (localhost:9094) | Cluster (multiple brokers) |
| Consumer groups | N/A | Phase 19D |
| Durability | In-memory only | Kafka persistence |

The default build remains completely Kafka-independent. All 1004 existing tests pass without Kafka installed.

## Testing Strategy

### Unit Tests (ENABLE_KAFKA=OFF)

- KafkaMessageBroker not compiled.
- All existing tests pass unchanged.

### Integration Tests (ENABLE_KAFKA=ON)

- 15 tests in `kafka_message_broker_test.cpp`.
- Require live Kafka at `localhost:9094`.
- Verify actual messages in Kafka topics.
- Test construction, publish, shutdown, statistics, lifecycle.

### Test Categories

| Category | Tests | What They Verify |
|----------|-------|------------------|
| Construction | 2 | Client creation, custom config |
| Publish | 4 | Indexed/Updated/Removed topics, payload preservation |
| Multiple | 2 | Batch publish, monotonic offsets |
| Lifecycle | 4 | Shutdown, idempotent stop, destructor |
| Stubs | 2 | Dead letters empty, was_processed false |
| Delivery | 1 | Delivery reports received |

## Explicit Non-Goals

Phase 19C does NOT implement:

- **Consumer groups** (Phase 19D)
- **Offset management** (Phase 19D)
- **Partition assignment** (Phase 19D)
- **Exactly-once semantics** (future)
- **Kafka transactions** (future)
- **Schema registry** (future)
- **Redis** (future)
- **PostgreSQL** (future)

## Lessons Learned

1. **`publish()` semantics matter**: Both in-memory and Kafka brokers return after local queue acceptance, not after end-to-end delivery. This consistency is critical for `EventDispatcher` compatibility.

2. **Poll thread is necessary**: `librdkafka` requires periodic `poll()` calls to process delivery reports. Without a poll thread, delivery reports accumulate and memory grows.

3. **Lazy thread start**: Starting the poll thread on first `publish()` avoids unnecessary overhead when the broker is constructed but unused.

4. **RAII ordering**: The `KafkaClient` must outlive all in-flight `produce_async()` calls. The shutdown sequence (dispatcher → broker → client) ensures this.

5. **Stubs are acceptable**: Not all `MessageBroker` methods need full implementation. Consumer-side methods can be stubs when consumer groups are explicitly deferred.

---

**Phase 19C is production-ready for producer-only Kafka delivery.** Consumer functionality belongs to Phase 19D.
