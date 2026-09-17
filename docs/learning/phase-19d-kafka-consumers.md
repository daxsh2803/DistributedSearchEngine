# Phase 19D — Kafka Consumer Groups

## Objective

Implement Kafka consumer-group-based message consumption within the existing `MessageBroker` interface, allowing the system to consume events from Kafka topics with automatic partition assignment, at-least-once delivery semantics, and consumer-group coordination.

## Motivation

Phases 19A–19C established Kafka infrastructure, the `KafkaClient` producer wrapper, and `KafkaMessageBroker` for event publishing. However, the consumer side was only stubbed — `subscribe()` was a no-op and `start()` only launched the producer poll thread.

Phase 18's `EventDispatcher` is producer-only (it publishes events). But many real-world distributed systems need to **consume** events from Kafka — for example:

- Downstream consumers that index, aggregate, or react to document events.
- Cross-node event forwarding in the distributed search engine.
- Event replay and auditing.

Consumer groups provide the coordination mechanism for distributed consumption: multiple consumers sharing a group will automatically partition Kafka topics among themselves, with rebalancing when consumers join or leave.

## Architecture

```
KafkaMessageBroker
    |
    ├── KafkaClient (Phase 19B — producer)
    |       └── librdkafka Producer
    |       └── poll thread (delivery reports)
    |
    └── KafkaConsumer (Phase 19D — consumer groups)
            └── librdkafka KafkaConsumer
            └── consumer thread (poll loop)
            └── handler dispatch
```

`KafkaMessageBroker` implements the existing `MessageBroker` interface. The consumer side wires `subscribe()` → handler registration and `start()` → consumer-thread launch.

## KafkaConsumer

### What It Is

`KafkaConsumer` is a RAII wrapper around `RdKafka::KafkaConsumer` (librdkafka's C++ consumer API). It provides:

- Consumer group support via librdkafka's built-in group coordination.
- Manual offset commits (`enable.auto.commit=false`) for at-least-once delivery.
- Rebalance callback for partition assignment/revocation.
- Pimpl pattern so librdkafka headers don't leak into public interfaces.

### What It Is NOT

`KafkaConsumer` is **not** a `MessageBroker` implementation. It is a low-level consumer wrapper. `KafkaMessageBroker` wraps `KafkaConsumer` to implement the `MessageBroker` interface.

### API

```cpp
class KafkaConsumer {
public:
    explicit KafkaConsumer(KafkaConsumerConfig config);
    ~KafkaConsumer();

    // Poll for a single message (blocking, up to timeout).
    KafkaConsumerMessage poll(int timeout_ms = -1);

    // Store a consumed message's offset for later commit.
    void store_offset(const KafkaConsumerMessage& msg);

    // Commit stored offsets.
    void commit();

    // Leave consumer group and close (idempotent).
    void close();

    // Health/status inspection.
    bool is_healthy() const;
    std::string last_error() const;
    std::uint64_t messages_consumed() const;
    std::uint64_t rebalance_count() const;
    int assigned_partitions() const;
};
```

## Consumer Groups

### How They Work

A **consumer group** is identified by its `group.id`. When multiple `KafkaConsumer` instances share the same `group.id`, Kafka automatically:

1. **Assigns partitions**: each partition is assigned to exactly one consumer in the group.
2. **Rebalances**: when a consumer joins or leaves, Kafka reassigns partitions.
3. **Tracks committed offsets**: each group has its own offset position per partition.

### Configuration

```cpp
KafkaConsumerConfig cfg;
cfg.bootstrap_servers = "localhost:9094";
cfg.client_id = "dse-consumer";
cfg.group_id = "dse-consumer-group";
cfg.topics = {"documents.indexed", "documents.updated", "documents.removed"};
cfg.auto_offset_reset = "earliest";  // start from beginning if no offset
cfg.enable_auto_commit = false;       // manual commits for reliability
```

### Key Settings

| Setting | Value | Why |
|---------|-------|-----|
| `enable.auto.commit` | `false` | Manual commits for at-least-once delivery. |
| `enable.auto.offset.store` | `false` | We store offsets explicitly after handler success. |
| `auto.offset.reset` | `earliest` | Consume from the beginning if no committed offset. |
| `session.timeout.ms` | `30000` | 30-second session timeout for group coordination. |

## Offset Semantics

### At-Least-Once Delivery

Phase 19D provides **at-least-once** delivery, not exactly-once.

The mechanism:

1. `KafkaConsumer::poll()` fetches a message.
2. The handler processes it.
3. **If the handler succeeds** → `store_offset(msg)` stores the offset → `commit()` commits it.
4. **If the handler fails** → offset is NOT committed → Kafka will redeliver on restart/rebalance.

```
poll → handler returns true  → store_offset → commit  → message processed
poll → handler returns false → (no commit)            → redelivered on restart
poll → handler throws        → (no commit)            → redelivered on restart
```

### Important Kafka Semantics

- **Within a single consumer session**, Kafka does NOT redeliver consumed-but-uncommitted messages. Once you've received a message via `poll()`, you won't see it again in that session.
- **Redelivery only happens across restarts/rebalances**, when Kafka reassigns partitions from the last committed offset.
- This means in-session "nack and retry" is **not possible** with Kafka's native offset model.

### store_offset vs commit

- `store_offset(msg)`: records the specific offset for a specific partition. Used after each successful handler execution.
- `commit()`: commits all stored offsets. Used during shutdown or batch commits.

Phase 19D uses per-message `store_offset()` for precise offset tracking.

## Rebalancing

### What Happens

When a consumer joins or leaves a consumer group:

1. Kafka triggers a **rebalance**.
2. All consumers in the group have their partitions **revoked**.
3. Partitions are **reassigned** across the remaining consumers.
4. Each consumer receives its new partition set.

### Rebalance Callback

`KafkaConsumer` installs a `RebalanceCb` that:

- On `ASSIGN_PARTITIONS`: calls `consumer->assign(partitions)`.
- On `REVOKE_PARTITIONS`: calls `consumer->unassign()`.
- Tracks assignment count and rebalance count for observability.

### Implications

- During a rebalance, no messages are consumed (all consumers are paused).
- After rebalance, each consumer resumes from its last committed offset for its assigned partitions.
- Long-running handlers can cause session timeouts and unnecessary rebalances.

## Threading Model

```
EventDispatcher thread     Consumer thread          librdkafka threads
        |                       |                          |
        | publish()             | poll()                   |
        v                       v                          v
  KafkaMessageBroker     consumer_loop()           TCP to Kafka broker
        |                       |
        v                       v
  KafkaClient            KafkaConsumer
  (producer)             (consumer)
```

- **Producer poll thread** (Phase 19C): calls `KafkaClient::poll()` for delivery reports.
- **Consumer thread** (Phase 19D): calls `KafkaConsumer::poll()` in a loop, dispatching messages to handlers.
- Handler execution is **synchronous** within the consumer thread.
- `stop()` joins both threads before closing Kafka handles.

## KafkaMessageBroker Consumer Integration

### subscribe()

Registers a handler for a topic. Must be called before `start()`.

```cpp
broker.subscribe("documents.indexed", [](const Message& msg) -> bool {
    // Process the event.
    return true;  // success → commit offset
});
```

### start()

Starts the consumer thread and subscribes to all registered topics.

### Consumer Loop

```
while (consumer_running) {
    msg = consumer_->poll(timeout);
    if (error) continue;

    handler = lookup_handler(msg.topic);
    if (!handler) continue;  // no handler, skip

    success = handler(convert(msg));

    if (success) {
        consumer_->store_offset(msg);  // track offset
    }
    // else: don't commit → redelivery on restart
}
```

## Retry Semantics

Phase 19D deliberately does NOT create an in-process retry system for consumer failures.

| Layer | Responsibility |
|-------|---------------|
| Kafka consumer | Redelivers uncommitted messages on restart/rebalance. |
| `KafkaMessageBroker` | Dispatches to handlers, commits on success, skips on failure. |
| EventDispatcher | Retries failed event publications (producer side). |

This avoids a second competing retry system. Kafka's native offset model provides the retry mechanism.

## Dead Letters

Phase 19D does NOT implement a process-local dead-letter queue for consumer failures. A process-local DLQ disappears on crash and is not durable.

Kafka DLQ topics can be designed in a later phase when the system has multi-topic event routing.

## Shutdown Semantics

`KafkaMessageBroker::stop()`:

1. **Stop consumer thread** → set `consumer_running = false` → join thread.
2. **Close consumer** → `consumer->close()` leaves the consumer group.
3. **Stop producer poll thread** → set `poll_running = false` → join thread.
4. **Flush producer** → `client_->flush(5000)`.
5. **Close producer** → `client_->close()`.

`stop()` is idempotent. The destructor calls `stop()` if not already stopped.

## Interaction with Phase 18

Phase 18's `EventDispatcher` is purely a **producer** — it publishes events and tracks delivery state. It does NOT consume events.

Phase 19D's consumer functionality is separate from the `EventDispatcher` pipeline:

```
ShardCoordinator → EventDispatcher → KafkaMessageBroker (publish)
                                           ↓
                                        Kafka
                                           ↓
                                  KafkaMessageBroker (consume) → Handler
```

The producer and consumer paths are independent. `EventDispatcher` does not know about consumer functionality.

## Testing

### Integration Tests (ENABLE_KAFKA=ON)

28 tests in `kafka_message_broker_test.cpp`:

**Producer tests (13):**
1. `Construction` — basic broker creation
2. `PublishToIndexedTopic` — publish to documents.indexed
3. `PublishToUpdatedTopic` — publish to documents.updated
4. `PublishToRemovedTopic` — publish to documents.removed
5. `MultipleMessages` — batch publish
6. `PublishWithTimeoutSuccess` — timeout API
7. `Statistics` — broker stats
8. `CleanShutdown` — graceful stop
9. `StopIsIdempotent` — double-stop safety
10. `DestructorStopsCleanly` — RAII shutdown
11. `DeadLettersEmpty` — no DLQ
12. `WasProcessedReturnsFalse` — idempotency stub
13. `DeliveryReportsReceived` — delivery report processing

**Consumer tests (14):**
14. `ConsumerConstruction` — consumer creation
15. `SingleTopicSubscription` — subscribe to one topic
16. `MultipleTopicSubscription` — subscribe to multiple topics
17. `ConsumerGroupConfiguration` — group_id configuration
18. `PublishConsumeRoundTrip` — publish → consume
19. `PayloadPreservation` — payload integrity
20. `SuccessfulHandlerCommitsOffset` — handler success → offset commit
21. `FailedHandlerNoCommit` — handler failure → no commit
22. `ConsumerRestartRecovery` — restart recovers from last committed offset
23. `GracefulShutdown` — consumer shutdown
24. `ConcurrentProducerConsumer` — simultaneous produce/consume
25. `ThreeDocumentEventTopics` — all three event topics
26. `ConsumerStatistics` — consumed/acked/nacked counters
27. `HandlerFailureAndRecovery` — handler error recovery

**Disabled-check test (1):**
28. `SkippedWhenKafkaNotEnabled` — verifies clean compilation without Kafka

### Default Build (ENABLE_KAFKA=OFF)

All Kafka code is excluded. The test target compiles only the disabled-check stub. The existing 1004 baseline tests remain completely unaffected.

### Test Isolation

Each consumer test uses a **unique group ID** (via `unique_group()` helper) to prevent cross-test offset contamination. Topics like `documents.indexed` are shared across tests, but unique group IDs ensure each test starts from the correct offset position.

## Configuration Reference

```cpp
struct KafkaBrokerConfig {
    std::string bootstrap_servers = "localhost:9094";
    std::string client_id = "dse-kafka-broker";
    int poll_interval_ms = 50;           // producer poll interval
    std::string group_id = "dse-consumer-group";
    std::string consumer_client_id = "dse-consumer";
    std::string auto_offset_reset = "earliest";
    int consumer_poll_timeout_ms = 100;
};
```

## Explicit Non-Goals

Phase 19D does NOT implement:

- Exactly-once semantics
- Kafka transactions
- Consumer lag monitoring
- Schema registry
- Kafka Streams
- Process-local dead-letter queues
- Consumer offset seeking
- Static partition assignment
- Custom partition assignment strategies

## Relationship to Other Phases

| Phase | What It Added |
|-------|---------------|
| 18A–18G | EventStore, EventDispatcher, retry/replay, PersistentEventStore, observability |
| 19A | Kafka Docker/KRaft infrastructure |
| 19B | KafkaClient (producer foundation) |
| 19C | KafkaMessageBroker (producer implementation) |
| **19D** | **KafkaConsumer, consumer groups, offset management, consumer integration** |
| 19E+ | Future: failure recovery, observability, consumer lag, etc. |
