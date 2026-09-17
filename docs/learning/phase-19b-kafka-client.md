# Phase 19B — Kafka Client Foundation

## Why Phase 19B Exists

Phase 18 introduced an in-memory message broker that demonstrated the core
messaging abstractions (producer/consumer, topics, offsets, delivery
semantics, backpressure). Phase 19A established the Kafka infrastructure
(Docker/KRaft). Phase 19B connects the C++ application to Kafka through a
clean client abstraction.

The goal is **not** to immediately replace the in-memory broker. It is to
establish the lowest-level integration point between the application and
Kafka so that Phase 19C (KafkaMessageBroker) can build on it.

## What Phase 19B Implements

Phase 19B introduces `KafkaClient` — a RAII wrapper around librdkafka's
C++ API. It provides:

- Asynchronous message production
- Delivery report processing
- Event polling
- Flush (synchronous wait for delivery)
- Graceful shutdown
- Health/status reporting

Phase 19B does **not** implement:

- A `MessageBroker` implementation (that is Phase 19C)
- Consumer groups (Phase 19D)
- Offset management (Phase 19D)
- Partition assignment / rebalancing (Phase 19D)
- Schema registry
- Transactions
- Exactly-once semantics

## Architecture

```
ShardCoordinator / EventDispatcher
       |
       | (future Phase 19C)
       v
KafkaMessageBroker : MessageBroker
       |
       | uses
       v
KafkaClient
       |
       | wraps RdKafka::Producer
       v
librdkafka (C++ API)
       |
       | connects to
       v
Kafka 4.3.1 (Docker, Phase 19A)
```

KafkaClient exists because librdkafka has a complex lifecycle that benefits
from RAII wrapping. Separating the client from the MessageBroker keeps each
class focused on a single responsibility.

## librdkafka

[librdkafka](https://github.com/confluentinc/librdkafka) is the
industry-standard C/C++ client library for Apache Kafka. It provides:

- **Producer**: publish messages to Kafka topics
- **Consumer**: subscribe to topics and consume messages
- **Admin**: create/delete topics, manage cluster
- **Rebalance**: consumer group coordination

In Phase 19B, we use only the **Producer** API.

### C vs C++ API

librdkafka provides both a C API (`rdkafka.h`) and a C++ API
(`rdkafkacpp.h`). We use the C++ API because:

1. **RAII**: C++ classes have constructors/destructors that manage lifecycle
2. **Type safety**: enum-based error handling instead of raw integers
3. **Callbacks**: virtual method overrides for delivery reports
4. **Fits the project style**: C++20 codebase with RAII patterns

The C++ API is a thin wrapper around the C API. There is no performance
difference.

### MSYS2 Installation

```bash
pacman -S mingw-w64-ucrt-x86_64-librdkafka
```

This installs:
- `/ucrt64/include/librdkafka/rdkafkacpp.h` (C++ header)
- `/ucrt64/include/librdkafka/rdkafka.h` (C header)
- `/ucrt64/bin/librdkafka++.dll` (C++ shared library)
- `/ucrt64/bin/librdkafka.dll` (C shared library)
- `/ucrt64/lib/cmake/RdKafka/` (CMake config)

### Runtime DLL Dependencies

The librdkafka shared library depends on:
- `libssl` / `libcrypto` (OpenSSL)
- `libcurl`
- `liblz4` (LZ4 compression)
- `libzstd` (Zstandard compression)
- `zlib1.dll`
- `libsasl.dll` (SASL authentication)

All DLLs must be on PATH when running Kafka-enabled executables.
The UCRT64 bin directory (`/c/msys64/ucrt64/bin`) provides all of them.

## KafkaClient API

### Configuration

```cpp
KafkaClientConfig cfg;
cfg.bootstrap_servers = "localhost:9094";  // Phase 19A EXTERNAL listener
cfg.client_id = "dse";
cfg.delivery_timeout_ms = 5000;
cfg.message_timeout_ms = 5000;
cfg.queue_buffering_max_ms = 5;          // linger.ms
cfg.queue_buffering_max_messages = 100000;
cfg.batch_num_messages = 10000;
```

### Asynchronous Produce

The fundamental operation is asynchronous:

```cpp
KafkaClient client(cfg);
bool accepted = client.produce_async("documents.indexed", json_payload, "key");
```

`produce_async()` returns immediately after the message is accepted into
librdkafka's internal queue. The actual delivery happens asynchronously.

### Delivery Reports

Delivery reports indicate whether a message was successfully delivered:

```cpp
DeliveryReport report = client.produce("topic", "payload", "key", 5000);
if (report.success) {
    // Message delivered to Kafka
    // report.topic, report.partition, report.offset
} else {
    // Delivery failed
    // report.error_code, report.error_message
}
```

### Polling

librdkafka requires periodic polling to process delivery reports:

```cpp
client.poll(0);  // non-blocking
client.poll(100);  // block up to 100ms
```

In practice, `flush()` calls `poll()` internally, so explicit polling is
only needed when using the async API between `produce_async()` calls.

### Flush

Wait for all pending deliveries:

```cpp
bool all_delivered = client.flush(5000);  // timeout in ms
```

### Graceful Shutdown

```cpp
client.close();  // flush + close producer
// Destructor also calls close() if not already closed
// Safe to call multiple times
```

## Asynchronous Delivery Model

KafkaClient uses an **async-first** design:

1. `produce_async()` enqueues the message into librdkafka's internal buffer
2. librdkafka batches messages and sends them to the broker
3. When the broker acknowledges, librdkafka calls the delivery report
   callback
4. `poll()` triggers these callbacks
5. `flush()` blocks until all pending messages are acknowledged

This is the standard Kafka producer model. It is optimized for throughput
(batching) while maintaining delivery guarantees.

### Why Async-First?

The `MessageBroker::publish()` interface is synchronous — it blocks until
delivery. But this synchronous behavior will be implemented on top of the
async foundation:

```cpp
// Phase 19C (conceptual)
Offset MessageBroker::publish(Message msg) {
    auto report = client_->produce(msg.topic, msg.payload, msg.key);
    // ... convert report to Offset
}
```

The synchronous `produce()` convenience method in KafkaClient already
demonstrates this pattern: it calls `produce_async()` + polls for the
delivery report.

## Pimpl Pattern

KafkaClient uses the Pimpl (Pointer to Implementation) pattern:

```cpp
// kafka_client.h — no librdkafka headers
class KafkaClient {
    struct Impl;
    std::unique_ptr<Impl> impl_;
};

// kafka_client.cpp — includes rdkafkacpp.h
struct KafkaClient::Impl {
    RdKafka::Producer* producer;
    DeliveryReportCallback dr_cb;
    // ...
};
```

This keeps librdkafka headers out of the public interface. Benefits:

1. **Compilation firewall**: changing librdkafka doesn't recompile every
   file that includes `kafka_client.h`
2. **Header hygiene**: consumers don't need librdkafka on their include path
3. **Conditional compilation**: when `ENABLE_KAFKA=OFF`, no librdkafka
   headers are needed at all

## CMake Integration

### Optional Dependency

```cmake
option(ENABLE_KAFKA "Build with Kafka support" OFF)
```

When `OFF` (default):
- `kafka_client.cpp` is not compiled
- No librdkafka headers needed
- No Kafka linker dependency
- All existing 1004 tests pass unchanged

When `ON`:
```cmake
find_package(RdKafka REQUIRED)
target_sources(dse_core PRIVATE src/kafka_client.cpp)
target_link_libraries(dse_core PUBLIC RdKafka::rdkafka++)
target_compile_definitions(dse_core PUBLIC DSE_KAFKA_ENABLED=1)
```

### Build Command

```bash
# Without Kafka (default):
cmake -S . -B build -G Ninja

# With Kafka:
cmake -S . -B build -G Ninja \
    -DENABLE_KAFKA=ON \
    -DCMAKE_PREFIX_PATH=/c/msys64/ucrt64 \
    -DFETCHCONTENT_UPDATES_DISCONNECTED=ON
```

### Why FETCHCONTENT_UPDATES_DISCONNECTED?

When reconfiguring an existing build directory, FetchContent may try to
re-download dependencies. This flag tells CMake to use cached sources.

### Why CMAKE_PREFIX_PATH?

The MSYS2 UCRT64 package installs librdkafka under `/c/msys64/ucrt64/`.
CMake's `find_package(RdKafka)` needs to find
`RdKafkaConfig.cmake` in that prefix. Setting `CMAKE_PREFIX_PATH` tells
CMake where to look.

The RdKafka config also requires finding transitive dependencies (ZLIB,
OpenSSL, LZ4, etc.) from the same prefix. The RdKafka package ships its
own `FindLZ4.cmake`, which we add to `CMAKE_MODULE_PATH` via the
RdKafka cmake directory.

## Relationship to Phase 18

Phase 18 established the messaging abstraction:

| Phase 18 Component | Phase 19B Relationship |
|--------------------|-----------------------|
| `MessageBroker` | KafkaClient will be used to implement this (Phase 19C) |
| `InMemoryMessageBroker` | Remains the default for unit tests |
| `EventDispatcher` | Will eventually dispatch through KafkaMessageBroker |
| `EventStore` | Unchanged |
| `PersistentEventStore` | Unchanged |

KafkaClient is a **new** component that sits below the Phase 18
abstractions. It does not replace or modify any Phase 18 code.

## Why KafkaClient Exists

One could implement `KafkaMessageBroker` directly using librdkafka. But:

1. **Lifecycle complexity**: librdkafka producer creation, configuration,
   callback registration, flushing, and destruction require careful
   sequencing. KafkaClient encapsulates this.

2. **Testability**: A thin client can be tested independently of the
   MessageBroker contract.

3. **Reuse**: Future phases may need direct Kafka access (admin operations,
   consumer groups, schema registry) beyond the MessageBroker interface.

4. **Separation**: KafkaMessageBroker handles MessageBroker semantics;
   KafkaClient handles librdkafka mechanics.

## Why Consumer Groups Are Deferred to Phase 19D

Consumer groups involve:

- Group coordinator protocol
- Partition assignment strategies
- Rebalance handling
- Offset commit/seek
- Consumer lifecycle (start, pause, resume)

These are fundamentally different from the producer model and require
their own design iteration. Phase 19B establishes the producer foundation;
Phase 19D will address the consumer side.

## Why KafkaMessageBroker Is Deferred to Phase 19C

KafkaMessageBroker must:

1. Map `MessageBroker::publish()` → `KafkaClient::produce()`
2. Implement consumer threads using librdkafka's consumer API
3. Integrate with `BrokerStats`
4. Handle dead-letter queue semantics
5. Implement idempotency tracking

This is a substantial integration effort that builds on KafkaClient.
Separating it into Phase 19C allows focused testing of both components.

## Threading Model

KafkaClient is **thread-safe for production**:

- Multiple threads may call `produce_async()` concurrently
- `flush()` is thread-safe
- `poll()` is thread-safe but ideally called from one thread
- `close()` is NOT safe with concurrent `produce_async()`

librdkafka itself handles all internal thread synchronization.

## Explicit Non-Goals

Phase 19B does **not** implement:

- KafkaMessageBroker (Phase 19C)
- Consumer groups (Phase 19D)
- Offset management (Phase 19D)
- Partition assignment / rebalancing (Phase 19D)
- Schema registry
- Transactions
- Exactly-once semantics
- Redis
- PostgreSQL
- Kubernetes
