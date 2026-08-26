# Phase 19A — Kafka Infrastructure

## Overview

Phase 19A establishes the Docker infrastructure for running Apache Kafka locally during development and testing. This is the first step in introducing Kafka as an alternative message broker implementation, building directly on the `MessageBroker` abstraction from Phase 18B.

**Phase 19A creates no C++ code changes.** All existing 1004 tests continue to pass unchanged. The `InMemoryMessageBroker` remains the default for unit tests and development without Docker.

## Why Kafka?

The current `InMemoryMessageBroker` + `PersistentEventStore` architecture has fundamental limitations:

| Limitation | InMemoryMessageBroker | Kafka Solution |
|-----------|----------------------|----------------|
| **Process-local** | Messages lost on restart | Distributed, durable event log |
| **Single consumer** | One consumer per topic | Consumer groups for parallel processing |
| **No event replay** | Only failed events can replay | Complete log with configurable retention |
| **No horizontal scaling** | Single-process broker | Multiple Kafka brokers |
| **No multi-consumer** | Cannot share events across services | Multiple consumer groups |
| **No cross-service** | Events local to process | Events accessible across processes |
| **No schema evolution** | Event schemas fixed at compile time | Schema registry for versioning |

Kafka solves these limitations while maintaining the `MessageBroker` interface contract.

## Kafka Architecture

### Brokers

A Kafka **broker** is a server that stores data and serves client requests.

```
┌─────────────────┐
│  Kafka Broker    │
│                  │
│  - Stores topics│
│  - Handles I/O  │
│  - Manages state│
└─────────────────┘
```

**Production deployments** use multiple brokers for fault tolerance and scalability.

**Development** uses a single broker (this phase).

### Topics

A **topic** is a named stream of records. Topics are the fundamental unit of organization in Kafka.

```
Topic: documents.indexed
├── Partition 0: [msg1, msg4, msg7, ...]
├── Partition 1: [msg2, msg5, msg8, ...]
└── Partition 2: [msg3, msg6, msg9, ...]
```

**Document event topics:**
- `documents.indexed` — published after successful document ingestion
- `documents.updated` — published after successful document update
- `documents.removed` — published after successful document removal

These match the topic names established in Phase 18C.

### Partitions

A **partition** is an ordered, immutable sequence of records within a topic.

**Why partitions matter:**
1. **Parallelism**: Multiple consumers can read different partitions simultaneously
2. **Scalability**: More partitions = more parallel consumers
3. **Ordering**: Records within a partition are ordered by offset
4. **Distribution**: Partitions can be distributed across brokers

**Partitioning strategy for document events:**
- **Key**: `document_id` (from `DocumentIndexedEvent.document_id`)
- **Benefit**: All events for the same document go to the same partition
- **Result**: Ordering guaranteed per document across events

```
document_id=42 → hash(42) % 3 → Partition 1

All events for document 42:
  documents.indexed  → Partition 1
  documents.updated  → Partition 1
  documents.removed  → Partition 1
```

**Why 3 partitions for development?**
- Allows testing multi-partition consumer scenarios
- Enables consumer group testing (19D)
- Reasonable default for development
- Production: increase based on throughput needs

### Replication

**Replication** means storing copies of partitions across multiple brokers.

**Replication factor** = number of copies of each partition.

| Setting | Replication Factor | Behavior |
|---------|-------------------|----------|
| **Development (this phase)** | 1 | Single copy, no fault tolerance |
| **Production** | 3+ | Copies survive broker failures |

**Why replication factor 1 for development?**
- Single Docker container cannot replicate across brokers
- Simplifies local setup (no multi-broker complexity)
- Development does not need fault tolerance
- Production configuration will use replication factor ≥ 3

**Why not use replication factor 3 in Docker?**
- Would require 3 Kafka broker containers
- Significantly increases Docker resource requirements
- Complicates local development workflow
- Not necessary for testing client code

### KRaft (Kafka Raft)

**KRaft** is Kafka's built-in metadata management system, replacing Apache ZooKeeper.

**Why KRaft instead of ZooKeeper?**
- Kafka 4.0+ removed ZooKeeper entirely (March 2025)
- KRaft is the only supported mode
- Single process instead of two (broker + ZooKeeper)
- Simpler deployment and configuration
- Better performance
- Recommended for all deployments

**KRaft components:**
- **Controller**: Manages metadata, topic partitions, leader election
- **Broker**: Handles produce/consume requests
- **Single node**: Both roles in one process (development)

```
┌─────────────────────────────────┐
│  Kafka (KRaft mode)             │
│                                 │
│  ┌───────────────────────────┐  │
│  │  Broker                   │  │
│  │  - Produces/consumes data │  │
│  │  - Stores partitions      │  │
│  └───────────────────────────┘  │
│                                 │
│  ┌───────────────────────────┐  │
│  │  Controller               │  │
│  │  - Manages metadata       │  │
│  │  - Leader election        │  │
│  │  - Topic/partition mgmt   │  │
│  └───────────────────────────┘  │
│                                 │
└─────────────────────────────────┘
```

### Producers and Consumers

**Producer** — publishes messages to a topic:
```
Producer
    │
    │ publish(message, key=42)
    ▼
┌─────────────────────┐
│  documents.indexed   │
│  Partition 1 (key=42)│
└─────────────────────┘
```

**Consumer** — reads messages from a topic:
```
┌─────────────────────┐
│  documents.indexed   │
│  Partition 0, 1, 2  │
└─────────────────────┘
    │
    │ poll() / consume()
    ▼
Consumer
    │
    │ process(message)
    ▼
    ack / nack
```

**Document event flow:**
```
ShardCoordinator
    │
    │ create_event(topic, payload)
    v
EventStore
    │
    │ enqueue_with_event()
    v
EventDispatcher
    │
    │ broker.publish(message)
    v
┌─────────────────────┐
│  Kafka (producer)    │
└─────────────────────┘
    │
    │ topic: documents.indexed
    ▼
┌─────────────────────┐
│  Kafka (broker)      │
└─────────────────────┘
    │
    │ poll() / consume()
    ▼
┌─────────────────────┐
│  Consumer (Phase 19C)│
└─────────────────────┘
```

### Message Keys

Each message has an optional **key** — a byte array used for partitioning.

**Document event keys:**
- `document_id` (serialized as bytes)
- Ensures all events for the same document go to the same partition
- Guarantees ordering per document

**Partitioning formula:**
```
partition = hash(message.key) % partition_count
```

**Benefits:**
- Ordering per document (all events for doc 42 arrive in order)
- Parallel processing across documents (different keys → different partitions)
- Consistent routing (same key always → same partition)

### Ordering Guarantees

**Within a partition:**
- Messages are strictly ordered by offset
- Consumer sees messages in publish order
- Offset monotonically increases

**Across partitions:**
- No ordering guarantee
- Consumer sees messages from multiple partitions in arrival order

**For document events:**
- All events for a given document go to the same partition (via document_id key)
- Events for document 42: indexed(42), updated(42), removed(42) — in order
- Events for document 100: indexed(100), updated(100) — in order
- But no ordering between document 42 and document 100

## Localhost ↔ Container Networking

### Network Architecture

```
┌─────────────────────────────────────────────────┐
│                  Host Machine                    │
│                                                  │
│  ┌──────────────────────────────────────────┐   │
│  │  Docker Desktop (WSL2 backend)           │   │
│  │                                          │   │
│  │  ┌────────────────────────────────────┐  │   │
│  │  │  apache/kafka:4.3.1                 │  │   │
│  │  │  Container                         │  │   │
│  │  │                                    │  │   │
│  │  │  Listeners:                        │  │   │
│  │  │    INTERNAL: kafka:9092            │  │   │
│  │  │    EXTERNAL: localhost:9094        │  │   │
│  │  │    CONTROLLER: localhost:9093      │  │   │
│  │  │                                    │  │   │
│  │  │  Port Mapping:                     │  │   │
│  │  │    9094 → host:9094               │  │   │
│  │  └────────────────────────────────────┘  │   │
│  │                                          │   │
│  └──────────────────────────────────────────┘   │
│                                                  │
│  Host connects to: localhost:9094                │
│                                                  │
└─────────────────────────────────────────────────┘
```

### Listener Configuration

Kafka uses three listeners for different communication paths:

| Listener | Bind Address | Advertised Address | Purpose |
|----------|-------------|-------------------|--------|
| **INTERNAL** | `0.0.0.0:9092` | `kafka:9092` | Container-to-container communication |
| **EXTERNAL** | `0.0.0.0:9094` | `localhost:9094` | Host machine access |
| **CONTROLLER** | `0.0.0.0:9093` | `localhost:9093` | KRaft controller communication |

**INTERNAL listener (9092):**
- For container-to-container communication (kafka-init, future app containers)
- Advertised as `kafka:9092` (Docker network hostname)
- Containers on the same Docker network can reach each other via hostname

**EXTERNAL listener (9094):**
- For host machine access (your development machine)
- Advertised as `localhost:9094`
- Host connects via Docker port mapping: `localhost:9094` → container's `9094`

**CONTROLLER listener (9093):**
- For KRaft controller communication
- Single-node: localhost:9093
- Not exposed to host (not needed for clients)

### Connection Details

| Parameter | Host Machine | Docker Container |
|-----------|-------------|------------------|
| **Bootstrap server** | `localhost:9094` | `kafka:9092` |
| **Protocol** | PLAINTEXT | PLAINTEXT |
| **Authentication** | None (development only) | None (development only) |
| **SSL/TLS** | None (development only) | None (development only) |

**Production considerations:**
- Use SSL/TLS for encryption
- Use SASL for authentication
- Use multiple bootstrap servers for redundancy
- Use private network addresses, not localhost

## Relationship to Phase 18

Phase 19A provides the infrastructure that enables Phase 19B-19D.

### What Phase 18 Established

```
MessageBroker (abstract interface)
    │
    ├── InMemoryMessageBroker (concrete, for testing)
    │
    └── [future] KafkaMessageBroker (concrete, for production)

EventDispatcher
    │
    └── Uses MessageBroker& (interface, not concrete)

EventStore
    │
    └── Tracks event lifecycle (independent of broker)
```

### What Phase 19A Adds

```
Docker Infrastructure
    │
    └── Kafka Broker (KRaft mode)
         │
         ├── INTERNAL: kafka:9092 (container-to-container)
         ├── EXTERNAL: localhost:9094 (host access)
         │
         └── Topics:
              ├── documents.indexed (3 partitions)
              ├── documents.updated (3 partitions)
              └── documents.removed (3 partitions)
```

### What Phase 19B-19D Will Add

**Phase 19B: Kafka Client Library**
- `KafkaClient` — C++ wrapper around librdkafka
- Connection management
- Producer/consumer API

**Phase 19C: KafkaMessageBroker**
- Implements `MessageBroker` interface
- Drop-in replacement for `InMemoryMessageBroker`
- Kafka-specific configuration (bootstrap servers, topics, etc.)

**Phase 19D: Consumer Groups**
- Parallel event processing
- Partition assignment
- Rebalancing

## Why InMemoryMessageBroker Remains for Unit Tests

### Test Isolation

Unit tests must be:
- **Fast**: No Docker startup overhead (30-60 seconds)
- **Deterministic**: No network variability
- **Independent**: No external dependencies
- **Parallelizable**: No shared Kafka broker state

### InMemoryMessageBroker Advantages

- **Instant startup**: No Docker container needed
- **No network**: Processes in memory, no latency
- **No cleanup**: No topics to delete after tests
- **No port conflicts**: No port 9092 contention
- **No Docker dependency**: Tests run anywhere

### KafkaMessageBroker Use Cases

- **Integration tests**: Full pipeline with Kafka
- **End-to-end tests**: Real message delivery
- **Consumer testing**: Consumer group behavior
- **Production**: Real message streaming

### Test Distribution

| Test Type | Broker Used | Docker Required? |
|-----------|-------------|------------------|
| Unit tests (1004 tests) | InMemoryMessageBroker | No |
| Kafka client tests (19B) | KafkaMessageBroker | Yes |
| Kafka integration tests (19C) | KafkaMessageBroker | Yes |
| Consumer group tests (19D) | KafkaMessageBroker | Yes |

## Why 3 Partitions

### Development Benefits

1. **Multi-partition testing**: Enables testing consumer behavior with multiple partitions
2. **Consumer group testing**: Phase 19D needs multiple partitions for parallel consumption
3. **Ordering verification**: Can verify partition-level ordering guarantees
4. **Hash distribution**: Can verify document_id → partition mapping

### Production Considerations

- **Throughput**: More partitions = higher throughput
- **Parallelism**: More partitions = more concurrent consumers
- **Resource usage**: More partitions = more broker resources
- **Rebalancing**: More partitions = longer rebalancing times

**Recommendation:** Start with 3 partitions for development, increase based on production load.

## Why Replication Factor 1 Locally

### Development Constraints

1. **Single Docker container**: Cannot replicate across brokers
2. **Resource usage**: Multiple brokers require more RAM/CPU
3. **Complexity**: Multi-broker setup complicates local development
4. **No fault tolerance needed**: Development does not require high availability

### Production Requirements

- **Minimum 3 replicas**: Survive 1 broker failure
- **Recommended 3 replicas**: Standard for production
- **High availability**: Automatic leader election
- **Durability**: Data survives broker crashes

### Configuration

```yaml
# Development (this phase)
KAFKA_DEFAULT_REPLICATION_FACTOR: 1
KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR: 1

# Production (future)
KAFKA_DEFAULT_REPLICATION_FACTOR: 3
KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR: 3
```

## Why No ZooKeeper

### ZooKeeper History

- **Pre-Kafka 4.0**: ZooKeeper managed Kafka metadata
- **Kafka 3.3+**: KRaft became production-ready
- **Kafka 4.0**: ZooKeeper removed entirely (March 2025)

### KRaft Advantages

1. **Single process**: No separate ZooKeeper process
2. **Simpler deployment**: One container instead of two
3. **Better performance**: No ZooKeeper overhead
4. **Simpler configuration**: No ZooKeeper connection strings
5. **Industry standard**: All new Kafka deployments use KRaft

### Why Not Use ZooKeeper in This Project?

- ZooKeeper is no longer supported (Kafka 4.0+)
- KRaft is simpler and more modern
- No reason to use deprecated technology
- Educational value of learning modern Kafka

## Development vs Production Configuration

### Development (Phase 19A)

| Setting | Value | Reason |
|---------|-------|--------|
| Brokers | 1 | Simple local setup |
| Replication factor | 1 | No fault tolerance needed |
| Listeners | PLAINTEXT | No encryption needed |
| Authentication | None | Simple local setup |
| Storage | Ephemeral | No persistence needed |
| Log retention | 1 hour | Save disk space |
| Heap | 256MB | Minimize Docker memory |

### Production (Future)

| Setting | Value | Reason |
|---------|-------|--------|
| Brokers | 3+ | High availability |
| Replication factor | 3 | Fault tolerance |
| Listeners | SSL/TLS | Encryption |
| Authentication | SASL | Security |
| Storage | Persistent | Durability |
| Log retention | 7+ days | Data retention |
| Heap | 4-8GB | Performance |

## What Phase 19B/19C/19D Will Add

### Phase 19B: Kafka Client Library

**Objective:** Integrate librdkafka as a C++ Kafka client wrapper.

**Components:**
- `src/kafka_client.h` — C++ wrapper around librdkafka
- `src/kafka_client.cpp` — Implementation
- Kafka client configuration (bootstrap servers, security, etc.)

**New concepts:**
- librdkafka API (rd_kafka_t, rd_kafka_topic_t, etc.)
- Kafka producer/consumer configuration
- Kafka error codes and retry semantics

### Phase 19C: KafkaMessageBroker

**Objective:** Implement `KafkaMessageBroker` as a concrete `MessageBroker` implementation.

**Components:**
- `src/kafka_message_broker.h` — implements `MessageBroker`
- `src/kafka_message_broker.cpp` — Implementation
- Kafka topic configuration
- Consumer group management

**New concepts:**
- Kafka topics and partitions
- Producer: `rd_kafka_produce()`, delivery reports
- Consumer: `rd_kafka_consumer_poll()`, offset management

### Phase 19D: Consumer Groups

**Objective:** Implement distributed consumer groups for parallel event processing.

**Components:**
- Consumer group configuration
- Partition assignment strategies
- Rebalancing handling

**New concepts:**
- Consumer groups
- Partition assignment
- Rebalancing
- Offset commit

## Explicit Non-Goals

The following are NOT part of Phase 19A:

- ❌ C++ code changes
- ❌ Kafka client library integration
- ❌ KafkaMessageBroker implementation
- ❌ Consumer groups
- ❌ Schema registry
- ❌ Event sourcing
- ❌ Redis
- ❌ PostgreSQL
- ❌ Kubernetes
- ❌ Production configuration
- ❌ Performance benchmarking
- ❌ Chaos engineering

## Testing Strategy

### Phase 19A Tests

Phase 19A creates no C++ tests. Validation is:

1. Docker Compose starts Kafka broker successfully
2. Kafka broker is healthy
3. Topics are created with correct configuration
4. Kafka is accessible from host at localhost:9094
5. Docker Compose stops cleanly

### Future Phase Tests

**Phase 19B:**
- Kafka client tests
- Connection tests
- Publish/subscribe tests

**Phase 19C:**
- KafkaMessageBroker tests
- Integration tests
- End-to-end pipeline tests

**Phase 19D:**
- Consumer group tests
- Rebalancing tests
- Parallel processing tests

## Architecture Diagram (Phase 19A)

```
┌─────────────────────────────────────────────────────────┐
│                      Host Machine                        │
│                                                          │
│  ┌──────────────────────────────────────────────────┐   │
│  │  Docker Desktop (WSL2 backend)                    │   │
│  │                                                   │   │
│  │  ┌─────────────────────────────────────────────┐  │   │
│  │  │  apache/kafka:4.3.1                         │  │   │
│  │  │  (KRaft mode, single node)                  │  │   │
│  │  │                                             │  │   │
│  │  │  Listeners:                                 │  │   │
│  │  │    INTERNAL: kafka:9092 (container-to-     │  │   │
│  │  │              container)                      │  │   │
│  │  │    EXTERNAL: localhost:9094 (host access)   │  │   │
│  │  │    CONTROLLER: localhost:9093 (KRaft)       │  │   │
│  │  │                                             │  │   │
│  │  │  Topics:                                    │  │   │
│  │  │    documents.indexed (3 partitions, RF=1)  │  │   │
│  │  │    documents.updated (3 partitions, RF=1)  │  │   │
│  │  │    documents.removed (3 partitions, RF=1)  │  │   │
│  │  └─────────────────────────────────────────────┘  │   │
│  │                                                   │   │
│  └──────────────────────────────────────────────────┘   │
│                                                          │
│  ┌──────────────────────────────────────────────────┐   │
│  │  DistributedSearchEngine.exe                      │   │
│  │  (Phase 18 code, unchanged)                       │   │
│  │                                                   │   │
│  │  EventDispatcher → InMemoryMessageBroker         │   │
│  │  (unchanged, for existing 1004 tests)            │   │
│  └──────────────────────────────────────────────────┘   │
│                                                          │
└─────────────────────────────────────────────────────────┘
         │
         │ localhost:9094 (EXTERNAL listener)
         ▼
    Kafka Broker (Docker)
```

## References

- [Apache Kafka Documentation](https://kafka.apache.org/documentation/)
- [KRaft Mode](https://kafka.apache.org/documentation/#kraft)
- [Docker Kafka Image](https://hub.docker.com/r/apache/kafka)
- [Kafka Docker Compose Example](https://github.com/apache/kafka/blob/trunk/docker/README.md)
- [librdkafka](https://github.com/confluentinc/librdkafka)
- [MessageBroker Interface (Phase 18B)](../learning/phase-18-async-messaging.md)
