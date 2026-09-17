# Distributed Search Engine — Project Structure & Repository Layout

This document details the architectural layout, directory organization, component responsibilities, and boundary rules of the Distributed Search Engine (DSE) codebase.

---

## 1. Top-Level Repository Organization

```
DistributedSearchEngine/
├── src/                  # Core search engine, replication, RPC, outbox, and messaging
├── tests/                # Unit, integration, fault injection, and E2E regression tests
├── benchmarks/           # Standalone performance harnesses (Benchmarks A–J)
├── docker/               # Docker Compose environments for Kafka and 3-node cluster
├── docs/                 # Authoritative documentation, architecture blueprints, and ADRs
│   ├── decisions/        # Architecture Decision Records (ADR-001 through ADR-018)
│   └── learning/         # Phase-by-phase learning and educational notes
├── CMakeLists.txt        # Top-level CMake build configuration
├── README.md             # Project overview, quickstart, and operational summary
├── AGENTS.md             # Development methodology and architectural boundaries
└── PROJECT_STATUS.md     # Final project status and validation summary
```

> [!NOTE]
> Build output directories (such as `build/`, `build_kafka/`, or `data/`) contain transient compiler artifacts, object files, binaries, or local runtime storage. They are intentionally excluded from source control and are not architectural source code.

---

## 2. Directory Responsibilities & Key Components

### 2.1 `src/` — Core Engine Implementation
Contains all production C++20 source and header files implementing the search engine, networking, and event pipelines:

- **Core Search & Storage:**
  - `tokenizer.h` / `.cpp`: UTF-8 token stream generation, case normalization, and punctuation filtering.
  - `inverted_index.h` / `.cpp`: In-memory posting list index protected by reader-writer locks (`std::shared_mutex`).
  - `document_store.h` / `.cpp`: Key-value document store recording document text and length.
  - `query_processor.h` / `.cpp`: Term set operations (intersection for AND, union for OR).
  - `ranker.h` / `.cpp`: TF-IDF scoring engine with global collection document counts.
  - `shard.h` / `.cpp`: Shard encapsulation combining an InvertedIndex and DocumentStore with append-only JSONL persistence.

- **Distributed Clustering & RPC:**
  - `shard_router.h` / `.cpp`: Hash-based document-to-shard routing (`doc_id % shard_count`).
  - `replica_placement.h` / `.cpp`: Deterministic shard replica set generation across $N$ nodes.
  - `node_config.h` / `.cpp`: Static peer topology parsing (`node_id=host:port`).
  - `node_client.h`: Abstract node client interface.
  - `local_node.h` / `.cpp`: In-process node implementation managing locally hosted shards.
  - `remote_node.h` / `.cpp`: Remote RPC node client with connection timeouts, retries, and circuit breakers.
  - `node_server.h` / `.cpp`: Lightweight HTTP/RPC server exposing internal shard operations to peers.
  - `shard_coordinator.h` / `.cpp`: Cluster coordinator managing synchronous all-replica replication, scatter-gather queries, read failover, and outbox event triggering.

- **Networking, Resilience & Concurrency:**
  - `http_server.h` / `.cpp`: Public REST HTTP interface (`/search`, `/documents`, `/health`, `/metrics`) with load-shedding concurrency guards (`RequestSlotGuard`).
  - `circuit_breaker.h` / `.cpp`: Three-state circuit breaker pattern protecting inter-node RPC calls.
  - `retry_policy.h` / `.cpp`: Exponential backoff with jitter retry algorithm.

- **Outbox & Event Subsystems:**
  - `event_store.h` / `event_store.cpp`: Abstract interface and in-memory outbox tracking event lifecycles.
  - `persistent_event_store.h` / `.cpp`: Durable, crash-safe JSONL outbox persisting mutation events to disk.
  - `event_dispatcher.h` / `.cpp`: Bounded worker queue dispatching outbox events to messaging brokers with retry handling and `replay_failed()`.
  - `document_event.h` / `.cpp`: Structured mutation event records (`DocumentIndexedEvent`, `DocumentUpdatedEvent`, `DocumentRemovedEvent`).
  - `message_broker.h` / `in_memory_message_broker.h`: Abstract broker interface and in-memory test implementation.
  - `kafka_client.h` / `.cpp`: Low-level wrapper around `librdkafka`.
  - `kafka_consumer.h` / `.cpp`: Bounded consumer thread pulling events from Kafka topics.
  - `kafka_message_broker.h` / `.cpp`: High-level messaging broker binding Kafka producer and consumers.
  - `remote_event_processor.h` / `.cpp`: Consumes mutation events, enforces loopback suppression, and applies mutations to local shards.

- **Observability:**
  - `metrics.h` / `.cpp`: Thread-safe, lock-free operational metrics collector recording operation counts, latencies, circuit states, and consumer lag.

---

### 2.2 `tests/` — Automated Verification Suites
Houses unit tests, distributed integration tests, and fault injection scenarios:
- **Unit Tests:** `tokenizer_test.cpp`, `inverted_index_test.cpp`, `query_processor_test.cpp`, `ranker_test.cpp`, `document_store_test.cpp`, `shard_test.cpp`.
- **Concurrency & Load Tests:** `concurrency_test.cpp`, `load_management_test.cpp`.
- **Fault Injection & Resilience:** `fault_injection_test.cpp` (Phase 28 Scenarios A–G verifying replica outages, Kafka partitions, and circuit breaks).
- **System Integration & E2E:**
  - `phase29_system_integration_test.cpp`: Comprehensive 3-node in-process system test verifying the full distributed lifecycle, read failover, persistence recovery, and Kafka replay.
  - `e2e_phase29_system_test.py`: Live Docker/Kafka orchestrator driving end-to-end multi-process verification.

---

### 2.3 `benchmarks/` — Performance & Load Harnesses
Independent benchmark scripts and harnesses validating throughput, latency percentiles, and scaling:
- `run_bench_D.ps1`: Multi-node concurrency and saturation benchmark ($c \in \{1, 2, 4, 8, 16\}$).
- Historical Benchmarks A–J covering tokenization, indexing, query parsing, network latency, and consumer lag.

---

### 2.4 `docker/` — Containerized Infrastructure
Docker Compose definitions and scripts for development and deployment:
- `docker-compose.kafka.yml`: Single-node Apache Kafka (KRaft mode, no ZooKeeper) on external port `9094` with automatic topic initialization (`documents.mutations`).
- `docker-compose.cluster.yml`: Complete 3-node containerized cluster with integrated Kafka broker and networking.
- `README.md`: Container operations and clustering guide.

---

### 2.5 `docs/` — Authoritative Documentation
Comprehensive engineering documentation:
- `architecture-blueprint.md`: Authoritative architectural specification and data flow diagrams.
- `operational-runbook.md`: 23-step reproducible operational lifecycle guide.
- `api-and-metrics-reference.md`: Complete HTTP routes, parameters, status codes, and metrics dictionary.
- `failure-semantics.md`: Formal specification of failure modes, non-guarantees, and empirical validation results.
- `adr-index.md`: Directory of all 18 Architecture Decision Records.
- `project-structure.md`: This repository layout document.
- `decisions/`: Individual ADR documents (ADR-001 through ADR-018).
- `learning/`: Deep-dive learning notes tracing each phase of construction.
