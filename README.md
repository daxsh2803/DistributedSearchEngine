# Distributed Search Engine (DSE)

A scalable, fault-tolerant distributed search engine implemented from first principles in modern C++20.

---

## 1. Project Overview

The Distributed Search Engine is a high-performance information retrieval system that partitions documents across shards, executes concurrent TF-IDF and BM25 queries, maintains synchronous multi-replica fault tolerance, and streams domain events through an asynchronous Kafka pipeline.

### Core Architectural Principles

- **Built from First Principles:** Zero external search platform dependencies. No Lucene, Apache Solr, Elasticsearch, or OpenSearch.
- **Modern C++20 Standard:** Extensive use of RAII, smart pointers, strict const-correctness, STL containers, lock-free atomics, and clean ownership boundaries.
- **Two-Tier Authority Model:**
  - **Authoritative Tier (Phase 17):** Synchronous All-Replica Replication via `ShardCoordinator`. Client write operations require all configured replicas to succeed before acknowledgment.
  - **Asynchronous Side Channel (Phases 18–19):** Durable Outbox Event Pipeline (`EventStore` + `EventDispatcher`) publishing to Apache Kafka (`documents.mutations`). Kafka is strictly an auxiliary side channel and **never** sits on the synchronous write path.
- **In-Process Telemetry (Phase 22):** Thread-safe metrics collection (`/metrics`) exposing write/search latency percentiles, circuit breaker states, outbox queues, and polymorphic consumer lag without external monitoring agents.

---

## 2. Project Status

- **Project State: COMPLETE (Phase 30 Final Finish Line)**
- **Completed Phases:**
  - **Phases 1–6:** Tokenization, Inverted Indexing, Query Processing (AND/OR), BM25/TF-IDF Ranking, REST Search API.
  - **Phases 7–10:** JSONL Persistence, Concurrency & Thread Safety, Document Lifecycle CRUD, Shard Partitioning & Hashing.
  - **Phases 11–15:** Node Abstraction, Remote RPC Network, Failure Semantics, Retries with Exponential Backoff, Circuit Breakers.
  - **Phases 16–17:** Metrics Foundation, **Synchronous All-Replica Replication / Cluster Authority Model**.
  - **Phases 18–19:** **Durable Outbox Event Pipeline** (`EventStore`, `EventDispatcher`), Native Apache Kafka Integration (`KafkaClient`, `KafkaConsumer`, `KafkaMessageBroker`, `RemoteEventProcessor`).
  - **Phases 20–22:** Multi-Node Resilience Verification, Automated Benchmarking Suite (Benchmarks A–J), **Unified Operational Observability**.
  - **Phases 24–26:** Distributed Read Resilience & Query Failover, Partial-Availability Search Semantics, Dockerized 3-Node Replicated Cluster.
  - **Phases 27–29:** Concurrency Limits & Edge Shedding, Failure Testing & Fault Injection Scenarios (A–G), **Integrated System Validation & Regression Suite**.
  - **Phase 30:** Comprehensive System Documentation, Operational Runbook, and Architecture Blueprint (Absolute finish line).
- **Test Suite Status:** **1085 / 1085 automated tests passing (100%)**.

---

## 3. Quickstart & Build Instructions

### Prerequisites
- C++20 compiler (MSVC 2022 v143+ on Windows, or GCC 11+ / Clang 13+ on Linux/MinGW)
- CMake 3.20+
- Docker & Docker Compose (optional, required only for native Kafka integration)

### Build Configuration A: Standard Build (In-Memory Fallback)
```bash
# Configure and build using MSVC
cmake -B build -S .
cmake --build build --config Debug

# Run the 1085-test suite
ctest --test-dir build -C Debug --output-on-failure
```

### Build Configuration B: Kafka-Enabled Build
```bash
# Configure and build with Kafka enabled
cmake -B build_kafka -S . -G Ninja -DENABLE_KAFKA=ON
cmake --build build_kafka
```

---

## 4. Running the Engine

### Single-Node Execution
```bash
./build/Debug/DistributedSearchEngine.exe --port 8080 --shards 3 --data data/node0
```

### Multi-Node Replicated Cluster ($N=3, S=3, R=2$)
To start a 3-node cluster on localhost, launch each command in a separate terminal:

```bash
# Node 0 (Public HTTP: 8080, RPC: 9081)
./build/Debug/DistributedSearchEngine.exe --node-id 0 --port 8080 --rpc-port 9081 \
  --peers "0=127.0.0.1:9081,1=127.0.0.1:9082,2=127.0.0.1:9083" \
  --shards 3 --replication-factor 2 --data data/node0

# Node 1 (Public HTTP: 8081, RPC: 9082)
./build/Debug/DistributedSearchEngine.exe --node-id 1 --port 8081 --rpc-port 9082 \
  --peers "0=127.0.0.1:9081,1=127.0.0.1:9082,2=127.0.0.1:9083" \
  --shards 3 --replication-factor 2 --data data/node1

# Node 2 (Public HTTP: 8082, RPC: 9083)
./build/Debug/DistributedSearchEngine.exe --node-id 2 --port 8082 --rpc-port 9083 \
  --peers "0=127.0.0.1:9081,1=127.0.0.1:9082,2=127.0.0.1:9083" \
  --shards 3 --replication-factor 2 --data data/node2
```

### Containerized 3-Node Cluster (Docker Compose)
To run the complete 3-node replicated cluster with Kafka via Docker Compose:

```bash
# Build the image and start the full cluster
docker compose -f docker/docker-compose.cluster.yml up -d --build

# View cluster status (Node 0: 8080, Node 1: 8081, Node 2: 8082)
docker compose -f docker/docker-compose.cluster.yml ps

# Stop the cluster
docker compose -f docker/docker-compose.cluster.yml down
```
For detailed operational and architecture documentation, see [docker/README.md](docker/README.md).

---

## 5. Public HTTP API Summary

| Endpoint | Method | Purpose | Example Request / Output |
| :--- | :---: | :--- | :--- |
| `/documents` | `POST` | Ingest new document | `{"id": 1, "content": "system resilience"}` |
| `/documents/:id` | `PUT` | Update document | `{"content": "updated content"}` |
| `/documents/:id` | `DELETE` | Remove document | Deletes document across all replicas |
| `/search` | `GET` | Cross-shard ranked query | `/search?q=system+resilience&mode=or&limit=10` |
| `/health` | `GET` | Process health check | `{"status": "ok"}` |
| `/metrics` | `GET` | In-process operational telemetry | Returns JSON with latencies, outbox, and lag |

For complete payload formats and error codes, see [docs/api-and-metrics-reference.md](docs/api-and-metrics-reference.md).

---

## 6. Repository Layout & Documentation Map

The comprehensive Phase 30 documentation package is organized as follows:

```
├── README.md                           # Project overview, quickstart, and status
├── AGENTS.md                           # Development methodology & architectural boundaries
├── PROJECT_STATUS.md                   # Final Phase 30 status & milestone verification
├── docs/
│   ├── architecture-blueprint.md       # Authoritative system blueprint & data flows
│   ├── operational-runbook.md          # 23-step reproducible operator & deployment runbook
│   ├── api-and-metrics-reference.md    # Exhaustive HTTP routes & telemetry metrics dictionary
│   ├── failure-semantics.md            # Failure matrix, non-guarantees & validation benchmarks
│   ├── adr-index.md                    # Architecture Decision Records index (ADR-001 to ADR-018)
│   ├── project-structure.md            # Codebase directory layout & component responsibilities
│   ├── decisions/                      # Architecture Decision Records (ADR-001 through ADR-018)
│   └── learning/                       # Deep-dive learning notes (Phases 0–22)
├── src/                                # Engine C++20 source & headers
├── tests/                              # GoogleTest suites (1085 tests) & E2E orchestrators
├── benchmarks/                         # Performance & chaos harness scripts (A–J)
├── results/                            # Recorded benchmark CSV outputs
└── docker/                             # Apache Kafka Docker Compose setup & cluster manifests
```

---

## 7. Deliberate Scope Boundaries

To maintain first-principles engineering rigor, the following components are **deliberately excluded**:
- **No Third-Party Query Engines:** No Lucene, Solr, or Elasticsearch.
- **No External Data Stores:** No Redis or PostgreSQL (document store and inverted index are implemented directly in C++).
- **No In-Band Consensus Frameworks:** No Raft or Paxos; replication is governed by the Synchronous All-Replica Replication / Cluster Authority Model in `ShardCoordinator`. Document mutations succeed only when the configured synchronous replica writes required by ShardCoordinator succeed. This is NOT Raft, Paxos, or another distributed consensus protocol.
- **No External Telemetry Agents:** No Prometheus or OpenTelemetry daemons; telemetry is native in-process JSON via `/metrics`.
- **No Exactly-Once Semantics or Distributed Transactions:** Messaging operates with at-least-once delivery and manual consumer commits.
