# Distributed Search Engine — Operator Runbook

This guide covers building, testing, configuring, running, and troubleshooting the Distributed Search Engine in both single-node and multi-node replicated modes.

---

## 1. Prerequisites

- **C++ Compiler:** Modern C++20 compliant compiler (MSVC v143+ on Windows, GCC 11+ or Clang 13+ on Linux/MinGW).
- **CMake:** Version 3.20 or newer.
- **Python:** Python 3.8+ (for benchmark execution and integration testing scripts).
- **Docker & Docker Compose (Optional):** Required only when running with Apache Kafka enabled (`-DENABLE_KAFKA=ON`).

---

## 2. Repository Layout

```
├── CMakeLists.txt              # CMake build configuration
├── docker/
│   ├── docker-compose.kafka.yml# Kafka 4.3.1 (KRaft mode) single-node compose
│   └── README.md               # Kafka Docker operational guide
├── docs/
│   ├── architecture/           # System architecture blueprint
│   ├── decisions/              # Architecture Decision Records (ADR-001 to ADR-018)
│   ├── learning/               # Detailed conceptual notes per phase
│   └── operations/             # This runbook
├── src/                        # Core C++20 engine sources & headers
├── tests/                      # GoogleTest integration and unit test suites
├── benchmarks/                 # Automated performance & resilience harnesses (A–J)
└── results/                    # Recorded benchmark data and latency CSVs
```

---

## 3. Building the Engine

The engine supports two primary build configurations:

### Configuration A: Standard Build (Kafka Disabled, In-Memory Broker)

Builds with pure STL and in-memory messaging fallback. Does not require external libraries or Docker.

```bash
# Generate build files using MSVC
cmake -B build -S .

# Compile all targets in Debug configuration
cmake --build build --config Debug
```

### Configuration B: Kafka-Enabled Build (librdkafka Integration)

Builds with native Kafka integration enabled (`DSE_KAFKA_ENABLED`).

```bash
# Generate Ninja build files with Kafka enabled
cmake -B build_kafka -S . -G Ninja -DENABLE_KAFKA=ON

# Compile with Ninja
cmake --build build_kafka
```

---

## 4. Running the Test Suites

### Running the Full CTest Suite (Standard Build)

```bash
# Run all 1051 unit, integration, and concurrency tests
ctest --test-dir build -C Debug --output-on-failure
```

### Running Specific Test Binaries

```bash
# HTTP and metrics regression test suite
./build/Debug/http_metrics_test.exe

# Cluster coordination metrics test suite
./build/Debug/coordinator_metrics_test.exe

# In-memory message broker test suite
./build/Debug/message_broker_test.exe

# Kafka message broker tests (requires live Kafka at localhost:9094)
./build_kafka/kafka_message_broker_test.exe
```

---

## 5. Command-Line Reference & Configuration

Configuration precedence: **CLI Flags > Environment Variables > Defaults**.

| CLI Argument | Environment Variable | Default | Description |
| :--- | :--- | :--- | :--- |
| `--port <num>` | `DSE_PORT` | `8080` | Public HTTP REST API port. |
| `--rpc-port <num>` | `DSE_RPC_PORT` | `0` (disabled) | Private Inter-Node RPC listener port. Set $>0$ in multi-node mode. |
| `--node-id <num>` | `DSE_NODE_ID` | `0` | Physical node identity index ($0 \le \text{node\_id} < N$). |
| `--peers <spec>` | `DSE_PEERS` | `""` | Comma-separated peer topology: `0=ip:port,1=ip:port,...` |
| `--shards <num>` | `DSE_SHARDS` | `1` | Total logical shard count across the cluster ($S$). |
| `--replication-factor <num>` | `DSE_REPLICATION_FACTOR` | `1` | Replication factor ($R$). Requires $1 \le R \le N$. |
| `--data <path>` | `DSE_DATA` | `data/` | Root directory for document store and event outbox persistence. |
| `--kafka-bootstrap <servers>`| `DSE_KAFKA_BOOTSTRAP` | `localhost:9094`| Kafka broker address (Kafka build only). |
| `--kafka-group <group_id>` | `DSE_KAFKA_GROUP` | `dse-consumer-group` | Kafka consumer group identifier. |

---

## 6. Starting Apache Kafka Infrastructure

When running with Kafka enabled:

```bash
# Start Kafka container in background
docker compose -f docker/docker-compose.kafka.yml up -d

# Verify container health (status should report 'healthy')
docker compose -f docker/docker-compose.kafka.yml ps

# Confirm the mutation topic was created automatically
docker compose -f docker/docker-compose.kafka.yml exec kafka \
  /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --list
```

The active single topic is:
```text
documents.mutations
```

---

## 7. Operational Modes & Examples

### Mode 1: Single-Node Standalone

Runs all shards locally with synchronous replication factor $R=1$.

```bash
./build/Debug/DistributedSearchEngine.exe --port 8080 --shards 3 --data data/node0
```

---

### Mode 2: Three-Node Replicated Cluster ($N=3, S=3, R=2$)

To run a fault-tolerant cluster on a single host, run 3 separate terminal sessions:

#### Terminal 1 — Node 0
```bash
./build/Debug/DistributedSearchEngine.exe \
  --node-id 0 \
  --port 8080 \
  --rpc-port 9081 \
  --peers "0=127.0.0.1:9081,1=127.0.0.1:9082,2=127.0.0.1:9083" \
  --shards 3 \
  --replication-factor 2 \
  --data data/node0
```

#### Terminal 2 — Node 1
```bash
./build/Debug/DistributedSearchEngine.exe \
  --node-id 1 \
  --port 8081 \
  --rpc-port 9082 \
  --peers "0=127.0.0.1:9081,1=127.0.0.1:9082,2=127.0.0.1:9083" \
  --shards 3 \
  --replication-factor 2 \
  --data data/node1
```

#### Terminal 3 — Node 2
```bash
./build/Debug/DistributedSearchEngine.exe \
  --node-id 2 \
  --port 8082 \
  --rpc-port 9083 \
  --peers "0=127.0.0.1:9081,1=127.0.0.1:9082,2=127.0.0.1:9083" \
  --shards 3 \
  --replication-factor 2 \
  --data data/node2
```

---

## 8. HTTP API Reference

All requests and responses use `application/json`.

### Document Ingestion
```http
POST /documents
Content-Type: application/json

{
  "id": 101,
  "content": "High performance distributed full text search engines"
}
```
*Response (201 Created):*
```json
{
  "document_id": 101,
  "terms_indexed": 6,
  "status": "created"
}
```

### Document Retrieval
```http
GET /documents/101
```
*Response (200 OK):*
```json
{
  "id": 101,
  "content": "High performance distributed full text search engines"
}
```

### Document Search
```http
GET /search?q=fault+tolerant&mode=and&limit=10
```
- `mode`: `and` (intersection, default) or `or` (union).
- `limit`: Maximum results to return (default: 10).

*Response (200 OK):*
```json
{
  "query": "fault tolerant",
  "total": 1,
  "complete": true,
  "results": [
    {
      "document_id": 101,
      "score": 1.4589
    }
  ]
}
```

### Health Check
```http
GET /health
```
*Response (200 OK):*
```json
{
  "status": "healthy",
  "timestamp": 1789374000
}
```

### Operational Metrics
```http
GET /metrics
```
*Response (200 OK):*
```json
{
  "searches_total": 420,
  "search_errors": 0,
  "search_incomplete": 0,
  "writes_total": 105,
  "write_errors": 0,
  "retries_total": 0,
  "circuit_open_events": 0,
  "coordinator_searches_total": 420,
  "coordinator_writes_total": 105,
  "events_total": 105,
  "events_published": 105,
  "dispatcher_enqueued": 105,
  "dispatcher_pending": 0,
  "dispatcher_rejected": 0,
  "consumer_lag": 0,
  "consumer_messages_consumed": 105,
  "consumer_messages_acked": 105,
  "consumer_messages_nacked": 0
}
```

---

## 9. Troubleshooting & Failure Recovery

### Common Startup Errors

1. **`Invalid peer topology: node IDs must be strictly contiguous`**
   - **Cause:** The `--peers` argument has missing IDs (e.g. `0=...,2=...` without `1`).
   - **Fix:** Peer specs must form a complete 0-indexed sequence: `0=...,1=...,2=...`.

2. **`Failed to bind NodeServer on RPC port`**
   - **Cause:** Another process is occupying the configured `--rpc-port`.
   - **Fix:** Verify open ports using `netstat -ano | findstr <port>` and assign an unused port.

3. **`Replication factor R cannot exceed node count N`**
   - **Cause:** Specifying `--replication-factor 3` on a cluster with only 2 peers defined.
   - **Fix:** Ensure $R \le N$.

### Node Recovery Procedure

When a node process crashes or is killed:
1. Re-launch the process using its original `--node-id` and `--data` directory.
2. The node automatically:
   - Loads local shard segments from `data/nodeX/shard_*.jsonl`.
   - Reconstructs its in-memory inverted index.
   - Reloads `data/nodeX/events/events.jsonl` into `PersistentEventStore`.
   - Transitions any events left in `DISPATCHING` state back to `PENDING`.
3. Peer nodes automatically reset circuit breakers from `OPEN` to `CLOSED` after the half-open probe succeeds.
