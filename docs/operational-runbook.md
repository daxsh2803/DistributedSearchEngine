# Distributed Search Engine — Operational Runbook

This runbook provides step-by-step, reproducible procedures for operators and engineers to build, configure, deploy, operate, monitor, and recover a 3-node Distributed Search Engine (DSE) cluster with Kafka integration.

All steps use verified commands, CLI flags, configuration parameters, and APIs taken directly from the codebase.

---

## 1. Prerequisites & Environment Setup

- **Operating System:** Windows (PowerShell / Command Prompt) or Linux (Bash)
- **Compiler:** C++20 compliant compiler (MSVC 2022 v143+ on Windows; GCC 11+ or Clang 13+ on Linux)
- **Build System:** CMake 3.20+ and Ninja (or MSVC / Make)
- **Container Runtime:** Docker and Docker Compose v2+ (for Kafka development infrastructure)
- **Networking Tools:** `curl` and Python 3.8+ (for test orchestrations)

---

## 2. End-to-End Operational Lifecycle (23-Step Verified Procedure)

### Step 1: Build the System
Build the project with native Apache Kafka support enabled (`ENABLE_KAFKA=ON`):

```bash
# Configure build directory
cmake -B build_kafka -S . -G Ninja -DENABLE_KAFKA=ON -DCMAKE_BUILD_TYPE=Release

# Compile all targets (binaries, tests, benchmarks)
cmake --build build_kafka --config Release
```

*For standard in-memory fallback builds without Kafka:*
```bash
cmake -B build -S .
cmake --build build --config Debug
```

---

### Step 2: Start Apache Kafka Infrastructure
Start the single-node Apache Kafka (KRaft mode, Kafka 4.3.1) container:

```bash
docker compose -f docker/docker-compose.kafka.yml up -d
```

This starts:
- `dse-kafka`: Broker listening on internal `kafka:9092` and external `localhost:9094`.
- `dse-kafka-init`: One-shot container that initializes the topic `documents.mutations` (3 partitions, replication factor 1).

---

### Step 3: Verify Kafka Availability & Topics
Ensure Kafka is healthy and the `documents.mutations` topic is registered:

```bash
# Check container status
docker compose -f docker/docker-compose.kafka.yml ps

# List topics inside the broker
docker compose -f docker/docker-compose.kafka.yml exec -T kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --list
```
**Expected Output:**
```
documents.mutations
```

---

### Step 4: Start the 3-Node Cluster
Launch the three cluster nodes across separate terminals (or background jobs) with full replication ($N=3, S=3, R=3$):

**Terminal 1 (Node 0):**
```bash
./build_kafka/DistributedSearchEngine.exe \
  --node-id 0 \
  --port 8081 \
  --rpc-port 9081 \
  --peers "0=127.0.0.1:9081,1=127.0.0.1:9082,2=127.0.0.1:9083" \
  --shards 3 \
  --replica-factor 3 \
  --data data/node0/
```

**Terminal 2 (Node 1):**
```bash
./build_kafka/DistributedSearchEngine.exe \
  --node-id 1 \
  --port 8082 \
  --rpc-port 9082 \
  --peers "0=127.0.0.1:9081,1=127.0.0.1:9082,2=127.0.0.1:9083" \
  --shards 3 \
  --replica-factor 3 \
  --data data/node1/
```

**Terminal 3 (Node 2):**
```bash
./build_kafka/DistributedSearchEngine.exe \
  --node-id 2 \
  --port 8083 \
  --rpc-port 9083 \
  --peers "0=127.0.0.1:9081,1=127.0.0.1:9082,2=127.0.0.1:9083" \
  --shards 3 \
  --replica-factor 3 \
  --data data/node2/
```

---

### Step 5: Verify Node Health
Check the HTTP `/health` endpoint on all three nodes:

```bash
curl -s http://127.0.0.1:8081/health
curl -s http://127.0.0.1:8082/health
curl -s http://127.0.0.1:8083/health
```
**Expected Response:**
```json
{"status":"ok"}
```

---

### Step 6: Ingest a New Document
Ingest document `1` via Node 0 (`POST /documents`):

```bash
curl -X POST http://127.0.0.1:8081/documents \
  -H "Content-Type: application/json" \
  -d '{"id": 1, "content": "distributed search engine systems and fault tolerance"}'
```
**Expected Response:** (HTTP 201 Created)
```json
{"document_id":1,"terms_indexed":7}
```
*Note:* The write is synchronously committed across all 3 nodes before HTTP 201 is returned.

---

### Step 7: Execute a Distributed Search
Query the cluster using `GET /search`:

```bash
curl -s "http://127.0.0.1:8081/search?q=distributed+fault&mode=or&limit=10"
```
**Expected Response:** (HTTP 200 OK)
```json
{
  "complete": true,
  "limit": 10,
  "mode": "or",
  "query": "distributed fault",
  "results": [
    {"document_id": 1, "score": 0.575364}
  ],
  "total": 1
}
```

---

### Step 8: Update an Existing Document
Update document `1` via `PUT /documents/:id`:

```bash
curl -X PUT http://127.0.0.1:8081/documents/1 \
  -H "Content-Type: application/json" \
  -d '{"content": "distributed search engine systems with high performance and fault tolerance"}'
```
**Expected Response:** (HTTP 200 OK)
```json
{"document_id":1,"terms_indexed":9}
```

---

### Step 9: Delete a Document
Delete document `1` via `DELETE /documents/:id`:

```bash
curl -i -X DELETE http://127.0.0.1:8081/documents/1
```
**Expected Response:** (HTTP 204 No Content)

---

### Step 10: Inspect Telemetry & Metrics
Inspect the internal metrics snapshot via `GET /metrics`:

```bash
curl -s http://127.0.0.1:8081/metrics
```
**Sample Output:**
```json
{
  "searches_total": 3,
  "search_errors": 0,
  "search_incomplete": 0,
  "writes_total": 6,
  "write_errors": 0,
  "retries_total": 0,
  "read_failovers_total": 0,
  "load_shed_rejections_total": 0,
  "coordinator_searches_total": 1,
  "coordinator_search_success": 1,
  "coordinator_writes_total": 3,
  "coordinator_write_success": 3,
  "events_total": 3,
  "events_published": 3,
  "events_failed": 0,
  "consumer_lag": 0
}
```

---

### Step 11: Stop a Node (Simulate Node Outage)
Ingest a document first, then terminate Node 2 (press `Ctrl+C` or kill process ID):

```bash
# Re-ingest document 100 on Node 0
curl -X POST http://127.0.0.1:8081/documents \
  -H "Content-Type: application/json" \
  -d '{"id": 100, "content": "resilience test document"}'

# Stop Node 2 process in Terminal 3
```

---

### Step 12: Exercise Read Failover
Execute a search while Node 2 is down. Node 0 will fail to reach Node 2 for Shard 2 and transparently fail over to the replica on Node 1:

```bash
curl -s "http://127.0.0.1:8081/search?q=resilience&mode=or&limit=10"
```
**Expected Response:**
- Returns `complete: true` (all shards answered because Node 0 and Node 1 host replicas for all shards).
- Inspecting `http://127.0.0.1:8081/metrics` shows `read_failovers_total` incremented by 1.

---

### Step 13: Restart the Terminated Node
Restart Node 2 in Terminal 3 using the identical command from Step 4:

```bash
./build_kafka/DistributedSearchEngine.exe \
  --node-id 2 \
  --port 8083 \
  --rpc-port 9083 \
  --peers "0=127.0.0.1:9081,1=127.0.0.1:9082,2=127.0.0.1:9083" \
  --shards 3 \
  --replica-factor 3 \
  --data data/node2/
```

---

### Step 14: Verify Shard Persistence Recovery
Inspect the console output of Node 2 upon startup. The node scans `data/node2/shard-*/documents.jsonl` and rebuilds the inverted index and document store without requiring network synchronization:

```
Loaded 1 persisted documents
NodeServer listening on http://127.0.0.1:9083 (RPC)
Server listening on http://127.0.0.1:8083
```

---

### Step 15: Stop Apache Kafka (Simulate Messaging Outage)
Stop the Kafka broker container:

```bash
docker compose -f docker/docker-compose.kafka.yml stop kafka
```

---

### Step 16: Perform Authoritative Write During Kafka Outage
Submit a document mutation while Kafka is completely offline:

```bash
curl -X POST http://127.0.0.1:8081/documents \
  -H "Content-Type: application/json" \
  -d '{"id": 200, "content": "authoritative write during kafka outage"}'
```
**Expected Response:** (HTTP 201 Created)
```json
{"document_id":200,"terms_indexed":5}
```
**Operational Principle:** Authoritative synchronous write replication across Node 0, Node 1, and Node 2 succeeds independently of Kafka. The client is **not** blocked or failed.

---

### Step 17: Verify EventStore Failure State
Inspect Node 0's metrics or the durable event log (`data/node0/events/events.jsonl`):

```bash
curl -s http://127.0.0.1:8081/metrics | grep "events_"
```
**Observed Metrics:**
- `events_total`: incremented.
- `dispatcher_broker_errors`: incremented.
- `events_failed`: 1.
The event is durably recorded with `status = FAILED` in the `PersistentEventStore`.

---

### Step 18: Restart Apache Kafka
Bring Kafka back online:

```bash
docker compose -f docker/docker-compose.kafka.yml start kafka
```
Verify Kafka readiness:
```bash
docker compose -f docker/docker-compose.kafka.yml exec -T kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --list
```

---

### Step 19: Verify Absence of Automatic Replay
Observe `http://127.0.0.1:8081/metrics`.
- `events_failed` remains `1`.
- `events_published` does **not** increase spontaneously.
- **Architectural Fact:** Kafka recovery does **not** trigger automatic event resend. Failed events stay in `FAILED` state until explicitly replayed.

---

### Step 20: Explicitly Replay Failed Events
In production or automated recovery scripts, trigger replay using the existing mechanism:
`EventDispatcher::replay_failed()`.

This moves the event in `EventStore` from `FAILED` to `PENDING` with its **stable original event ID** preserved, and re-enqueues it to the worker queue.

---

### Step 21: Verify Event Publication & Final Consistency
After triggering `replay_failed()`:
```bash
curl -s http://127.0.0.1:8081/metrics
```
- `events_failed` drops to `0`.
- `events_replayed` increments by `1`.
- `events_published` increments by `1`.
- `consumer_lag` reaches `0`.

---

### Step 22: Run Full Automated Test Suite
Run all 1085 automated tests:

```bash
ctest --test-dir build_kafka -C Release --output-on-failure
```
**Expected Result:**
```
100% tests passed, 0 tests failed out of 1085
```

---

### Step 23: Run Benchmark D (Cluster Concurrency & Load Test)
Execute the Benchmark D regression harness:

```powershell
powershell -ExecutionPolicy Bypass -File benchmarks/run_bench_D.ps1
```
**Observed Benchmark D Characteristics:**
- Zero dropped connections across concurrency levels $c \in \{1, 2, 4, 8, 16\}$.
- Throughput scaling from 20.29 req/s ($c=1$) to 69.43 req/s ($c=16$).
- P50 latency maintained under 120 ms under heavy concurrent saturation.

---

## 3. Configuration Reference

| Parameter / CLI Flag | Environment Variable | Default | Operational Description |
| :--- | :--- | :--- | :--- |
| `--node-id` | `DSE_NODE_ID` | `0` | Unique integer ID of the local node. |
| `--port` | `DSE_PORT` | `8080` | Public HTTP listener port (`HttpServer`). |
| `--rpc-port` | `DSE_RPC_PORT` | `0` (off) | Internal peer-to-peer RPC port (`NodeServer`). |
| `--peers` | `DSE_PEERS` | `""` | Comma-separated list of peer endpoints (e.g. `0=127.0.0.1:9081,1=...`). |
| `--shards` | `DSE_SHARDS` | `1` | Total logical shard count in cluster. |
| `--replica-factor` | `DSE_REPLICATION_FACTOR` | `1` | Number of physical replicas per logical shard. |
| `--data` | `DSE_DATA` | `data/` | Directory path for persisted JSONL shards and event logs. |
| `--max-concurrent` | `DSE_MAX_CONCURRENT_REQUESTS` | `64` | Maximum concurrent in-flight HTTP requests before load-shedding (HTTP 429). |
| *(built-in)* | `DSE_KAFKA_BROKERS` | `localhost:9094` | Comma-separated Kafka bootstrap broker list. |
