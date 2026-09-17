# Phase 26 — Dockerization and Multi-Node Cluster Orchestration

This document details the design, architecture, and operation of the containerized Distributed Search Engine (DSE) cluster, implementing Phase 26.

---

## 1. Architectural Motivation & Principles

The primary objective of Phase 26 is to containerize the existing distributed search engine into a portable, reproducible 3-node cluster with Kafka KRaft integration, without modifying the core distributed architecture.

### Invariants Preserved
1. **Synchronous Replication Authority (Phase 17):** Write mutations continue to require synchronous execution across all designated shard replicas. Containerization introduces no asynchronous write shortcuts.
2. **Asynchronous Side Channel (Phases 18–19):** `EventStore` and `EventDispatcher` continue to manage local outbox persistence and asynchronous event publishing to Kafka (`documents.mutations`). Kafka remains an auxiliary pub/sub side channel.
3. **Query Resilience & Pinning (Phase 24):** Shard coordinator pins replicas per query; mid-query failover behavior is unchanged.
4. **Partial Availability Semantics (Phase 25):** Partial search responses (`complete: false`) remain valid when replicas become unreachable.
5. **Circuit Breakers & Retries (Phases 14–15):** Unchanged per-node RPC failure detection and backoff.
6. **No External Abstractions:** No Kubernetes, no Redis, no distributed consensus engines (Raft/Paxos), and no transaction coordinators are introduced.

> **Crucial System Distinction:** Containerization is purely an infrastructure and runtime packaging mechanism. It does **not** provide stronger replication, ordering, or consistency guarantees than what the engine's internal C++ implementation provides.

---

## 2. Containerization Architecture

### 2.1 Multi-Stage Linux Build (`Dockerfile`)

The root `Dockerfile` adopts a multi-stage build using `debian:bookworm-slim` to produce a minimal, security-hardened production image:

- **Stage 1 (Builder):**
  - Installs compilation tools: `build-essential`, `cmake`, `git`, and `librdkafka-dev`.
  - Configures CMake with `-DCMAKE_BUILD_TYPE=Release`, `-DENABLE_KAFKA=ON`, and `-DBUILD_TESTING=OFF`.
  - Builds the `DistributedSearchEngine` binary.
- **Stage 2 (Runtime):**
  - Fresh `debian:bookworm-slim` container base.
  - Installs only runtime shared libraries: `librdkafka1`, `librdkafka++1`, `ca-certificates`, and `curl` (for health checking).
  - Copies the compiled binary to `/usr/local/bin/DistributedSearchEngine`.
  - Sets entrypoint directly to the executable.

### 2.2 Network Topology (`dse-network`)

All services communicate over a dedicated user-defined Docker bridge network named `dse-network`:

```
               Host Machine (Windows / macOS / Linux)
             ┌────────────────────────────────────────┐
             │  :8080        :8081        :8082       │
             └────┬────────────┬────────────┬─────────┘
                  │            │            │
══════════════════╪════════════╪════════════╪══════════════════ Bridge: dse-network
                  │            │            │
            ┌─────▼──────┐┌────▼───────┐┌───▼────────┐
            │ dse-node-0 ││ dse-node-1 ││ dse-node-2 │
            │  RPC: 9000 ││  RPC: 9001 ││  RPC: 9002 │
            └─────┬──────┘└────┬───────┘└───┬────────┘
                  │            │            │ (HTTP RPC)
                  └────────────┼────────────┘
                               │
                      kafka:9092 (Internal)
                               │
                       ┌───────▼───────┐
                       │     kafka     │ (KRaft Broker/Controller)
                       │  Host: :9094  │
                       └───────────────┘
```

- **HTTP Service Ports:** Mapped from host ports `8080`, `8081`, `8082` to container port `8080`.
- **NodeServer RPC Ports:** Internal to `dse-network` (`9000`, `9001`, `9002`). Not exposed to the host, protecting cluster-internal RPC endpoints.
- **Docker DNS Resolution:** Nodes address each other using Docker container hostnames (`dse-node-0`, `dse-node-1`, `dse-node-2`).

### 2.3 Peer Configuration Format

The peer topology is injected via the existing `DSE_PEERS` environment variable:
```bash
DSE_PEERS="0=dse-node-0:9000,1=dse-node-1:9001,2=dse-node-2:9002"
```
This is parsed by `dse::parse_peer_topology()` to build the cluster node directory.

---

## 3. Storage Persistence Topology

Named Docker volumes ensure fast, reliable storage isolated per container, avoiding host-to-VM translation bottlenecks on Windows Docker Desktop:

| Volume Name | Container Mount Path | Service | Contents |
|:---|:---|:---|:---|
| `dse-data-0` | `/app/data` | `dse-node-0` | Shard JSONL files (`shard-*/documents.jsonl`), EventStore outbox (`events/`) |
| `dse-data-1` | `/app/data` | `dse-node-1` | Shard JSONL files, EventStore outbox |
| `dse-data-2` | `/app/data` | `dse-node-2` | Shard JSONL files, EventStore outbox |
| `kafka-data` | `/var/lib/kafka/data` | `kafka` | KRaft metadata log, partition segment files |

> **Isolation Rule:** Nodes never share storage volumes. Each node maintains its own independent persistence directory.

---

## 4. Operational Guide

### 4.1 Prerequisites
- **Docker Desktop** (v24.0+) or Docker Engine on Linux.
- **Docker Compose** (v2.0+).
- At least 4 GB RAM allocated to Docker.

### 4.2 Building the Cluster Image
```bash
docker compose -f docker/docker-compose.cluster.yml build
```

### 4.3 Starting the Cluster
```bash
docker compose -f docker/docker-compose.cluster.yml up -d
```

**Startup Sequencing:**
1. `kafka` starts in KRaft mode and initiates self-health checks.
2. When `kafka` is healthy, `kafka-init` executes `kafka-topics.sh` to provision `documents.mutations` with 3 partitions and replication factor 1.
3. Upon completion of `kafka-init`, `dse-node-0`, `dse-node-1`, and `dse-node-2` start concurrently.
4. Each search node loads its local persisted shards from `/app/data/`, binds RPC and HTTP listeners, and begins listening.

### 4.4 Monitoring and Status Verification
```bash
# Check container status and health
docker compose -f docker/docker-compose.cluster.yml ps

# View unified cluster logs
docker compose -f docker/docker-compose.cluster.yml logs

# Follow logs from a single node
docker compose -f docker/docker-compose.cluster.yml logs -f dse-node-0
```

### 4.5 Cluster Endpoints
- **Node 0:** `http://localhost:8080` (`/health`, `/metrics`, `/search`, `/documents`)
- **Node 1:** `http://localhost:8081`
- **Node 2:** `http://localhost:8082`
- **Kafka Host Access:** `localhost:9094`

### 4.6 Stopping the Cluster
```bash
# Stop containers gracefully; persistent data in named volumes is preserved
docker compose -f docker/docker-compose.cluster.yml down
```

### 4.7 Intentionally Purging Cluster State
To wipe all documents, shards, outbox events, and Kafka logs:
```bash
# CAUTION: Irreversible data deletion
docker compose -f docker/docker-compose.cluster.yml down -v
```

---

## 5. Verification and Failure Semantics

### 5.1 Replicated Mutation Test
```bash
# Ingest document into Node 0
curl -i -X POST http://localhost:8080/documents \
  -H "Content-Type: application/json" \
  -d '{"id": 50, "content": "fault tolerant distributed search systems"}'

# Search immediately from Node 1 (proves synchronous replication)
curl -s "http://localhost:8081/search?q=tolerant&mode=and" | jq .
```

### 5.2 Failure and Partial Availability Test
Simulate a node crash:
```bash
docker stop dse-node-2
```
1. Perform search on `http://localhost:8080/search?q=tolerant`.
2. Observe `complete: false` when queried shards reside on the offline node, verifying Phase 25 partial-availability semantics.
3. Restart node:
```bash
docker start dse-node-2
```
4. Perform search again; verify response recovers to `complete: true`.
