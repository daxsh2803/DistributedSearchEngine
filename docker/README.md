# Docker Infrastructure

Docker configuration for local development and testing infrastructure.

## Prerequisites

- **Docker Desktop** installed and running (v24+)
- **Docker Compose** v2.0+ (included with Docker Desktop)
- **4 GB RAM** allocated to Docker Desktop (Kafka needs ~256MB)

### Starting Docker Desktop

**Windows:**
1. Open Docker Desktop from Start Menu
2. Wait for the status indicator to show "Docker Desktop is running"

**macOS:**
1. Open Docker from Applications
2. Wait for the whale icon in the menu bar to stop animating

**Linux:**
```bash
sudo systemctl start docker
```

### Verifying Docker is Running

```bash
docker --version
docker compose version
docker info | head -5
```

## Kafka Development Setup

### Starting Kafka

```bash
# From the project root directory
docker compose -f docker/docker-compose.kafka.yml up -d
```

This starts:
- **Apache Kafka 4.3.1** in KRaft mode (no ZooKeeper)
- Single node acting as both broker and controller
- Ephemeral storage (data lost on container removal)
- Internal listener on `kafka:9092` (container-to-container)
- External listener on `localhost:9094` (host access)

### Checking Kafka Health

```bash
# Check container status
docker compose -f docker/docker-compose.kafka.yml ps

# Wait for health check to pass (may take 30-60 seconds)
# Note: Inside the container, use localhost:9092 (INTERNAL listener)
docker compose -f docker/docker-compose.kafka.yml exec kafka \
  /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --list
```

The health check runs `kafka-topics.sh --list` every 10 seconds. When it returns successfully, Kafka is ready.

### Creating Topics

Topics are automatically created by the `kafka-init` container when Kafka starts.

To manually create or recreate topics:

```bash
./docker/kafka/create-topics.sh
```

### Listing Topics

```bash
# Note: Inside the container, use localhost:9092 (INTERNAL listener)
docker compose -f docker/docker-compose.kafka.yml exec kafka \
  /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --list
```

Expected output:
```
documents.mutations
```

### Describing Topics

```bash
# Describe all topics (inside container, use INTERNAL listener)
docker compose -f docker/docker-compose.kafka.yml exec kafka \
  /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --describe

# Describe specific topic
docker compose -f docker/docker-compose.kafka.yml exec kafka \
  /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 \
  --describe --topic documents.mutations
```

Expected output for a topic:
```
Topic: documents.mutations	TopicId: ...	PartitionCount: 3	ReplicationFactor: 1	Configs: segment.bytes=1073741824
	Topic: documents.mutations	Partition: 0	Leader: 1	Replicas: 1	Isr: 1
	Topic: documents.mutations	Partition: 1	Leader: 1	Replicas: 1	Isr: 1
	Topic: documents.mutations	Partition: 2	Leader: 1	Replicas: 1	Isr: 1
```

### Connecting from Applications

**Bootstrap server (from host machine):** `localhost:9094`

**Bootstrap server (from Docker container):** `kafka:9092`

**Protocol:** PLAINTEXT (no authentication)

**Connection example (librdkafka):**
```c
rd_kafka_conf_t *conf = rd_kafka_conf_new();
// For host machine connection:
rd_kafka_conf_set(conf, "bootstrap.servers", "localhost:9094", NULL, 0);
// For Docker container connection:
// rd_kafka_conf_set(conf, "bootstrap.servers", "kafka:9092", NULL, 0);
```

**Connection example (Java):**
```properties
# For host machine connection:
bootstrap.servers=localhost:9094
# For Docker container connection:
# bootstrap.servers=kafka:9092
```

### Stopping Kafka

```bash
# Stop containers (preserves data in ephemeral storage)
docker compose -f docker/docker-compose.kafka.yml stop

# Stop and remove containers (data is lost)
docker compose -f docker/docker-compose.kafka.yml down

# Stop and remove containers + volumes
docker compose -f docker/docker-compose.kafka.yml down -v
```

### Cleanup

To completely clean up all Kafka data:

```bash
docker compose -f docker/docker-compose.kafka.yml down -v
docker system prune -f
```

## Architecture

### KRaft Mode (No ZooKeeper)

Apache Kafka 4.0+ uses KRaft (Kafka Raft) for metadata management, replacing ZooKeeper.

**Benefits:**
- Single process (no separate ZooKeeper)
- Simpler deployment and configuration
- Better performance
- Recommended for all deployments

**Components in this setup:**
- **Broker**: Handles produce/consume requests
- **Controller**: Manages metadata, topic partitions, leader election
- Both roles run in a single process (single-node setup)

### Listener Configuration

```
┌─────────────────────────────────────┐
│         Kafka Container              │
│                                      │
│  INTERNAL://0.0.0.0:9092            │
│     ↕ (container-to-container)      │
│  kafka:9092                         │
│                                      │
│  EXTERNAL://0.0.0.0:9094            │
│     ↕ (host port mapping)           │
│  localhost:9094                      │
│                                      │
│  CONTROLLER://0.0.0.0:9093          │
│     ↕ (internal only)               │
│  localhost:9093                      │
└─────────────────────────────────────┘
```

- **INTERNAL (9092)**: Container-to-container communication (kafka-init, future app containers)
- **EXTERNAL (9094)**: Host machine access (your development machine)
- **CONTROLLER (9093)**: Internal controller communication (single-node, so localhost)

### Topics

| Topic | Partitions | Replication | Purpose |
|-------|-----------|-------------|---------|
| `documents.mutations` | 3 | 1 | Unified document mutation events (indexed, updated, removed) |

**Why 3 partitions?**
- Allows testing multi-partition consumer scenarios
- Enables future consumer group testing
- Reasonable default for development

**Why replication factor 1?**
- Single Docker container cannot replicate across nodes
- Production deployments should use replication factor ≥ 3
- Simplifies local development setup

### Message Keys

Events use `document_id` as the message key:
- Ensures all events for the same document go to the same partition
- Guarantees ordering per document within a partition
- Enables partition-level parallel processing

## Troubleshooting

### Docker Desktop Not Starting

**Error:** `Cannot connect to the Docker daemon`

**Solution:**
1. Open Docker Desktop application
2. Wait for "Docker Desktop is running" status
3. If Docker Desktop fails to start, check system requirements (4GB RAM minimum)

### Kafka Health Check Failing

**Error:** Health check retries exceeded

**Solutions:**
1. Wait 60 seconds after starting (Kafka needs time to initialize)
2. Check container logs: `docker compose -f docker/docker-compose.kafka.yml logs kafka`
3. Check available memory: Docker Desktop needs at least 4GB
4. Restart Docker Desktop and try again

### Port 9092 Already in Use

**Error:** `Port is already allocated`

**Solutions:**
1. Stop other Kafka instances: `docker stop $(docker ps -q --filter ancestor=apache/kafka)`
2. Change the port in docker-compose.kafka.yml:
   ```yaml
   ports:
     - "9093:9092"
   ```
3. Update KAFKA_ADVERTISED_LISTENERS to match

### Topics Not Created

**Solutions:**
1. Wait for Kafka to be healthy first
2. Run topic creation manually: `./docker/kafka/create-topics.sh`
3. Check kafka-init logs: `docker compose -f docker/docker-compose.kafka.yml logs kafka-init`

### Container Won't Start

**Check logs:**
```bash
docker compose -f docker/docker-compose.kafka.yml logs kafka
```

**Common issues:**
- Insufficient memory (need 256MB for Kafka)
- Port conflicts
- Corrupted container state (try `docker compose down -v` and restart)

## Development Workflow

### Starting Development Session

```bash
# 1. Start Kafka
docker compose -f docker/docker-compose.kafka.yml up -d

# 2. Wait for health
docker compose -f docker/docker-compose.kafka.yml ps
# Wait until kafka shows "healthy" status

# 3. Verify topics (inside container, use INTERNAL listener)
docker compose -f docker/docker-compose.kafka.yml exec kafka \
  /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --list

# 4. Run development (Kafka is available at localhost:9094 from host)
```

### Running Tests

```bash
# Kafka must be running for Kafka-specific tests
# Other tests run without Kafka

# Run all tests (Kafka tests will be skipped if Kafka is not running)
cd build && ctest --output-on-failure

# Run only Kafka-related tests (when available)
cd build && ctest -R kafka
```

### Ending Development Session

```bash
# Stop Kafka (preserves nothing - ephemeral storage)
docker compose -f docker/docker-compose.kafka.yml down -v
```

---

## Full Replicated Cluster Deployment (Phase 26)

Phase 26 provides a complete, containerized 3-node distributed search cluster with synchronous replication and asynchronous Kafka event streaming.

### Prerequisites

- **Docker Desktop** (Windows/macOS) or **Docker Engine** (Linux) v24.0+
- **Docker Compose** v2.0+
- Minimum 4 GB RAM allocated to Docker Desktop

### Cluster Architecture

The Compose deployment mirrors the bare-metal 3-node topology ($N=3, S=3, R=3$):
- **dse-node-0**: Node ID 0, public HTTP on container port 8080 (mapped to `localhost:8080`), RPC port 9000.
- **dse-node-1**: Node ID 1, public HTTP on container port 8080 (mapped to `localhost:8081`), RPC port 9001.
- **dse-node-2**: Node ID 2, public HTTP on container port 8080 (mapped to `localhost:8082`), RPC port 9002.
- **kafka**: Apache Kafka 4.3.1 in KRaft mode, accessible internally at `kafka:9092` and from the host at `localhost:9094`.
- **kafka-init**: Ephemeral setup container that pre-provisions the `documents.mutations` topic with 3 partitions before search nodes start.

> **Architectural Guarantee Notice:** Containerization strictly reproduces the existing system's architectural guarantees. Docker does NOT alter or strengthen the Phase 17 synchronous all-replica authority model, the Phase 24 read failover semantics, or the Phase 25 partial-availability behavior.

### Network Architecture

All containers reside on a dedicated Docker bridge network (`dse-network`):
- Node RPC communication (`9000`, `9001`, `9002`) remains internal to `dse-network` and is never exposed to the host.
- Kafka communication uses the internal listener `kafka:9092` for container-to-container messaging. Host machine access is available via `localhost:9094`.
- Peer discovery uses Docker internal DNS names: `0=dse-node-0:9000,1=dse-node-1:9001,2=dse-node-2:9002`.

### Building the Cluster Image

```bash
docker compose -f docker/docker-compose.cluster.yml build
```

This triggers the multi-stage Linux build (`debian:bookworm-slim`), compiling the engine with `-DENABLE_KAFKA=ON` and bundling runtime dependencies.

### Starting the Cluster

```bash
docker compose -f docker/docker-compose.cluster.yml up -d
```

Startup sequence:
1. `kafka` launches and runs KRaft controller initialization.
2. `kafka` passes its internal healthcheck.
3. `kafka-init` executes, provisions `documents.mutations` (3 partitions), and exits successfully.
4. `dse-node-0`, `dse-node-1`, and `dse-node-2` start up, discover peers, connect to Kafka at `kafka:9092`, and expose HTTP endpoints.

### Inspecting Cluster Status and Health

```bash
# Check service states and healthcheck status
docker compose -f docker/docker-compose.cluster.yml ps

# View aggregate cluster logs
docker compose -f docker/docker-compose.cluster.yml logs

# Stream logs for a specific node
docker compose -f docker/docker-compose.cluster.yml logs -f dse-node-0
```

### Public HTTP Endpoints

| Node | Endpoint URL | Health Check | Metrics |
|:---|:---|:---|:---|
| Node 0 | `http://localhost:8080` | `http://localhost:8080/health` | `http://localhost:8080/metrics` |
| Node 1 | `http://localhost:8081` | `http://localhost:8081/health` | `http://localhost:8081/metrics` |
| Node 2 | `http://localhost:8082` | `http://localhost:8082/health` | `http://localhost:8082/metrics` |

Example operations from host:
```bash
# Health check
curl http://localhost:8080/health

# Index document via Node 0
curl -X POST http://localhost:8080/documents \
  -H "Content-Type: application/json" \
  -d '{"id": 101, "content": "distributed search system containerization"}'

# Search via Node 1
curl "http://localhost:8081/search?q=containerization&mode=and"
```

### Storage Persistence and Named Volumes

Data persistence is managed via dedicated named Docker volumes:
- `dse-data-0` → `/app/data` inside `dse-node-0` (holds shard JSONL files and event store).
- `dse-data-1` → `/app/data` inside `dse-node-1`.
- `dse-data-2` → `/app/data` inside `dse-node-2`.
- `kafka-data` → `/var/lib/kafka/data` inside `kafka`.

Volumes are isolated per node: nodes never share storage volumes.

### Stopping the Cluster

```bash
# Stop containers while preserving all persistent volume data
docker compose -f docker/docker-compose.cluster.yml down
```

### Resetting Persistent Data (Explicit Removal)

To completely wipe all persisted shard data, event outboxes, and Kafka topics/offsets:

```bash
# CAUTION: Permanently deletes named volumes dse-data-0, dse-data-1, dse-data-2, and kafka-data
docker compose -f docker/docker-compose.cluster.yml down -v
```

---

## Configuration Files

### docker-compose.cluster.yml

Full multi-node replicated search cluster with 3 search engine nodes and Kafka.

### docker-compose.kafka.yml

Standalone single-node Kafka infrastructure (for local non-containerized node development).

Main Docker Compose configuration for Kafka infrastructure.

### kafka/create-topics.sh

Script to create initial topics. Run manually or automatically via kafka-init container.

### .gitignore

The following Docker-related patterns are ignored:
- `docker/data/` - Persistent Kafka data (not used in this setup)
- `docker/*.log` - Docker log files

## Integration with Phase 18

Phase 19A provides the Docker infrastructure that enables Phase 19B (client library) and Phase 19C (KafkaMessageBroker).

The existing Phase 18 event system continues to use `InMemoryMessageBroker`. Kafka is an alternative `MessageBroker` implementation that will be introduced in Phase 19C.

**No changes to Phase 18 code are made in Phase 19A.**

## Next Steps

After Phase 19A:
- **Phase 19B**: Kafka client library integration (librdkafka)
- **Phase 19C**: KafkaMessageBroker implementation
- **Phase 19D**: Consumer groups for distributed processing

## References

- [Apache Kafka Documentation](https://kafka.apache.org/documentation/)
- [KRaft Mode](https://kafka.apache.org/documentation/#kraft)
- [Docker Kafka Image](https://hub.docker.com/r/apache/kafka)
- [librdkafka](https://github.com/confluentinc/librdkafka)
