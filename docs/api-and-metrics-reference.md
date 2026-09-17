# Distributed Search Engine — API and Metrics Reference

This document provides the exhaustive reference for all public HTTP endpoints and operational telemetry metrics implemented in the Distributed Search Engine.

---

## 1. Public HTTP API Reference

All routes are hosted by `HttpServer` using `cpp-httplib` and process incoming requests through `ShardCoordinator`.

### 1.1 `GET /health`
Lightweight process health probe for load balancers and orchestrators.

- **HTTP Method:** `GET`
- **Path:** `/health`
- **Headers:** None required
- **Request Body:** None
- **Response Status:** `200 OK`
- **Response Format:** `application/json`
```json
{
  "status": "ok"
}
```

---

### 1.2 `GET /search`
Executes a distributed, cross-shard full-text search query with global TF-IDF ranking.

- **HTTP Method:** `GET`
- **Path:** `/search`
- **Query Parameters:**
  - `q` (*string, required*): The search query string (e.g. `q=quick+fox`).
  - `mode` (*string, optional*): Match mode. Allowed values: `or` (default), `and`.
  - `limit` (*integer, optional*): Maximum result count to return. Allowed range: `1` to `100` (default: `10`).
- **Response Status Codes:**
  - `200 OK`: Query processed successfully (complete or partial).
  - `400 Bad Request`: Empty query string, invalid `mode`, or `limit` out of range `[1, 100]`.
  - `429 Too Many Requests`: Active concurrent requests exceed `DSE_MAX_CONCURRENT_REQUESTS`.
  - `500 Internal Server Error`: Unexpected internal coordinator exception.
- **Response Format:** `application/json`

**Sample Response (Complete Results):**
```json
{
  "complete": true,
  "limit": 10,
  "mode": "or",
  "query": "quick fox",
  "results": [
    {
      "document_id": 1,
      "score": 0.5753641449035617
    },
    {
      "document_id": 3,
      "score": 0.5753641449035617
    }
  ],
  "total": 2
}
```

**Sample Response (Partial Availability - Some Shards Down):**
```json
{
  "complete": false,
  "limit": 10,
  "mode": "or",
  "query": "quick fox",
  "results": [
    {
      "document_id": 1,
      "score": 0.38421
    }
  ],
  "total": 1,
  "errors": [
    {
      "category": "count_failure",
      "message": "all replicas failed for shard 2",
      "node_id": 2,
      "shard_id": 2
    }
  ]
}
```

---

### 1.3 `POST /documents`
Ingests and indexes a new document across all synchronous replicas.

- **HTTP Method:** `POST`
- **Path:** `/documents`
- **Headers:** `Content-Type: application/json`
- **Request Body:**
```json
{
  "id": 101,
  "content": "Distributed consensus and synchronous replication"
}
```
  - `id` (*unsigned integer, required*): Non-negative 64-bit document ID.
  - `content` (*string, required*): Non-empty text content of the document.
- **Response Status Codes:**
  - `201 Created`: Document ingested, indexed, persisted, and replicated across all replicas.
  - `400 Bad Request`: Malformed JSON, missing fields, or negative ID.
  - `409 Conflict`: Document with this `id` already exists in the cluster.
  - `429 Too Many Requests`: Concurrency slot saturated (load-shedding).
  - `500 Internal Server Error`: Replication failure or disk I/O error.
- **Response Format:** `application/json`
```json
{
  "document_id": 101,
  "terms_indexed": 5
}
```

---

### 1.4 `PUT /documents/:id`
Updates the text content of an existing document across all replicas.

- **HTTP Method:** `PUT`
- **Path:** `/documents/:id` (where `:id` is the unsigned integer document ID)
- **Headers:** `Content-Type: application/json`
- **Request Body:**
```json
{
  "content": "Updated content with additional search terms"
}
```
  - `content` (*string, required*): Non-empty, non-whitespace string.
- **Response Status Codes:**
  - `200 OK`: Document successfully updated and re-indexed across all replicas.
  - `400 Bad Request`: Malformed JSON or empty/whitespace-only content.
  - `404 Not Found`: Document with specified `id` does not exist.
  - `429 Too Many Requests`: Concurrency limit reached.
  - `500 Internal Server Error`: Replication or storage failure.
- **Response Format:** `application/json`
```json
{
  "document_id": 101,
  "terms_indexed": 6
}
```

---

### 1.5 `DELETE /documents/:id`
Deletes a document by ID across all replicas.

- **HTTP Method:** `DELETE`
- **Path:** `/documents/:id`
- **Headers:** None required
- **Request Body:** None
- **Response Status Codes:**
  - `204 No Content`: Document successfully deleted from all replicas.
  - `400 Bad Request`: Invalid document ID format.
  - `404 Not Found`: Document does not exist.
  - `429 Too Many Requests`: Concurrency limit reached.
  - `500 Internal Server Error`: Replication or internal failure.
- **Response Body:** None

---

### 1.6 `GET /metrics`
Exposes point-in-time telemetry snapshot from `MetricsCollector`, `EventStore`, `EventDispatcher`, and `MessageBroker`.

- **HTTP Method:** `GET`
- **Path:** `/metrics`
- **Response Status Codes:**
  - `200 OK`: Metrics retrieved successfully.
  - `503 Service Unavailable`: Metrics subsystem not initialized on this instance.
- **Response Format:** `application/json`

---

## 2. Operational Metrics Reference

The following table documents every metric emitted by `/metrics` directly from `src/metrics.h`, `src/event_store.h`, `src/event_dispatcher.h`, and `src/message_broker.h`.

| Metric Key | Type | Subsystem | Description & Operational Interpretation |
| :--- | :--- | :--- | :--- |
| **Node-Level Search Metrics** | | | |
| `searches_total` | Counter | Search | Cumulative search operations executed across local shards. |
| `search_errors` | Counter | Search | Count of failed local shard search operations. |
| `search_incomplete` | Counter | Search | Count of searches executed where one or more shards could not participate. |
| `search_latency.average_ms` | Gauge | Latency | Average latency in ms computed from circular buffer (recent 1000 samples). |
| `search_latency.p99_ms` | Gauge | Latency | Deterministic 99th-percentile latency in ms of recent samples. |
| `search_latency.sample_count` | Gauge | Latency | Number of samples currently held in circular latency buffer. |
| **Node-Level Write Metrics** | | | |
| `writes_total` | Counter | Storage | Total write operations (add, update, delete) handled at node level. |
| `write_errors` | Counter | Storage | Failed node-level write operations. |
| **Resilience & Concurrency Metrics** | | | |
| `retries_total` | Counter | RPC | Total RPC retry attempts executed against peer nodes. |
| `read_failovers_total` | Counter | Failover | Count of primary replica query failures triggering failover to secondary replica. |
| `load_shed_rejections_total` | Counter | Edge | HTTP 429 rejections emitted when active requests exceed `max_concurrent_requests`. |
| `circuit_open_events` | Counter | Resilience | Number of times a peer node circuit breaker tripped to `Open`. |
| `circuit_close_events` | Counter | Resilience | Number of times a peer node circuit breaker recovered to `Closed`. |
| **Coordinator Search Metrics** | | | |
| `coordinator_searches_total` | Counter | Coordinator | Total user-facing `/search` HTTP requests received. |
| `coordinator_search_success` | Counter | Coordinator | Count of successful user-facing searches (`complete == true`). |
| `coordinator_search_incomplete` | Counter | Coordinator | Count of user-facing searches completing partially (`complete == false`). |
| `coordinator_search_errors` | Counter | Coordinator | Count of search queries returning an error response (HTTP 400/500). |
| `coordinator_search_latency` | Object | Latency | Contains `average_ms`, `p99_ms`, and `sample_count` for end-to-end HTTP searches. |
| **Coordinator Write Metrics** | | | |
| `coordinator_writes_total` | Counter | Coordinator | Total user-facing write mutations received (`POST`, `PUT`, `DELETE`). |
| `coordinator_write_success` | Counter | Coordinator | Total mutations where **all** replicas acknowledged synchronously. |
| `coordinator_write_errors` | Counter | Coordinator | Total mutations rejected due to validation or replica failure. |
| `coordinator_write_latency` | Object | Latency | Contains `average_ms`, `p99_ms`, and `sample_count` for end-to-end HTTP writes. |
| **Per-Node Breakdown (`per_node.<node_id>`)** | | | |
| `searches` | Counter | Per-Node | Searches dispatched to this peer node. |
| `search_errors` | Counter | Per-Node | Search errors encountered when calling this peer node. |
| `search_incomplete` | Counter | Per-Node | Incomplete searches involving this node. |
| `writes` | Counter | Per-Node | Writes routed to this peer node. |
| `write_errors` | Counter | Per-Node | Write failures returned by this peer node. |
| `retries` | Counter | Per-Node | RPC retries executed against this peer node. |
| `circuit_state` | Gauge | Per-Node | Circuit breaker state: `0` (Closed), `1` (HalfOpen), `2` (Open). |
| `circuit_open_events` | Counter | Per-Node | Number of times this node's circuit breaker opened. |
| `circuit_close_events` | Counter | Per-Node | Number of times this node's circuit breaker closed. |
| **Outbox & EventStore Metrics** | | | |
| `events_total` | Counter | Outbox | Total domain mutation events created in `EventStore`. |
| `events_pending` | Gauge | Outbox | Events currently in `PENDING` state awaiting initial dispatch. |
| `events_dispatching` | Gauge | Outbox | Events currently in `DISPATCHING` state being processed by worker. |
| `events_published` | Counter | Outbox | Events successfully published to Kafka and acknowledged. |
| `events_failed` | Gauge | Outbox | Events in `FAILED` state after retry exhaustion (persisted on disk). |
| `events_retried` | Counter | Outbox | Internal retry attempts recorded by `EventStore`. |
| `events_replayed` | Counter | Outbox | Events moved from `FAILED` to `PENDING` via explicit replay API. |
| **EventDispatcher Metrics** | | | |
| `dispatcher_enqueued` | Counter | Dispatcher | Events admitted into in-memory dispatch queue. |
| `dispatcher_pending` | Gauge | Dispatcher | Current in-memory queue depth awaiting worker thread pickup. |
| `dispatcher_rejected` | Counter | Dispatcher | Events rejected due to queue-full backpressure timeout. |
| `dispatcher_broker_errors` | Counter | Dispatcher | Transient broker publication failures encountered. |
| `dispatcher_retried` | Counter | Dispatcher | Retry attempts initiated by dispatcher worker. |
| **Broker & Kafka Consumer Metrics** | | | |
| `consumer_lag` | Gauge | Kafka | Highest partition lag across assigned partitions in consumer group. |
| `consumer_messages_consumed` | Counter | Kafka | Total messages pulled and delivered to consumer callbacks. |
| `consumer_messages_acked` | Counter | Kafka | Messages processed successfully and committed. |
| `consumer_messages_nacked` | Counter | Kafka | Messages rejected or retried by consumer callback. |
