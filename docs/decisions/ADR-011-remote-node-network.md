# ADR-011: Remote Node / Network Transport

## Status

Accepted — Phase 12

## Context

Phase 11 introduced a transport-independent `NodeClient` interface between `ShardCoordinator` and shards, with a `LocalNode` implementation for in-process shards. The coordinator operates identically regardless of the `NodeClient` implementation. Phase 12 introduces a `RemoteNode` implementation that communicates with a `NodeServer` over HTTP/JSON, enabling shards to run in separate processes or machines.

The key architectural goal is that `ShardCoordinator` must NOT know whether a `NodeClient` is `LocalNode` or `RemoteNode`.

## Decision

Introduce `RemoteNode` (NodeClient over HTTP) and `NodeServer` (HTTP server wrapping LocalNode), connected via nlohmann/json wire format over cpp-httplib HTTP transport.

### Wire Format

All NodeClient request/response types are serialized to/from JSON using nlohmann/json. A dedicated `node_wire.h/cpp` module provides `to_json()` and `*_from_json()` functions for each type.

Wire format example (search request):
```json
{"shard_id": 0, "terms": ["quick", "fox"]}
```

Wire format example (search response):
```json
{
  "shard_id": 0,
  "local_document_count": 5,
  "terms_postings": [[{"document_id": 1, "term_frequency": 2}]],
  "is_error": false,
  "error_message": ""
}
```

### NodeServer

`NodeServer` wraps a `LocalNode` and exposes HTTP POST endpoints:

| Endpoint | Operation |
|----------|-----------|
| `POST /node/search` | Search a shard |
| `POST /node/add` | Add a document |
| `POST /node/update` | Update a document |
| `POST /node/remove` | Remove a document |
| `POST /node/get` | Get a document |
| `POST /node/count` | Document count |
| `POST /node/save` | Persist a shard |
| `POST /node/load` | Load a shard |

All endpoints use POST with JSON bodies. HTTP status codes: 200 (success), 400 (malformed request), 404 (unknown shard/document), 409 (duplicate document).

The NodeServer supports concurrent requests via cpp-httplib's built-in thread-per-request model.

### RemoteNode

`RemoteNode` implements `NodeClient` over HTTP. Each method:
1. Creates a fresh `httplib::Client` (safe for concurrent use)
2. Serializes the request to JSON
3. Sends an HTTP POST
4. Deserializes the JSON response
5. Returns the NodeClient-level response

Network failures become NodeClient-level errors (`is_error = true`). The coordinator's existing fail-fast behavior handles them.

### Timeouts

RemoteNode supports configurable connection/read/write timeouts (default 30 seconds). cpp-httplib's `set_connection_timeout`, `set_read_timeout`, and `set_write_timeout` are used.

### No Coordinator Changes

`ShardCoordinator` is unchanged from Phase 11. It continues to operate on `vector<unique_ptr<NodeClient>>`. The coordinator:
- Routes writes via `ShardRouter` → `ShardPlacement` → `NodeClient`
- Performs cross-shard search via parallel `std::async` fan-out
- Computes global TF-IDF from data returned through `NodeClient`
- Does NOT access `InvertedIndex` or `DocumentStore` directly

## Architecture

```
HttpServer → ShardCoordinator → NodeClient
                                  ├── LocalNode   (in-process)
                                  └── RemoteNode  (HTTP/JSON)
                                        ↕
                                    NodeServer
                                        ↓
                                    LocalNode → Shard
```

## Concurrency

- RemoteNode creates a fresh `httplib::Client` per request (no shared mutable state)
- NodeServer handles concurrent requests via cpp-httplib's thread pool
- LocalNode/Shard thread-safety from previous phases remains intact
- Coordinator's `std::async` fan-out works unchanged with RemoteNode

## Persistence

Persistence is delegated through the transport:
- `RemoteNode::save_shard()` → HTTP POST `/node/save` → `NodeServer` → `LocalNode::save_shard()` → `Shard::save()`
- `RemoteNode::load_shard()` → HTTP POST `/node/load` → `NodeServer` → `LocalNode::load_shard()` → `Shard::load()`

Each shard retains its own persistence path. The coordinator never directly accesses remote persistence.

## Failure Model

Phase 12 extends the existing Phase 11 failure semantics to network transport:

- **NodeClient level:** Network failures become `is_error = true` in the response.
- **Coordinator search level:** When a node search returns an error, `collect_postings()` silently omits that shard's postings. The search proceeds with results from remaining shards. This means a single failed node produces **incomplete results without error indication** — this is inherited Phase 11 behavior.
- **Coordinator write level:** Write errors (add/update/remove) are propagated to the HTTP client as errors.

This is NOT fail-fast for search. It is silent degradation. This is a known architectural limitation documented here for transparency. A future phase may add error propagation or partial-result-awareness to the coordinator.

No retries, no circuit breakers.

## Consequences

### Positive

- Real network boundary between coordinator and shards
- Coordinator unchanged — same interface for LocalNode and RemoteNode
- Multiple shards per remote node supported
- Global TF-IDF preserved across remote nodes
- Concurrent requests supported end-to-end
- Existing 591 Phase 11 tests continue passing

### Negative

- HTTP overhead per request (acceptable for development/education)
- NodeServer thread-per-request model (not suitable for high-concurrency production)
- Search silently omits failed shards without error indication (known limitation from Phase 11, documented in Failure Model section)

### Non-Goals (Phase 12)

Phase 12 does NOT implement:
- Replication
- Consensus
- Leader election
- Automatic failover
- Retries / circuit breakers / explicit partial-result handling
- Shard migration / rebalancing
- Service discovery
- Dynamic membership
- Distributed transactions / distributed WAL
- TLS / authentication
- Container orchestration
- Production load balancing
- Custom async I/O
