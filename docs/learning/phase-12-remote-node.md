# Phase 12: Remote Node / Network Transport

## Overview

Phase 12 introduces real network transport between the coordinator and nodes. A `RemoteNode` implements the `NodeClient` interface over HTTP/JSON, communicating with a `NodeServer` that wraps `LocalNode`. The coordinator cannot distinguish between local and remote nodes.

## What Changed

### Before (Phase 11)
```
ShardCoordinator → NodeClient → LocalNode → Shard
```

### After (Phase 12)
```
ShardCoordinator → NodeClient
                     ├── LocalNode   (in-process)
                     └── RemoteNode  (HTTP/JSON)
                           ↕
                       NodeServer → LocalNode → Shard
```

## Key Concepts

### Wire Format

All NodeClient request/response types are serialized to JSON using nlohmann/json. The `node_wire.h/cpp` module provides bidirectional serialization:
- `to_json()` for serialization
- `*_from_json()` for deserialization

Every field is explicitly named in the JSON. No pointers, references, or implementation internals are serialized.

### NodeServer

An HTTP server that wraps a `LocalNode` and exposes POST endpoints:
- `/node/search` — search a shard
- `/node/add` — add a document
- `/node/update` — update a document
- `/node/remove` — remove a document
- `/node/get` — get a document
- `/node/count` — document count
- `/node/save` — persist a shard
- `/node/load` — load from persistence

All endpoints accept and return `application/json`. Uses POST consistently because this is an internal RPC protocol, not a public REST API.

### RemoteNode

Implements `NodeClient` over HTTP. Each method:
1. Creates a fresh `httplib::Client` (safe for concurrency)
2. Serializes request to JSON via `node_wire.h`
3. Sends HTTP POST to the NodeServer
4. Deserializes JSON response
5. Returns the NodeClient-level response

Network failures become `is_error = true` in the response. The coordinator handles this via its existing fail-fast behavior.

### Timeouts

RemoteNode supports configurable timeouts (default 30 seconds) via cpp-httplib's connection, read, and write timeout settings.

### No Coordinator Changes

`ShardCoordinator` is completely unchanged from Phase 11. It continues to:
- Route writes via `ShardRouter` → `ShardPlacement` → `NodeClient`
- Fan out searches via `std::async`
- Compute global TF-IDF from data returned through `NodeClient`

## HTTP/JSON Protocol

### Request Format
All requests are JSON POST bodies:
```json
{"shard_id": 0, "terms": ["quick", "fox"]}
```

### Response Format
All responses are JSON with consistent fields:
```json
{
  "shard_id": 0,
  "is_error": false,
  "error_message": ""
}
```

### HTTP Status Codes
- 200: Success
- 400: Malformed/invalid request
- 404: Unknown shard or document
- 409: Document already exists
- 500: Server-side error

## Concurrency Model

- RemoteNode creates a fresh `httplib::Client` per request (no shared mutable state)
- NodeServer handles concurrent requests via cpp-httplib's built-in thread pool
- Shard-level SharedMutex serializes writes within each shard
- Coordinator's `std::async` fan-out works unchanged across local and remote nodes

## Persistence

Persistence flows through the transport:
```
Coordinator::save_all()
  → RemoteNode::save_shard(shard_id)
    → HTTP POST /node/save
      → NodeServer → LocalNode::save_shard()
        → Shard::save() → DocumentStore::save(path)
```

Each shard retains its own persistence path. The coordinator never accesses remote persistence directly.

## Failure Behavior

| Failure | RemoteNode | Coordinator |
|---------|-----------|-------------|
| Connection refused | `is_error = true` | Search: shard silently omitted (partial results). Write: error propagated. |
| Connection timeout | `is_error = true` | Same as above |
| Read timeout | `is_error = true` | Same as above |
| Malformed response | `is_error = true` | Same as above |
| HTTP 400/404/500 | `is_error = true` (from response body) | Same as above |

No retries. No circuit breakers.

**Known limitation (inherited from Phase 11):** For search operations, the coordinator's `collect_postings()` silently omits shards that return errors. This means a failed node produces **incomplete results without error indication**. The user receives only results from working shards and has no way to know a shard was unreachable. This is NOT fail-fast behavior for search. A future phase may add explicit error propagation or partial-result-awareness to the coordinator.

## Testing

| Test Suite | Tests | What It Proves |
|-----------|-------|----------------|
| NodeWireTest | 27 | JSON round-trip serialization for all types |
| NodeServerTest | 21 | All HTTP endpoints, error handling, multi-shard, concurrency, persistence |
| RemoteNodeTest | 28 | All NodeClient operations over real HTTP, connection failures, concurrency, behavioral equivalence with LocalNode |
| RemoteCoordinatorTest | 13 | Coordinator with remote nodes, global TF-IDF, lifecycle, mixed local+remote, persistence |

**Total new tests: 89**
**Full suite: 632/632 (100%)**
**Compiler warnings: 0**

## Invariants Preserved

- DocumentStore state == InvertedIndex state (per shard)
- DocumentStore::size() == InvertedIndex::document_count()
- Global TF-IDF ranking unchanged
- All HTTP endpoints preserved
- Thread safety preserved
- Persistence per shard preserved
- `shard_count = 1` equivalent to Phase 11

## Files Created

| File | Purpose |
|------|---------|
| `src/node_wire.h` | JSON serialization declarations |
| `src/node_wire.cpp` | Serialization implementation |
| `src/node_server.h` | NodeServer header |
| `src/node_server.cpp` | NodeServer implementation |
| `src/remote_node.h` | RemoteNode header |
| `src/remote_node.cpp` | RemoteNode implementation |
| `tests/node_wire_test.cpp` | Wire format tests |
| `tests/node_server_test.cpp` | NodeServer tests |
| `tests/remote_node_test.cpp` | RemoteNode tests |
| `tests/remote_coordinator_test.cpp` | Remote coordinator integration tests |

## Files Modified

| File | Change |
|------|--------|
| `CMakeLists.txt` | Added source files and test targets |
| `src/shard.cpp` | Fixed `load()` to clear index before rebuilding (bug discovered in Phase 12B) |

## Phase 13 Preparation

Phase 12 establishes the exact architecture for Phase 13:
- RemoteNode can be replaced with a different transport (gRPC, raw TCP)
- NodeServer can be extended with authentication, TLS
- The coordinator is already ready for distributed deployment
- No coordinator changes needed for transport upgrades
