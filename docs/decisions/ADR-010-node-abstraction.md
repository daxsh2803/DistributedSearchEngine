# ADR-010: Node Abstraction

## Status

Accepted — Phase 11

## Context

Phase 10 established an in-process shard architecture where `ShardCoordinator` directly owns `vector<unique_ptr<Shard>>`. This coupling means shards must live in the same process. To prepare for future distribution (Phase 12+), we need a clean abstraction boundary between the coordinator and shards.

## Decision

Introduce a transport-independent `NodeClient` interface between `ShardCoordinator` and shards.

### NodeClient Interface

```cpp
class NodeClient {
public:
    virtual ~NodeClient() = default;
    virtual std::size_t node_id() const = 0;
    virtual ShardSearchResponse search(const ShardSearchRequest&) = 0;
    virtual ShardWriteResponse add_document(const ShardWriteRequest&) = 0;
    virtual ShardWriteResponse update_document(const ShardWriteRequest&) = 0;
    virtual ShardRemoveResponse remove_document(const ShardRemoveRequest&) = 0;
    virtual ShardGetResponse get_document(const ShardGetRequest&) = 0;
    virtual ShardCountResponse document_count(const ShardCountRequest&) = 0;
    virtual bool save_shard(std::size_t shard_id) = 0;
    virtual bool load_shard(std::size_t shard_id) = 0;
};
```

All request/response types are transport-independent (no sockets, HTTP, or serialization).

### LocalNode

`LocalNode` implements `NodeClient` for in-process shards. It owns a collection of `Shard` instances indexed by `shard_id`. Every operation routes to the correct shard.

### ShardPlacement

`ShardPlacement` maps `shard_id → node_id`. Supports multiple shards per node.

### Coordinator Migration

`ShardCoordinator` now owns:
- `ShardRouter` (doc_id → shard_id)
- `ShardPlacement` (shard_id → node_id)
- `vector<unique_ptr<NodeClient>>` (node_id → NodeClient)

All shard access goes through `NodeClient`. The coordinator never accesses `InvertedIndex` or `DocumentStore` directly.

## Consequences

### Positive

- Clean boundary enables future `RemoteNode` without coordinator redesign
- Multiple shards per node supported
- Each node has a stable `node_id()`
- Global TF-IDF computed using data returned through `NodeClient`
- `shard_count = 1` equivalent to Phase 10

### Negative

- Slight indirection overhead for in-process calls
- More types (request/response structs) than direct shard access

### Non-Goals

Phase 11 does NOT implement:
- Network transport (sockets, HTTP between nodes)
- RemoteNode
- Service discovery
- Replication
- Fault tolerance
- Timeouts
- Retry logic
