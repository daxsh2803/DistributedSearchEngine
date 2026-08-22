// Distributed Search Engine - Node Configuration (Phase 11).
//
// Static configuration for nodes and shard placement.
// ShardPlacement maps shard_id → node_id, supporting multiple shards
// per node. The coordinator uses placement to route operations.

#pragma once

#include <cstddef>
#include <stdexcept>
#include <unordered_map>
#include <vector>

namespace dse {

// Maps shard_id → node_id for deterministic routing.
// Supports multiple shards per node.
class ShardPlacement {
public:
    // Construct a placement for the given number of shards and nodes.
    // placement must have exactly shard_count entries.
    // Each shard_id in [0, shard_count) must appear exactly once.
    // Each node_id must be in [0, node_count).
    ShardPlacement(std::size_t shard_count,
                   std::size_t node_count,
                   const std::vector<std::size_t>& placement);

    // Get the node_id owning a given shard.
    std::size_t node_of(std::size_t shard_id) const;

    std::size_t shard_count() const;
    std::size_t node_count() const;

private:
    std::size_t shard_count_;
    std::size_t node_count_;
    std::vector<std::size_t> shard_to_node_;  // index = shard_id
};

} // namespace dse
