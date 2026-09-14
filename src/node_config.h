// Distributed Search Engine - Node Configuration (Phase 11).
//
// Static configuration for nodes and shard placement.
// ShardPlacement maps shard_id → node_id, supporting multiple shards
// per node. The coordinator uses placement to route operations.

#pragma once

#include <cstddef>
#include <stdexcept>
#include <string>
#include <string_view>
#include <unordered_map>
#include <vector>

#include "replica_placement.h"

namespace dse {

// Peer node network endpoint for multi-node cluster configuration.
struct NodeEndpoint {
    std::size_t node_id = 0;
    std::string host;
    int port = 0;
};

// Parses a comma-separated list of peer specifications:
// e.g. "0=127.0.0.1:9081,1=127.0.0.1:9082,2=127.0.0.1:9083"
// Validates:
//   - spec must not be empty
//   - node IDs must be strictly contiguous: {0, 1, ..., N-1} where N = endpoints.size()
//   - each node_id must appear exactly once
//   - host must be non-empty
//   - port must be in range [1, 65535]
// Returns endpoints sorted by node_id (so endpoints[i].node_id == i).
// Throws std::invalid_argument on any validation failure.
std::vector<NodeEndpoint> parse_peer_topology(std::string_view spec);

// Generates deterministic replica sets for N nodes, S shards, and replication factor R.
// For shard sid:
//   node_ids = { (sid + r) % node_count for r in [0, replication_factor) }
// Validates:
//   - shard_count > 0
//   - node_count > 0
//   - replication_factor > 0 && replication_factor <= node_count
std::vector<ShardReplicaSet> make_deterministic_replica_sets(
    std::size_t shard_count,
    std::size_t node_count,
    std::size_t replication_factor);

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
