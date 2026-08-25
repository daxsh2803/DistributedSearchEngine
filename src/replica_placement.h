// Distributed Search Engine - Shard Replica Placement (Phase 17A).
//
// Maps shard_id → ordered replica set for replicated shard architecture.
// Each shard has exactly one replica set containing R node IDs.
// node_ids[0] is the primary; node_ids[1...] are secondary replicas.
//
// The placement is immutable after construction and safe for concurrent
// const reads. No internal synchronization is needed.
//
// Replication factor 1 is fully supported and represents the existing
// single-node placement model (each shard on exactly one node).

#pragma once

#include <cstddef>
#include <stdexcept>
#include <vector>

namespace dse {

// A replica set for a single shard: ordered list of node IDs.
// node_ids[0] is the primary replica.
struct ShardReplicaSet {
    std::size_t shard_id;
    std::vector<std::size_t> node_ids;
};

// Maps shard_id → ordered replica set.
// Immutable after construction. Thread-safe for concurrent const reads.
class ShardReplicaPlacement {
public:
    // Construct a placement.
    //
    // Parameters:
    //   shard_count         — total number of logical shards (must be > 0)
    //   node_count          — total number of nodes (must be > 0)
    //   replication_factor  — replicas per shard (must be > 0 and <= node_count)
    //   replica_sets        — exactly shard_count entries, one per shard
    //
    // Validates:
    //   - shard_count > 0
    //   - node_count > 0
    //   - replication_factor > 0
    //   - replication_factor <= node_count
    //   - replica_sets.size() == shard_count
    //   - every shard_id in [0, shard_count) appears exactly once
    //   - each replica set has exactly replication_factor entries
    //   - all node_ids are in [0, node_count)
    //   - no duplicate node_ids within a single replica set
    //
    // Throws std::invalid_argument on any validation failure.
    ShardReplicaPlacement(
        std::size_t shard_count,
        std::size_t node_count,
        std::size_t replication_factor,
        const std::vector<ShardReplicaSet>& replica_sets);

    // Get the ordered replica set for a shard.
    // Returns a const reference to avoid copies.
    // Throws std::out_of_range if shard_id is invalid.
    const std::vector<std::size_t>& replicas_of(std::size_t shard_id) const;

    // Get the primary node (first replica) for a shard.
    // Equivalent to replicas_of(shard_id)[0].
    // Throws std::out_of_range if shard_id is invalid.
    std::size_t primary_of(std::size_t shard_id) const;

    // Accessors returning values from the constructor.
    std::size_t shard_count() const;
    std::size_t node_count() const;
    std::size_t replication_factor() const;

private:
    std::size_t shard_count_;
    std::size_t node_count_;
    std::size_t replication_factor_;
    // Indexed by shard_id. Each entry is the ordered replica set.
    std::vector<std::vector<std::size_t>> shard_replicas_;
};

} // namespace dse
