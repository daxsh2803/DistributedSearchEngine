// Distributed Search Engine - Shard Replica Placement (Phase 17A).
//
// Implementation of the contract in src/replica_placement.h.
// Validates placement and provides deterministic shard→replica-set routing.

#include "replica_placement.h"

#include <algorithm>
#include <cstddef>
#include <stdexcept>
#include <string>
#include <vector>

namespace dse {

ShardReplicaPlacement::ShardReplicaPlacement(
    std::size_t shard_count,
    std::size_t node_count,
    std::size_t replication_factor,
    const std::vector<ShardReplicaSet>& replica_sets)
    : shard_count_(shard_count)
    , node_count_(node_count)
    , replication_factor_(replication_factor)
    , shard_replicas_(shard_count)
{
    // --- Validate parameters ---
    if (shard_count == 0) {
        throw std::invalid_argument(
            "ShardReplicaPlacement: shard_count must be > 0");
    }
    if (node_count == 0) {
        throw std::invalid_argument(
            "ShardReplicaPlacement: node_count must be > 0");
    }
    if (replication_factor == 0) {
        throw std::invalid_argument(
            "ShardReplicaPlacement: replication_factor must be > 0");
    }
    if (replication_factor > node_count) {
        throw std::invalid_argument(
            "ShardReplicaPlacement: replication_factor (" +
            std::to_string(replication_factor) +
            ") must be <= node_count (" +
            std::to_string(node_count) + ")");
    }
    if (replica_sets.size() != shard_count) {
        throw std::invalid_argument(
            "ShardReplicaPlacement: replica_sets size (" +
            std::to_string(replica_sets.size()) +
            ") must equal shard_count (" +
            std::to_string(shard_count) + ")");
    }

    // Track which shard_ids have been seen (for duplicate detection).
    std::vector<bool> shard_seen(shard_count, false);

    for (const auto& rs : replica_sets) {
        // Validate shard_id is in range.
        if (rs.shard_id >= shard_count) {
            throw std::invalid_argument(
                "ShardReplicaPlacement: shard_id " +
                std::to_string(rs.shard_id) + " is out of range");
        }

        // Validate no duplicate shard_id.
        if (shard_seen[rs.shard_id]) {
            throw std::invalid_argument(
                "ShardReplicaPlacement: duplicate shard_id " +
                std::to_string(rs.shard_id));
        }
        shard_seen[rs.shard_id] = true;

        // Validate replica count matches replication_factor.
        if (rs.node_ids.size() != replication_factor) {
            throw std::invalid_argument(
                "ShardReplicaPlacement: shard " +
                std::to_string(rs.shard_id) +
                " has " + std::to_string(rs.node_ids.size()) +
                " replicas but replication_factor is " +
                std::to_string(replication_factor));
        }

        // Validate each node_id and check for duplicates within the set.
        std::vector<bool> node_seen(node_count, false);
        for (std::size_t i = 0; i < rs.node_ids.size(); ++i) {
            const std::size_t nid = rs.node_ids[i];
            if (nid >= node_count) {
                throw std::invalid_argument(
                    "ShardReplicaPlacement: shard " +
                    std::to_string(rs.shard_id) +
                    " references unknown node " + std::to_string(nid));
            }
            if (node_seen[nid]) {
                throw std::invalid_argument(
                    "ShardReplicaPlacement: shard " +
                    std::to_string(rs.shard_id) +
                    " has duplicate node " + std::to_string(nid));
            }
            node_seen[nid] = true;
        }

        // Store the replica set.
        shard_replicas_[rs.shard_id] = rs.node_ids;
    }

    // Verify every shard was placed (redundant with duplicate check, but
    // explicit for safety).
    for (std::size_t i = 0; i < shard_count; ++i) {
        if (!shard_seen[i]) {
            throw std::invalid_argument(
                "ShardReplicaPlacement: shard " +
                std::to_string(i) + " has no replica set");
        }
    }
}

const std::vector<std::size_t>& ShardReplicaPlacement::replicas_of(
    std::size_t shard_id) const
{
    return shard_replicas_.at(shard_id);
}

std::size_t ShardReplicaPlacement::primary_of(std::size_t shard_id) const
{
    return replicas_of(shard_id).at(0);
}

std::size_t ShardReplicaPlacement::shard_count() const
{
    return shard_count_;
}

std::size_t ShardReplicaPlacement::node_count() const
{
    return node_count_;
}

std::size_t ShardReplicaPlacement::replication_factor() const
{
    return replication_factor_;
}

} // namespace dse
