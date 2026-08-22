// Distributed Search Engine - Node Configuration (Phase 11).
//
// Implementation of the contract in src/node_config.h.
// Validates shard placement and provides deterministic shard→node routing.

#include "node_config.h"

#include <cstddef>
#include <stdexcept>
#include <string>
#include <vector>

namespace dse {

ShardPlacement::ShardPlacement(std::size_t shard_count,
                               std::size_t node_count,
                               const std::vector<std::size_t>& placement)
    : shard_count_(shard_count)
    , node_count_(node_count)
    , shard_to_node_(shard_count)
{
    if (shard_count == 0) {
        throw std::invalid_argument("ShardPlacement: shard_count must be > 0");
    }
    if (node_count == 0) {
        throw std::invalid_argument("ShardPlacement: node_count must be > 0");
    }
    if (placement.size() != shard_count) {
        throw std::invalid_argument(
            "ShardPlacement: placement size (" +
            std::to_string(placement.size()) +
            ") must equal shard_count (" +
            std::to_string(shard_count) + ")");
    }

    for (std::size_t i = 0; i < shard_count; ++i) {
        if (placement[i] >= node_count) {
            throw std::invalid_argument(
                "ShardPlacement: shard " + std::to_string(i) +
                " references unknown node " + std::to_string(placement[i]));
        }
        shard_to_node_[i] = placement[i];
    }
}

std::size_t ShardPlacement::node_of(std::size_t shard_id) const
{
    return shard_to_node_.at(shard_id);
}

std::size_t ShardPlacement::shard_count() const
{
    return shard_count_;
}

std::size_t ShardPlacement::node_count() const
{
    return node_count_;
}

} // namespace dse
