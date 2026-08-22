// Distributed Search Engine - Shard Router (Phase 10).
//
// Implementation of the contract in src/shard_router.h.
// Deterministic routing via std::hash<doc_id> % shard_count.

#include "shard_router.h"

#include <functional>

#include "inverted_index.h"

namespace dse {

ShardRouter::ShardRouter(std::size_t shard_count)
    : shard_count_(shard_count)
{
    if (shard_count == 0) {
        throw std::invalid_argument("ShardRouter: shard_count must be > 0");
    }
}

std::size_t ShardRouter::route(doc_id id) const
{
    // std::hash<uint32_t> is well-distributed across typical ID ranges.
    // Combined with modulo, this gives uniform shard assignment.
    return std::hash<doc_id>{}(id) % shard_count_;
}

std::size_t ShardRouter::shard_count() const
{
    return shard_count_;
}

} // namespace dse
