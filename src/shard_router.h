// Distributed Search Engine - Shard Router (Phase 10).
//
// Deterministic document-to-shard routing. Given a doc_id, the router
// deterministically maps it to a fixed shard index. The same doc_id
// always maps to the same shard for a given shard_count.
//
// Routing strategy: std::hash<doc_id>{}(id) % shard_count.
// This provides uniform distribution for typical document ID patterns.
//
// IMPORTANT: Changing shard_count for an existing dataset is NOT supported
// in Phase 10. Data migration when changing shard count is deferred to
// a later phase.

#pragma once

#include <cstddef>
#include <cstdint>
#include <functional>
#include <stdexcept>

#include "inverted_index.h"  // for doc_id

namespace dse {

class ShardRouter {
public:
    // Create a router for the given number of shards.
    // shard_count must be > 0.
    explicit ShardRouter(std::size_t shard_count);

    // Route a document ID to its owning shard index.
    // Returns a value in [0, shard_count).
    // Deterministic: same id + same shard_count => same result.
    // Thread-safe: const method, no mutable state.
    std::size_t route(doc_id id) const;

    // Number of shards.
    std::size_t shard_count() const;

private:
    std::size_t shard_count_;
};

} // namespace dse
