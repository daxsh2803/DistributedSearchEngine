// Distributed Search Engine - Shard Replica Placement Tests (Phase 17A).
//
// Tests for dse::ShardReplicaPlacement: shard→replica-set mapping,
// validation, R=1 backward compatibility, and concurrent read safety.

#include "replica_placement.h"

#include <gtest/gtest.h>

#include <atomic>
#include <cstddef>
#include <stdexcept>
#include <thread>
#include <vector>

namespace dse {
namespace {

// =========================================================================
// 1. Valid construction: R=1, R=2, R=3
// =========================================================================

TEST(ShardReplicaPlacementTest, ConstructionR1)
{
    // 3 shards, 3 nodes, R=1: each shard on exactly one node
    std::vector<ShardReplicaSet> sets = {
        {0, {0}},
        {1, {1}},
        {2, {2}},
    };
    ShardReplicaPlacement p(3, 3, 1, sets);
    EXPECT_EQ(p.shard_count(), 3u);
    EXPECT_EQ(p.node_count(), 3u);
    EXPECT_EQ(p.replication_factor(), 1u);
}

TEST(ShardReplicaPlacementTest, ConstructionR2)
{
    // 3 shards, 3 nodes, R=2: each shard on two nodes
    std::vector<ShardReplicaSet> sets = {
        {0, {0, 1}},
        {1, {1, 2}},
        {2, {2, 0}},
    };
    ShardReplicaPlacement p(3, 3, 2, sets);
    EXPECT_EQ(p.shard_count(), 3u);
    EXPECT_EQ(p.node_count(), 3u);
    EXPECT_EQ(p.replication_factor(), 2u);
}

TEST(ShardReplicaPlacementTest, ConstructionR3)
{
    // 2 shards, 3 nodes, R=3: each shard on all nodes
    std::vector<ShardReplicaSet> sets = {
        {0, {0, 1, 2}},
        {1, {2, 0, 1}},
    };
    ShardReplicaPlacement p(2, 3, 3, sets);
    EXPECT_EQ(p.shard_count(), 2u);
    EXPECT_EQ(p.node_count(), 3u);
    EXPECT_EQ(p.replication_factor(), 3u);
}

// =========================================================================
// 2. replicas_of() and primary_of()
// =========================================================================

TEST(ShardReplicaPlacementTest, ReplicasOfReturnsCorrectNodes)
{
    std::vector<ShardReplicaSet> sets = {
        {0, {0, 1}},
        {1, {1, 2}},
        {2, {2, 0}},
    };
    ShardReplicaPlacement p(3, 3, 2, sets);

    const auto& r0 = p.replicas_of(0);
    ASSERT_EQ(r0.size(), 2u);
    EXPECT_EQ(r0[0], 0u);
    EXPECT_EQ(r0[1], 1u);

    const auto& r1 = p.replicas_of(1);
    ASSERT_EQ(r1.size(), 2u);
    EXPECT_EQ(r1[0], 1u);
    EXPECT_EQ(r1[1], 2u);

    const auto& r2 = p.replicas_of(2);
    ASSERT_EQ(r2.size(), 2u);
    EXPECT_EQ(r2[0], 2u);
    EXPECT_EQ(r2[1], 0u);
}

TEST(ShardReplicaPlacementTest, ReplicaOrderingIsPreserved)
{
    // Verify the caller-defined order is exactly preserved
    std::vector<ShardReplicaSet> sets = {
        {0, {3, 1, 0}},
    };
    ShardReplicaPlacement p(1, 4, 3, sets);

    const auto& r = p.replicas_of(0);
    ASSERT_EQ(r.size(), 3u);
    EXPECT_EQ(r[0], 3u);
    EXPECT_EQ(r[1], 1u);
    EXPECT_EQ(r[2], 0u);
}

TEST(ShardReplicaPlacementTest, PrimaryOfReturnsFirstNode)
{
    std::vector<ShardReplicaSet> sets = {
        {0, {2, 0, 1}},
        {1, {1, 2, 0}},
    };
    ShardReplicaPlacement p(2, 3, 3, sets);

    EXPECT_EQ(p.primary_of(0), 2u);
    EXPECT_EQ(p.primary_of(1), 1u);
}

TEST(ShardReplicaPlacementTest, PrimaryOfR1)
{
    std::vector<ShardReplicaSet> sets = {
        {0, {5}},
    };
    ShardReplicaPlacement p(1, 6, 1, sets);
    EXPECT_EQ(p.primary_of(0), 5u);
}

// =========================================================================
// 3. Accessors
// =========================================================================

TEST(ShardReplicaPlacementTest, ShardCountAccessor)
{
    std::vector<ShardReplicaSet> sets = {
        {0, {0, 1}},
        {1, {1, 0}},
    };
    ShardReplicaPlacement p(2, 2, 2, sets);
    EXPECT_EQ(p.shard_count(), 2u);
}

TEST(ShardReplicaPlacementTest, NodeCountAccessor)
{
    std::vector<ShardReplicaSet> sets = {
        {0, {0, 1, 2}},
    };
    ShardReplicaPlacement p(1, 3, 3, sets);
    EXPECT_EQ(p.node_count(), 3u);
}

TEST(ShardReplicaPlacementTest, ReplicationFactorAccessor)
{
    std::vector<ShardReplicaSet> sets = {
        {0, {0, 1}},
    };
    ShardReplicaPlacement p(1, 2, 2, sets);
    EXPECT_EQ(p.replication_factor(), 2u);
}

// =========================================================================
// 4. Validation: invalid inputs
// =========================================================================

TEST(ShardReplicaPlacementTest, ZeroShardsThrows)
{
    EXPECT_THROW(
        ShardReplicaPlacement(0, 3, 2, {}),
        std::invalid_argument);
}

TEST(ShardReplicaPlacementTest, ZeroNodesThrows)
{
    EXPECT_THROW(
        ShardReplicaPlacement(1, 0, 1, {{0, {0}}}),
        std::invalid_argument);
}

TEST(ShardReplicaPlacementTest, ZeroReplicationFactorThrows)
{
    EXPECT_THROW(
        ShardReplicaPlacement(1, 3, 0, {{0, {0}}}),
        std::invalid_argument);
}

TEST(ShardReplicaPlacementTest, ReplicationFactorExceedsNodeCountThrows)
{
    EXPECT_THROW(
        ShardReplicaPlacement(1, 2, 3, {{0, {0, 1, 2}}}),
        std::invalid_argument);
}

TEST(ShardReplicaPlacementTest, ReplicaSetsSizeMismatchThrows)
{
    // 3 shards but only 2 replica sets provided
    EXPECT_THROW(
        ShardReplicaPlacement(3, 2, 2, {
            {0, {0, 1}},
            {1, {1, 0}},
        }),
        std::invalid_argument);
}

TEST(ShardReplicaPlacementTest, DuplicateShardIdThrows)
{
    EXPECT_THROW(
        ShardReplicaPlacement(2, 2, 1, {
            {0, {0}},
            {0, {1}},  // duplicate shard_id = 0
        }),
        std::invalid_argument);
}

TEST(ShardReplicaPlacementTest, WrongReplicaCountThrows)
{
    // replication_factor = 2 but shard 0 has only 1 replica
    EXPECT_THROW(
        ShardReplicaPlacement(1, 2, 2, {
            {0, {0}},
        }),
        std::invalid_argument);
}

TEST(ShardReplicaPlacementTest, NodeOutOfRangeThrows)
{
    // Node 5 doesn't exist in a 3-node cluster
    EXPECT_THROW(
        ShardReplicaPlacement(1, 3, 2, {
            {0, {0, 5}},
        }),
        std::invalid_argument);
}

TEST(ShardReplicaPlacementTest, DuplicateNodeInReplicaSetThrows)
{
    // Same node (0) appears twice in shard 0's replica set
    EXPECT_THROW(
        ShardReplicaPlacement(1, 3, 2, {
            {0, {0, 0}},
        }),
        std::invalid_argument);
}

TEST(ShardReplicaPlacementTest, InvalidShardIdThrows)
{
    // shard_id = 5 is out of range for shard_count = 2
    EXPECT_THROW(
        ShardReplicaPlacement(2, 2, 1, {
            {5, {0}},
            {1, {1}},
        }),
        std::invalid_argument);
}

// =========================================================================
// 5. R=1 backward compatibility
// =========================================================================

TEST(ShardReplicaPlacementTest, R1BackwardCompatibility)
{
    // R=1: each shard on exactly one node, same as ShardPlacement model
    std::vector<ShardReplicaSet> sets = {
        {0, {2}},
        {1, {0}},
        {2, {1}},
    };
    ShardReplicaPlacement p(3, 3, 1, sets);

    EXPECT_EQ(p.replicas_of(0).size(), 1u);
    EXPECT_EQ(p.primary_of(0), 2u);

    EXPECT_EQ(p.replicas_of(1).size(), 1u);
    EXPECT_EQ(p.primary_of(1), 0u);

    EXPECT_EQ(p.replicas_of(2).size(), 1u);
    EXPECT_EQ(p.primary_of(2), 1u);
}

TEST(ShardReplicaPlacementTest, R1SingleNodeCluster)
{
    // R=1, 1 shard, 1 node
    std::vector<ShardReplicaSet> sets = {
        {0, {0}},
    };
    ShardReplicaPlacement p(1, 1, 1, sets);
    EXPECT_EQ(p.primary_of(0), 0u);
    EXPECT_EQ(p.replicas_of(0).size(), 1u);
}

// =========================================================================
// 6. Multiple shards can share nodes
// =========================================================================

TEST(ShardReplicaPlacementTest, ShardsShareNodes)
{
    // 3 shards, 2 nodes, R=2: shards can overlap on nodes
    // as long as no individual shard has duplicate nodes
    std::vector<ShardReplicaSet> sets = {
        {0, {0, 1}},
        {1, {0, 1}},
        {2, {0, 1}},
    };
    ShardReplicaPlacement p(3, 2, 2, sets);

    // All shards share the same two nodes — valid
    EXPECT_EQ(p.primary_of(0), 0u);
    EXPECT_EQ(p.primary_of(1), 0u);
    EXPECT_EQ(p.primary_of(2), 0u);
}

// =========================================================================
// 7. Out-of-range access
// =========================================================================

TEST(ShardReplicaPlacementTest, ReplicasOfOutOfRangeThrows)
{
    std::vector<ShardReplicaSet> sets = {
        {0, {0, 1}},
    };
    ShardReplicaPlacement p(1, 2, 2, sets);
    EXPECT_THROW(p.replicas_of(5), std::out_of_range);
}

TEST(ShardReplicaPlacementTest, PrimaryOfOutOfRangeThrows)
{
    std::vector<ShardReplicaSet> sets = {
        {0, {0, 1}},
    };
    ShardReplicaPlacement p(1, 2, 2, sets);
    EXPECT_THROW(p.primary_of(5), std::out_of_range);
}

// =========================================================================
// 8. Concurrent read access
// =========================================================================

TEST(ShardReplicaPlacementTest, ConcurrentReadAccess)
{
    // 5 shards, 4 nodes, R=2
    std::vector<ShardReplicaSet> sets = {
        {0, {0, 1}},
        {1, {1, 2}},
        {2, {2, 3}},
        {3, {3, 0}},
        {4, {0, 2}},
    };
    ShardReplicaPlacement p(5, 4, 2, sets);

    // Expected primary and replicas
    const std::size_t expected[][2] = {
        {0, 1},  // shard 0
        {1, 2},  // shard 1
        {2, 3},  // shard 2
        {3, 0},  // shard 3
        {0, 2},  // shard 4
    };

    std::atomic<int> errors{0};

    auto reader = [&](std::size_t /*thread_id*/) {
        for (std::size_t iter = 0; iter < 1000; ++iter) {
            const std::size_t sid = iter % 5;
            const auto& replicas = p.replicas_of(sid);
            const auto primary = p.primary_of(sid);

            if (replicas.size() != 2u) {
                errors.fetch_add(1, std::memory_order_relaxed);
                continue;
            }
            if (replicas[0] != expected[sid][0]) {
                errors.fetch_add(1, std::memory_order_relaxed);
                continue;
            }
            if (replicas[1] != expected[sid][1]) {
                errors.fetch_add(1, std::memory_order_relaxed);
                continue;
            }
            if (primary != expected[sid][0]) {
                errors.fetch_add(1, std::memory_order_relaxed);
                continue;
            }
        }
    };

    std::vector<std::thread> threads;
    for (std::size_t t = 0; t < 8; ++t) {
        threads.emplace_back(reader, t);
    }
    for (auto& t : threads) {
        t.join();
    }

    EXPECT_EQ(errors.load(), 0);
}

// =========================================================================
// 9. Larger placement
// =========================================================================

TEST(ShardReplicaPlacementTest, LargerPlacement)
{
    // 10 shards, 5 nodes, R=3
    std::vector<ShardReplicaSet> sets;
    for (std::size_t s = 0; s < 10; ++s) {
        sets.push_back({s, {s % 5, (s + 1) % 5, (s + 2) % 5}});
    }
    ShardReplicaPlacement p(10, 5, 3, sets);

    EXPECT_EQ(p.shard_count(), 10u);
    EXPECT_EQ(p.node_count(), 5u);
    EXPECT_EQ(p.replication_factor(), 3u);

    for (std::size_t s = 0; s < 10; ++s) {
        EXPECT_EQ(p.primary_of(s), s % 5);
        EXPECT_EQ(p.replicas_of(s).size(), 3u);
    }
}

} // namespace
} // namespace dse
