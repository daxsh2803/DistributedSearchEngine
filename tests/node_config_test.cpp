// Distributed Search Engine - Node Configuration Tests (Phase 11A).
//
// Tests for dse::ShardPlacement: shard-to-node mapping, validation,
// and multiple shards per node.

#include "node_config.h"

#include <gtest/gtest.h>

#include <cstddef>
#include <stdexcept>
#include <vector>

namespace dse {
namespace {

// =========================================================================
// 1. Valid construction
// =========================================================================

TEST(ShardPlacementTest, BasicConstruction)
{
    // 3 shards, 2 nodes: shard 0→node 0, shard 1→node 1, shard 2→node 0
    ShardPlacement p(3, 2, {0, 1, 0});
    EXPECT_EQ(p.shard_count(), 3u);
    EXPECT_EQ(p.node_count(), 2u);
}

TEST(ShardPlacementTest, NodeOf)
{
    ShardPlacement p(3, 2, {0, 1, 0});
    EXPECT_EQ(p.node_of(0), 0u);
    EXPECT_EQ(p.node_of(1), 1u);
    EXPECT_EQ(p.node_of(2), 0u);
}

TEST(ShardPlacementTest, SingleShardSingleNode)
{
    ShardPlacement p(1, 1, {0});
    EXPECT_EQ(p.node_of(0), 0u);
}

TEST(ShardPlacementTest, AllShardsOnOneNode)
{
    ShardPlacement p(3, 1, {0, 0, 0});
    EXPECT_EQ(p.node_of(0), 0u);
    EXPECT_EQ(p.node_of(1), 0u);
    EXPECT_EQ(p.node_of(2), 0u);
}

TEST(ShardPlacementTest, EvenDistribution)
{
    ShardPlacement p(6, 3, {0, 1, 2, 0, 1, 2});
    for (std::size_t i = 0; i < 6; ++i) {
        EXPECT_EQ(p.node_of(i), i % 3);
    }
}

// =========================================================================
// 2. Validation: invalid inputs
// =========================================================================

TEST(ShardPlacementTest, ZeroShardsThrows)
{
    EXPECT_THROW(ShardPlacement(0, 1, {}), std::invalid_argument);
}

TEST(ShardPlacementTest, ZeroNodesThrows)
{
    EXPECT_THROW(ShardPlacement(1, 0, {0}), std::invalid_argument);
}

TEST(ShardPlacementTest, MismatchedSizeThrows)
{
    EXPECT_THROW(ShardPlacement(3, 2, {0, 1}), std::invalid_argument);
}

TEST(ShardPlacementTest, NodeOutOfRangeThrows)
{
    EXPECT_THROW(ShardPlacement(3, 2, {0, 1, 5}), std::invalid_argument);
}

TEST(ShardPlacementTest, UnknownNodeThrows)
{
    EXPECT_THROW(ShardPlacement(2, 3, {3, 4}), std::invalid_argument);
}

// =========================================================================
// 3. Edge cases
// =========================================================================

TEST(ShardPlacementTest, SingleShardMultipleNodes)
{
    ShardPlacement p(1, 5, {3});
    EXPECT_EQ(p.node_of(0), 3u);
}

TEST(ShardPlacementTest, ManyShardsFewNodes)
{
    ShardPlacement p(10, 2, {0, 1, 0, 1, 0, 1, 0, 1, 0, 1});
    for (std::size_t i = 0; i < 10; ++i) {
        EXPECT_EQ(p.node_of(i), i % 2);
    }
}

// =========================================================================
// 4. at() exception for out-of-range shard_id
// =========================================================================

TEST(ShardPlacementTest, OutOfRangeShardThrows)
{
    ShardPlacement p(3, 2, {0, 1, 0});
    EXPECT_THROW(p.node_of(5), std::out_of_range);
}

} // namespace
} // namespace dse
