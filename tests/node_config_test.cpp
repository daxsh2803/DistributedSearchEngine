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

// =========================================================================
// 5. parse_peer_topology tests
// =========================================================================

TEST(NodeConfigTopologyTest, ParseValid3Nodes)
{
    const auto endpoints = parse_peer_topology(
        "0=127.0.0.1:9081,1=127.0.0.1:9082,2=127.0.0.1:9083");
    ASSERT_EQ(endpoints.size(), 3u);

    EXPECT_EQ(endpoints[0].node_id, 0u);
    EXPECT_EQ(endpoints[0].host, "127.0.0.1");
    EXPECT_EQ(endpoints[0].port, 9081);

    EXPECT_EQ(endpoints[1].node_id, 1u);
    EXPECT_EQ(endpoints[1].host, "127.0.0.1");
    EXPECT_EQ(endpoints[1].port, 9082);

    EXPECT_EQ(endpoints[2].node_id, 2u);
    EXPECT_EQ(endpoints[2].host, "127.0.0.1");
    EXPECT_EQ(endpoints[2].port, 9083);
}

TEST(NodeConfigTopologyTest, ParseSingleNodeTopology)
{
    const auto endpoints = parse_peer_topology("0=127.0.0.1:9081");
    ASSERT_EQ(endpoints.size(), 1u);
    EXPECT_EQ(endpoints[0].node_id, 0u);
    EXPECT_EQ(endpoints[0].host, "127.0.0.1");
    EXPECT_EQ(endpoints[0].port, 9081);
}

TEST(NodeConfigTopologyTest, ParseUnorderedIsSorted)
{
    const auto endpoints = parse_peer_topology(
        "2=127.0.0.1:9083,0=127.0.0.1:9081,1=127.0.0.1:9082");
    ASSERT_EQ(endpoints.size(), 3u);
    EXPECT_EQ(endpoints[0].node_id, 0u);
    EXPECT_EQ(endpoints[1].node_id, 1u);
    EXPECT_EQ(endpoints[2].node_id, 2u);
}

TEST(NodeConfigTopologyTest, ParseWhitespaceTolerant)
{
    const auto endpoints = parse_peer_topology(
        "  0 = 127.0.0.1 : 9081 ,  1 = 127.0.0.1 : 9082  ");
    ASSERT_EQ(endpoints.size(), 2u);
    EXPECT_EQ(endpoints[0].node_id, 0u);
    EXPECT_EQ(endpoints[0].port, 9081);
    EXPECT_EQ(endpoints[1].node_id, 1u);
    EXPECT_EQ(endpoints[1].port, 9082);
}

TEST(NodeConfigTopologyTest, EmptyThrows)
{
    EXPECT_THROW(parse_peer_topology(""), std::invalid_argument);
    EXPECT_THROW(parse_peer_topology("   "), std::invalid_argument);
}

TEST(NodeConfigTopologyTest, NonContiguousIdsThrows)
{
    // Missing node 1
    EXPECT_THROW(parse_peer_topology("0=127.0.0.1:9081,2=127.0.0.1:9083"),
                 std::invalid_argument);
    // Starts at 1 instead of 0
    EXPECT_THROW(parse_peer_topology("1=127.0.0.1:9081,2=127.0.0.1:9082"),
                 std::invalid_argument);
}

TEST(NodeConfigTopologyTest, DuplicateNodeIdThrows)
{
    EXPECT_THROW(parse_peer_topology("0=127.0.0.1:9081,0=127.0.0.1:9082"),
                 std::invalid_argument);
}

TEST(NodeConfigTopologyTest, InvalidPortThrows)
{
    EXPECT_THROW(parse_peer_topology("0=127.0.0.1:0"), std::invalid_argument);
    EXPECT_THROW(parse_peer_topology("0=127.0.0.1:70000"), std::invalid_argument);
    EXPECT_THROW(parse_peer_topology("0=127.0.0.1:xyz"), std::invalid_argument);
}

TEST(NodeConfigTopologyTest, MalformedSyntaxThrows)
{
    EXPECT_THROW(parse_peer_topology("0-127.0.0.1:9081"), std::invalid_argument);
    EXPECT_THROW(parse_peer_topology("0=:9081"), std::invalid_argument);
    EXPECT_THROW(parse_peer_topology("0=127.0.0.1"), std::invalid_argument);
    EXPECT_THROW(parse_peer_topology("0=127.0.0.1:"), std::invalid_argument);
    EXPECT_THROW(parse_peer_topology("=127.0.0.1:9081"), std::invalid_argument);
}

TEST(NodeConfigTopologyTest, MalformedHostThrows)
{
    EXPECT_THROW(parse_peer_topology("0=127.0.0.1 extra:9081"), std::invalid_argument);
    EXPECT_THROW(parse_peer_topology("0=bad host:9081"), std::invalid_argument);
}

// =========================================================================
// 6. make_deterministic_replica_sets tests
// =========================================================================

TEST(NodeConfigTopologyTest, DeterministicReplicaSetsN3R3)
{
    // For 3 shards, 3 nodes, R=3:
    // Shard 0 -> {0, 1, 2}
    // Shard 1 -> {1, 2, 0}
    // Shard 2 -> {2, 0, 1}
    const auto sets = make_deterministic_replica_sets(3, 3, 3);
    ASSERT_EQ(sets.size(), 3u);

    EXPECT_EQ(sets[0].shard_id, 0u);
    const std::vector<std::size_t> expected0 = {0, 1, 2};
    EXPECT_EQ(sets[0].node_ids, expected0);

    EXPECT_EQ(sets[1].shard_id, 1u);
    const std::vector<std::size_t> expected1 = {1, 2, 0};
    EXPECT_EQ(sets[1].node_ids, expected1);

    EXPECT_EQ(sets[2].shard_id, 2u);
    const std::vector<std::size_t> expected2 = {2, 0, 1};
    EXPECT_EQ(sets[2].node_ids, expected2);
}

TEST(NodeConfigTopologyTest, DeterministicReplicaSetsN3R2)
{
    const auto sets = make_deterministic_replica_sets(3, 3, 2);
    ASSERT_EQ(sets.size(), 3u);

    const std::vector<std::size_t> expected0 = {0, 1};
    EXPECT_EQ(sets[0].node_ids, expected0);

    const std::vector<std::size_t> expected1 = {1, 2};
    EXPECT_EQ(sets[1].node_ids, expected1);

    const std::vector<std::size_t> expected2 = {2, 0};
    EXPECT_EQ(sets[2].node_ids, expected2);
}

TEST(NodeConfigTopologyTest, DeterministicReplicaSetsValidationThrows)
{
    EXPECT_THROW(make_deterministic_replica_sets(0, 3, 3), std::invalid_argument);
    EXPECT_THROW(make_deterministic_replica_sets(3, 0, 1), std::invalid_argument);
    EXPECT_THROW(make_deterministic_replica_sets(3, 3, 0), std::invalid_argument);
    EXPECT_THROW(make_deterministic_replica_sets(3, 2, 3), std::invalid_argument);
}

} // namespace
} // namespace dse
