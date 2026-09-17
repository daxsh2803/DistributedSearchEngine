// Distributed Search Engine - Distributed Coordinator Tests (Phase 11).
//
// Tests for ShardCoordinator with multi-node, multi-shard configurations.
// Verifies:
//   - Multiple shards per node
//   - Multiple nodes
//   - Correct shard→node placement routing
//   - Global TF-IDF across nodes
//   - Search parallel fan-out
//   - Lifecycle operations through NodeClient
//   - Persistence across nodes

#include "shard_coordinator.h"

#include <gtest/gtest.h>

#include <cstddef>
#include <cstdint>
#include <cstdio>
#include <memory>
#include <string>
#include <vector>

#include "inverted_index.h"
#include "local_node.h"
#include "node_client.h"
#include "node_config.h"
#include "search_service.h"
#include "shard.h"
#include "shard_router.h"

namespace dse {
namespace {

// Helper: create a coordinator with specific placement.
// placement[i] = node_id for shard i.
std::unique_ptr<ShardCoordinator> make_coordinator_with_placement(
    std::size_t shard_count,
    std::size_t node_count,
    const std::vector<std::size_t>& placement)
{
    auto router = std::make_unique<ShardRouter>(shard_count);
    auto shard_placement = std::make_unique<ShardPlacement>(
        shard_count, node_count, placement);

    // Group shards by node_id.
    std::vector<std::unique_ptr<NodeClient>> nodes;
    nodes.reserve(node_count);
    for (std::size_t nid = 0; nid < node_count; ++nid) {
        auto node = std::make_unique<LocalNode>(nid);
        for (std::size_t sid = 0; sid < shard_count; ++sid) {
            if (placement[sid] == nid) {
                node->add_shard(sid, std::make_unique<Shard>());
            }
        }
        nodes.push_back(std::move(node));
    }

    return std::make_unique<ShardCoordinator>(
        std::move(router), std::move(shard_placement), std::move(nodes));
}

// =========================================================================
// 1. Multiple shards on one node
// =========================================================================

TEST(DistributedCoordinatorTest, MultipleShardsOnOneNode)
{
    // 3 shards, all on node 0.
    auto coord = make_coordinator_with_placement(3, 1, {0, 0, 0});

    EXPECT_EQ(coord->shard_count(), 3u);
    EXPECT_EQ(coord->node_count(), 1u);

    coord->ingest({1, "alpha"});
    coord->ingest({2, "beta"});
    coord->ingest({3, "gamma"});

    EXPECT_EQ(coord->total_document_count(), 3u);
}

TEST(DistributedCoordinatorTest, SearchAcrossMultipleShardsOnOneNode)
{
    auto coord = make_coordinator_with_placement(3, 1, {0, 0, 0});

    coord->ingest({1, "cat dog"});
    coord->ingest({5, "bird fish"});
    coord->ingest({10, "cat bird"});

    SearchRequest req;
    req.query = "cat";
    req.mode = SearchMode::Or;
    req.limit = 10;

    const auto resp = coord->search(req);
    EXPECT_FALSE(resp.is_error);
    EXPECT_EQ(resp.total, 2u);  // docs 1 and 10
}

// =========================================================================
// 2. Multiple nodes, each with one shard
// =========================================================================

TEST(DistributedCoordinatorTest, OneShardPerNode)
{
    auto coord = make_coordinator_with_placement(3, 3, {0, 1, 2});

    EXPECT_EQ(coord->shard_count(), 3u);
    EXPECT_EQ(coord->node_count(), 3u);

    coord->ingest({1, "apple"});
    coord->ingest({2, "banana"});
    coord->ingest({3, "cherry"});

    EXPECT_EQ(coord->total_document_count(), 3u);
}

// =========================================================================
// 3. Mixed: multiple shards per node
// =========================================================================

TEST(DistributedCoordinatorTest, MixedPlacement)
{
    // Shard 0→node 0, Shard 1→node 1, Shard 2→node 0, Shard 3→node 2
    auto coord = make_coordinator_with_placement(4, 3, {0, 1, 0, 2});

    EXPECT_EQ(coord->shard_count(), 4u);
    EXPECT_EQ(coord->node_count(), 3u);

    // Node 0 has shards 0 and 2.
    // Node 1 has shard 1.
    // Node 2 has shard 3.

    coord->ingest({1, "document one"});
    coord->ingest({2, "document two"});
    coord->ingest({3, "document three"});
    coord->ingest({4, "document four"});

    EXPECT_EQ(coord->total_document_count(), 4u);
}

// =========================================================================
// 4. Global TF-IDF across nodes
// =========================================================================

TEST(DistributedCoordinatorTest, GlobalTfIdfAcrossNodes)
{
    // 3 shards on 3 nodes for clear separation.
    auto coord = make_coordinator_with_placement(3, 3, {0, 1, 2});

    // "cat" appears in shard 0 (doc 1) and shard 1 (doc 5).
    // "dog" appears in shard 2 (doc 10).
    // "fish" appears only in shard 0 (doc 15).
    coord->ingest({1, "cat cat dog"});
    coord->ingest({5, "cat bird"});
    coord->ingest({10, "dog dog dog"});
    coord->ingest({15, "fish fish fish"});

    SearchRequest req;
    req.query = "cat";
    req.mode = SearchMode::Or;
    req.limit = 10;

    const auto resp = coord->search(req);
    EXPECT_FALSE(resp.is_error);
    // 2 documents contain "cat": docs 1 and 5.
    EXPECT_EQ(resp.total, 2u);

    // doc 1: TF("cat")=2, df("cat")=2, N=4
    // doc 5: TF("cat")=1, df("cat")=2, N=4
    // IDF = ln(4/2) ≈ 0.693
    // doc 1 score ≈ 2*0.693 ≈ 1.386
    // doc 5 score ≈ 1*0.693 ≈ 0.693
    EXPECT_EQ(resp.results[0].document_id, 1u);
    EXPECT_EQ(resp.results[1].document_id, 5u);
}

// =========================================================================
// 5. AND search across nodes
// =========================================================================

TEST(DistributedCoordinatorTest, AndSearchAcrossNodes)
{
    auto coord = make_coordinator_with_placement(3, 3, {0, 1, 2});

    coord->ingest({1, "quick brown fox"});
    coord->ingest({5, "quick red car"});
    coord->ingest({10, "brown lazy dog"});

    SearchRequest req;
    req.query = "quick brown";
    req.mode = SearchMode::And;
    req.limit = 10;

    const auto resp = coord->search(req);
    EXPECT_FALSE(resp.is_error);
    // Only doc 1 has both "quick" and "brown".
    EXPECT_EQ(resp.total, 1u);
}

// =========================================================================
// 6. Lifecycle across nodes
// =========================================================================

TEST(DistributedCoordinatorTest, UpdateAcrossNodes)
{
    auto coord = make_coordinator_with_placement(2, 2, {0, 1});

    coord->ingest({1, "original content"});
    coord->ingest({50, "other content"});

    const auto resp = coord->update({1, "updated content"});
    EXPECT_FALSE(resp.is_error);

    const auto doc = coord->get_document(1);
    ASSERT_TRUE(doc.has_value());
    EXPECT_EQ(doc->content, "updated content");
}

TEST(DistributedCoordinatorTest, DeleteAcrossNodes)
{
    auto coord = make_coordinator_with_placement(2, 2, {0, 1});

    coord->ingest({1, "alpha"});
    coord->ingest({50, "beta"});

    const auto resp = coord->remove(1);
    EXPECT_FALSE(resp.is_error);
    EXPECT_FALSE(coord->get_document(1).has_value());
    EXPECT_TRUE(coord->get_document(50).has_value());
}

TEST(DistributedCoordinatorTest, UpdateMissingAcrossNodes)
{
    auto coord = make_coordinator_with_placement(3, 3, {0, 1, 2});
    const auto resp = coord->update({999, "new content"});
    EXPECT_TRUE(resp.is_error);
}

TEST(DistributedCoordinatorTest, DeleteMissingAcrossNodes)
{
    auto coord = make_coordinator_with_placement(3, 3, {0, 1, 2});
    const auto resp = coord->remove(999);
    EXPECT_TRUE(resp.is_error);
}

// =========================================================================
// 7. Persistence across nodes
// =========================================================================

TEST(DistributedCoordinatorTest, PersistenceAcrossNodes)
{
    // Shard 0 → node 0, Shard 1 → node 1.
    auto router = std::make_unique<ShardRouter>(2);
    std::vector<std::size_t> placement = {0, 1};
    auto shard_placement = std::make_unique<ShardPlacement>(2, 2, placement);

    auto node0 = std::make_unique<LocalNode>(0);
    node0->add_shard(0, std::make_unique<Shard>("test_dist_node0.jsonl"));
    auto node1 = std::make_unique<LocalNode>(1);
    node1->add_shard(1, std::make_unique<Shard>("test_dist_node1.jsonl"));

    std::vector<std::unique_ptr<NodeClient>> nodes;
    nodes.push_back(std::move(node0));
    nodes.push_back(std::move(node1));

    {
        auto coord = std::make_unique<ShardCoordinator>(
            std::move(router), std::move(shard_placement), std::move(nodes));
        coord->ingest({1, "hello"});
        coord->ingest({50, "world"});
        coord->save_all();
    }

    // Reload.
    auto router2 = std::make_unique<ShardRouter>(2);
    std::vector<std::size_t> placement2 = {0, 1};
    auto shard_placement2 = std::make_unique<ShardPlacement>(2, 2, placement2);

    auto node0b = std::make_unique<LocalNode>(0);
    node0b->add_shard(0, std::make_unique<Shard>("test_dist_node0.jsonl"));
    auto node1b = std::make_unique<LocalNode>(1);
    node1b->add_shard(1, std::make_unique<Shard>("test_dist_node1.jsonl"));

    std::vector<std::unique_ptr<NodeClient>> nodes2;
    nodes2.push_back(std::move(node0b));
    nodes2.push_back(std::move(node1b));

    auto coord2 = std::make_unique<ShardCoordinator>(
        std::move(router2), std::move(shard_placement2), std::move(nodes2));
    coord2->load_all();

    EXPECT_EQ(coord2->total_document_count(), 2u);
    EXPECT_TRUE(coord2->get_document(1).has_value());
    EXPECT_TRUE(coord2->get_document(50).has_value());

    std::remove("test_dist_node0.jsonl");
    std::remove("test_dist_node1.jsonl");
}

// =========================================================================
// 8. Tie-breaking across nodes
// =========================================================================

TEST(DistributedCoordinatorTest, TieBreakAcrossNodes)
{
    auto coord = make_coordinator_with_placement(3, 3, {0, 1, 2});

    coord->ingest({5, "identical"});
    coord->ingest({1, "identical"});
    coord->ingest({3, "identical"});

    SearchRequest req;
    req.query = "identical";
    req.mode = SearchMode::Or;
    req.limit = 10;

    const auto resp = coord->search(req);
    EXPECT_FALSE(resp.is_error);
    EXPECT_EQ(resp.total, 3u);

    // Tie-break by doc_id ascending.
    EXPECT_EQ(resp.results[0].document_id, 1u);
    EXPECT_EQ(resp.results[1].document_id, 3u);
    EXPECT_EQ(resp.results[2].document_id, 5u);
}

// =========================================================================
// 9. Single-shard / single-node compatibility
// =========================================================================

TEST(DistributedCoordinatorTest, SingleShardSingleNodeBehavesLikePhase10)
{
    auto coord = make_coordinator_with_placement(1, 1, {0});

    coord->ingest({1, "quick brown fox"});
    coord->ingest({2, "lazy dog"});
    coord->ingest({3, "another fox"});

    EXPECT_EQ(coord->total_document_count(), 3u);

    SearchRequest req;
    req.query = "fox";
    req.mode = SearchMode::Or;
    req.limit = 10;

    const auto resp = coord->search(req);
    EXPECT_FALSE(resp.is_error);
    EXPECT_EQ(resp.total, 2u);
}

// =========================================================================
// 10. Node identity preserved
// =========================================================================

TEST(DistributedCoordinatorTest, NodeIdentityPreserved)
{
    auto coord = make_coordinator_with_placement(2, 3, {0, 1});

    EXPECT_EQ(coord->node(0).node_id(), 0u);
    EXPECT_EQ(coord->node(1).node_id(), 1u);
    EXPECT_EQ(coord->node_count(), 3u);
}

// =========================================================================
// 11. Unrelated documents preserved across nodes
// =========================================================================

TEST(DistributedCoordinatorTest, UpdatePreservesUnrelatedAcrossNodes)
{
    auto coord = make_coordinator_with_placement(3, 3, {0, 1, 2});

    coord->ingest({1, "alpha"});
    coord->ingest({50, "beta"});
    coord->ingest({100, "gamma"});

    coord->update({1, "alpha_new"});

    EXPECT_EQ(coord->total_document_count(), 3u);
    EXPECT_TRUE(coord->get_document(50).has_value());
    EXPECT_TRUE(coord->get_document(100).has_value());

    const auto doc = coord->get_document(1);
    ASSERT_TRUE(doc.has_value());
    EXPECT_EQ(doc->content, "alpha_new");
}

TEST(DistributedCoordinatorTest, DeletePreservesUnrelatedAcrossNodes)
{
    auto coord = make_coordinator_with_placement(3, 3, {0, 1, 2});

    coord->ingest({1, "alpha"});
    coord->ingest({50, "beta"});
    coord->ingest({100, "gamma"});

    coord->remove(50);

    EXPECT_EQ(coord->total_document_count(), 2u);
    EXPECT_TRUE(coord->get_document(1).has_value());
    EXPECT_TRUE(coord->get_document(100).has_value());
    EXPECT_FALSE(coord->get_document(50).has_value());
}

} // namespace
} // namespace dse
