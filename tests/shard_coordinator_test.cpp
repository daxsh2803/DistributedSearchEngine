// Distributed Search Engine - Shard Coordinator Tests (Phase 10C, 11B).
//
// Tests for dse::ShardCoordinator: write routing, cross-shard search,
// global TF-IDF scoring, AND/OR modes, single-shard compatibility,
// and NodeClient-based architecture.

#include "shard_coordinator.h"

#include <gtest/gtest.h>

#include <cstddef>
#include <cstdint>
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

// Helper: create a coordinator with N shards on 1 node, no persistence.
std::unique_ptr<ShardCoordinator> make_coordinator(std::size_t n)
{
    auto router = std::make_unique<ShardRouter>(n);
    std::vector<std::size_t> placement(n, 0);  // all shards on node 0
    auto shard_placement = std::make_unique<ShardPlacement>(n, 1, placement);

    auto node = std::make_unique<LocalNode>(0);
    for (std::size_t i = 0; i < n; ++i) {
        node->add_shard(i, std::make_unique<Shard>());
    }

    std::vector<std::unique_ptr<NodeClient>> nodes;
    nodes.push_back(std::move(node));

    return std::make_unique<ShardCoordinator>(
        std::move(router), std::move(shard_placement), std::move(nodes));
}

// =========================================================================
// 1. Routing correctness
// =========================================================================

TEST(ShardCoordinatorTest, CreateRoutesToCorrectShard)
{
    auto coord = make_coordinator(3);

    CoordinatorIngestResponse resp = coord->ingest({1, "hello world"});
    EXPECT_FALSE(resp.is_error);
    EXPECT_EQ(resp.document_id, 1u);

    // The document should be retrievable.
    const auto doc = coord->get_document(1);
    EXPECT_TRUE(doc.has_value());
}

TEST(ShardCoordinatorTest, SameIDAlwaysRoutesToSameShard)
{
    auto coord = make_coordinator(3);

    coord->ingest({42, "first"});
    const auto resp = coord->ingest({42, "second"});
    EXPECT_TRUE(resp.is_error);  // Duplicate.
}

TEST(ShardCoordinatorTest, DifferentIDsDistribute)
{
    auto coord = make_coordinator(3);

    for (doc_id i = 0; i < 100; ++i) {
        coord->ingest({i, "doc " + std::to_string(i)});
    }

    EXPECT_EQ(coord->total_document_count(), 100u);
}

// =========================================================================
// 2. Create / Read
// =========================================================================

TEST(ShardCoordinatorTest, IngestAndRetrieve)
{
    auto coord = make_coordinator(3);

    coord->ingest({1, "quick brown fox"});
    const auto doc = coord->get_document(1);
    ASSERT_TRUE(doc.has_value());
    EXPECT_EQ(doc->content, "quick brown fox");
}

TEST(ShardCoordinatorTest, IngestDuplicateFails)
{
    auto coord = make_coordinator(3);

    coord->ingest({1, "first"});
    const auto resp = coord->ingest({1, "second"});
    EXPECT_TRUE(resp.is_error);
    EXPECT_EQ(coord->total_document_count(), 1u);
}

TEST(ShardCoordinatorTest, IngestEmptyContentFails)
{
    auto coord = make_coordinator(3);
    const auto resp = coord->ingest({1, ""});
    EXPECT_TRUE(resp.is_error);
}

// =========================================================================
// 3. Update
// =========================================================================

TEST(ShardCoordinatorTest, UpdateExistingDocument)
{
    auto coord = make_coordinator(3);
    coord->ingest({1, "original content"});

    const auto resp = coord->update({1, "updated content"});
    EXPECT_FALSE(resp.is_error);

    const auto doc = coord->get_document(1);
    ASSERT_TRUE(doc.has_value());
    EXPECT_EQ(doc->content, "updated content");
}

TEST(ShardCoordinatorTest, UpdateMissingDocumentFails)
{
    auto coord = make_coordinator(3);
    const auto resp = coord->update({999, "new content"});
    EXPECT_TRUE(resp.is_error);
    EXPECT_NE(resp.error_message.find("not found"), std::string::npos);
}

TEST(ShardCoordinatorTest, UpdatePreservesDocumentCount)
{
    auto coord = make_coordinator(3);
    coord->ingest({1, "alpha"});
    coord->ingest({2, "beta"});

    coord->update({1, "new alpha"});
    EXPECT_EQ(coord->total_document_count(), 2u);
}

// =========================================================================
// 4. Delete
// =========================================================================

TEST(ShardCoordinatorTest, DeleteExistingDocument)
{
    auto coord = make_coordinator(3);
    coord->ingest({1, "to be deleted"});

    const auto resp = coord->remove(1);
    EXPECT_FALSE(resp.is_error);
    EXPECT_FALSE(coord->get_document(1).has_value());
}

TEST(ShardCoordinatorTest, DeleteMissingDocumentFails)
{
    auto coord = make_coordinator(3);
    const auto resp = coord->remove(999);
    EXPECT_TRUE(resp.is_error);
}

TEST(ShardCoordinatorTest, DeleteDecreasesDocumentCount)
{
    auto coord = make_coordinator(3);
    coord->ingest({1, "a"});
    coord->ingest({2, "b"});
    coord->remove(1);
    EXPECT_EQ(coord->total_document_count(), 1u);
}

TEST(ShardCoordinatorTest, DeletePreservesOtherDocuments)
{
    auto coord = make_coordinator(3);
    coord->ingest({1, "alpha"});
    coord->ingest({2, "beta"});
    coord->remove(1);

    EXPECT_FALSE(coord->get_document(1).has_value());
    EXPECT_TRUE(coord->get_document(2).has_value());
}

// =========================================================================
// 5. Cross-shard search — OR mode
// =========================================================================

TEST(ShardCoordinatorTest, CrossShardOrSearch)
{
    auto coord = make_coordinator(3);

    coord->ingest({1, "quick brown fox"});
    coord->ingest({5, "lazy dog"});
    coord->ingest({10, "another fox jumps"});

    SearchRequest req;
    req.query = "fox";
    req.mode = SearchMode::Or;
    req.limit = 10;

    const auto resp = coord->search(req);
    EXPECT_FALSE(resp.is_error);
    EXPECT_EQ(resp.total, 2u);
}

TEST(ShardCoordinatorTest, CrossShardOrSearchMultipleTerms)
{
    auto coord = make_coordinator(3);

    coord->ingest({1, "cat dog"});
    coord->ingest({5, "bird fish"});
    coord->ingest({10, "cat bird"});

    SearchRequest req;
    req.query = "cat bird";
    req.mode = SearchMode::Or;
    req.limit = 10;

    const auto resp = coord->search(req);
    EXPECT_FALSE(resp.is_error);
    EXPECT_EQ(resp.total, 3u);
}

// =========================================================================
// 6. Cross-shard search — AND mode
// =========================================================================

TEST(ShardCoordinatorTest, CrossShardAndSearch)
{
    auto coord = make_coordinator(3);

    coord->ingest({1, "quick brown fox"});
    coord->ingest({5, "quick red car"});
    coord->ingest({10, "brown lazy dog"});

    SearchRequest req;
    req.query = "quick brown";
    req.mode = SearchMode::And;
    req.limit = 10;

    const auto resp = coord->search(req);
    EXPECT_FALSE(resp.is_error);
    EXPECT_EQ(resp.total, 1u);
}

TEST(ShardCoordinatorTest, AndSearchMissingTermReturnsEmpty)
{
    auto coord = make_coordinator(3);
    coord->ingest({1, "hello world"});

    SearchRequest req;
    req.query = "hello nonexistent_xyz";
    req.mode = SearchMode::And;
    req.limit = 10;

    const auto resp = coord->search(req);
    EXPECT_FALSE(resp.is_error);
    EXPECT_EQ(resp.total, 0u);
}

TEST(ShardCoordinatorTest, OrSearchMissingTermIgnoresIt)
{
    auto coord = make_coordinator(3);
    coord->ingest({1, "hello world"});

    SearchRequest req;
    req.query = "hello nonexistent_xyz";
    req.mode = SearchMode::Or;
    req.limit = 10;

    const auto resp = coord->search(req);
    EXPECT_FALSE(resp.is_error);
    EXPECT_EQ(resp.total, 1u);
}

// =========================================================================
// 7. Global TF-IDF
// =========================================================================

TEST(ShardCoordinatorTest, GlobalTfIdfRanking)
{
    auto coord = make_coordinator(3);

    coord->ingest({1, "cat cat dog"});
    coord->ingest({5, "cat bird"});
    coord->ingest({10, "fish fish fish"});
    coord->ingest({15, "cat cat cat mouse"});

    SearchRequest req;
    req.query = "cat";
    req.mode = SearchMode::Or;
    req.limit = 10;

    const auto resp = coord->search(req);
    EXPECT_FALSE(resp.is_error);
    EXPECT_EQ(resp.total, 3u);

    EXPECT_EQ(resp.results[0].document_id, 15u);
    EXPECT_EQ(resp.results[1].document_id, 1u);
    EXPECT_EQ(resp.results[2].document_id, 5u);
}

// =========================================================================
// 8. Limit
// =========================================================================

TEST(ShardCoordinatorTest, LimitAppliedCorrectly)
{
    auto coord = make_coordinator(3);

    for (doc_id i = 0; i < 20; ++i) {
        coord->ingest({i, "common_term doc " + std::to_string(i)});
    }

    SearchRequest req;
    req.query = "common_term";
    req.mode = SearchMode::Or;
    req.limit = 5;

    const auto resp = coord->search(req);
    EXPECT_FALSE(resp.is_error);
    EXPECT_EQ(resp.total, 20u);
    EXPECT_EQ(resp.results.size(), 5u);
}

// =========================================================================
// 9. Tie-breaking
// =========================================================================

TEST(ShardCoordinatorTest, TieBreakByDocIdAscending)
{
    auto coord = make_coordinator(3);

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

    EXPECT_EQ(resp.results[0].document_id, 1u);
    EXPECT_EQ(resp.results[1].document_id, 3u);
    EXPECT_EQ(resp.results[2].document_id, 5u);
}

// =========================================================================
// 10. Single-shard compatibility
// =========================================================================

TEST(ShardCoordinatorTest, SingleShardBehavesLikePhase9)
{
    auto coord = make_coordinator(1);

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

TEST(ShardCoordinatorTest, SingleShardLifecycleWorks)
{
    auto coord = make_coordinator(1);

    coord->ingest({1, "hello"});
    coord->ingest({2, "world"});

    EXPECT_EQ(coord->total_document_count(), 2u);

    const auto upd = coord->update({1, "goodbye"});
    EXPECT_FALSE(upd.is_error);
    EXPECT_EQ(coord->total_document_count(), 2u);

    const auto del = coord->remove(2);
    EXPECT_FALSE(del.is_error);
    EXPECT_EQ(coord->total_document_count(), 1u);
}

// =========================================================================
// 11. Unrelated documents preserved
// =========================================================================

TEST(ShardCoordinatorTest, UpdatePreservesUnrelatedDocuments)
{
    auto coord = make_coordinator(3);
    coord->ingest({1, "alpha"});
    coord->ingest({2, "beta"});
    coord->ingest({3, "gamma"});

    coord->update({1, "alpha_new"});

    EXPECT_EQ(coord->total_document_count(), 3u);
    EXPECT_TRUE(coord->get_document(2).has_value());
    EXPECT_TRUE(coord->get_document(3).has_value());

    const auto doc = coord->get_document(1);
    ASSERT_TRUE(doc.has_value());
    EXPECT_EQ(doc->content, "alpha_new");
}

TEST(ShardCoordinatorTest, DeletePreservesUnrelatedDocuments)
{
    auto coord = make_coordinator(3);
    coord->ingest({1, "alpha"});
    coord->ingest({2, "beta"});
    coord->ingest({3, "gamma"});

    coord->remove(2);

    EXPECT_EQ(coord->total_document_count(), 2u);
    EXPECT_TRUE(coord->get_document(1).has_value());
    EXPECT_TRUE(coord->get_document(3).has_value());
}

// =========================================================================
// 12. Persistence
// =========================================================================

TEST(ShardCoordinatorTest, SaveAndLoadRoundTrip)
{
    auto router = std::make_unique<ShardRouter>(2);
    std::vector<std::size_t> placement = {0, 0};
    auto shard_placement = std::make_unique<ShardPlacement>(2, 1, placement);

    auto node = std::make_unique<LocalNode>(0);
    node->add_shard(0, std::make_unique<Shard>("test_coord_shard_0.jsonl"));
    node->add_shard(1, std::make_unique<Shard>("test_coord_shard_1.jsonl"));

    std::vector<std::unique_ptr<NodeClient>> nodes;
    nodes.push_back(std::move(node));

    {
        auto coord = std::make_unique<ShardCoordinator>(
            std::move(router), std::move(shard_placement), std::move(nodes));
        coord->ingest({1, "hello"});
        coord->ingest({2, "world"});
        coord->save_all();
    }

    // Reload.
    auto router2 = std::make_unique<ShardRouter>(2);
    std::vector<std::size_t> placement2 = {0, 0};
    auto shard_placement2 = std::make_unique<ShardPlacement>(2, 1, placement2);

    auto node2 = std::make_unique<LocalNode>(0);
    node2->add_shard(0, std::make_unique<Shard>("test_coord_shard_0.jsonl"));
    node2->add_shard(1, std::make_unique<Shard>("test_coord_shard_1.jsonl"));

    std::vector<std::unique_ptr<NodeClient>> nodes2;
    nodes2.push_back(std::move(node2));

    auto coord2 = std::make_unique<ShardCoordinator>(
        std::move(router2), std::move(shard_placement2), std::move(nodes2));
    coord2->load_all();

    EXPECT_EQ(coord2->total_document_count(), 2u);
    EXPECT_TRUE(coord2->get_document(1).has_value());
    EXPECT_TRUE(coord2->get_document(2).has_value());

    std::remove("test_coord_shard_0.jsonl");
    std::remove("test_coord_shard_1.jsonl");
}

// =========================================================================
// 13. Error responses
// =========================================================================

TEST(ShardCoordinatorTest, SearchWithEmptyQueryReturnsError)
{
    auto coord = make_coordinator(3);
    SearchRequest req;
    req.query = "";
    req.mode = SearchMode::Or;
    req.limit = 10;

    const auto resp = coord->search(req);
    EXPECT_TRUE(resp.is_error);
}

TEST(ShardCoordinatorTest, GetMissingDocumentReturnsNullopt)
{
    auto coord = make_coordinator(3);
    EXPECT_FALSE(coord->get_document(999).has_value());
}

} // namespace
} // namespace dse
