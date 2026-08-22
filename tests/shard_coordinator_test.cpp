// Distributed Search Engine - Shard Coordinator Tests (Phase 10C).
//
// Tests for dse::ShardCoordinator: write routing, cross-shard search,
// global TF-IDF scoring, AND/OR modes, and single-shard compatibility.

#include "shard_coordinator.h"

#include <gtest/gtest.h>

#include <cstddef>
#include <cstdint>
#include <memory>
#include <string>
#include <vector>

#include "inverted_index.h"
#include "search_service.h"
#include "shard.h"
#include "shard_router.h"

namespace dse {
namespace {

// Helper: create a coordinator with N shards and no persistence.
std::unique_ptr<ShardCoordinator> make_coordinator(std::size_t n)
{
    auto router = std::make_unique<ShardRouter>(n);
    std::vector<std::unique_ptr<Shard>> shards;
    shards.reserve(n);
    for (std::size_t i = 0; i < n; ++i) {
        shards.push_back(std::make_unique<Shard>());
    }
    return std::make_unique<ShardCoordinator>(std::move(router),
                                              std::move(shards));
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

    // The document should exist in exactly one shard.
    int found_in = -1;
    for (std::size_t i = 0; i < 3; ++i) {
        if (coord->shard(i).contains_document(1)) {
            found_in = static_cast<int>(i);
        }
    }
    EXPECT_GE(found_in, 0);
}

TEST(ShardCoordinatorTest, SameIDAlwaysRoutesToSameShard)
{
    auto coord = make_coordinator(3);

    // Ingest the same document twice (second should fail).
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

    // All documents should be found.
    EXPECT_EQ(coord->total_document_count(), 100u);

    // Each shard should have some documents.
    for (std::size_t i = 0; i < 3; ++i) {
        EXPECT_GT(coord->shard(i).document_count(), 0u)
            << "Shard " << i << " has no documents";
    }
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

    // Force documents across shards by using IDs that route differently.
    coord->ingest({1, "quick brown fox"});
    coord->ingest({5, "lazy dog"});
    coord->ingest({10, "another fox jumps"});

    SearchRequest req;
    req.query = "fox";
    req.mode = SearchMode::Or;
    req.limit = 10;

    const auto resp = coord->search(req);
    EXPECT_FALSE(resp.is_error);
    EXPECT_EQ(resp.total, 2u);  // docs 1 and 10 contain "fox"
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
    EXPECT_EQ(resp.total, 3u);  // all three match at least one term
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
    EXPECT_EQ(resp.total, 1u);  // only doc 1 has both "quick" and "brown"
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
    EXPECT_EQ(resp.total, 1u);  // "hello" matches doc 1
}

// =========================================================================
// 7. Global TF-IDF
// =========================================================================

TEST(ShardCoordinatorTest, GlobalTfIdfRanking)
{
    auto coord = make_coordinator(3);

    // doc 1: "cat" appears 2 times, also has "dog"
    coord->ingest({1, "cat cat dog"});
    // doc 5: "cat" appears 1 time, also has "bird"
    coord->ingest({5, "cat bird"});
    // doc 10: "cat" does NOT appear — only "fish"
    coord->ingest({10, "fish fish fish"});
    // doc 15: "cat" appears 3 times, also has "mouse"
    coord->ingest({15, "cat cat cat mouse"});

    SearchRequest req;
    req.query = "cat";
    req.mode = SearchMode::Or;
    req.limit = 10;

    const auto resp = coord->search(req);
    EXPECT_FALSE(resp.is_error);
    // 3 documents contain "cat": docs 1, 5, 15
    EXPECT_EQ(resp.total, 3u);

    // TF-IDF: tfidf(t,d) = tf(t,d) * ln(N/df(t))
    // N=4, df("cat")=3, IDF = ln(4/3) ≈ 0.2877
    // doc 15: TF=3, score = 3 * 0.2877 ≈ 0.863
    // doc 1:  TF=2, score = 2 * 0.2877 ≈ 0.575
    // doc 5:  TF=1, score = 1 * 0.2877 ≈ 0.288
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

    // All documents have identical content → same TF-IDF score.
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

    // Tie-break by doc_id ascending: 1, 3, 5.
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
    // Create coordinator with persistence paths.
    auto router = std::make_unique<ShardRouter>(2);
    std::vector<std::unique_ptr<Shard>> shards;
    shards.push_back(std::make_unique<Shard>("test_coord_shard_0.jsonl"));
    shards.push_back(std::make_unique<Shard>("test_coord_shard_1.jsonl"));

    {
        auto coord = std::make_unique<ShardCoordinator>(
            std::move(router), std::move(shards));
        coord->ingest({1, "hello"});
        coord->ingest({2, "world"});
        coord->save_all();
    }

    // Reload.
    auto router2 = std::make_unique<ShardRouter>(2);
    std::vector<std::unique_ptr<Shard>> shards2;
    shards2.push_back(std::make_unique<Shard>("test_coord_shard_0.jsonl"));
    shards2.push_back(std::make_unique<Shard>("test_coord_shard_1.jsonl"));

    auto coord2 = std::make_unique<ShardCoordinator>(
        std::move(router2), std::move(shards2));
    coord2->load_all();

    EXPECT_EQ(coord2->total_document_count(), 2u);
    EXPECT_TRUE(coord2->get_document(1).has_value());
    EXPECT_TRUE(coord2->get_document(2).has_value());

    // Cleanup.
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
