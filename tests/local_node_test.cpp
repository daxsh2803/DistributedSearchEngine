// Distributed Search Engine - Local Node Tests (Phase 11A).
//
// Tests for dse::LocalNode: node identity, multi-shard ownership,
// search, lifecycle, unknown-shard rejection, and persistence.

#include "local_node.h"

#include <gtest/gtest.h>

#include <cstddef>
#include <cstdint>
#include <memory>
#include <string>
#include <unordered_set>
#include <vector>

#include "node_client.h"
#include "shard.h"

namespace dse {
namespace {

// ---------------------------------------------------------------------------
// Helper: create a LocalNode with N empty shards.
std::unique_ptr<LocalNode> make_node(std::size_t node_id,
                                     std::size_t num_shards,
                                     std::size_t first_shard_id = 0)
{
    auto node = std::make_unique<LocalNode>(node_id);
    for (std::size_t i = 0; i < num_shards; ++i) {
        node->add_shard(first_shard_id + i,
                        std::make_unique<Shard>());
    }
    return node;
}

// =========================================================================
// 1. Node identity
// =========================================================================

TEST(LocalNodeTest, NodeIdentity)
{
    LocalNode node(42);
    EXPECT_EQ(node.node_id(), 42u);
}

TEST(LocalNodeTest, NodeIdentityZero)
{
    LocalNode node(0);
    EXPECT_EQ(node.node_id(), 0u);
}

// =========================================================================
// 2. Shard ownership
// =========================================================================

TEST(LocalNodeTest, HasShardReturnsTrue)
{
    auto node = make_node(0, 3, 0);
    EXPECT_TRUE(node->has_shard(0));
    EXPECT_TRUE(node->has_shard(1));
    EXPECT_TRUE(node->has_shard(2));
}

TEST(LocalNodeTest, HasShardReturnsFalse)
{
    auto node = make_node(0, 2, 0);
    EXPECT_FALSE(node->has_shard(5));
    EXPECT_FALSE(node->has_shard(99));
}

TEST(LocalNodeTest, ShardCount)
{
    auto node = make_node(0, 5, 10);
    EXPECT_EQ(node->shard_count(), 5u);
}

TEST(LocalNodeTest, MultipleShardsNonContiguous)
{
    LocalNode node(0);
    node.add_shard(0, std::make_unique<Shard>());
    node.add_shard(3, std::make_unique<Shard>());
    node.add_shard(7, std::make_unique<Shard>());

    EXPECT_EQ(node.shard_count(), 3u);
    EXPECT_TRUE(node.has_shard(0));
    EXPECT_TRUE(node.has_shard(3));
    EXPECT_TRUE(node.has_shard(7));
    EXPECT_FALSE(node.has_shard(1));
    EXPECT_FALSE(node.has_shard(4));
}

// =========================================================================
// 3. Search
// =========================================================================

TEST(LocalNodeTest, SearchReturnsPostings)
{
    auto node = make_node(0, 2, 0);
    node->add_document({0, 1, "cat dog"});
    node->add_document({0, 2, "cat bird"});

    ShardSearchRequest req;
    req.shard_id = 0;
    req.terms = {"cat"};

    auto resp = node->search(req);
    EXPECT_FALSE(resp.is_error);
    EXPECT_EQ(resp.shard_id, 0u);
    EXPECT_EQ(resp.terms_postings.size(), 1u);
    EXPECT_EQ(resp.terms_postings[0].size(), 2u);  // both docs have "cat"
}

TEST(LocalNodeTest, SearchReturnsLocalDocumentCount)
{
    auto node = make_node(0, 1, 0);
    node->add_document({0, 1, "hello"});
    node->add_document({0, 2, "world"});

    ShardSearchRequest req;
    req.shard_id = 0;
    req.terms = {"hello"};

    auto resp = node->search(req);
    EXPECT_FALSE(resp.is_error);
    EXPECT_EQ(resp.local_document_count, 2u);
}

TEST(LocalNodeTest, SearchMultipleTerms)
{
    auto node = make_node(0, 1, 0);
    node->add_document({0, 1, "cat dog"});
    node->add_document({0, 2, "bird fish"});

    ShardSearchRequest req;
    req.shard_id = 0;
    req.terms = {"cat", "bird"};

    auto resp = node->search(req);
    EXPECT_FALSE(resp.is_error);
    EXPECT_EQ(resp.terms_postings.size(), 2u);
    EXPECT_EQ(resp.terms_postings[0].size(), 1u);  // "cat" → doc 1
    EXPECT_EQ(resp.terms_postings[1].size(), 1u);  // "bird" → doc 2
}

TEST(LocalNodeTest, SearchUnknownShardFails)
{
    auto node = make_node(0, 1, 0);

    ShardSearchRequest req;
    req.shard_id = 99;
    req.terms = {"hello"};

    auto resp = node->search(req);
    EXPECT_TRUE(resp.is_error);
    EXPECT_FALSE(resp.error_message.empty());
}

TEST(LocalNodeTest, SearchMissingTermReturnsEmptyPostings)
{
    auto node = make_node(0, 1, 0);
    node->add_document({0, 1, "hello world"});

    ShardSearchRequest req;
    req.shard_id = 0;
    req.terms = {"nonexistent"};

    auto resp = node->search(req);
    EXPECT_FALSE(resp.is_error);
    EXPECT_EQ(resp.terms_postings[0].size(), 0u);
}

// =========================================================================
// 4. Write operations
// =========================================================================

TEST(LocalNodeTest, AddDocumentSuccess)
{
    auto node = make_node(0, 1, 0);

    ShardWriteRequest req;
    req.shard_id = 0;
    req.document_id = 1;
    req.content = "hello world";

    auto resp = node->add_document(req);
    EXPECT_FALSE(resp.is_error);
    EXPECT_EQ(resp.document_id, 1u);
    EXPECT_GT(resp.terms_indexed, 0u);
}

TEST(LocalNodeTest, AddDocumentDuplicateFails)
{
    auto node = make_node(0, 1, 0);

    ShardWriteRequest req;
    req.shard_id = 0;
    req.document_id = 1;
    req.content = "hello";

    auto resp1 = node->add_document(req);
    EXPECT_FALSE(resp1.is_error);

    auto resp2 = node->add_document(req);
    EXPECT_TRUE(resp2.is_error);
}

TEST(LocalNodeTest, AddDocumentUnknownShardFails)
{
    auto node = make_node(0, 1, 0);

    ShardWriteRequest req;
    req.shard_id = 99;
    req.document_id = 1;
    req.content = "hello";

    auto resp = node->add_document(req);
    EXPECT_TRUE(resp.is_error);
}

TEST(LocalNodeTest, UpdateDocumentSuccess)
{
    auto node = make_node(0, 1, 0);
    node->add_document({0, 1, "original"});

    ShardWriteRequest req;
    req.shard_id = 0;
    req.document_id = 1;
    req.content = "updated";

    auto resp = node->update_document(req);
    EXPECT_FALSE(resp.is_error);

    ShardGetRequest get_req;
    get_req.shard_id = 0;
    get_req.document_id = 1;
    auto get_resp = node->get_document(get_req);
    EXPECT_TRUE(get_resp.found);
    EXPECT_EQ(get_resp.content, "updated");
}

TEST(LocalNodeTest, UpdateMissingDocumentFails)
{
    auto node = make_node(0, 1, 0);

    ShardWriteRequest req;
    req.shard_id = 0;
    req.document_id = 999;
    req.content = "new content";

    auto resp = node->update_document(req);
    EXPECT_TRUE(resp.is_error);
}

TEST(LocalNodeTest, RemoveDocumentSuccess)
{
    auto node = make_node(0, 1, 0);
    node->add_document({0, 1, "to delete"});

    ShardRemoveRequest req;
    req.shard_id = 0;
    req.document_id = 1;

    auto resp = node->remove_document(req);
    EXPECT_FALSE(resp.is_error);

    ShardGetRequest get_req;
    get_req.shard_id = 0;
    get_req.document_id = 1;
    auto get_resp = node->get_document(get_req);
    EXPECT_FALSE(get_resp.found);
}

TEST(LocalNodeTest, RemoveMissingDocumentFails)
{
    auto node = make_node(0, 1, 0);

    ShardRemoveRequest req;
    req.shard_id = 0;
    req.document_id = 999;

    auto resp = node->remove_document(req);
    EXPECT_TRUE(resp.is_error);
}

// =========================================================================
// 5. Read operations
// =========================================================================

TEST(LocalNodeTest, GetDocumentFound)
{
    auto node = make_node(0, 1, 0);
    node->add_document({0, 42, "important content"});

    ShardGetRequest req;
    req.shard_id = 0;
    req.document_id = 42;

    auto resp = node->get_document(req);
    EXPECT_TRUE(resp.found);
    EXPECT_EQ(resp.content, "important content");
}

TEST(LocalNodeTest, GetDocumentNotFound)
{
    auto node = make_node(0, 1, 0);

    ShardGetRequest req;
    req.shard_id = 0;
    req.document_id = 999;

    auto resp = node->get_document(req);
    EXPECT_FALSE(resp.found);
}

TEST(LocalNodeTest, DocumentCount)
{
    auto node = make_node(0, 1, 0);
    node->add_document({0, 1, "a"});
    node->add_document({0, 2, "b"});

    ShardCountRequest req;
    req.shard_id = 0;

    auto resp = node->document_count(req);
    EXPECT_EQ(resp.document_count, 2u);
}

TEST(LocalNodeTest, DocumentCountUnknownShard)
{
    auto node = make_node(0, 1, 0);

    ShardCountRequest req;
    req.shard_id = 99;

    auto resp = node->document_count(req);
    EXPECT_EQ(resp.document_count, 0u);
}

// =========================================================================
// 6. Multi-shard operations
// =========================================================================

TEST(LocalNodeTest, AddToMultipleShards)
{
    auto node = make_node(0, 3, 0);

    ShardWriteRequest req0 = {0, 1, "shard 0 doc"};
    ShardWriteRequest req1 = {1, 2, "shard 1 doc"};
    ShardWriteRequest req2 = {2, 3, "shard 2 doc"};

    EXPECT_FALSE(node->add_document(req0).is_error);
    EXPECT_FALSE(node->add_document(req1).is_error);
    EXPECT_FALSE(node->add_document(req2).is_error);

    ShardCountRequest cr;
    cr.shard_id = 0;
    EXPECT_EQ(node->document_count(cr).document_count, 1u);

    cr.shard_id = 1;
    EXPECT_EQ(node->document_count(cr).document_count, 1u);

    cr.shard_id = 2;
    EXPECT_EQ(node->document_count(cr).document_count, 1u);
}

TEST(LocalNodeTest, SearchAcrossDifferentShards)
{
    auto node = make_node(0, 2, 0);
    node->add_document({0, 1, "cat dog"});
    node->add_document({1, 2, "cat bird"});

    ShardSearchRequest req0 = {0, {"cat"}};
    ShardSearchRequest req1 = {1, {"cat"}};

    auto resp0 = node->search(req0);
    auto resp1 = node->search(req1);

    EXPECT_FALSE(resp0.is_error);
    EXPECT_FALSE(resp1.is_error);
    EXPECT_EQ(resp0.terms_postings[0].size(), 1u);  // shard 0: doc 1
    EXPECT_EQ(resp1.terms_postings[0].size(), 1u);  // shard 1: doc 2
}

// =========================================================================
// 7. Persistence
// =========================================================================

TEST(LocalNodeTest, SaveAndLoadShard)
{
    auto path = "test_local_node_persist.jsonl";

    {
        auto node = std::make_unique<LocalNode>(0);
        node->add_shard(0, std::make_unique<Shard>(path));
        ShardWriteRequest req = {0, 1, "persistent content"};
        node->add_document(req);
        EXPECT_TRUE(node->save_shard(0));
    }

    {
        auto node = std::make_unique<LocalNode>(0);
        node->add_shard(0, std::make_unique<Shard>(path));
        EXPECT_TRUE(node->load_shard(0));

        ShardGetRequest req = {0, 1};
        auto resp = node->get_document(req);
        EXPECT_TRUE(resp.found);
        EXPECT_EQ(resp.content, "persistent content");
    }

    std::remove(path);
}

TEST(LocalNodeTest, SaveUnknownShardFails)
{
    auto node = make_node(0, 1, 0);
    EXPECT_FALSE(node->save_shard(99));
}

TEST(LocalNodeTest, LoadUnknownShardFails)
{
    auto node = make_node(0, 1, 0);
    EXPECT_FALSE(node->load_shard(99));
}

} // namespace
} // namespace dse
