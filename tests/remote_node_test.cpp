// Distributed Search Engine - Remote Node Tests (Phase 12C).
//
// Tests RemoteNode by running a real NodeServer in a background thread
// and issuing real HTTP requests via RemoteNode (which uses httplib::Client
// internally). Verifies all NodeClient operations, error handling,
// multiple shards, concurrent access, and behavioral equivalence with
// LocalNode.

#include <gtest/gtest.h>

#include <chrono>
#include <cstddef>
#include <memory>
#include <string>
#include <thread>
#include <vector>

#include "local_node.h"
#include "node_client.h"
#include "node_server.h"
#include "remote_node.h"
#include "shard.h"

using namespace dse;

// ---------------------------------------------------------------------------
// Helper: start a NodeServer in a background thread, return port.
// ---------------------------------------------------------------------------

struct ServerHandle {
    std::unique_ptr<NodeServer> server;
    std::thread thread;

    void stop_and_join()
    {
        server->stop();
        if (thread.joinable()) {
            thread.join();
        }
    }
};

static ServerHandle start_server(std::size_t node_id,
                                 std::unique_ptr<LocalNode> node)
{
    ServerHandle h;
    h.server = std::make_unique<NodeServer>(node_id, std::move(node));
    h.thread = std::thread([&h]() { h.server->listen(0); });
    h.server->wait_until_ready();
    return h;
}

// ===========================================================================
// Basic operations
// ===========================================================================

TEST(RemoteNodeTest, NodeIdPreserved)
{
    auto node = std::make_unique<LocalNode>(42);
    node->add_shard(0, std::make_unique<Shard>());
    auto h = start_server(42, std::move(node));

    RemoteNode remote(42, "127.0.0.1", h.server->port());
    EXPECT_EQ(remote.node_id(), 42u);

    h.stop_and_join();
}

TEST(RemoteNodeTest, AddDocument)
{
    auto node = std::make_unique<LocalNode>(0);
    node->add_shard(0, std::make_unique<Shard>());
    auto h = start_server(0, std::move(node));

    RemoteNode remote(0, "127.0.0.1", h.server->port());
    ShardWriteRequest req{0, 1, "hello world"};
    auto resp = remote.add_document(req);

    EXPECT_FALSE(resp.is_error) << resp.error_message;
    EXPECT_EQ(resp.document_id, 1u);
    EXPECT_GT(resp.terms_indexed, 0u);

    h.stop_and_join();
}

TEST(RemoteNodeTest, AddDuplicateFails)
{
    auto node = std::make_unique<LocalNode>(0);
    node->add_shard(0, std::make_unique<Shard>());
    auto h = start_server(0, std::move(node));

    RemoteNode remote(0, "127.0.0.1", h.server->port());
    remote.add_document({0, 1, "first"});
    auto resp = remote.add_document({0, 1, "second"});

    EXPECT_TRUE(resp.is_error);

    h.stop_and_join();
}

TEST(RemoteNodeTest, GetDocumentFound)
{
    auto node = std::make_unique<LocalNode>(0);
    node->add_shard(0, std::make_unique<Shard>());
    auto h = start_server(0, std::move(node));

    RemoteNode remote(0, "127.0.0.1", h.server->port());
    remote.add_document({0, 10, "the quick brown fox"});

    auto resp = remote.get_document({0, 10});
    EXPECT_TRUE(resp.found);
    EXPECT_EQ(resp.content, "the quick brown fox");

    h.stop_and_join();
}

TEST(RemoteNodeTest, GetDocumentNotFound)
{
    auto node = std::make_unique<LocalNode>(0);
    node->add_shard(0, std::make_unique<Shard>());
    auto h = start_server(0, std::move(node));

    RemoteNode remote(0, "127.0.0.1", h.server->port());
    auto resp = remote.get_document({0, 999});
    EXPECT_FALSE(resp.found);

    h.stop_and_join();
}

TEST(RemoteNodeTest, UpdateDocument)
{
    auto node = std::make_unique<LocalNode>(0);
    node->add_shard(0, std::make_unique<Shard>());
    auto h = start_server(0, std::move(node));

    RemoteNode remote(0, "127.0.0.1", h.server->port());
    remote.add_document({0, 1, "old content"});

    auto resp = remote.update_document({0, 1, "new content"});
    EXPECT_FALSE(resp.is_error) << resp.error_message;

    // Verify old content is gone and new content is present.
    auto search_req = ShardSearchRequest{0, {"old"}};
    auto search_resp = remote.search(search_req);
    ASSERT_EQ(search_resp.terms_postings.size(), 1u);
    EXPECT_TRUE(search_resp.terms_postings[0].empty());

    auto search_req2 = ShardSearchRequest{0, {"new"}};
    auto search_resp2 = remote.search(search_req2);
    ASSERT_EQ(search_resp2.terms_postings.size(), 1u);
    EXPECT_FALSE(search_resp2.terms_postings[0].empty());

    h.stop_and_join();
}

TEST(RemoteNodeTest, UpdateMissingFails)
{
    auto node = std::make_unique<LocalNode>(0);
    node->add_shard(0, std::make_unique<Shard>());
    auto h = start_server(0, std::move(node));

    RemoteNode remote(0, "127.0.0.1", h.server->port());
    auto resp = remote.update_document({0, 999, "content"});
    EXPECT_TRUE(resp.is_error);

    h.stop_and_join();
}

TEST(RemoteNodeTest, RemoveDocument)
{
    auto node = std::make_unique<LocalNode>(0);
    node->add_shard(0, std::make_unique<Shard>());
    auto h = start_server(0, std::move(node));

    RemoteNode remote(0, "127.0.0.1", h.server->port());
    remote.add_document({0, 1, "to be removed"});

    auto resp = remote.remove_document({0, 1});
    EXPECT_FALSE(resp.is_error) << resp.error_message;

    auto get_resp = remote.get_document({0, 1});
    EXPECT_FALSE(get_resp.found);

    h.stop_and_join();
}

TEST(RemoteNodeTest, RemoveMissingFails)
{
    auto node = std::make_unique<LocalNode>(0);
    node->add_shard(0, std::make_unique<Shard>());
    auto h = start_server(0, std::move(node));

    RemoteNode remote(0, "127.0.0.1", h.server->port());
    auto resp = remote.remove_document({0, 999});
    EXPECT_TRUE(resp.is_error);

    h.stop_and_join();
}

TEST(RemoteNodeTest, DocumentCount)
{
    auto node = std::make_unique<LocalNode>(0);
    node->add_shard(0, std::make_unique<Shard>());
    auto h = start_server(0, std::move(node));

    RemoteNode remote(0, "127.0.0.1", h.server->port());

    auto cnt0 = remote.document_count({0});
    EXPECT_EQ(cnt0.document_count, 0u);

    remote.add_document({0, 1, "first"});
    remote.add_document({0, 2, "second"});

    auto cnt2 = remote.document_count({0});
    EXPECT_EQ(cnt2.document_count, 2u);

    h.stop_and_join();
}

TEST(RemoteNodeTest, SearchFindsDocument)
{
    auto node = std::make_unique<LocalNode>(0);
    node->add_shard(0, std::make_unique<Shard>());
    auto h = start_server(0, std::move(node));

    RemoteNode remote(0, "127.0.0.1", h.server->port());
    remote.add_document({0, 1, "distributed search engine"});

    ShardSearchRequest req{0, {"distributed"}};
    auto resp = remote.search(req);

    EXPECT_FALSE(resp.is_error) << resp.error_message;
    EXPECT_EQ(resp.local_document_count, 1u);
    ASSERT_FALSE(resp.terms_postings.empty());
    EXPECT_FALSE(resp.terms_postings[0].empty());
    EXPECT_EQ(resp.terms_postings[0][0].document_id, 1u);

    h.stop_and_join();
}

// ===========================================================================
// Multiple shards on one remote node
// ===========================================================================

TEST(RemoteNodeTest, MultipleShardsOnOneNode)
{
    auto node = std::make_unique<LocalNode>(0);
    node->add_shard(0, std::make_unique<Shard>());
    node->add_shard(2, std::make_unique<Shard>());
    auto h = start_server(0, std::move(node));

    RemoteNode remote(0, "127.0.0.1", h.server->port());

    // Add to shard 0.
    auto resp0 = remote.add_document({0, 1, "shard zero"});
    EXPECT_FALSE(resp0.is_error) << resp0.error_message;

    // Add to shard 2.
    auto resp2 = remote.add_document({2, 2, "shard two"});
    EXPECT_FALSE(resp2.is_error) << resp2.error_message;

    // Verify counts per shard.
    EXPECT_EQ(remote.document_count({0}).document_count, 1u);
    EXPECT_EQ(remote.document_count({2}).document_count, 1u);

    // Unknown shard returns error.
    auto resp_bad = remote.add_document({1, 3, "no shard 1"});
    EXPECT_TRUE(resp_bad.is_error);

    h.stop_and_join();
}

// ===========================================================================
// Connection failure
// ===========================================================================

TEST(RemoteNodeTest, ConnectionFailureReturnsError)
{
    // Point at a port that is not listening.
    RemoteNode remote(0, "127.0.0.1", 1);

    ShardSearchRequest req{0, {"test"}};
    auto resp = remote.search(req);
    EXPECT_TRUE(resp.is_error);
    EXPECT_FALSE(resp.error_message.empty());
}

TEST(RemoteNodeTest, ConnectionFailureOnWrite)
{
    RemoteNode remote(0, "127.0.0.1", 1);

    auto resp = remote.add_document({0, 1, "hello"});
    EXPECT_TRUE(resp.is_error);
}

TEST(RemoteNodeTest, ConnectionFailureOnRemove)
{
    RemoteNode remote(0, "127.0.0.1", 1);

    auto resp = remote.remove_document({0, 1});
    EXPECT_TRUE(resp.is_error);
}

TEST(RemoteNodeTest, ConnectionFailureOnGet)
{
    RemoteNode remote(0, "127.0.0.1", 1);

    auto resp = remote.get_document({0, 1});
    EXPECT_FALSE(resp.found);
}

TEST(RemoteNodeTest, ConnectionFailureOnCount)
{
    RemoteNode remote(0, "127.0.0.1", 1);

    auto resp = remote.document_count({0});
    EXPECT_EQ(resp.document_count, 0u);
}

TEST(RemoteNodeTest, ConnectionFailureOnSave)
{
    RemoteNode remote(0, "127.0.0.1", 1);
    EXPECT_FALSE(remote.save_shard(0));
}

TEST(RemoteNodeTest, ConnectionFailureOnLoad)
{
    RemoteNode remote(0, "127.0.0.1", 1);
    EXPECT_FALSE(remote.load_shard(0));
}

// ===========================================================================
// Persistence through RemoteNode
// ===========================================================================

TEST(RemoteNodeTest, SaveAndLoadShard)
{
    const std::string path = "test_remote_node_persist.jsonl";

    auto node = std::make_unique<LocalNode>(0);
    node->add_shard(0, std::make_unique<Shard>(path));
    auto h = start_server(0, std::move(node));

    RemoteNode remote(0, "127.0.0.1", h.server->port());

    remote.add_document({0, 1, "persistent content"});

    EXPECT_TRUE(remote.save_shard(0));
    EXPECT_TRUE(remote.load_shard(0));

    // Data should still be accessible after load.
    auto cnt = remote.document_count({0});
    EXPECT_EQ(cnt.document_count, 1u);

    h.stop_and_join();
    std::remove(path.c_str());
}

// ===========================================================================
// Concurrent RemoteNode requests
// ===========================================================================

TEST(RemoteNodeTest, ConcurrentSearches)
{
    auto node = std::make_unique<LocalNode>(0);
    node->add_shard(0, std::make_unique<Shard>());
    auto h = start_server(0, std::move(node));

    RemoteNode remote(0, "127.0.0.1", h.server->port());

    // Seed some data.
    for (int i = 0; i < 10; ++i) {
        remote.add_document({0, static_cast<doc_id>(i + 100),
                             "document " + std::to_string(i)});
    }

    std::vector<std::thread> threads;
    for (int i = 0; i < 5; ++i) {
        threads.emplace_back([&remote]() {
            for (int j = 0; j < 20; ++j) {
                ShardSearchRequest req{0, {"document"}};
                auto resp = remote.search(req);
                EXPECT_FALSE(resp.is_error) << resp.error_message;
            }
        });
    }

    for (auto& t : threads) {
        t.join();
    }

    h.stop_and_join();
}

TEST(RemoteNodeTest, ConcurrentAdds)
{
    auto node = std::make_unique<LocalNode>(0);
    node->add_shard(0, std::make_unique<Shard>());
    auto h = start_server(0, std::move(node));

    RemoteNode remote(0, "127.0.0.1", h.server->port());

    std::vector<std::thread> threads;
    for (int i = 0; i < 10; ++i) {
        threads.emplace_back([&remote, i]() {
            ShardWriteRequest req{0, static_cast<doc_id>(i + 200),
                                  "concurrent doc " + std::to_string(i)};
            auto resp = remote.add_document(req);
            EXPECT_FALSE(resp.is_error) << resp.error_message;
        });
    }

    for (auto& t : threads) {
        t.join();
    }

    auto cnt = remote.document_count({0});
    EXPECT_EQ(cnt.document_count, 10u);

    h.stop_and_join();
}

TEST(RemoteNodeTest, ConcurrentMixedReadsAndWrites)
{
    auto node = std::make_unique<LocalNode>(0);
    node->add_shard(0, std::make_unique<Shard>());
    auto h = start_server(0, std::move(node));

    RemoteNode remote(0, "127.0.0.1", h.server->port());

    // Seed data.
    for (int i = 0; i < 5; ++i) {
        remote.add_document({0, static_cast<doc_id>(i),
                             "seed doc " + std::to_string(i)});
    }

    std::vector<std::thread> threads;

    // Readers.
    for (int i = 0; i < 3; ++i) {
        threads.emplace_back([&remote]() {
            for (int j = 0; j < 10; ++j) {
                ShardSearchRequest req{0, {"seed"}};
                auto resp = remote.search(req);
                EXPECT_FALSE(resp.is_error);
            }
        });
    }

    // Writers.
    for (int i = 0; i < 3; ++i) {
        threads.emplace_back([&remote, i]() {
            ShardWriteRequest req{0, static_cast<doc_id>(i + 100),
                                  "new doc " + std::to_string(i)};
            remote.add_document(req);
        });
    }

    // Counter.
    threads.emplace_back([&remote]() {
        for (int j = 0; j < 10; ++j) {
            remote.document_count({0});
        }
    });

    for (auto& t : threads) {
        t.join();
    }

    h.stop_and_join();
}

// ===========================================================================
// Behavioral equivalence: RemoteNode vs LocalNode
// ===========================================================================

class RemoteNodeEquivalenceTest : public ::testing::Test {
protected:
    void SetUp() override
    {
        auto node = std::make_unique<LocalNode>(0);
        node->add_shard(0, std::make_unique<Shard>());
        h_ = start_server(0, std::move(node));
        remote_ = std::make_unique<RemoteNode>(0, "127.0.0.1",
                                                h_.server->port());

        // Also create a LocalNode for direct comparison.
        local_ = std::make_unique<LocalNode>(0);
        local_->add_shard(0, std::make_unique<Shard>());
    }

    void TearDown() override
    {
        remote_.reset();
        local_.reset();
        h_.stop_and_join();
    }

    ServerHandle h_;
    std::unique_ptr<RemoteNode> remote_;
    std::unique_ptr<LocalNode> local_;
};

TEST_F(RemoteNodeEquivalenceTest, AddThenSearch)
{
    ShardWriteRequest wreq{0, 1, "the quick brown fox"};
    auto r1 = remote_->add_document(wreq);
    auto l1 = local_->add_document(wreq);

    EXPECT_EQ(r1.is_error, l1.is_error);
    EXPECT_EQ(r1.document_id, l1.document_id);
    EXPECT_EQ(r1.terms_indexed, l1.terms_indexed);

    ShardSearchRequest sreq{0, {"quick"}};
    auto rs = remote_->search(sreq);
    auto ls = local_->search(sreq);

    EXPECT_EQ(rs.is_error, ls.is_error);
    EXPECT_EQ(rs.local_document_count, ls.local_document_count);
    ASSERT_EQ(rs.terms_postings.size(), ls.terms_postings.size());
    if (!rs.terms_postings.empty()) {
        EXPECT_EQ(rs.terms_postings[0].size(),
                  ls.terms_postings[0].size());
    }
}

TEST_F(RemoteNodeEquivalenceTest, GetDocument)
{
    ShardWriteRequest wreq{0, 5, "content"};
    remote_->add_document(wreq);
    local_->add_document(wreq);

    auto rr = remote_->get_document({0, 5});
    auto lr = local_->get_document({0, 5});

    EXPECT_EQ(rr.found, lr.found);
    EXPECT_EQ(rr.content, lr.content);
}

TEST_F(RemoteNodeEquivalenceTest, DocumentCount)
{
    for (int i = 0; i < 5; ++i) {
        ShardWriteRequest req{0, static_cast<doc_id>(i), "doc"};
        remote_->add_document(req);
        local_->add_document(req);
    }

    auto rc = remote_->document_count({0});
    auto lc = local_->document_count({0});

    EXPECT_EQ(rc.document_count, lc.document_count);
}

TEST_F(RemoteNodeEquivalenceTest, RemoveThenSearch)
{
    ShardWriteRequest wreq{0, 1, "to be removed"};
    remote_->add_document(wreq);
    local_->add_document(wreq);

    auto rr = remote_->remove_document({0, 1});
    auto lr = local_->remove_document({0, 1});

    EXPECT_EQ(rr.is_error, lr.is_error);

    ShardSearchRequest sreq{0, {"removed"}};
    auto rs = remote_->search(sreq);
    auto ls = local_->search(sreq);

    EXPECT_EQ(rs.is_error, ls.is_error);
    EXPECT_EQ(rs.terms_postings.size(), ls.terms_postings.size());
}

TEST_F(RemoteNodeEquivalenceTest, UpdateThenSearch)
{
    remote_->add_document({0, 1, "old content"});
    local_->add_document({0, 1, "old content"});

    remote_->update_document({0, 1, "new content"});
    local_->update_document({0, 1, "new content"});

    ShardSearchRequest old_req{0, {"old"}};
    auto r_old = remote_->search(old_req);
    auto l_old = local_->search(old_req);
    EXPECT_EQ(r_old.terms_postings.size(), l_old.terms_postings.size());

    ShardSearchRequest new_req{0, {"new"}};
    auto r_new = remote_->search(new_req);
    auto l_new = local_->search(new_req);
    EXPECT_EQ(r_new.terms_postings.size(), l_new.terms_postings.size());
}
