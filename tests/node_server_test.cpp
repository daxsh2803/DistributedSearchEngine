// Distributed Search Engine - Node Server Tests (Phase 12B).
//
// Tests NodeServer by running it in a background thread and making
// real HTTP requests via httplib::Client. Verifies every endpoint,
// error handling, multiple shards, and concurrent requests.

#include <gtest/gtest.h>

#include <chrono>
#include <cstddef>
#include <memory>
#include <string>
#include <thread>
#include <vector>

#include <httplib.h>
#include <nlohmann/json.hpp>

#include "local_node.h"
#include "node_server.h"
#include "shard.h"

using namespace dse;

// ---------------------------------------------------------------------------
// Test fixture: creates a NodeServer with a single shard on a random port
// ---------------------------------------------------------------------------

class NodeServerTest : public ::testing::Test {
protected:
    void SetUp() override
    {
        auto node = std::make_unique<LocalNode>(0);
        node->add_shard(0, std::make_unique<Shard>());
        server_ = std::make_unique<NodeServer>(0, std::move(node));
        server_thread_ = std::thread([this]() {
            server_->listen(0);
        });
        server_->wait_until_ready();
        port_ = server_->port();
    }

    void TearDown() override
    {
        server_->stop();
        if (server_thread_.joinable()) {
            server_thread_.join();
        }
    }

    httplib::Client client() const
    {
        httplib::Client c("127.0.0.1", port_);
        return c;
    }

    std::unique_ptr<NodeServer> server_;
    int port_ = 0;
    std::thread server_thread_;
};

// ===========================================================================
// POST /node/add
// ===========================================================================

TEST_F(NodeServerTest, AddDocumentSuccess)
{
    auto c = client();
    nlohmann::json req;
    req["shard_id"] = 0;
    req["document_id"] = 1;
    req["content"] = "hello world";

    auto res = c.Post("/node/add", req.dump(), "application/json");
    ASSERT_NE(res, nullptr);
    EXPECT_EQ(res->status, 200);

    auto body = nlohmann::json::parse(res->body);
    EXPECT_EQ(body["shard_id"].get<std::size_t>(), 0u);
    EXPECT_EQ(body["document_id"].get<doc_id>(), 1u);
    EXPECT_GT(body["terms_indexed"].get<std::size_t>(), 0u);
    EXPECT_FALSE(body["is_error"].get<bool>());
}

TEST_F(NodeServerTest, AddDocumentDuplicate)
{
    auto c = client();
    nlohmann::json req;
    req["shard_id"] = 0;
    req["document_id"] = 1;
    req["content"] = "hello world";

    auto res1 = c.Post("/node/add", req.dump(), "application/json");
    ASSERT_NE(res1, nullptr);
    EXPECT_EQ(res1->status, 200);

    auto res2 = c.Post("/node/add", req.dump(), "application/json");
    ASSERT_NE(res2, nullptr);
    EXPECT_EQ(res2->status, 409);

    auto body = nlohmann::json::parse(res2->body);
    EXPECT_TRUE(body["is_error"].get<bool>());
}

TEST_F(NodeServerTest, AddDocumentInvalidJson)
{
    auto c = client();
    auto res = c.Post("/node/add", "not json", "application/json");
    ASSERT_NE(res, nullptr);
    EXPECT_EQ(res->status, 400);
}

// ===========================================================================
// POST /node/get
// ===========================================================================

TEST_F(NodeServerTest, GetDocumentFound)
{
    auto c = client();
    nlohmann::json add_req;
    add_req["shard_id"] = 0;
    add_req["document_id"] = 10;
    add_req["content"] = "the quick brown fox";
    c.Post("/node/add", add_req.dump(), "application/json");

    nlohmann::json get_req;
    get_req["shard_id"] = 0;
    get_req["document_id"] = 10;

    auto res = c.Post("/node/get", get_req.dump(), "application/json");
    ASSERT_NE(res, nullptr);
    EXPECT_EQ(res->status, 200);

    auto body = nlohmann::json::parse(res->body);
    EXPECT_TRUE(body["found"].get<bool>());
    EXPECT_EQ(body["content"].get<std::string>(), "the quick brown fox");
}

TEST_F(NodeServerTest, GetDocumentNotFound)
{
    auto c = client();
    nlohmann::json req;
    req["shard_id"] = 0;
    req["document_id"] = 999;

    auto res = c.Post("/node/get", req.dump(), "application/json");
    ASSERT_NE(res, nullptr);
    EXPECT_EQ(res->status, 200);

    auto body = nlohmann::json::parse(res->body);
    EXPECT_FALSE(body["found"].get<bool>());
}

// ===========================================================================
// POST /node/count
// ===========================================================================

TEST_F(NodeServerTest, CountEmpty)
{
    auto c = client();
    nlohmann::json req;
    req["shard_id"] = 0;

    auto res = c.Post("/node/count", req.dump(), "application/json");
    ASSERT_NE(res, nullptr);
    EXPECT_EQ(res->status, 200);

    auto body = nlohmann::json::parse(res->body);
    EXPECT_EQ(body["document_count"].get<std::size_t>(), 0u);
}

TEST_F(NodeServerTest, CountAfterAdd)
{
    auto c = client();
    nlohmann::json add_req;
    add_req["shard_id"] = 0;
    add_req["document_id"] = 1;
    add_req["content"] = "test";
    c.Post("/node/add", add_req.dump(), "application/json");

    nlohmann::json count_req;
    count_req["shard_id"] = 0;

    auto res = c.Post("/node/count", count_req.dump(), "application/json");
    ASSERT_NE(res, nullptr);

    auto body = nlohmann::json::parse(res->body);
    EXPECT_EQ(body["document_count"].get<std::size_t>(), 1u);
}

// ===========================================================================
// POST /node/search
// ===========================================================================

TEST_F(NodeServerTest, SearchFindsDocument)
{
    auto c = client();
    nlohmann::json add_req;
    add_req["shard_id"] = 0;
    add_req["document_id"] = 1;
    add_req["content"] = "distributed search engine";
    c.Post("/node/add", add_req.dump(), "application/json");

    nlohmann::json search_req;
    search_req["shard_id"] = 0;
    search_req["terms"] = {"distributed"};

    auto res = c.Post("/node/search", search_req.dump(), "application/json");
    ASSERT_NE(res, nullptr);
    EXPECT_EQ(res->status, 200);

    auto body = nlohmann::json::parse(res->body);
    EXPECT_FALSE(body["is_error"].get<bool>());
    EXPECT_EQ(body["local_document_count"].get<std::size_t>(), 1u);
    ASSERT_FALSE(body["terms_postings"].empty());
    EXPECT_FALSE(body["terms_postings"][0].empty());
    EXPECT_EQ(body["terms_postings"][0][0]["document_id"].get<doc_id>(), 1u);
}

TEST_F(NodeServerTest, SearchNoMatch)
{
    auto c = client();
    nlohmann::json search_req;
    search_req["shard_id"] = 0;
    search_req["terms"] = {"nonexistent"};

    auto res = c.Post("/node/search", search_req.dump(), "application/json");
    ASSERT_NE(res, nullptr);

    auto body = nlohmann::json::parse(res->body);
    EXPECT_FALSE(body["is_error"].get<bool>());
    EXPECT_TRUE(body["terms_postings"][0].empty());
}

// ===========================================================================
// POST /node/update
// ===========================================================================

TEST_F(NodeServerTest, UpdateDocumentSuccess)
{
    auto c = client();
    nlohmann::json add_req;
    add_req["shard_id"] = 0;
    add_req["document_id"] = 1;
    add_req["content"] = "old content";
    c.Post("/node/add", add_req.dump(), "application/json");

    nlohmann::json update_req;
    update_req["shard_id"] = 0;
    update_req["document_id"] = 1;
    update_req["content"] = "new content";

    auto res = c.Post("/node/update", update_req.dump(), "application/json");
    ASSERT_NE(res, nullptr);
    EXPECT_EQ(res->status, 200);

    // Verify new content is searchable.
    nlohmann::json search_req;
    search_req["shard_id"] = 0;
    search_req["terms"] = {"new"};
    auto search_res = c.Post("/node/search", search_req.dump(), "application/json");
    auto search_body = nlohmann::json::parse(search_res->body);
    EXPECT_FALSE(search_body["terms_postings"][0].empty());

    // Verify old content is gone.
    nlohmann::json search_req2;
    search_req2["shard_id"] = 0;
    search_req2["terms"] = {"old"};
    auto search_res2 = c.Post("/node/search", search_req2.dump(), "application/json");
    auto search_body2 = nlohmann::json::parse(search_res2->body);
    EXPECT_TRUE(search_body2["terms_postings"][0].empty());
}

TEST_F(NodeServerTest, UpdateDocumentNotFound)
{
    auto c = client();
    nlohmann::json req;
    req["shard_id"] = 0;
    req["document_id"] = 999;
    req["content"] = "content";

    auto res = c.Post("/node/update", req.dump(), "application/json");
    ASSERT_NE(res, nullptr);
    EXPECT_EQ(res->status, 404);
}

// ===========================================================================
// POST /node/remove
// ===========================================================================

TEST_F(NodeServerTest, RemoveDocumentSuccess)
{
    auto c = client();
    nlohmann::json add_req;
    add_req["shard_id"] = 0;
    add_req["document_id"] = 1;
    add_req["content"] = "to be removed";
    c.Post("/node/add", add_req.dump(), "application/json");

    nlohmann::json remove_req;
    remove_req["shard_id"] = 0;
    remove_req["document_id"] = 1;

    auto res = c.Post("/node/remove", remove_req.dump(), "application/json");
    ASSERT_NE(res, nullptr);
    EXPECT_EQ(res->status, 200);

    // Verify gone from search.
    nlohmann::json search_req;
    search_req["shard_id"] = 0;
    search_req["terms"] = {"removed"};
    auto search_res = c.Post("/node/search", search_req.dump(), "application/json");
    auto body = nlohmann::json::parse(search_res->body);
    EXPECT_TRUE(body["terms_postings"][0].empty());
}

TEST_F(NodeServerTest, RemoveDocumentNotFound)
{
    auto c = client();
    nlohmann::json req;
    req["shard_id"] = 0;
    req["document_id"] = 999;

    auto res = c.Post("/node/remove", req.dump(), "application/json");
    ASSERT_NE(res, nullptr);
    EXPECT_EQ(res->status, 404);
}

// ===========================================================================
// Multiple shards on one node
// ===========================================================================

class NodeServerMultiShardTest : public ::testing::Test {
protected:
    void SetUp() override
    {
        auto node = std::make_unique<LocalNode>(1);
        node->add_shard(0, std::make_unique<Shard>());
        node->add_shard(2, std::make_unique<Shard>());
        server_ = std::make_unique<NodeServer>(1, std::move(node));
        server_thread_ = std::thread([this]() {
            server_->listen(0);
        });
        server_->wait_until_ready();
        port_ = server_->port();
    }

    void TearDown() override
    {
        server_->stop();
        if (server_thread_.joinable()) {
            server_thread_.join();
        }
    }

    httplib::Client client() const
    {
        return httplib::Client("127.0.0.1", port_);
    }

    std::unique_ptr<NodeServer> server_;
    int port_ = 0;
    std::thread server_thread_;
};

TEST_F(NodeServerMultiShardTest, AddToDifferentShards)
{
    auto c = client();

    nlohmann::json req0;
    req0["shard_id"] = 0;
    req0["document_id"] = 1;
    req0["content"] = "shard zero document";
    auto res0 = c.Post("/node/add", req0.dump(), "application/json");
    EXPECT_EQ(res0->status, 200);

    nlohmann::json req2;
    req2["shard_id"] = 2;
    req2["document_id"] = 2;
    req2["content"] = "shard two document";
    auto res2 = c.Post("/node/add", req2.dump(), "application/json");
    EXPECT_EQ(res2->status, 200);

    // Count each shard.
    nlohmann::json count0;
    count0["shard_id"] = 0;
    auto cr0 = c.Post("/node/count", count0.dump(), "application/json");
    auto body0 = nlohmann::json::parse(cr0->body);
    EXPECT_EQ(body0["document_count"].get<std::size_t>(), 1u);

    nlohmann::json count2;
    count2["shard_id"] = 2;
    auto cr2 = c.Post("/node/count", count2.dump(), "application/json");
    auto body2 = nlohmann::json::parse(cr2->body);
    EXPECT_EQ(body2["document_count"].get<std::size_t>(), 1u);
}

TEST_F(NodeServerMultiShardTest, UnknownShardReturnsError)
{
    auto c = client();
    nlohmann::json req;
    req["shard_id"] = 1;  // not owned by this node
    req["document_id"] = 1;
    req["content"] = "test";

    auto res = c.Post("/node/add", req.dump(), "application/json");
    ASSERT_NE(res, nullptr);
    auto body = nlohmann::json::parse(res->body);
    EXPECT_TRUE(body["is_error"].get<bool>());
}

// ===========================================================================
// Persistence
// ===========================================================================

TEST(NodeServerPersistenceTest, SaveAndLoadShard)
{
    const std::string path = "test_node_server_persist.jsonl";

    // Create server with a persistence path.
    auto node = std::make_unique<LocalNode>(0);
    node->add_shard(0, std::make_unique<Shard>(path));
    NodeServer srv(0, std::move(node));
    std::thread srv_thread([&srv]() { srv.listen(0); });
    srv.wait_until_ready();
    int p = srv.port();

    httplib::Client c("127.0.0.1", p);

    // Add a document.
    nlohmann::json add_req;
    add_req["shard_id"] = 0;
    add_req["document_id"] = 1;
    add_req["content"] = "persistent content";
    c.Post("/node/add", add_req.dump(), "application/json");

    // Save.
    nlohmann::json save_req;
    save_req["shard_id"] = 0;
    auto save_res = c.Post("/node/save", save_req.dump(), "application/json");
    ASSERT_NE(save_res, nullptr);
    auto save_body = nlohmann::json::parse(save_res->body);
    EXPECT_TRUE(save_body["success"].get<bool>());

    // Load.
    nlohmann::json load_req;
    load_req["shard_id"] = 0;
    auto load_res = c.Post("/node/load", load_req.dump(), "application/json");
    ASSERT_NE(load_res, nullptr);
    auto load_body = nlohmann::json::parse(load_res->body);
    EXPECT_TRUE(load_body["success"].get<bool>());

    srv.stop();
    srv_thread.join();
    std::remove(path.c_str());
}

// ===========================================================================
// Concurrent requests
// ===========================================================================

TEST_F(NodeServerTest, ConcurrentAdds)
{
    std::vector<std::thread> threads;

    for (int i = 0; i < 10; ++i) {
        threads.emplace_back([this, i]() {
            httplib::Client cl("127.0.0.1", port_);
            nlohmann::json req;
            req["shard_id"] = 0;
            req["document_id"] = static_cast<doc_id>(i + 100);
            req["content"] = "concurrent document " + std::to_string(i);
            auto res = cl.Post("/node/add", req.dump(), "application/json");
            ASSERT_NE(res, nullptr);
            EXPECT_EQ(res->status, 200);
        });
    }

    for (auto& t : threads) {
        t.join();
    }

    // Verify all 10 documents exist.
    auto c = client();
    nlohmann::json count_req;
    count_req["shard_id"] = 0;
    auto res = c.Post("/node/count", count_req.dump(), "application/json");
    auto body = nlohmann::json::parse(res->body);
    EXPECT_EQ(body["document_count"].get<std::size_t>(), 10u);
}

TEST_F(NodeServerTest, ConcurrentSearchAndAdd)
{
    // Add some initial documents.
    auto c = client();
    for (int i = 0; i < 5; ++i) {
        nlohmann::json req;
        req["shard_id"] = 0;
        req["document_id"] = static_cast<doc_id>(i);
        req["content"] = "document " + std::to_string(i);
        c.Post("/node/add", req.dump(), "application/json");
    }

    std::vector<std::thread> threads;

    // Readers.
    for (int i = 0; i < 5; ++i) {
        threads.emplace_back([this]() {
            httplib::Client cl("127.0.0.1", port_);
            for (int j = 0; j < 5; ++j) {
                nlohmann::json req;
                req["shard_id"] = 0;
                req["terms"] = {"document"};
                auto res = cl.Post("/node/search", req.dump(), "application/json");
                ASSERT_NE(res, nullptr);
                EXPECT_EQ(res->status, 200);
            }
        });
    }

    // Writers.
    for (int i = 5; i < 10; ++i) {
        threads.emplace_back([this, i]() {
            httplib::Client cl("127.0.0.1", port_);
            nlohmann::json req;
            req["shard_id"] = 0;
            req["document_id"] = static_cast<doc_id>(i + 100);
            req["content"] = "new document " + std::to_string(i);
            auto res = cl.Post("/node/add", req.dump(), "application/json");
            ASSERT_NE(res, nullptr);
        });
    }

    for (auto& t : threads) {
        t.join();
    }

    // Final count should be >= 5 (initial) and <= 10 (some adds may have
    // been rejected as duplicates if IDs collide, but IDs are unique here).
    nlohmann::json count_req;
    count_req["shard_id"] = 0;
    auto res = c.Post("/node/count", count_req.dump(), "application/json");
    auto body = nlohmann::json::parse(res->body);
    EXPECT_GE(body["document_count"].get<std::size_t>(), 5u);
}

// ===========================================================================
// Node identity
// ===========================================================================

TEST_F(NodeServerTest, NodeIdIsCorrect)
{
    EXPECT_EQ(server_->node_id(), 0u);
}

// ===========================================================================
// HTTP status codes
// ===========================================================================

TEST_F(NodeServerTest, MalformedJsonReturns400)
{
    auto c = client();
    auto res = c.Post("/node/add", "{bad json}", "application/json");
    ASSERT_NE(res, nullptr);
    EXPECT_EQ(res->status, 400);
}

TEST_F(NodeServerTest, MissingFieldReturns400)
{
    auto c = client();
    nlohmann::json req;
    req["shard_id"] = 0;
    // Missing document_id and content.
    auto res = c.Post("/node/add", req.dump(), "application/json");
    ASSERT_NE(res, nullptr);
    EXPECT_EQ(res->status, 400);
}
