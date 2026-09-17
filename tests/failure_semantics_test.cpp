// Distributed Search Engine - Failure Semantics Tests (Phase 13).
//
// Tests that verify the coordinator correctly reports degradation metadata
// when nodes/shards are unavailable.

#include <gtest/gtest.h>

#include <chrono>
#include <cstddef>
#include <memory>
#include <string>
#include <thread>
#include <vector>

#include "local_node.h"
#include "node_client.h"
#include "node_config.h"
#include "node_server.h"
#include "remote_node.h"
#include "shard.h"
#include "shard_coordinator.h"
#include "shard_router.h"

using namespace dse;

// ---------------------------------------------------------------------------
// Helper: start a NodeServer in a background thread
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
// All shards available: complete = true
// ===========================================================================

TEST(FailureSemanticsTest, AllShardsAvailableCompleteIsTrue)
{
    std::vector<ServerHandle> handles;
    for (std::size_t i = 0; i < 3; ++i) {
        auto node = std::make_unique<LocalNode>(i);
        node->add_shard(i, std::make_unique<Shard>());
        handles.push_back(start_server(i, std::move(node)));
    }

    auto router = std::make_unique<ShardRouter>(3);
    std::vector<std::size_t> placement = {0, 1, 2};
    auto sp = std::make_unique<ShardPlacement>(3, 3, placement);

    std::vector<std::unique_ptr<NodeClient>> nodes;
    for (std::size_t i = 0; i < 3; ++i) {
        nodes.push_back(std::make_unique<RemoteNode>(
            i, "127.0.0.1", handles[i].server->port()));
    }

    ShardCoordinator coord(std::move(router), std::move(sp), std::move(nodes));

    coord.ingest({1, "quick brown fox"});
    coord.ingest({2, "lazy dog"});
    coord.ingest({3, "fast cat"});

    auto r = coord.search({"quick", SearchMode::Or, 10});
    EXPECT_TRUE(r.complete);
    EXPECT_TRUE(r.errors.empty());
    EXPECT_EQ(r.total, 1u);

    for (auto& h : handles) h.stop_and_join();
}

// ===========================================================================
// One node unavailable: complete = false, partial results
// ===========================================================================

TEST(FailureSemanticsTest, OneUnavailableNodeSetsCompleteFalse)
{
    std::vector<ServerHandle> handles;
    for (std::size_t i = 0; i < 3; ++i) {
        auto node = std::make_unique<LocalNode>(i);
        node->add_shard(i, std::make_unique<Shard>());
        handles.push_back(start_server(i, std::move(node)));
    }

    auto router = std::make_unique<ShardRouter>(3);
    std::vector<std::size_t> placement = {0, 1, 2};
    auto sp = std::make_unique<ShardPlacement>(3, 3, placement);

    std::vector<std::unique_ptr<NodeClient>> nodes;
    for (std::size_t i = 0; i < 3; ++i) {
        nodes.push_back(std::make_unique<RemoteNode>(
            i, "127.0.0.1", handles[i].server->port()));
    }

    ShardCoordinator coord(std::move(router), std::move(sp), std::move(nodes));

    // Ingest many documents so all shards have data.
    for (doc_id i = 1; i <= 30; ++i) {
        coord.ingest({i, "common unique_" + std::to_string(i)});
    }

    // Stop node 2 before searching.
    handles[2].stop_and_join();

    auto r = coord.search({"common", SearchMode::Or, 100});
    EXPECT_FALSE(r.complete);
    EXPECT_FALSE(r.errors.empty());

    // The error should reference the failed shard/node.
    bool found = false;
    for (const auto& e : r.errors) {
        if (e.shard_id == 2) {
            found = true;
            EXPECT_FALSE(e.message.empty());
        }
    }
    EXPECT_TRUE(found) << "Expected error for shard 2";

    // We should still get some results from the available shards.
    EXPECT_GT(r.total, 0u);

    for (auto& h : handles) h.stop_and_join();
}

// ===========================================================================
// Multiple nodes unavailable
// ===========================================================================

TEST(FailureSemanticsTest, MultipleUnavailableNodes)
{
    std::vector<ServerHandle> handles;
    for (std::size_t i = 0; i < 3; ++i) {
        auto node = std::make_unique<LocalNode>(i);
        node->add_shard(i, std::make_unique<Shard>());
        handles.push_back(start_server(i, std::move(node)));
    }

    auto router = std::make_unique<ShardRouter>(3);
    std::vector<std::size_t> placement = {0, 1, 2};
    auto sp = std::make_unique<ShardPlacement>(3, 3, placement);

    std::vector<std::unique_ptr<NodeClient>> nodes;
    for (std::size_t i = 0; i < 3; ++i) {
        nodes.push_back(std::make_unique<RemoteNode>(
            i, "127.0.0.1", handles[i].server->port()));
    }

    ShardCoordinator coord(std::move(router), std::move(sp), std::move(nodes));

    for (doc_id i = 1; i <= 30; ++i) {
        coord.ingest({i, "test document_" + std::to_string(i)});
    }

    // Stop nodes 1 and 2.
    handles[1].stop_and_join();
    handles[2].stop_and_join();

    auto r = coord.search({"test", SearchMode::Or, 100});
    EXPECT_FALSE(r.complete);
    EXPECT_GE(r.errors.size(), 2u);

    // Should still get results from node 0.
    EXPECT_GT(r.total, 0u);

    for (auto& h : handles) h.stop_and_join();
}

// ===========================================================================
// Global TF-IDF under incomplete statistics
// ===========================================================================

TEST(FailureSemanticsTest, GlobalTfIdfWithMissingShard)
{
    std::vector<ServerHandle> handles;
    for (std::size_t i = 0; i < 3; ++i) {
        auto node = std::make_unique<LocalNode>(i);
        node->add_shard(i, std::make_unique<Shard>());
        handles.push_back(start_server(i, std::move(node)));
    }

    auto router = std::make_unique<ShardRouter>(3);
    std::vector<std::size_t> placement = {0, 1, 2};
    auto sp = std::make_unique<ShardPlacement>(3, 3, placement);

    std::vector<std::unique_ptr<NodeClient>> nodes;
    for (std::size_t i = 0; i < 3; ++i) {
        nodes.push_back(std::make_unique<RemoteNode>(
            i, "127.0.0.1", handles[i].server->port()));
    }

    ShardCoordinator coord(std::move(router), std::move(sp), std::move(nodes));

    // Ingest docs so every shard has something.
    for (doc_id i = 1; i <= 30; ++i) {
        coord.ingest({i, "unique_" + std::to_string(i)});
    }

    auto count_before = coord.total_document_count();

    // Stop node 2 — its documents are missing from global stats.
    handles[2].stop_and_join();

    auto r = coord.search({"unique", SearchMode::Or, 100});
    EXPECT_FALSE(r.complete);
    EXPECT_FALSE(r.errors.empty());

    // We should get fewer results than before.
    EXPECT_LT(r.total, count_before);

    // But still some results from the available shards.
    EXPECT_GT(r.total, 0u);

    // Ranking should still be deterministic.
    for (std::size_t i = 1; i < r.results.size(); ++i) {
        if (r.results[i - 1].score == r.results[i].score) {
            EXPECT_LT(r.results[i - 1].document_id, r.results[i].document_id);
        }
    }

    for (auto& h : handles) h.stop_and_join();
}

// ===========================================================================
// Successful write propagation
// ===========================================================================

TEST(FailureSemanticsTest, WriteToUnavailableNodeReturnsError)
{
    auto router = std::make_unique<ShardRouter>(1);
    std::vector<std::size_t> placement = {0};
    auto sp = std::make_unique<ShardPlacement>(1, 1, placement);

    std::vector<std::unique_ptr<NodeClient>> nodes;
    // Point to a port that nothing is listening on.
    nodes.push_back(std::make_unique<RemoteNode>(0, "127.0.0.1", 1));

    ShardCoordinator coord(std::move(router), std::move(sp), std::move(nodes));

    auto ingest = coord.ingest({1, "test content"});
    EXPECT_TRUE(ingest.is_error);
    EXPECT_FALSE(ingest.error_message.empty());
}

// ===========================================================================
// Update failure propagation
// ===========================================================================

TEST(FailureSemanticsTest, UpdateToUnavailableNodeReturnsError)
{
    auto router = std::make_unique<ShardRouter>(1);
    std::vector<std::size_t> placement = {0};
    auto sp = std::make_unique<ShardPlacement>(1, 1, placement);

    std::vector<std::unique_ptr<NodeClient>> nodes;
    nodes.push_back(std::make_unique<RemoteNode>(0, "127.0.0.1", 1));

    ShardCoordinator coord(std::move(router), std::move(sp), std::move(nodes));

    auto update = coord.update({1, "new content"});
    EXPECT_TRUE(update.is_error);
    EXPECT_FALSE(update.error_message.empty());
}

// ===========================================================================
// Delete failure propagation
// ===========================================================================

TEST(FailureSemanticsTest, DeleteToUnavailableNodeReturnsError)
{
    auto router = std::make_unique<ShardRouter>(1);
    std::vector<std::size_t> placement = {0};
    auto sp = std::make_unique<ShardPlacement>(1, 1, placement);

    std::vector<std::unique_ptr<NodeClient>> nodes;
    nodes.push_back(std::make_unique<RemoteNode>(0, "127.0.0.1", 1));

    ShardCoordinator coord(std::move(router), std::move(sp), std::move(nodes));

    auto del = coord.remove(1);
    EXPECT_TRUE(del.is_error);
    EXPECT_FALSE(del.error_message.empty());
}

// ===========================================================================
// Node recovery: failed request, then node becomes available
// ===========================================================================

TEST(FailureSemanticsTest, NodeRecoveryAfterFailure)
{
    std::string persist_path = "test_recovery.jsonl";
    std::size_t node_id = 0;

    std::unique_ptr<ServerHandle> handle;
    {
        auto node = std::make_unique<LocalNode>(node_id);
        node->add_shard(0, std::make_unique<Shard>(persist_path));
        handle = std::make_unique<ServerHandle>(
            start_server(node_id, std::move(node)));
    }

    auto router = std::make_unique<ShardRouter>(1);
    std::vector<std::size_t> placement = {0};
    auto sp = std::make_unique<ShardPlacement>(1, 1, placement);

    std::vector<std::unique_ptr<NodeClient>> nodes;
    nodes.push_back(std::make_unique<RemoteNode>(
        node_id, "127.0.0.1", handle->server->port()));

    {
        ShardCoordinator coord(std::move(router), std::move(sp),
                               std::move(nodes));

        coord.ingest({1, "recovery test"});
        coord.save_all();
    }

    // Stop the server.
    handle->stop_and_join();
    handle.reset();

    // Create new server on same persistence.
    {
        auto node = std::make_unique<LocalNode>(node_id);
        node->add_shard(0, std::make_unique<Shard>(persist_path));
        handle = std::make_unique<ServerHandle>(
            start_server(node_id, std::move(node)));
    }

    // Verify recovery.
    {
        auto router2 = std::make_unique<ShardRouter>(1);
        std::vector<std::size_t> placement2 = {0};
        auto sp2 = std::make_unique<ShardPlacement>(1, 1, placement2);

        std::vector<std::unique_ptr<NodeClient>> nodes2;
        nodes2.push_back(std::make_unique<RemoteNode>(
            node_id, "127.0.0.1", handle->server->port()));

        ShardCoordinator coord2(std::move(router2), std::move(sp2),
                                std::move(nodes2));
        coord2.load_all();

        auto r = coord2.search({"recovery", SearchMode::Or, 10});
        EXPECT_TRUE(r.complete);
        EXPECT_EQ(r.total, 1u);
    }

    handle->stop_and_join();
    std::remove(persist_path.c_str());
}

// ===========================================================================
// AND search with one unavailable shard returns degraded results
// ===========================================================================

TEST(FailureSemanticsTest, AndSearchWithUnavailableShard)
{
    std::vector<ServerHandle> handles;
    for (std::size_t i = 0; i < 3; ++i) {
        auto node = std::make_unique<LocalNode>(i);
        node->add_shard(i, std::make_unique<Shard>());
        handles.push_back(start_server(i, std::move(node)));
    }

    auto router = std::make_unique<ShardRouter>(3);
    std::vector<std::size_t> placement = {0, 1, 2};
    auto sp = std::make_unique<ShardPlacement>(3, 3, placement);

    std::vector<std::unique_ptr<NodeClient>> nodes;
    for (std::size_t i = 0; i < 3; ++i) {
        nodes.push_back(std::make_unique<RemoteNode>(
            i, "127.0.0.1", handles[i].server->port()));
    }

    ShardCoordinator coord(std::move(router), std::move(sp), std::move(nodes));

    for (doc_id i = 1; i <= 30; ++i) {
        coord.ingest({i, "alpha beta_" + std::to_string(i)});
    }

    // Stop shard 1.
    handles[1].stop_and_join();

    // AND: "alpha" — only some shards contribute.
    auto r = coord.search({"alpha", SearchMode::And, 100});
    EXPECT_FALSE(r.complete);
    EXPECT_FALSE(r.errors.empty());

    // Still get some results.
    EXPECT_GT(r.total, 0u);

    for (auto& h : handles) h.stop_and_join();
}

// ===========================================================================
// Error message contains useful diagnostic info
// ===========================================================================

TEST(FailureSemanticsTest, ErrorMessageContainsUsefulInfo)
{
    auto router = std::make_unique<ShardRouter>(1);
    std::vector<std::size_t> placement = {0};
    auto sp = std::make_unique<ShardPlacement>(1, 1, placement);

    std::vector<std::unique_ptr<NodeClient>> nodes;
    // Use a port that's almost certainly not in use.
    nodes.push_back(std::make_unique<RemoteNode>(0, "127.0.0.1", 59999));

    ShardCoordinator coord(std::move(router), std::move(sp), std::move(nodes));

    auto r = coord.search({"test", SearchMode::Or, 10});
    EXPECT_FALSE(r.complete);
    EXPECT_FALSE(r.errors.empty());

    // The error message should contain the node_id and some diagnostic info.
    const auto& err = r.errors[0];
    EXPECT_EQ(err.node_id, 0u);
    EXPECT_EQ(err.shard_id, 0u);
    EXPECT_FALSE(err.category.empty());
    EXPECT_FALSE(err.message.empty());

    // For writes, the error propagates as is_error.
    auto ingest = coord.ingest({1, "test"});
    EXPECT_TRUE(ingest.is_error);
    EXPECT_FALSE(ingest.error_message.empty());
}

// ===========================================================================
// Empty query still returns valid response
// ===========================================================================

TEST(FailureSemanticsTest, EmptyQueryReturnsValidResponse)
{
    std::vector<ServerHandle> handles;
    for (std::size_t i = 0; i < 2; ++i) {
        auto node = std::make_unique<LocalNode>(i);
        node->add_shard(i, std::make_unique<Shard>());
        handles.push_back(start_server(i, std::move(node)));
    }

    auto router = std::make_unique<ShardRouter>(2);
    std::vector<std::size_t> placement = {0, 1};
    auto sp = std::make_unique<ShardPlacement>(2, 2, placement);

    std::vector<std::unique_ptr<NodeClient>> nodes;
    for (std::size_t i = 0; i < 2; ++i) {
        nodes.push_back(std::make_unique<RemoteNode>(
            i, "127.0.0.1", handles[i].server->port()));
    }

    ShardCoordinator coord(std::move(router), std::move(sp), std::move(nodes));

    // Empty query should be rejected as invalid request.
    auto r = coord.search({"", SearchMode::Or, 10});
    EXPECT_TRUE(r.is_error);

    for (auto& h : handles) h.stop_and_join();
}

// ===========================================================================
// OR search collects from all available shards
// ===========================================================================

TEST(FailureSemanticsTest, OrSearchCollectsFromAvailableShards)
{
    std::vector<ServerHandle> handles;
    for (std::size_t i = 0; i < 2; ++i) {
        auto node = std::make_unique<LocalNode>(i);
        node->add_shard(i, std::make_unique<Shard>());
        handles.push_back(start_server(i, std::move(node)));
    }

    auto router = std::make_unique<ShardRouter>(2);
    std::vector<std::size_t> placement = {0, 1};
    auto sp = std::make_unique<ShardPlacement>(2, 2, placement);

    std::vector<std::unique_ptr<NodeClient>> nodes;
    for (std::size_t i = 0; i < 2; ++i) {
        nodes.push_back(std::make_unique<RemoteNode>(
            i, "127.0.0.1", handles[i].server->port()));
    }

    ShardCoordinator coord(std::move(router), std::move(sp), std::move(nodes));

    for (doc_id i = 1; i <= 20; ++i) {
        coord.ingest({i, "target_" + std::to_string(i)});
    }

    // Stop one node.
    handles[1].stop_and_join();

    auto r = coord.search({"target", SearchMode::Or, 100});
    EXPECT_FALSE(r.complete);
    EXPECT_GT(r.total, 0u);

    for (auto& h : handles) h.stop_and_join();
}

// ===========================================================================
// Get document on unavailable node returns nothing (not error)
// ===========================================================================

TEST(FailureSemanticsTest, GetDocumentOnUnavailableNodeReturnsNothing)
{
    auto router = std::make_unique<ShardRouter>(1);
    std::vector<std::size_t> placement = {0};
    auto sp = std::make_unique<ShardPlacement>(1, 1, placement);

    std::vector<std::unique_ptr<NodeClient>> nodes;
    nodes.push_back(std::make_unique<RemoteNode>(0, "127.0.0.1", 59999));

    ShardCoordinator coord(std::move(router), std::move(sp), std::move(nodes));

    auto doc = coord.get_document(1);
    EXPECT_FALSE(doc.has_value());
}
