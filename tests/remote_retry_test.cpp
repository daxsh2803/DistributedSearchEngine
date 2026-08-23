// Distributed Search Engine - Remote Retry Tests (Phase 14).
//
// Integration tests for retry behavior with RemoteNode.
// Verifies transient failure retry, retry exhaustion, and bounded retry counts.

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
#include "retry_policy.h"
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
// Retry policy integration with RemoteNode
// ===========================================================================

TEST(RemoteRetryTest, DefaultRemoteNodeUsesNoRetries)
{
    // RemoteNode defaults to no retries — backward compatible.
    RemoteNode node(0, "127.0.0.1", 59999);

    ShardCountRequest req;
    req.shard_id = 0;
    auto resp = node.document_count(req);

    // Should fail immediately without retry.
    EXPECT_TRUE(resp.is_error);
}

TEST(RemoteRetryTest, RetryPolicyRespected)
{
    // Create a node with explicit retry policy (2 attempts).
    RetryPolicy policy;
    policy.max_attempts = 2;
    policy.initial_delay_ms = 10;  // Short delay for testing

    RemoteNode node(0, "127.0.0.1", 59999, 5, policy);

    ShardCountRequest req;
    req.shard_id = 0;

    auto start = std::chrono::steady_clock::now();
    auto resp = node.document_count(req);
    auto elapsed = std::chrono::steady_clock::now() - start;

    // Should fail after 2 attempts.
    EXPECT_TRUE(resp.is_error);

    // Should have waited at least once (initial delay).
    EXPECT_GE(elapsed, std::chrono::milliseconds(10));
}

TEST(RemoteRetryTest, RetryExhaustionReturnsError)
{
    // Configure 3 attempts to an unavailable node.
    RetryPolicy policy;
    policy.max_attempts = 3;
    policy.initial_delay_ms = 10;

    RemoteNode node(0, "127.0.0.1", 59999, 5, policy);

    ShardSearchRequest req;
    req.shard_id = 0;
    req.terms = {"test"};

    auto resp = node.search(req);

    // After exhausting retries, should return error.
    EXPECT_TRUE(resp.is_error);
    EXPECT_FALSE(resp.error_message.empty());
}

TEST(RemoteRetryTest, SuccessfulRequestWithRealNode)
{
    // Start a real server and verify successful operations.
    auto local_node = std::make_unique<LocalNode>(0);
    local_node->add_shard(0, std::make_unique<Shard>());
    auto h = start_server(0, std::move(local_node));

    RetryPolicy policy;
    policy.max_attempts = 1;  // No retries needed for successful request

    RemoteNode node(0, "127.0.0.1", h.server->port(), 5, policy);

    // Document count should succeed.
    ShardCountRequest count_req;
    count_req.shard_id = 0;
    auto count_resp = node.document_count(count_req);
    EXPECT_FALSE(count_resp.is_error);
    EXPECT_EQ(count_resp.document_count, 0u);

    // Search for non-existent term should succeed (empty results).
    ShardSearchRequest search_req;
    search_req.shard_id = 0;
    search_req.terms = {"nonexistent"};
    auto search_resp = node.search(search_req);
    EXPECT_FALSE(search_resp.is_error);

    h.stop_and_join();
}

TEST(RemoteRetryTest, ApplicationErrorNotRetried)
{
    // Start a server and try to get a non-existent document.
    auto local_node = std::make_unique<LocalNode>(0);
    local_node->add_shard(0, std::make_unique<Shard>());
    auto h = start_server(0, std::move(local_node));

    RetryPolicy policy;
    policy.max_attempts = 3;
    policy.initial_delay_ms = 100;

    RemoteNode node(0, "127.0.0.1", h.server->port(), 5, policy);

    // Get a non-existent document — should return found=false.
    ShardGetRequest req;
    req.shard_id = 0;
    req.document_id = 999;

    auto start = std::chrono::steady_clock::now();
    auto resp = node.get_document(req);
    auto elapsed = std::chrono::steady_clock::now() - start;

    // Should return found=false without retrying (application-level).
    EXPECT_FALSE(resp.found);
    EXPECT_LT(elapsed, std::chrono::milliseconds(50));

    h.stop_and_join();
}

// ===========================================================================
// Coordinator integration with retry policy
// ===========================================================================

TEST(RemoteRetryTest, CoordinatorSearchWithRetryPolicy)
{
    // Verify that the coordinator works correctly when RemoteNode
    // has a retry policy configured.
    std::vector<ServerHandle> handles;
    for (std::size_t i = 0; i < 2; ++i) {
        auto node = std::make_unique<LocalNode>(i);
        node->add_shard(i, std::make_unique<Shard>());
        handles.push_back(start_server(i, std::move(node)));
    }

    auto router = std::make_unique<ShardRouter>(2);
    std::vector<std::size_t> placement = {0, 1};
    auto sp = std::make_unique<ShardPlacement>(2, 2, placement);

    RetryPolicy policy;
    policy.max_attempts = 2;
    policy.initial_delay_ms = 10;

    std::vector<std::unique_ptr<NodeClient>> nodes;
    for (std::size_t i = 0; i < 2; ++i) {
        nodes.push_back(std::make_unique<RemoteNode>(
            i, "127.0.0.1", handles[i].server->port(), 5, policy));
    }

    ShardCoordinator coord(std::move(router), std::move(sp), std::move(nodes));

    // Ingest documents.
    for (doc_id i = 1; i <= 10; ++i) {
        coord.ingest({i, "unique_" + std::to_string(i)});
    }

    auto r = coord.search({"unique", SearchMode::Or, 10});
    EXPECT_TRUE(r.complete);
    EXPECT_EQ(r.total, 10u);

    for (auto& h : handles) h.stop_and_join();
}

// ===========================================================================
// Search degradation after retry exhaustion
// ===========================================================================

TEST(RemoteRetryTest, SearchDegradationAfterRetryExhaustion)
{
    // Start 2 nodes, stop one, verify search returns partial results
    // with complete=false after retries are exhausted.
    std::vector<ServerHandle> handles;
    for (std::size_t i = 0; i < 2; ++i) {
        auto node = std::make_unique<LocalNode>(i);
        node->add_shard(i, std::make_unique<Shard>());
        handles.push_back(start_server(i, std::move(node)));
    }

    auto router = std::make_unique<ShardRouter>(2);
    std::vector<std::size_t> placement = {0, 1};
    auto sp = std::make_unique<ShardPlacement>(2, 2, placement);

    RetryPolicy policy;
    policy.max_attempts = 2;
    policy.initial_delay_ms = 10;

    std::vector<std::unique_ptr<NodeClient>> nodes;
    for (std::size_t i = 0; i < 2; ++i) {
        nodes.push_back(std::make_unique<RemoteNode>(
            i, "127.0.0.1", handles[i].server->port(), 5, policy));
    }

    ShardCoordinator coord(std::move(router), std::move(sp), std::move(nodes));

    // Ingest documents.
    for (doc_id i = 1; i <= 20; ++i) {
        coord.ingest({i, "unique_" + std::to_string(i)});
    }

    // Stop one node.
    handles[1].stop_and_join();

    // Search — should return partial results after retry exhaustion.
    auto r = coord.search({"unique", SearchMode::Or, 100});
    EXPECT_FALSE(r.complete);
    EXPECT_FALSE(r.errors.empty());
    EXPECT_GT(r.total, 0u);

    for (auto& h : handles) h.stop_and_join();
}

// ===========================================================================
// Concurrent retrying requests
// ===========================================================================

TEST(RemoteRetryTest, ConcurrentRetryRequests)
{
    auto local_node = std::make_unique<LocalNode>(0);
    local_node->add_shard(0, std::make_unique<Shard>());
    auto h = start_server(0, std::move(local_node));

    RetryPolicy policy;
    policy.max_attempts = 1;

    RemoteNode node(0, "127.0.0.1", h.server->port(), 5, policy);

    // Run multiple concurrent searches.
    std::vector<std::thread> threads;
    for (int i = 0; i < 5; ++i) {
        threads.emplace_back([&node, i]() {
            ShardSearchRequest req;
            req.shard_id = 0;
            req.terms = {"test_" + std::to_string(i)};
            auto resp = node.search(req);
            // Response may be empty (no matching docs), but should not error.
            EXPECT_FALSE(resp.is_error);
        });
    }

    for (auto& t : threads) {
        t.join();
    }

    h.stop_and_join();
}
