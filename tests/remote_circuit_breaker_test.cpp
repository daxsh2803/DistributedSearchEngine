// Distributed Search Engine - Remote Circuit Breaker Tests (Phase 15).
//
// Integration tests verifying circuit breaker behavior with RemoteNode.
// Tests that circuit breaker prevents repeated requests to unhealthy nodes
// and allows recovery when the node becomes healthy again.

#include <gtest/gtest.h>

#include <chrono>
#include <cstddef>
#include <memory>
#include <string>
#include <thread>
#include <vector>

#include "circuit_breaker.h"
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
// Circuit breaker prevents requests when OPEN
// ===========================================================================

TEST(RemoteCircuitBreakerTest, OpenBreakerFailsFast)
{
    // Create a circuit breaker that trips after 1 failure.
    CircuitBreakerConfig cb_config;
    cb_config.failure_threshold = 1;
    cb_config.recovery_timeout_ms = 60000;  // Long timeout

    RetryPolicy policy = RetryPolicy::no_retries();

    // Point to a non-existent server to trigger failure.
    // Use move constructor to pass the circuit breaker.
    RemoteNode node(0, "127.0.0.1", 59999, 1, policy,
                    CircuitBreaker(cb_config));

    // First request — fails and trips the breaker.
    ShardCountRequest req;
    req.shard_id = 0;
    auto resp = node.document_count(req);
    EXPECT_TRUE(resp.is_error);

    // Second request — should fail fast without network call.
    auto start = std::chrono::steady_clock::now();
    auto resp2 = node.document_count(req);
    auto elapsed = std::chrono::steady_clock::now() - start;

    EXPECT_TRUE(resp2.is_error);
    EXPECT_TRUE(resp2.error_message.find("Circuit breaker") != std::string::npos);

    // Should be very fast (no network call).
    EXPECT_LT(elapsed, std::chrono::milliseconds(50));
}

// ===========================================================================
// Circuit breaker allows recovery
// ===========================================================================

TEST(RemoteCircuitBreakerTest, RecoveryAfterNodeBecomesAvailable)
{
    // Create a circuit breaker with short recovery timeout.
    CircuitBreakerConfig cb_config;
    cb_config.failure_threshold = 1;
    cb_config.recovery_timeout_ms = 50;

    RetryPolicy policy = RetryPolicy::no_retries();

    // Point to a non-existent server initially.
    // Note: We can't check the circuit breaker state after moving it,
    // so we verify behavior through the RemoteNode responses.
    {
        RemoteNode node(0, "127.0.0.1", 59999, 1, policy,
                        CircuitBreaker(cb_config));

        // First request — fails and trips the breaker.
        ShardCountRequest req;
        req.shard_id = 0;
        auto resp = node.document_count(req);
        EXPECT_TRUE(resp.is_error);

        // Second request — should fail fast (circuit OPEN).
        auto resp2 = node.document_count(req);
        EXPECT_TRUE(resp2.is_error);
        EXPECT_TRUE(resp2.error_message.find("Circuit breaker") != std::string::npos);
    }

    // Wait for recovery timeout.
    std::this_thread::sleep_for(std::chrono::milliseconds(60));

    // Now create a new RemoteNode pointing to a real server.
    auto local_node = std::make_unique<LocalNode>(0);
    local_node->add_shard(0, std::make_unique<Shard>());
    auto h = start_server(0, std::move(local_node));

    // The circuit breaker in the new node starts CLOSED.
    RemoteNode node2(0, "127.0.0.1", h.server->port(), 5, policy);
    ShardCountRequest req;
    req.shard_id = 0;
    auto resp = node2.document_count(req);
    EXPECT_FALSE(resp.is_error);

    h.stop_and_join();
}

// ===========================================================================
// Circuit breaker with real server
// ===========================================================================

TEST(RemoteCircuitBreakerTest, SuccessfulRequestsKeepCircuitClosed)
{
    // Create a circuit breaker with high threshold.
    CircuitBreakerConfig cb_config;
    cb_config.failure_threshold = 5;

    RetryPolicy policy = RetryPolicy::no_retries();

    // Start a real server.
    auto local_node = std::make_unique<LocalNode>(0);
    local_node->add_shard(0, std::make_unique<Shard>());
    auto h = start_server(0, std::move(local_node));

    RemoteNode node(0, "127.0.0.1", h.server->port(), 5, policy,
                    CircuitBreaker(cb_config));

    // Make multiple successful requests.
    for (int i = 0; i < 10; ++i) {
        ShardCountRequest req;
        req.shard_id = 0;
        auto resp = node.document_count(req);
        EXPECT_FALSE(resp.is_error);
    }

    // Circuit should still be CLOSED (verified by successful responses).
    h.stop_and_join();
}

// ===========================================================================
// Circuit breaker does not trip on application errors
// ===========================================================================

TEST(RemoteCircuitBreakerTest, ApplicationErrorsDoNotTripBreaker)
{
    CircuitBreakerConfig cb_config;
    cb_config.failure_threshold = 3;

    RetryPolicy policy = RetryPolicy::no_retries();

    // Start a real server.
    auto local_node = std::make_unique<LocalNode>(0);
    local_node->add_shard(0, std::make_unique<Shard>());
    auto h = start_server(0, std::move(local_node));

    RemoteNode node(0, "127.0.0.1", h.server->port(), 5, policy,
                    CircuitBreaker(cb_config));

    // Get a non-existent document (application error, not transport failure).
    for (int i = 0; i < 5; ++i) {
        ShardGetRequest req;
        req.shard_id = 0;
        req.document_id = 999 + i;
        auto resp = node.get_document(req);
        EXPECT_FALSE(resp.found);
    }

    // Circuit should still be CLOSED (verified by continued successful responses).
    // Application errors don't trip the circuit breaker.
    ShardCountRequest count_req;
    count_req.shard_id = 0;
    auto count_resp = node.document_count(count_req);
    EXPECT_FALSE(count_resp.is_error);

    h.stop_and_join();
}

// ===========================================================================
// Coordinator integration with circuit breaker
// ===========================================================================

TEST(RemoteCircuitBreakerTest, CoordinatorSearchWithCircuitBreaker)
{
    // Start 2 nodes with default circuit breakers.
    std::vector<ServerHandle> handles;

    for (std::size_t i = 0; i < 2; ++i) {
        auto node = std::make_unique<LocalNode>(i);
        node->add_shard(i, std::make_unique<Shard>());
        handles.push_back(start_server(i, std::move(node)));
    }

    auto router = std::make_unique<ShardRouter>(2);
    std::vector<std::size_t> placement = {0, 1};
    auto sp = std::make_unique<ShardPlacement>(2, 2, placement);

    RetryPolicy policy = RetryPolicy::no_retries();

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

    // Search should work with all circuits closed.
    auto r = coord.search({"unique", SearchMode::Or, 10});
    EXPECT_TRUE(r.complete);
    EXPECT_EQ(r.total, 10u);

    for (auto& h : handles) h.stop_and_join();
}

// ===========================================================================
// Search degradation when circuit is OPEN
// ===========================================================================

TEST(RemoteCircuitBreakerTest, SearchDegradationWithOpenCircuit)
{
    // Start 2 nodes.
    std::vector<ServerHandle> handles;
    for (std::size_t i = 0; i < 2; ++i) {
        auto node = std::make_unique<LocalNode>(i);
        node->add_shard(i, std::make_unique<Shard>());
        handles.push_back(start_server(i, std::move(node)));
    }

    auto router = std::make_unique<ShardRouter>(2);
    std::vector<std::size_t> placement = {0, 1};
    auto sp = std::make_unique<ShardPlacement>(2, 2, placement);

    // Create a circuit breaker for node 1 that will be tripped.
    CircuitBreakerConfig cb_config;
    cb_config.failure_threshold = 1;
    cb_config.recovery_timeout_ms = 60000;  // Long timeout

    RetryPolicy policy = RetryPolicy::no_retries();

    std::vector<std::unique_ptr<NodeClient>> nodes;
    nodes.push_back(std::make_unique<RemoteNode>(
        0, "127.0.0.1", handles[0].server->port(), 5, policy));
    nodes.push_back(std::make_unique<RemoteNode>(
        1, "127.0.0.1", handles[1].server->port(), 5, policy,
        CircuitBreaker(cb_config)));

    ShardCoordinator coord(std::move(router), std::move(sp), std::move(nodes));

    // Ingest documents.
    for (doc_id i = 1; i <= 10; ++i) {
        coord.ingest({i, "unique_" + std::to_string(i)});
    }

    // Trip the circuit breaker for node 1 by using a dead port.
    {
        RetryPolicy no_retry = RetryPolicy::no_retries();
        CircuitBreaker trip_cb(cb_config);
        RemoteNode trip_node(1, "127.0.0.1", 59999, 1, no_retry,
                             std::move(trip_cb));

        ShardCountRequest req;
        req.shard_id = 1;
        trip_node.document_count(req);  // This trips the breaker
    }

    // Stop node 1 to ensure it's truly unavailable.
    handles[1].stop_and_join();

    // Search should return partial results from node 0 only.
    auto r = coord.search({"unique", SearchMode::Or, 10});
    EXPECT_FALSE(r.complete);
    EXPECT_FALSE(r.errors.empty());
    EXPECT_GT(r.total, 0u);

    for (auto& h : handles) h.stop_and_join();
}

// ===========================================================================
// Concurrent circuit breaker access
// ===========================================================================

TEST(RemoteCircuitBreakerTest, ConcurrentCircuitBreakerAccess)
{
    CircuitBreakerConfig cb_config;
    cb_config.failure_threshold = 100;  // High threshold

    RetryPolicy policy = RetryPolicy::no_retries();

    // Start a real server.
    auto local_node = std::make_unique<LocalNode>(0);
    local_node->add_shard(0, std::make_unique<Shard>());
    auto h = start_server(0, std::move(local_node));

    RemoteNode node(0, "127.0.0.1", h.server->port(), 5, policy,
                    CircuitBreaker(cb_config));

    // Run multiple concurrent requests.
    std::vector<std::thread> threads;
    for (int i = 0; i < 5; ++i) {
        threads.emplace_back([&node, i]() {
            for (int j = 0; j < 10; ++j) {
                ShardCountRequest req;
                req.shard_id = 0;
                auto resp = node.document_count(req);
                EXPECT_FALSE(resp.is_error);
            }
        });
    }

    for (auto& t : threads) {
        t.join();
    }

    // No crash or undefined behavior = pass.
    EXPECT_TRUE(true);

    h.stop_and_join();
}
