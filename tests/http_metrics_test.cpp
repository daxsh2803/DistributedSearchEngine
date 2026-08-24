// Distributed Search Engine - HTTP Metrics Tests (Phase 16D).
//
// Tests for GET /health and GET /metrics endpoints.
// Verifies: health endpoint, metrics endpoint with/without MetricsCollector,
// coordinator vs node-level metrics separation, thread safety, and
// backward compatibility.

#include <gtest/gtest.h>

#include "http_server.h"
#include "local_node.h"
#include "metrics.h"
#include "node_client.h"
#include "node_config.h"
#include "search_service.h"
#include "shard.h"
#include "shard_coordinator.h"
#include "shard_router.h"

#include <httplib.h>
#include <nlohmann/json.hpp>

#include <atomic>
#include <cstddef>
#include <memory>
#include <string>
#include <thread>
#include <vector>

using namespace dse;

// ---------------------------------------------------------------------------
// Helper: create a coordinator with N shards on 1 node.
// ---------------------------------------------------------------------------

static std::unique_ptr<ShardCoordinator> make_coordinator(std::size_t n)
{
    auto router = std::make_unique<ShardRouter>(n);
    std::vector<std::size_t> placement(n, 0);
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

// ---------------------------------------------------------------------------
// Test fixture: HTTP server with metrics.
// ---------------------------------------------------------------------------

class HttpMetricsTest : public ::testing::Test {
protected:
    void SetUp() override
    {
        coordinator_ = make_coordinator(3);
        metrics_ = std::make_unique<MetricsCollector>();
        coordinator_->set_metrics(metrics_.get());

        server_ = std::make_unique<HttpServer>(*coordinator_, metrics_.get());
    }

    void start_server()
    {
        server_thread_ = std::thread([this]() { server_->listen(0); });
        server_->wait_until_ready();
    }

    void TearDown() override
    {
        server_->stop();
        if (server_thread_.joinable()) {
            server_thread_.join();
        }
    }

    std::pair<int, std::string> get(const std::string& path)
    {
        httplib::Client client("localhost", server_->port());
        client.set_connection_timeout(5);
        client.set_read_timeout(5);
        auto res = client.Get(path);
        if (!res) return {0, ""};
        return {res->status, res->body};
    }

    std::unique_ptr<ShardCoordinator> coordinator_;
    std::unique_ptr<MetricsCollector> metrics_;
    std::unique_ptr<HttpServer> server_;
    std::thread server_thread_;
};

// ===========================================================================
// 1. Health endpoint returns HTTP 200
// ===========================================================================

TEST_F(HttpMetricsTest, HealthReturns200)
{
    start_server();
    const auto [status, body] = get("/health");
    EXPECT_EQ(status, 200);
}

// ===========================================================================
// 2. Health endpoint returns correct JSON
// ===========================================================================

TEST_F(HttpMetricsTest, HealthReturnsCorrectJson)
{
    start_server();
    const auto [status, body] = get("/health");
    EXPECT_EQ(status, 200);

    const auto j = nlohmann::json::parse(body);
    EXPECT_TRUE(j.contains("status"));
    EXPECT_EQ(j["status"], "ok");
}

// ===========================================================================
// 3. Metrics endpoint returns HTTP 200 when configured
// ===========================================================================

TEST_F(HttpMetricsTest, MetricsReturns200WhenConfigured)
{
    start_server();
    const auto [status, body] = get("/metrics");
    EXPECT_EQ(status, 200);
}

// ===========================================================================
// 4. Metrics endpoint returns valid JSON
// ===========================================================================

TEST_F(HttpMetricsTest, MetricsReturnsValidJson)
{
    start_server();
    const auto [status, body] = get("/metrics");
    EXPECT_EQ(status, 200);

    const auto j = nlohmann::json::parse(body);
    EXPECT_TRUE(j.is_object());
}

// ===========================================================================
// 5. Metrics endpoint exposes coordinator search metrics
// ===========================================================================

TEST_F(HttpMetricsTest, MetricsExposesCoordinatorSearchMetrics)
{
    start_server();
    const auto [status, body] = get("/metrics");
    EXPECT_EQ(status, 200);

    const auto j = nlohmann::json::parse(body);
    EXPECT_TRUE(j.contains("coordinator_searches_total"));
    EXPECT_TRUE(j.contains("coordinator_search_success"));
    EXPECT_TRUE(j.contains("coordinator_search_incomplete"));
    EXPECT_TRUE(j.contains("coordinator_search_errors"));
    EXPECT_TRUE(j.contains("coordinator_search_latency"));
}

// ===========================================================================
// 6. Perform search and verify /metrics reflects it
// ===========================================================================

TEST_F(HttpMetricsTest, SearchReflectedInMetrics)
{
    coordinator_->ingest({1, "hello world"});
    start_server();

    // Perform a search via HTTP.
    const auto [s1, b1] = get("/search?q=hello");
    EXPECT_EQ(s1, 200);

    // Check metrics.
    const auto [status, body] = get("/metrics");
    EXPECT_EQ(status, 200);

    const auto j = nlohmann::json::parse(body);
    EXPECT_EQ(j["coordinator_searches_total"], 1u);
    EXPECT_EQ(j["coordinator_search_success"], 1u);
}

// ===========================================================================
// 7. Multi-shard search — coordinator_searches_total == 1
// ===========================================================================

TEST_F(HttpMetricsTest, MultiShardSearchOneCoordinatorSearch)
{
    for (doc_id i = 0; i < 30; ++i) {
        coordinator_->ingest({i, "common_term doc " + std::to_string(i)});
    }
    start_server();

    // Search via HTTP.
    const auto [s1, b1] = get("/search?q=common_term");
    EXPECT_EQ(s1, 200);

    const auto [status, body] = get("/metrics");
    EXPECT_EQ(status, 200);

    const auto j = nlohmann::json::parse(body);
    // Exactly ONE coordinator search.
    EXPECT_EQ(j["coordinator_searches_total"], 1u);
    // Node-level searches may be greater (one per shard).
    EXPECT_GE(j["searches_total"].get<std::uint64_t>(), 0u);
}

// ===========================================================================
// 8. Node-level and coordinator-level metrics remain separate
// ===========================================================================

TEST_F(HttpMetricsTest, NodeAndCoordinatorMetricsSeparate)
{
    for (doc_id i = 0; i < 30; ++i) {
        coordinator_->ingest({i, "common_term doc " + std::to_string(i)});
    }
    start_server();

    // Search via HTTP.
    const auto [s1, b1] = get("/search?q=common_term");
    EXPECT_EQ(s1, 200);

    const auto [status, body] = get("/metrics");
    EXPECT_EQ(status, 200);

    const auto j = nlohmann::json::parse(body);
    // Coordinator: exactly 1 search.
    EXPECT_EQ(j["coordinator_searches_total"], 1u);
    // Node-level: LocalNode doesn't record metrics, so this is 0.
    EXPECT_EQ(j["searches_total"].get<std::uint64_t>(), 0u);
}

// ===========================================================================
// 9. Without MetricsCollector — /metrics returns 503
// ===========================================================================

TEST(HttpMetricsNoCollectorTest, MetricsReturns503WithoutCollector)
{
    auto coordinator = make_coordinator(3);
    HttpServer server(*coordinator);  // No metrics

    std::thread server_thread([&server]() { server.listen(0); });
    server.wait_until_ready();

    httplib::Client client("localhost", server.port());
    client.set_connection_timeout(5);
    client.set_read_timeout(5);
    auto res = client.Get("/metrics");

    ASSERT_TRUE(res);
    EXPECT_EQ(res->status, 503);

    const auto j = nlohmann::json::parse(res->body);
    EXPECT_TRUE(j.contains("error"));
    EXPECT_EQ(j["error"], "Metrics unavailable");

    server.stop();
    server_thread.join();
}

// ===========================================================================
// 10. /metrics does NOT reset counters
// ===========================================================================

TEST_F(HttpMetricsTest, MetricsDoesNotResetCounters)
{
    coordinator_->ingest({1, "test"});
    start_server();

    // Perform a search.
    const auto [s1, b1] = get("/search?q=test");
    EXPECT_EQ(s1, 200);

    // First /metrics call.
    const auto [status1, body1] = get("/metrics");
    EXPECT_EQ(status1, 200);
    const auto j1 = nlohmann::json::parse(body1);
    EXPECT_EQ(j1["coordinator_searches_total"], 1u);

    // Second /metrics call.
    const auto [status2, body2] = get("/metrics");
    EXPECT_EQ(status2, 200);
    const auto j2 = nlohmann::json::parse(body2);
    EXPECT_EQ(j2["coordinator_searches_total"], 1u);

    // Counter unchanged between calls.
    EXPECT_EQ(j1["coordinator_searches_total"], j2["coordinator_searches_total"]);
}

// ===========================================================================
// 11. Concurrent /metrics requests are safe
// ===========================================================================

TEST_F(HttpMetricsTest, ConcurrentMetricsRequestsAreSafe)
{
    for (doc_id i = 0; i < 30; ++i) {
        coordinator_->ingest({i, "doc " + std::to_string(i)});
    }
    start_server();

    constexpr int kThreads = 5;
    std::atomic<int> success_count{0};

    std::vector<std::thread> threads;
    for (int t = 0; t < kThreads; ++t) {
        threads.emplace_back([this, &success_count]() {
            httplib::Client client("localhost", server_->port());
            client.set_connection_timeout(5);
            client.set_read_timeout(5);
            auto res = client.Get("/metrics");
            if (res && res->status == 200) {
                const auto j = nlohmann::json::parse(res->body);
                if (j.contains("coordinator_searches_total")) {
                    success_count.fetch_add(1, std::memory_order_relaxed);
                }
            }
        });
    }

    for (auto& th : threads) {
        th.join();
    }

    EXPECT_EQ(success_count.load(), kThreads);
}

// ===========================================================================
// 12. Existing HTTP routes continue to work
// ===========================================================================

TEST_F(HttpMetricsTest, ExistingRoutesStillWork)
{
    coordinator_->ingest({1, "hello world"});
    start_server();

    // Search still works.
    const auto [s1, b1] = get("/search?q=hello");
    EXPECT_EQ(s1, 200);

    // Health still works.
    const auto [s2, b2] = get("/health");
    EXPECT_EQ(s2, 200);
}

// ===========================================================================
// 13. Metrics endpoint exposes ALL MetricsSnapshot fields
// ===========================================================================

TEST_F(HttpMetricsTest, MetricsExposesAllSnapshotFields)
{
    start_server();
    const auto [status, body] = get("/metrics");
    EXPECT_EQ(status, 200);

    const auto j = nlohmann::json::parse(body);
    EXPECT_TRUE(j.is_object());

    // Node-level search metrics
    EXPECT_TRUE(j.contains("searches_total"));
    EXPECT_TRUE(j.contains("search_errors"));
    EXPECT_TRUE(j.contains("search_incomplete"));
    EXPECT_TRUE(j.contains("search_latency"));
    EXPECT_TRUE(j["search_latency"].contains("average_ms"));
    EXPECT_TRUE(j["search_latency"].contains("p99_ms"));
    EXPECT_TRUE(j["search_latency"].contains("sample_count"));

    // Node-level write metrics
    EXPECT_TRUE(j.contains("writes_total"));
    EXPECT_TRUE(j.contains("write_errors"));

    // Retry metrics
    EXPECT_TRUE(j.contains("retries_total"));

    // Circuit breaker metrics
    EXPECT_TRUE(j.contains("circuit_open_events"));
    EXPECT_TRUE(j.contains("circuit_close_events"));

    // Coordinator-level search metrics
    EXPECT_TRUE(j.contains("coordinator_searches_total"));
    EXPECT_TRUE(j.contains("coordinator_search_success"));
    EXPECT_TRUE(j.contains("coordinator_search_incomplete"));
    EXPECT_TRUE(j.contains("coordinator_search_errors"));
    EXPECT_TRUE(j.contains("coordinator_search_latency"));
    EXPECT_TRUE(j["coordinator_search_latency"].contains("average_ms"));
    EXPECT_TRUE(j["coordinator_search_latency"].contains("p99_ms"));
    EXPECT_TRUE(j["coordinator_search_latency"].contains("sample_count"));

    // Coordinator-level write metrics
    EXPECT_TRUE(j.contains("coordinator_writes_total"));
    EXPECT_TRUE(j.contains("coordinator_write_success"));
    EXPECT_TRUE(j.contains("coordinator_write_errors"));
    EXPECT_TRUE(j.contains("coordinator_write_latency"));
    EXPECT_TRUE(j["coordinator_write_latency"].contains("average_ms"));
    EXPECT_TRUE(j["coordinator_write_latency"].contains("p99_ms"));
    EXPECT_TRUE(j["coordinator_write_latency"].contains("sample_count"));

    // Per-node metrics
    EXPECT_TRUE(j.contains("per_node"));
    EXPECT_TRUE(j["per_node"].is_object());
}
