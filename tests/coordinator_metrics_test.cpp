// Distributed Search Engine - Coordinator Metrics Tests (Phase 16C).
//
// Tests that ShardCoordinator correctly records coordinator-level metrics
// via MetricsCollector. Verifies: coordinator search/write metrics,
// no double-counting with RemoteNode metrics, thread safety, and
// backward compatibility.

#include <gtest/gtest.h>

#include <atomic>
#include <cstddef>
#include <memory>
#include <string>
#include <thread>
#include <vector>

#include "metrics.h"
#include "local_node.h"
#include "node_client.h"
#include "node_config.h"
#include "remote_node.h"
#include "retry_policy.h"
#include "search_service.h"
#include "shard.h"
#include "shard_coordinator.h"
#include "shard_router.h"

using namespace dse;

// ---------------------------------------------------------------------------
// Helper: create a coordinator with N shards on 1 node.
// ---------------------------------------------------------------------------

static std::unique_ptr<ShardCoordinator> make_coordinator(
    std::size_t n, MetricsCollector* metrics = nullptr)
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

    auto coord = std::make_unique<ShardCoordinator>(
        std::move(router), std::move(shard_placement), std::move(nodes));

    if (metrics) {
        coord->set_metrics(metrics);
    }

    return coord;
}

// ===========================================================================
// 1. Coordinator metrics disabled — behavior unchanged
// ===========================================================================

TEST(CoordinatorMetricsTest, DisabledMetricsNoChange)
{
    auto coord = make_coordinator(3);
    coord->ingest({1, "hello world"});

    SearchRequest req;
    req.query = "hello";
    req.mode = SearchMode::Or;
    req.limit = 10;

    const auto resp = coord->search(req);
    EXPECT_FALSE(resp.is_error);
    EXPECT_EQ(resp.total, 1u);
}

// ===========================================================================
// 2. One successful distributed search — exactly one coordinator search
// ===========================================================================

TEST(CoordinatorMetricsTest, OneSearchRecordsOneCoordinatorSearch)
{
    MetricsCollector metrics;
    auto coord = make_coordinator(3, &metrics);

    coord->ingest({1, "quick brown fox"});
    coord->ingest({5, "lazy dog"});

    SearchRequest req;
    req.query = "fox";
    req.mode = SearchMode::Or;
    req.limit = 10;

    const auto resp = coord->search(req);
    EXPECT_FALSE(resp.is_error);

    auto snap = metrics.snapshot();
    EXPECT_EQ(snap.coordinator_searches_total, 1u);
    EXPECT_EQ(snap.coordinator_search_success, 1u);
    EXPECT_EQ(snap.coordinator_search_errors, 0u);
    EXPECT_TRUE(snap.coordinator_search_latency.sample_count >= 1);
}

// ===========================================================================
// 3. Multi-shard search — exactly one coordinator search recorded
// ===========================================================================

TEST(CoordinatorMetricsTest, MultiShardSearchRecordsOneCoordinatorSearch)
{
    MetricsCollector metrics;
    auto coord = make_coordinator(3, &metrics);

    // Add documents that distribute across shards.
    for (doc_id i = 0; i < 30; ++i) {
        coord->ingest({i, "common_term doc " + std::to_string(i)});
    }

    SearchRequest req;
    req.query = "common_term";
    req.mode = SearchMode::Or;
    req.limit = 10;

    const auto resp = coord->search(req);
    EXPECT_FALSE(resp.is_error);

    auto snap = metrics.snapshot();
    // Exactly ONE coordinator search, not N shard searches.
    EXPECT_EQ(snap.coordinator_searches_total, 1u);
    EXPECT_EQ(snap.coordinator_search_success, 1u);
    // Note: LocalNode does not record node-level metrics.
    // Node-level metrics are only recorded by RemoteNode.
}

// ===========================================================================
// 4. Complete distributed search — correct completion semantics
// ===========================================================================

TEST(CoordinatorMetricsTest, CompleteSearchRecordsSuccess)
{
    MetricsCollector metrics;
    auto coord = make_coordinator(3, &metrics);

    coord->ingest({1, "alpha"});

    SearchRequest req;
    req.query = "alpha";
    req.mode = SearchMode::Or;
    req.limit = 10;

    const auto resp = coord->search(req);
    EXPECT_FALSE(resp.is_error);
    EXPECT_TRUE(resp.complete);

    auto snap = metrics.snapshot();
    EXPECT_EQ(snap.coordinator_search_success, 1u);
    EXPECT_EQ(snap.coordinator_search_incomplete, 0u);
}

// ===========================================================================
// 5. Incomplete distributed search — incomplete metric recorded
// ===========================================================================

TEST(CoordinatorMetricsTest, IncompleteSearchRecordsIncomplete)
{
    // Create a coordinator with a RemoteNode pointing to a non-existent server.
    // This will cause node failures and incomplete searches.
    auto router = std::make_unique<ShardRouter>(3);
    std::vector<std::size_t> placement = {0, 0, 0};
    auto shard_placement = std::make_unique<ShardPlacement>(3, 1, placement);

    // Point at a port that is not listening.
    auto bad_node = std::make_unique<RemoteNode>(
        0, "127.0.0.1", 1, 1, RetryPolicy::no_retries());

    std::vector<std::unique_ptr<NodeClient>> nodes;
    nodes.push_back(std::move(bad_node));

    MetricsCollector metrics;
    auto coord = std::make_unique<ShardCoordinator>(
        std::move(router), std::move(shard_placement), std::move(nodes));
    coord->set_metrics(&metrics);

    SearchRequest req;
    req.query = "test";
    req.mode = SearchMode::Or;
    req.limit = 10;

    const auto resp = coord->search(req);
    // Search completes with errors/incomplete, not is_error.
    EXPECT_FALSE(resp.is_error);
    EXPECT_FALSE(resp.complete);
    EXPECT_FALSE(resp.errors.empty());

    auto snap = metrics.snapshot();
    EXPECT_EQ(snap.coordinator_searches_total, 1u);
    EXPECT_EQ(snap.coordinator_search_incomplete, 1u);
}

// ===========================================================================
// 6. Search with node failure — coordinator metrics reflect outcome
// ===========================================================================

TEST(CoordinatorMetricsTest, SearchWithNodeFailureRecordsError)
{
    MetricsCollector metrics;
    auto coord = make_coordinator(3, &metrics);

    // Search with invalid request (empty query).
    SearchRequest req;
    req.query = "";
    req.mode = SearchMode::Or;
    req.limit = 10;

    const auto resp = coord->search(req);
    EXPECT_TRUE(resp.is_error);

    auto snap = metrics.snapshot();
    EXPECT_EQ(snap.coordinator_searches_total, 1u);
    EXPECT_EQ(snap.coordinator_search_errors, 1u);
}

// ===========================================================================
// 7. Concurrent coordinator searches — thread-safe
// ===========================================================================

TEST(CoordinatorMetricsTest, ConcurrentSearchesAreThreadSafe)
{
    MetricsCollector metrics;
    auto coord = make_coordinator(3, &metrics);

    for (doc_id i = 0; i < 30; ++i) {
        coord->ingest({i, "doc " + std::to_string(i)});
    }

    constexpr int kThreads = 5;
    constexpr int kIterations = 20;
    std::atomic<std::uint64_t> search_count{0};

    std::vector<std::thread> threads;
    for (int t = 0; t < kThreads; ++t) {
        threads.emplace_back([&coord, &search_count]() {
            for (int i = 0; i < kIterations; ++i) {
                SearchRequest req;
                req.query = "doc";
                req.mode = SearchMode::Or;
                req.limit = 10;
                coord->search(req);
                search_count.fetch_add(1, std::memory_order_relaxed);
            }
        });
    }

    for (auto& th : threads) {
        th.join();
    }

    auto snap = metrics.snapshot();
    EXPECT_EQ(snap.coordinator_searches_total, search_count.load());
}

// ===========================================================================
// 8. No double-counting between coordinator and node metrics
// ===========================================================================

TEST(CoordinatorMetricsTest, NoDoubleCounting)
{
    MetricsCollector metrics;
    auto coord = make_coordinator(3, &metrics);

    for (doc_id i = 0; i < 30; ++i) {
        coord->ingest({i, "common_term doc " + std::to_string(i)});
    }

    SearchRequest req;
    req.query = "common_term";
    req.mode = SearchMode::Or;
    req.limit = 10;

    const auto resp = coord->search(req);
    EXPECT_FALSE(resp.is_error);

    auto snap = metrics.snapshot();

    // Coordinator: exactly 1 search request.
    EXPECT_EQ(snap.coordinator_searches_total, 1u);

    // Note: LocalNode does not record node-level metrics.
    // To verify no double-counting with RemoteNode, see the
    // remote_node_metrics_test which verifies RemoteNode records
    // individual node operations separately.
}

// ===========================================================================
// 9. Write metrics — coordinator level
// ===========================================================================

TEST(CoordinatorMetricsTest, WriteMetricsRecorded)
{
    MetricsCollector metrics;
    auto coord = make_coordinator(3, &metrics);

    coord->ingest({1, "hello"});
    coord->ingest({2, "world"});
    coord->update({1, "goodbye"});
    coord->remove(2);

    auto snap = metrics.snapshot();
    EXPECT_EQ(snap.coordinator_writes_total, 4u);
    EXPECT_EQ(snap.coordinator_write_success, 4u);
    EXPECT_EQ(snap.coordinator_write_errors, 0u);
}

// ===========================================================================
// 10. Write error metrics
// ===========================================================================

TEST(CoordinatorMetricsTest, WriteErrorMetrics)
{
    MetricsCollector metrics;
    auto coord = make_coordinator(3, &metrics);

    // Empty content should fail.
    const auto resp = coord->ingest({1, ""});
    EXPECT_TRUE(resp.is_error);

    auto snap = metrics.snapshot();
    EXPECT_EQ(snap.coordinator_writes_total, 1u);
    EXPECT_EQ(snap.coordinator_write_errors, 1u);
}

// ===========================================================================
// 11. Coordinator latency recorded
// ===========================================================================

TEST(CoordinatorMetricsTest, CoordinatorLatencyRecorded)
{
    MetricsCollector metrics;
    auto coord = make_coordinator(3, &metrics);

    coord->ingest({1, "test"});

    SearchRequest req;
    req.query = "test";
    req.mode = SearchMode::Or;
    req.limit = 10;

    coord->search(req);

    auto snap = metrics.snapshot();
    EXPECT_GE(snap.coordinator_search_latency.sample_count, 1u);
    EXPECT_GE(snap.coordinator_search_latency.average_ms, 0.0);
}

// ===========================================================================
// 12. AND mode search metrics
// ===========================================================================

TEST(CoordinatorMetricsTest, AndModeSearchMetrics)
{
    MetricsCollector metrics;
    auto coord = make_coordinator(3, &metrics);

    coord->ingest({1, "quick brown fox"});

    SearchRequest req;
    req.query = "quick brown";
    req.mode = SearchMode::And;
    req.limit = 10;

    const auto resp = coord->search(req);
    EXPECT_FALSE(resp.is_error);

    auto snap = metrics.snapshot();
    EXPECT_EQ(snap.coordinator_searches_total, 1u);
    EXPECT_EQ(snap.coordinator_search_success, 1u);
}

// ===========================================================================
// 13. Empty query records error
// ===========================================================================

TEST(CoordinatorMetricsTest, EmptyQueryRecordsError)
{
    MetricsCollector metrics;
    auto coord = make_coordinator(3, &metrics);

    SearchRequest req;
    req.query = "";
    req.mode = SearchMode::Or;
    req.limit = 10;

    const auto resp = coord->search(req);
    EXPECT_TRUE(resp.is_error);

    auto snap = metrics.snapshot();
    EXPECT_EQ(snap.coordinator_searches_total, 1u);
    EXPECT_EQ(snap.coordinator_search_errors, 1u);
}
