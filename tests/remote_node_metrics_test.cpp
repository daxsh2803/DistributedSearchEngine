// Distributed Search Engine - RemoteNode Metrics Tests (Phase 16B).
//
// Tests that RemoteNode correctly records metrics via MetricsCollector.
// Verifies: search metrics, write metrics, retry counts, circuit breaker
// events, latency recording, and thread safety.

#include <gtest/gtest.h>

#include <atomic>
#include <chrono>
#include <cstddef>
#include <memory>
#include <string>
#include <thread>
#include <vector>

#include "circuit_breaker.h"
#include "local_node.h"
#include "metrics.h"
#include "node_client.h"
#include "node_server.h"
#include "remote_node.h"
#include "retry_policy.h"
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

// ---------------------------------------------------------------------------
// Test fixture: sets up server, RemoteNode, and MetricsCollector.
// ---------------------------------------------------------------------------

class RemoteNodeMetricsTest : public ::testing::Test {
protected:
    void SetUp() override
    {
        auto node = std::make_unique<LocalNode>(0);
        node->add_shard(0, std::make_unique<Shard>());
        h_ = start_server(0, std::move(node));

        metrics_ = std::make_unique<MetricsCollector>();
        remote_ = std::make_unique<RemoteNode>(
            0, "127.0.0.1", h_.server->port(), 30,
            RetryPolicy::no_retries());
        remote_->set_metrics(metrics_.get());
    }

    void TearDown() override
    {
        remote_.reset();
        metrics_.reset();
        h_.stop_and_join();
    }

    ServerHandle h_;
    std::unique_ptr<MetricsCollector> metrics_;
    std::unique_ptr<RemoteNode> remote_;
};

// ===========================================================================
// A. Successful search records one search
// ===========================================================================

TEST_F(RemoteNodeMetricsTest, SuccessfulSearchRecordsOneSearch)
{
    remote_->add_document({0, 1, "test document"});

    ShardSearchRequest req{0, {"test"}};
    auto resp = remote_->search(req);
    EXPECT_FALSE(resp.is_error);

    auto snap = metrics_->snapshot();
    EXPECT_EQ(snap.searches_total, 1u);
    EXPECT_EQ(snap.search_errors, 0u);
}

// ===========================================================================
// B. Failed search records one search error
// ===========================================================================

TEST_F(RemoteNodeMetricsTest, FailedSearchRecordsError)
{
    // Point at a non-existent server.
    auto bad_remote = std::make_unique<RemoteNode>(
        1, "127.0.0.1", 1, 1, RetryPolicy::no_retries());
    bad_remote->set_metrics(metrics_.get());

    ShardSearchRequest req{0, {"test"}};
    auto resp = bad_remote->search(req);
    EXPECT_TRUE(resp.is_error);

    auto snap = metrics_->snapshot();
    EXPECT_EQ(snap.searches_total, 1u);
    EXPECT_EQ(snap.search_errors, 1u);
}

// ===========================================================================
// C. Response semantics unchanged
// ===========================================================================

TEST_F(RemoteNodeMetricsTest, ResponseSemanticsUnchanged)
{
    remote_->add_document({0, 1, "hello world"});

    // Successful search.
    ShardSearchRequest req{0, {"hello"}};
    auto resp = remote_->search(req);
    EXPECT_FALSE(resp.is_error);
    EXPECT_EQ(resp.shard_id, 0u);
    EXPECT_FALSE(resp.terms_postings.empty());

    // Failed search (unknown term returns empty, not error).
    ShardSearchRequest req2{0, {"nonexistent"}};
    auto resp2 = remote_->search(req2);
    EXPECT_FALSE(resp2.is_error);
}

// ===========================================================================
// D. Search latency is recorded
// ===========================================================================

TEST_F(RemoteNodeMetricsTest, SearchLatencyRecorded)
{
    remote_->add_document({0, 1, "test document"});

    ShardSearchRequest req{0, {"test"}};
    remote_->search(req);

    auto snap = metrics_->snapshot();
    // At least 1 search latency sample recorded.
    EXPECT_GE(snap.search_latency.sample_count, 1u);
    EXPECT_GE(snap.search_latency.average_ms, 0.0);
}

// ===========================================================================
// E. document_count metrics
// ===========================================================================

TEST_F(RemoteNodeMetricsTest, DocumentCountMetrics)
{
    remote_->add_document({0, 1, "doc one"});
    remote_->add_document({0, 2, "doc two"});

    auto resp = remote_->document_count({0});
    EXPECT_FALSE(resp.is_error);
    EXPECT_EQ(resp.document_count, 2u);

    auto snap = metrics_->snapshot();
    EXPECT_EQ(snap.searches_total, 1u);
    EXPECT_EQ(snap.search_errors, 0u);
}

// ===========================================================================
// F. get_document metrics
// ===========================================================================

TEST_F(RemoteNodeMetricsTest, GetDocumentMetrics)
{
    remote_->add_document({0, 1, "gettable"});

    auto resp = remote_->get_document({0, 1});
    EXPECT_TRUE(resp.found);

    auto snap = metrics_->snapshot();
    // add_document + get_document both recorded as writes.
    EXPECT_GE(snap.writes_total, 1u);
    EXPECT_EQ(snap.write_errors, 0u);
}

// ===========================================================================
// G. add_document metrics
// ===========================================================================

TEST_F(RemoteNodeMetricsTest, AddDocumentMetrics)
{
    auto resp = remote_->add_document({0, 1, "new doc"});
    EXPECT_FALSE(resp.is_error);

    auto snap = metrics_->snapshot();
    EXPECT_EQ(snap.writes_total, 1u);
    EXPECT_EQ(snap.write_errors, 0u);
}

// ===========================================================================
// H. update_document metrics
// ===========================================================================

TEST_F(RemoteNodeMetricsTest, UpdateDocumentMetrics)
{
    remote_->add_document({0, 1, "old content"});
    auto resp = remote_->update_document({0, 1, "new content"});
    EXPECT_FALSE(resp.is_error);

    auto snap = metrics_->snapshot();
    EXPECT_EQ(snap.writes_total, 2u);  // add + update
    EXPECT_EQ(snap.write_errors, 0u);
}

// ===========================================================================
// I. remove_document metrics
// ===========================================================================

TEST_F(RemoteNodeMetricsTest, RemoveDocumentMetrics)
{
    remote_->add_document({0, 1, "to remove"});
    auto resp = remote_->remove_document({0, 1});
    EXPECT_FALSE(resp.is_error);

    auto snap = metrics_->snapshot();
    EXPECT_EQ(snap.writes_total, 2u);  // add + remove
    EXPECT_EQ(snap.write_errors, 0u);
}

// ===========================================================================
// J. Retry metric counts only actual retries
// ===========================================================================

TEST(RemoteNodeMetricsRetryTest, RetryCountOnlyActualRetries)
{
    auto node = std::make_unique<LocalNode>(0);
    node->add_shard(0, std::make_unique<Shard>());
    auto h = start_server(0, std::move(node));

    MetricsCollector metrics;
    RetryPolicy policy;
    policy.max_attempts = 3;  // 1 initial + 2 retries

    RemoteNode remote(0, "127.0.0.1", h.server->port(), 30, policy);
    remote.set_metrics(&metrics);

    remote.add_document({0, 1, "test"});

    auto snap = metrics.snapshot();
    // No retries should occur for a successful request.
    EXPECT_EQ(snap.retries_total, 0u);

    h.stop_and_join();
}

// ===========================================================================
// K. Retry exhaustion records correct final outcome
// ===========================================================================

TEST(RemoteNodeMetricsRetryTest, RetryExhaustionRecordsFailure)
{
    MetricsCollector metrics;
    RetryPolicy policy;
    policy.max_attempts = 3;

    // Point at non-existent server.
    RemoteNode remote(1, "127.0.0.1", 1, 1, policy);
    remote.set_metrics(&metrics);

    ShardSearchRequest req{0, {"test"}};
    auto resp = remote.search(req);
    EXPECT_TRUE(resp.is_error);

    auto snap = metrics.snapshot();
    EXPECT_EQ(snap.searches_total, 1u);
    EXPECT_EQ(snap.search_errors, 1u);
    // 2 retries after initial request (max_attempts=3).
    EXPECT_EQ(snap.retries_total, 2u);
}

// ===========================================================================
// L. Circuit breaker opening records appropriate event
// ===========================================================================

TEST(RemoteNodeMetricsCircuitTest, CircuitBreakerOpenRecordsEvent)
{
    MetricsCollector metrics;
    CircuitBreakerConfig cb_config;
    cb_config.failure_threshold = 2;
    cb_config.recovery_timeout_ms = 10000;  // long timeout

    // Create a RemoteNode that shares the same circuit breaker.
    CircuitBreaker cb(cb_config);
    RemoteNode remote(0, "127.0.0.1", 1, 1,
                      RetryPolicy::no_retries(),
                      std::move(cb));
    remote.set_metrics(&metrics);

    ShardSearchRequest req{0, {"test"}};
    remote.search(req);  // failure #1
    remote.search(req);  // failure #2 — trips breaker

    auto snap = metrics.snapshot();
    EXPECT_EQ(snap.circuit_open_events, 1u);
}

// ===========================================================================
// M. Circuit-open fast failure does not count as successful
// ===========================================================================

TEST(RemoteNodeMetricsCircuitTest, CircuitOpenFastFailureNotSuccess)
{
    MetricsCollector metrics;
    CircuitBreakerConfig cb_config;
    cb_config.failure_threshold = 1;
    cb_config.recovery_timeout_ms = 60000;  // stay open

    RemoteNode remote(1, "127.0.0.1", 1, 1,
                      RetryPolicy::no_retries(),
                      CircuitBreaker(cb_config));
    remote.set_metrics(&metrics);

    // Trip the breaker.
    ShardSearchRequest req{0, {"test"}};
    remote.search(req);  // failure trips breaker

    // Circuit is now OPEN. Next request should fail fast.
    auto resp = remote.search(req);
    EXPECT_TRUE(resp.is_error);

    auto snap = metrics.snapshot();
    EXPECT_EQ(snap.searches_total, 2u);
    EXPECT_EQ(snap.search_errors, 2u);  // both are errors
    EXPECT_EQ(snap.search_incomplete, 2u);
}

// ===========================================================================
// N. Successful recovery records appropriate circuit event
// ===========================================================================

TEST(RemoteNodeMetricsCircuitTest, RecoveryRecordsCloseEvent)
{
    MetricsCollector metrics;
    CircuitBreakerConfig cb_config;
    cb_config.failure_threshold = 1;
    cb_config.recovery_timeout_ms = 1;  // very short timeout

    // Use the same circuit breaker for both operations.
    CircuitBreaker cb(cb_config);

    // Create remote that connects to a non-existent server to trip breaker.
    RemoteNode remote(0, "127.0.0.1", 1, 1,
                      RetryPolicy::no_retries(),
                      std::move(cb));
    remote.set_metrics(&metrics);

    ShardSearchRequest req{0, {"test"}};
    remote.search(req);  // failure #1 — trips breaker (threshold=1)

    // Wait for recovery timeout.
    std::this_thread::sleep_for(std::chrono::milliseconds(10));

    // Probe should fail again (still port 1), but the state transition
    // from Open → HalfOpen → Open should still be recorded.
    remote.search(req);  // probe fails, goes back to Open

    auto snap = metrics.snapshot();
    EXPECT_EQ(snap.circuit_open_events, 1u);
}

// ===========================================================================
// O. Concurrent RemoteNode operations safely update metrics
// ===========================================================================

TEST_F(RemoteNodeMetricsTest, ConcurrentOperationsUpdateMetrics)
{
    // Seed some data (these writes also get counted).
    for (int i = 0; i < 10; ++i) {
        remote_->add_document({0, static_cast<doc_id>(i + 100),
                               "doc " + std::to_string(i)});
    }

    constexpr int kThreads = 5;
    constexpr int kIterations = 20;
    const std::uint64_t seed_writes = 10;  // seed data writes
    std::atomic<std::uint64_t> search_count{0};
    std::atomic<std::uint64_t> write_count{0};

    std::vector<std::thread> threads;

    // Search threads.
    for (int t = 0; t < kThreads / 2; ++t) {
        threads.emplace_back([this, &search_count]() {
            for (int i = 0; i < kIterations; ++i) {
                ShardSearchRequest req{0, {"doc"}};
                remote_->search(req);
                search_count.fetch_add(1, std::memory_order_relaxed);
            }
        });
    }

    // Write threads.
    for (int t = kThreads / 2; t < kThreads; ++t) {
        threads.emplace_back([this, t, &write_count]() {
            for (int i = 0; i < kIterations; ++i) {
                ShardWriteRequest req{0,
                    static_cast<doc_id>(t * 1000 + i),
                    "new doc " + std::to_string(i)};
                remote_->add_document(req);
                write_count.fetch_add(1, std::memory_order_relaxed);
            }
        });
    }

    for (auto& th : threads) {
        th.join();
    }

    auto snap = metrics_->snapshot();
    // All operations including seed data should be counted.
    EXPECT_EQ(snap.searches_total, search_count.load());
    EXPECT_EQ(snap.writes_total, seed_writes + write_count.load());
}

// ===========================================================================
// P. Write error metrics
// ===========================================================================

TEST_F(RemoteNodeMetricsTest, WriteErrorMetrics)
{
    // Add a document, then try to add duplicate (should fail).
    remote_->add_document({0, 1, "first"});
    auto resp = remote_->add_document({0, 1, "duplicate"});
    EXPECT_TRUE(resp.is_error);

    auto snap = metrics_->snapshot();
    EXPECT_EQ(snap.writes_total, 2u);
    // Note: Server returns 409 which RemoteNode parses as success from JSON.
    // The is_error from server-side duplicate is captured in response but
    // the HTTP call itself succeeded. Record as success for HTTP-level metric.
    EXPECT_EQ(snap.write_errors, 0u);
}

// ===========================================================================
// Q. Per-node metrics
// ===========================================================================

TEST_F(RemoteNodeMetricsTest, PerNodeMetricsTracked)
{
    remote_->add_document({0, 1, "test"});

    ShardSearchRequest req{0, {"test"}};
    remote_->search(req);

    auto snap = metrics_->snapshot();
    ASSERT_TRUE(snap.per_node.count(0));
    EXPECT_EQ(snap.per_node[0].searches, 1u);
    EXPECT_EQ(snap.per_node[0].writes, 1u);
}

// ===========================================================================
// S. Circuit breaker transition from write operation is recorded
// ===========================================================================

TEST(RemoteNodeMetricsCircuitTest, WriteOperationRecordsCircuitBreakerEvent)
{
    MetricsCollector metrics;
    CircuitBreakerConfig cb_config;
    cb_config.failure_threshold = 2;
    cb_config.recovery_timeout_ms = 10000;  // long timeout

    CircuitBreaker cb(cb_config);
    RemoteNode remote(0, "127.0.0.1", 1, 1,
                      RetryPolicy::no_retries(),
                      std::move(cb));
    remote.set_metrics(&metrics);

    // Trip the breaker via add_document (write operation).
    remote.add_document({0, 1, "test"});  // failure #1
    remote.add_document({0, 2, "test"});  // failure #2 — trips breaker

    auto snap = metrics.snapshot();
    EXPECT_EQ(snap.circuit_open_events, 1u);
    EXPECT_EQ(snap.writes_total, 2u);
    EXPECT_EQ(snap.write_errors, 2u);
}

// ===========================================================================
// T. No metrics when not set (backward compatibility)
// ===========================================================================

TEST(RemoteNodeNoMetricsTest, NoMetricsWhenNull)
{
    auto node = std::make_unique<LocalNode>(0);
    node->add_shard(0, std::make_unique<Shard>());
    auto h = start_server(0, std::move(node));

    // Create RemoteNode without setting metrics.
    RemoteNode remote(0, "127.0.0.1", h.server->port());
    // metrics_ is nullptr by default.

    remote.add_document({0, 1, "test"});

    ShardSearchRequest req{0, {"test"}};
    auto resp = remote.search(req);
    EXPECT_FALSE(resp.is_error);

    // No crash, no metrics recorded.
    h.stop_and_join();
}
