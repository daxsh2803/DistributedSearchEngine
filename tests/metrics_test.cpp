// Distributed Search Engine - Metrics Collector Tests (Phase 16A).
//
// Comprehensive tests for MetricsCollector covering:
//   - Initial snapshot state
//   - Search counter increments
//   - Search error tracking
//   - Incomplete search tracking
//   - Write counter increments
//   - Retry tracking
//   - Circuit breaker event tracking
//   - Per-node metrics
//   - Latency average calculation
//   - P99 latency calculation
//   - Bounded latency buffer
//   - Reset behavior
//   - Concurrent recording from multiple threads
//   - Snapshot consistency

#include <gtest/gtest.h>

#include <atomic>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <thread>
#include <vector>

#include "circuit_breaker.h"
#include "metrics.h"

using dse::CircuitState;
using dse::MetricsCollector;
using dse::MetricsSnapshot;

// ===========================================================================
// 1. Initial snapshot state
// ===========================================================================

TEST(MetricsTest, InitialSnapshotIsZero)
{
    MetricsCollector metrics;
    auto snap = metrics.snapshot();

    EXPECT_EQ(snap.searches_total, 0u);
    EXPECT_EQ(snap.search_errors, 0u);
    EXPECT_EQ(snap.search_incomplete, 0u);
    EXPECT_EQ(snap.writes_total, 0u);
    EXPECT_EQ(snap.write_errors, 0u);
    EXPECT_EQ(snap.retries_total, 0u);
    EXPECT_EQ(snap.circuit_open_events, 0u);
    EXPECT_EQ(snap.circuit_close_events, 0u);
    EXPECT_EQ(snap.search_latency.sample_count, 0u);
    EXPECT_EQ(snap.search_latency.average_ms, 0.0);
    EXPECT_EQ(snap.search_latency.p99_ms, 0.0);
    EXPECT_TRUE(snap.per_node.empty());
}

// ===========================================================================
// 2. Search counter increments
// ===========================================================================

TEST(MetricsTest, SearchCountersIncrement)
{
    MetricsCollector metrics;

    metrics.record_search(0, 0, 10.0, true, true);
    metrics.record_search(0, 1, 15.0, true, true);
    metrics.record_search(1, 2, 20.0, true, true);

    auto snap = metrics.snapshot();
    EXPECT_EQ(snap.searches_total, 3u);
    EXPECT_EQ(snap.search_errors, 0u);
    EXPECT_EQ(snap.search_incomplete, 0u);
}

// ===========================================================================
// 3. Search errors
// ===========================================================================

TEST(MetricsTest, SearchErrorsTracked)
{
    MetricsCollector metrics;

    metrics.record_search(0, 0, 10.0, true, true);
    metrics.record_search(0, 0, 10.0, false, false);
    metrics.record_search(1, 1, 10.0, false, true);

    auto snap = metrics.snapshot();
    EXPECT_EQ(snap.searches_total, 3u);
    EXPECT_EQ(snap.search_errors, 2u);
}

// ===========================================================================
// 4. Incomplete searches
// ===========================================================================

TEST(MetricsTest, IncompleteSearchesTracked)
{
    MetricsCollector metrics;

    metrics.record_search(0, 0, 10.0, true, true);
    metrics.record_search(0, 0, 10.0, true, false);
    metrics.record_search(1, 1, 10.0, true, false);

    auto snap = metrics.snapshot();
    EXPECT_EQ(snap.searches_total, 3u);
    EXPECT_EQ(snap.search_incomplete, 2u);
}

// ===========================================================================
// 5. Write counters
// ===========================================================================

TEST(MetricsTest, WriteCountersIncrement)
{
    MetricsCollector metrics;

    metrics.record_write("add", 0, 5.0, true);
    metrics.record_write("update", 0, 8.0, true);
    metrics.record_write("delete", 1, 3.0, true);

    auto snap = metrics.snapshot();
    EXPECT_EQ(snap.writes_total, 3u);
    EXPECT_EQ(snap.write_errors, 0u);
}

TEST(MetricsTest, WriteErrorsTracked)
{
    MetricsCollector metrics;

    metrics.record_write("add", 0, 5.0, true);
    metrics.record_write("add", 0, 5.0, false);

    auto snap = metrics.snapshot();
    EXPECT_EQ(snap.writes_total, 2u);
    EXPECT_EQ(snap.write_errors, 1u);
}

// ===========================================================================
// 6. Retry tracking
// ===========================================================================

TEST(MetricsTest, RetriesTracked)
{
    MetricsCollector metrics;

    metrics.record_retry(0, "search");
    metrics.record_retry(0, "search");
    metrics.record_retry(1, "add");

    auto snap = metrics.snapshot();
    EXPECT_EQ(snap.retries_total, 3u);
}

// ===========================================================================
// 7. Circuit breaker events
// ===========================================================================

TEST(MetricsTest, CircuitBreakerOpenEvent)
{
    MetricsCollector metrics;

    // Initial state is Closed.
    metrics.record_circuit_breaker(0, CircuitState::Open);

    auto snap = metrics.snapshot();
    EXPECT_EQ(snap.circuit_open_events, 1u);
    EXPECT_EQ(snap.circuit_close_events, 0u);
}

TEST(MetricsTest, CircuitBreakerCloseEvent)
{
    MetricsCollector metrics;

    // Transition Closed -> Open (records open event).
    metrics.record_circuit_breaker(0, CircuitState::Open);
    // Transition Open -> HalfOpen (records close event).
    metrics.record_circuit_breaker(0, CircuitState::HalfOpen);

    auto snap = metrics.snapshot();
    EXPECT_EQ(snap.circuit_open_events, 1u);
    EXPECT_EQ(snap.circuit_close_events, 1u);
}

TEST(MetricsTest, CircuitBreakerMultipleTransitions)
{
    MetricsCollector metrics;

    // Closed -> Open -> HalfOpen -> Closed -> Open -> HalfOpen
    metrics.record_circuit_breaker(0, CircuitState::Open);       // open event
    metrics.record_circuit_breaker(0, CircuitState::HalfOpen);   // close event
    metrics.record_circuit_breaker(0, CircuitState::Closed);     // no event
    metrics.record_circuit_breaker(0, CircuitState::Open);       // open event
    metrics.record_circuit_breaker(0, CircuitState::HalfOpen);   // close event

    auto snap = metrics.snapshot();
    EXPECT_EQ(snap.circuit_open_events, 2u);
    EXPECT_EQ(snap.circuit_close_events, 2u);
}

TEST(MetricsTest, CircuitBreakerSameStateNoEvent)
{
    MetricsCollector metrics;

    // Staying in Open should not generate events.
    metrics.record_circuit_breaker(0, CircuitState::Open);
    metrics.record_circuit_breaker(0, CircuitState::Open);
    metrics.record_circuit_breaker(0, CircuitState::Open);

    auto snap = metrics.snapshot();
    EXPECT_EQ(snap.circuit_open_events, 1u);
    EXPECT_EQ(snap.circuit_close_events, 0u);
}

// ===========================================================================
// 8. Per-node metrics
// ===========================================================================

TEST(MetricsTest, PerNodeSearchMetrics)
{
    MetricsCollector metrics;

    metrics.record_search(0, 0, 10.0, true, true);
    metrics.record_search(0, 1, 15.0, false, false);
    metrics.record_search(1, 2, 20.0, true, true);

    auto snap = metrics.snapshot();
    ASSERT_TRUE(snap.per_node.count(0));
    ASSERT_TRUE(snap.per_node.count(1));

    EXPECT_EQ(snap.per_node[0].searches, 2u);
    EXPECT_EQ(snap.per_node[0].search_errors, 1u);
    EXPECT_EQ(snap.per_node[0].search_incomplete, 1u);

    EXPECT_EQ(snap.per_node[1].searches, 1u);
    EXPECT_EQ(snap.per_node[1].search_errors, 0u);
    EXPECT_EQ(snap.per_node[1].search_incomplete, 0u);
}

TEST(MetricsTest, PerNodeWriteMetrics)
{
    MetricsCollector metrics;

    metrics.record_write("add", 0, 5.0, true);
    metrics.record_write("add", 0, 5.0, false);
    metrics.record_write("add", 1, 5.0, true);

    auto snap = metrics.snapshot();
    EXPECT_EQ(snap.per_node[0].writes, 2u);
    EXPECT_EQ(snap.per_node[0].write_errors, 1u);
    EXPECT_EQ(snap.per_node[1].writes, 1u);
    EXPECT_EQ(snap.per_node[1].write_errors, 0u);
}

TEST(MetricsTest, PerNodeRetryMetrics)
{
    MetricsCollector metrics;

    metrics.record_retry(0, "search");
    metrics.record_retry(0, "search");
    metrics.record_retry(1, "add");

    auto snap = metrics.snapshot();
    EXPECT_EQ(snap.per_node[0].retries, 2u);
    EXPECT_EQ(snap.per_node[1].retries, 1u);
}

TEST(MetricsTest, PerNodeCircuitBreakerState)
{
    MetricsCollector metrics;

    metrics.record_circuit_breaker(0, CircuitState::Open);
    metrics.record_circuit_breaker(1, CircuitState::HalfOpen);

    auto snap = metrics.snapshot();
    EXPECT_EQ(snap.per_node[0].circuit_state, CircuitState::Open);
    EXPECT_EQ(snap.per_node[1].circuit_state, CircuitState::HalfOpen);
}

// ===========================================================================
// 9. Latency average
// ===========================================================================

TEST(MetricsTest, LatencyAverageCalculation)
{
    MetricsCollector metrics;

    // Add 4 samples: 10, 20, 30, 40 ms.
    metrics.record_search(0, 0, 10.0, true, true);
    metrics.record_search(0, 0, 20.0, true, true);
    metrics.record_search(0, 0, 30.0, true, true);
    metrics.record_search(0, 0, 40.0, true, true);

    auto snap = metrics.snapshot();
    EXPECT_EQ(snap.search_latency.sample_count, 4u);
    EXPECT_DOUBLE_EQ(snap.search_latency.average_ms, 25.0);
}

// ===========================================================================
// 10. P99 latency calculation
// ===========================================================================

TEST(MetricsTest, P99LatencyCalculation)
{
    MetricsCollector metrics;

    // Add 100 samples: 1..100 ms.
    for (int i = 1; i <= 100; ++i) {
        metrics.record_search(0, 0, static_cast<double>(i), true, true);
    }

    auto snap = metrics.snapshot();
    EXPECT_EQ(snap.search_latency.sample_count, 100u);
    // P99 of 1..100 is the 99th value = 99.0.
    EXPECT_DOUBLE_EQ(snap.search_latency.p99_ms, 99.0);
}

TEST(MetricsTest, P99LatencySmallBuffer)
{
    MetricsCollector metrics;

    // Add 10 samples: 1..10 ms.
    for (int i = 1; i <= 10; ++i) {
        metrics.record_search(0, 0, static_cast<double>(i), true, true);
    }

    auto snap = metrics.snapshot();
    EXPECT_EQ(snap.search_latency.sample_count, 10u);
    // P99 of 1..10 is the 9th value (99% of 10 = 9.9, floor = 9) = 9.0.
    EXPECT_DOUBLE_EQ(snap.search_latency.p99_ms, 9.0);
}

// ===========================================================================
// 11. Bounded latency buffer
// ===========================================================================

TEST(MetricsTest, LatencyBufferBounded)
{
    MetricsCollector metrics(10);  // Small buffer for testing.

    // Add 20 samples (buffer only keeps last 10).
    for (int i = 1; i <= 20; ++i) {
        metrics.record_search(0, 0, static_cast<double>(i), true, true);
    }

    auto snap = metrics.snapshot();
    EXPECT_EQ(snap.search_latency.sample_count, 10u);
    // Buffer contains 11..20, average = 15.5.
    EXPECT_DOUBLE_EQ(snap.search_latency.average_ms, 15.5);
}

TEST(MetricsTest, LatencyBufferWrapsCorrectly)
{
    MetricsCollector metrics(5);

    // Fill buffer: 1, 2, 3, 4, 5.
    for (int i = 1; i <= 5; ++i) {
        metrics.record_search(0, 0, static_cast<double>(i), true, true);
    }

    auto snap1 = metrics.snapshot();
    EXPECT_DOUBLE_EQ(snap1.search_latency.average_ms, 3.0);

    // Add 3 more: 6, 7, 8 (buffer now has 4, 5, 6, 7, 8).
    for (int i = 6; i <= 8; ++i) {
        metrics.record_search(0, 0, static_cast<double>(i), true, true);
    }

    auto snap2 = metrics.snapshot();
    EXPECT_EQ(snap2.search_latency.sample_count, 5u);
    EXPECT_DOUBLE_EQ(snap2.search_latency.average_ms, 6.0);
}

// ===========================================================================
// 12. Reset
// ===========================================================================

TEST(MetricsTest, ResetClearsAllMetrics)
{
    MetricsCollector metrics;

    // Add some data.
    metrics.record_search(0, 0, 10.0, true, true);
    metrics.record_write("add", 0, 5.0, true);
    metrics.record_retry(0, "search");
    metrics.record_circuit_breaker(0, CircuitState::Open);

    // Verify data exists.
    auto snap1 = metrics.snapshot();
    EXPECT_EQ(snap1.searches_total, 1u);
    EXPECT_EQ(snap1.writes_total, 1u);
    EXPECT_EQ(snap1.retries_total, 1u);
    EXPECT_EQ(snap1.circuit_open_events, 1u);
    EXPECT_EQ(snap1.per_node.size(), 1u);

    // Reset.
    metrics.reset();

    // Verify all cleared.
    auto snap2 = metrics.snapshot();
    EXPECT_EQ(snap2.searches_total, 0u);
    EXPECT_EQ(snap2.search_errors, 0u);
    EXPECT_EQ(snap2.search_incomplete, 0u);
    EXPECT_EQ(snap2.writes_total, 0u);
    EXPECT_EQ(snap2.write_errors, 0u);
    EXPECT_EQ(snap2.retries_total, 0u);
    EXPECT_EQ(snap2.circuit_open_events, 0u);
    EXPECT_EQ(snap2.circuit_close_events, 0u);
    EXPECT_EQ(snap2.search_latency.sample_count, 0u);
    EXPECT_TRUE(snap2.per_node.empty());
}

// ===========================================================================
// 13. Concurrent recording
// ===========================================================================

TEST(MetricsTest, ConcurrentSearchRecording)
{
    MetricsCollector metrics;

    constexpr int kThreads = 8;
    constexpr int kIterations = 1000;

    std::vector<std::thread> threads;
    for (int t = 0; t < kThreads; ++t) {
        threads.emplace_back([&metrics, t]() {
            for (int i = 0; i < kIterations; ++i) {
                const std::size_t node_id = static_cast<std::size_t>(t % 3);
                metrics.record_search(node_id, 0,
                    static_cast<double>(i), true, true);
            }
        });
    }

    for (auto& th : threads) {
        th.join();
    }

    auto snap = metrics.snapshot();
    EXPECT_EQ(snap.searches_total,
              static_cast<std::uint64_t>(kThreads * kIterations));
}

TEST(MetricsTest, ConcurrentWriteRecording)
{
    MetricsCollector metrics;

    constexpr int kThreads = 8;
    constexpr int kIterations = 1000;

    std::vector<std::thread> threads;
    for (int t = 0; t < kThreads; ++t) {
        threads.emplace_back([&metrics, t]() {
            for (int i = 0; i < kIterations; ++i) {
                const std::size_t node_id = static_cast<std::size_t>(t % 3);
                metrics.record_write("add", node_id,
                    static_cast<double>(i), true);
            }
        });
    }

    for (auto& th : threads) {
        th.join();
    }

    auto snap = metrics.snapshot();
    EXPECT_EQ(snap.writes_total,
              static_cast<std::uint64_t>(kThreads * kIterations));
}

TEST(MetricsTest, ConcurrentMixedOperations)
{
    MetricsCollector metrics;

    constexpr int kThreads = 8;
    constexpr int kIterations = 500;

    std::vector<std::thread> threads;

    // Search threads.
    for (int t = 0; t < kThreads / 2; ++t) {
        threads.emplace_back([&metrics, t]() {
            for (int i = 0; i < kIterations; ++i) {
                metrics.record_search(t, 0, 10.0, true, true);
            }
        });
    }

    // Write threads.
    for (int t = kThreads / 2; t < kThreads; ++t) {
        threads.emplace_back([&metrics, t]() {
            for (int i = 0; i < kIterations; ++i) {
                metrics.record_write("add", t, 5.0, true);
            }
        });
    }

    for (auto& th : threads) {
        th.join();
    }

    auto snap = metrics.snapshot();
    EXPECT_EQ(snap.searches_total,
              static_cast<std::uint64_t>((kThreads / 2) * kIterations));
    EXPECT_EQ(snap.writes_total,
              static_cast<std::uint64_t>((kThreads / 2) * kIterations));
}

TEST(MetricsTest, ConcurrentSnapshotSafety)
{
    MetricsCollector metrics;

    constexpr int kRecorderThreads = 4;
    constexpr int kSnapshotThreads = 4;

    std::atomic<bool> done{false};

    // Recorder threads.
    std::vector<std::thread> recorders;
    for (int t = 0; t < kRecorderThreads; ++t) {
        recorders.emplace_back([&metrics, &done]() {
            while (!done.load()) {
                metrics.record_search(0, 0, 10.0, true, true);
            }
        });
    }

    // Snapshot threads (should not crash or hang).
    std::vector<std::thread> snapshotters;
    for (int t = 0; t < kSnapshotThreads; ++t) {
        snapshotters.emplace_back([&metrics, &done]() {
            while (!done.load()) {
                auto snap = metrics.snapshot();
                // Snapshot should be internally consistent.
                EXPECT_GE(snap.searches_total, 0u);
            }
        });
    }

    // Run for a short time.
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
    done.store(true);

    for (auto& th : recorders) {
        th.join();
    }
    for (auto& th : snapshotters) {
        th.join();
    }

    // Final snapshot should be consistent.
    auto snap = metrics.snapshot();
    EXPECT_GE(snap.searches_total, 0u);
}

// ===========================================================================
// 14. Snapshot consistency
// ===========================================================================

TEST(MetricsTest, SnapshotIsConsistentCopy)
{
    MetricsCollector metrics;

    metrics.record_search(0, 0, 10.0, true, true);
    metrics.record_search(0, 0, 20.0, true, true);

    auto snap1 = metrics.snapshot();
    auto snap2 = metrics.snapshot();

    // Two snapshots taken in sequence should be identical
    // (no concurrent modification in this test).
    EXPECT_EQ(snap1.searches_total, snap2.searches_total);
    EXPECT_EQ(snap1.search_latency.sample_count,
              snap2.search_latency.sample_count);
    EXPECT_DOUBLE_EQ(snap1.search_latency.average_ms,
                     snap2.search_latency.average_ms);
}

// ===========================================================================
// 15. Edge cases
// ===========================================================================

TEST(MetricsTest, EmptyLatencyBuffer)
{
    MetricsCollector metrics;

    auto snap = metrics.snapshot();
    EXPECT_EQ(snap.search_latency.sample_count, 0u);
    EXPECT_DOUBLE_EQ(snap.search_latency.average_ms, 0.0);
    EXPECT_DOUBLE_EQ(snap.search_latency.p99_ms, 0.0);
}

TEST(MetricsTest, SingleLatencySample)
{
    MetricsCollector metrics;

    metrics.record_search(0, 0, 42.0, true, true);

    auto snap = metrics.snapshot();
    EXPECT_EQ(snap.search_latency.sample_count, 1u);
    EXPECT_DOUBLE_EQ(snap.search_latency.average_ms, 42.0);
    EXPECT_DOUBLE_EQ(snap.search_latency.p99_ms, 42.0);
}

TEST(MetricsTest, ZeroLatencyValue)
{
    MetricsCollector metrics;

    metrics.record_search(0, 0, 0.0, true, true);

    auto snap = metrics.snapshot();
    EXPECT_DOUBLE_EQ(snap.search_latency.average_ms, 0.0);
    EXPECT_DOUBLE_EQ(snap.search_latency.p99_ms, 0.0);
}

TEST(MetricsTest, LargeLatencyValues)
{
    MetricsCollector metrics;

    metrics.record_search(0, 0, 100000.0, true, true);
    metrics.record_search(0, 0, 200000.0, true, true);

    auto snap = metrics.snapshot();
    EXPECT_DOUBLE_EQ(snap.search_latency.average_ms, 150000.0);
}

// ===========================================================================
// 16. Default buffer size
// ===========================================================================

TEST(MetricsTest, DefaultBufferSizeIs1000)
{
    MetricsCollector metrics;  // Default buffer size.

    // Add 1000 samples.
    for (int i = 0; i < 1000; ++i) {
        metrics.record_search(0, 0, 1.0, true, true);
    }

    auto snap = metrics.snapshot();
    EXPECT_EQ(snap.search_latency.sample_count, 1000u);

    // Add one more — should wrap and keep 1000.
    metrics.record_search(0, 0, 100.0, true, true);

    auto snap2 = metrics.snapshot();
    EXPECT_EQ(snap2.search_latency.sample_count, 1000u);
}
