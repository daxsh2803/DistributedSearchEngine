// Distributed Search Engine - Metrics Collector (Phase 16A).
//
// Lightweight, thread-safe, in-process metrics collection for
// distributed search operations. Provides atomic counters for
// operations, errors, retries, and circuit breaker events, plus
// a bounded circular buffer for latency tracking.
//
// Design principles:
//   - Lock-free atomic counters for high-frequency operations
//   - Mutex-protected latency buffer (infrequent updates)
//   - Thread-safe snapshot() for consistent reads
//   - No external dependencies (Prometheus, OpenTelemetry, etc.)
//   - Composable: optional MetricsCollector* parameter
//   - Testable: reset() for clean test state
//
// Latency tracking:
//   - Fixed-size circular buffer (default 1000 samples)
//   - Average computed from buffer contents
//   - P99 computed deterministically from stored samples
//   - Buffer is in-memory only (no persistence)
//   - Represents RECENT samples, not lifetime exact percentile

#pragma once

#include <atomic>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <functional>
#include <mutex>
#include <unordered_map>
#include <vector>

#include "circuit_breaker.h"  // for CircuitState

namespace dse {

// ---------------------------------------------------------------------------
// Per-node metrics
// ---------------------------------------------------------------------------

struct NodeMetrics {
    std::uint64_t searches = 0;
    std::uint64_t search_errors = 0;
    std::uint64_t search_incomplete = 0;
    std::uint64_t writes = 0;
    std::uint64_t write_errors = 0;
    std::uint64_t retries = 0;
    CircuitState circuit_state = CircuitState::Closed;
    std::uint64_t circuit_open_events = 0;
    std::uint64_t circuit_close_events = 0;
};

// ---------------------------------------------------------------------------
// Latency statistics
// ---------------------------------------------------------------------------

struct LatencyStats {
    double average_ms = 0.0;
    double p99_ms = 0.0;
    std::size_t sample_count = 0;
};

// ---------------------------------------------------------------------------
// Metrics snapshot (consistent point-in-time view)
// ---------------------------------------------------------------------------

struct MetricsSnapshot {
    // Search metrics
    std::uint64_t searches_total = 0;
    std::uint64_t search_errors = 0;
    std::uint64_t search_incomplete = 0;
    LatencyStats search_latency;

    // Write metrics
    std::uint64_t writes_total = 0;
    std::uint64_t write_errors = 0;

    // Retry metrics
    std::uint64_t retries_total = 0;

    // Circuit breaker metrics
    std::uint64_t circuit_open_events = 0;
    std::uint64_t circuit_close_events = 0;

    // Per-node metrics
    std::unordered_map<std::size_t, NodeMetrics> per_node;
};

// ---------------------------------------------------------------------------
// MetricsCollector
// ---------------------------------------------------------------------------

// Thread-safe metrics collector for distributed search operations.
//
// Usage:
//   MetricsCollector metrics;
//   metrics.record_search(node_id, shard_id, latency_ms, true, true);
//   auto snap = metrics.snapshot();
//   std::cout << "Searches: " << snap.searches_total << "\n";
//
// Thread safety:
//   All methods are safe for concurrent use by multiple threads.
//   Atomic counters for high-frequency operations.
//   Mutex-protected latency buffer for consistency.
class MetricsCollector {
public:
    // Buffer size for latency tracking (configurable at construction).
    static constexpr std::size_t kDefaultLatencyBufferSize = 1000;

    // Create a metrics collector with the given latency buffer size.
    explicit MetricsCollector(
        std::size_t latency_buffer_size = kDefaultLatencyBufferSize);

    // Record a search operation.
    //   node_id      — the node that served the search
    //   shard_id     — the shard that was searched
    //   latency_ms   — request duration in milliseconds
    //   success      — whether the search succeeded
    //   complete     — whether all shards participated
    void record_search(std::size_t node_id,
                       std::size_t shard_id,
                       double latency_ms,
                       bool success,
                       bool complete);

    // Record a write operation (add/update/delete).
    //   operation    — "add", "update", or "delete"
    //   node_id      — the node that handled the write
    //   latency_ms   — request duration in milliseconds
    //   success      — whether the write succeeded
    void record_write(const std::string& operation,
                      std::size_t node_id,
                      double latency_ms,
                      bool success);

    // Record a retry attempt.
    //   node_id      — the node being retried
    //   operation    — the operation being retried ("search", "add", etc.)
    void record_retry(std::size_t node_id,
                      const std::string& operation);

    // Record a circuit breaker state change.
    //   node_id     — the node whose breaker changed
    //   new_state   — the new state after transition
    void record_circuit_breaker(std::size_t node_id,
                                CircuitState new_state);

    // Get a consistent snapshot of all metrics.
    // Thread-safe: acquires mutex, copies data, releases mutex.
    MetricsSnapshot snapshot() const;

    // Reset all metrics (primarily for testing).
    // Thread-safe: acquires mutex.
    void reset();

private:
    // Circular buffer for latency samples.
    void push_latency(double latency_ms);

    // Compute statistics from the latency buffer.
    LatencyStats compute_latency_stats() const;

    // --- Atomic counters (lock-free) ---
    std::atomic<std::uint64_t> searches_total_{0};
    std::atomic<std::uint64_t> search_errors_{0};
    std::atomic<std::uint64_t> search_incomplete_{0};
    std::atomic<std::uint64_t> writes_total_{0};
    std::atomic<std::uint64_t> write_errors_{0};
    std::atomic<std::uint64_t> retries_total_{0};
    std::atomic<std::uint64_t> circuit_open_events_{0};
    std::atomic<std::uint64_t> circuit_close_events_{0};

    // --- Mutex-protected state ---
    mutable std::mutex mutex_;
    std::vector<double> latency_buffer_;  // circular buffer
    std::size_t latency_index_ = 0;      // next write position
    std::size_t latency_count_ = 0;      // total samples (capped at buffer size)
    std::size_t latency_buffer_size_;

    // Per-node metrics (protected by mutex_)
    std::unordered_map<std::size_t, NodeMetrics> per_node_;
};

} // namespace dse
