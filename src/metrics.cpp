// Distributed Search Engine - Metrics Collector (Phase 16A).
//
// Implementation of the contract in src/metrics.h.
// Uses atomic counters for high-frequency operations and a
// mutex-protected circular buffer for latency tracking.

#include "metrics.h"

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <mutex>
#include <numeric>
#include <unordered_map>
#include <vector>

namespace dse {

// ---------------------------------------------------------------------------
// Construction
// ---------------------------------------------------------------------------

MetricsCollector::MetricsCollector(std::size_t latency_buffer_size)
    : latency_buffer_size_(latency_buffer_size)
{
    latency_buffer_.resize(latency_buffer_size);
}

// ---------------------------------------------------------------------------
// Search metrics
// ---------------------------------------------------------------------------

void MetricsCollector::record_search(std::size_t node_id,
                                     std::size_t /*shard_id*/,
                                     double latency_ms,
                                     bool success,
                                     bool complete)
{
    searches_total_.fetch_add(1, std::memory_order_relaxed);

    if (!success) {
        search_errors_.fetch_add(1, std::memory_order_relaxed);
    }

    if (!complete) {
        search_incomplete_.fetch_add(1, std::memory_order_relaxed);
    }

    // Record latency.
    push_latency(latency_ms);

    // Update per-node metrics.
    {
        std::lock_guard lock(mutex_);
        auto& nm = per_node_[node_id];
        nm.searches++;
        if (!success) {
            nm.search_errors++;
        }
        if (!complete) {
            nm.search_incomplete++;
        }
    }
}

// ---------------------------------------------------------------------------
// Write metrics
// ---------------------------------------------------------------------------

void MetricsCollector::record_write(const std::string& /*operation*/,
                                    std::size_t node_id,
                                    double latency_ms,
                                    bool success)
{
    writes_total_.fetch_add(1, std::memory_order_relaxed);

    if (!success) {
        write_errors_.fetch_add(1, std::memory_order_relaxed);
    }

    // Record latency (reuse the same buffer for writes).
    push_latency(latency_ms);

    // Update per-node metrics.
    {
        std::lock_guard lock(mutex_);
        auto& nm = per_node_[node_id];
        nm.writes++;
        if (!success) {
            nm.write_errors++;
        }
    }
}

// ---------------------------------------------------------------------------
// Retry metrics
// ---------------------------------------------------------------------------

void MetricsCollector::record_retry(std::size_t node_id,
                                    const std::string& /*operation*/)
{
    retries_total_.fetch_add(1, std::memory_order_relaxed);

    // Update per-node metrics.
    {
        std::lock_guard lock(mutex_);
        auto& nm = per_node_[node_id];
        nm.retries++;
    }
}

// ---------------------------------------------------------------------------
// Circuit breaker metrics
// ---------------------------------------------------------------------------

void MetricsCollector::record_circuit_breaker(std::size_t node_id,
                                              CircuitState new_state)
{
    std::lock_guard lock(mutex_);

    auto& nm = per_node_[node_id];
    const auto old_state = nm.circuit_state;
    nm.circuit_state = new_state;

    if (old_state != CircuitState::Open && new_state == CircuitState::Open) {
        circuit_open_events_.fetch_add(1, std::memory_order_relaxed);
        nm.circuit_open_events++;
    }

    if (old_state == CircuitState::Open && new_state != CircuitState::Open) {
        circuit_close_events_.fetch_add(1, std::memory_order_relaxed);
        nm.circuit_close_events++;
    }
}

// ---------------------------------------------------------------------------
// Snapshot
// ---------------------------------------------------------------------------

MetricsSnapshot MetricsCollector::snapshot() const
{
    MetricsSnapshot snap;

    // Copy atomic counters (relaxed is fine for monitoring).
    snap.searches_total = searches_total_.load(std::memory_order_relaxed);
    snap.search_errors = search_errors_.load(std::memory_order_relaxed);
    snap.search_incomplete = search_incomplete_.load(std::memory_order_relaxed);
    snap.writes_total = writes_total_.load(std::memory_order_relaxed);
    snap.write_errors = write_errors_.load(std::memory_order_relaxed);
    snap.retries_total = retries_total_.load(std::memory_order_relaxed);
    snap.circuit_open_events = circuit_open_events_.load(std::memory_order_relaxed);
    snap.circuit_close_events = circuit_close_events_.load(std::memory_order_relaxed);

    // Compute latency stats from the buffer.
    snap.search_latency = compute_latency_stats();

    // Copy per-node metrics.
    {
        std::lock_guard lock(mutex_);
        snap.per_node = per_node_;
    }

    return snap;
}

// ---------------------------------------------------------------------------
// Reset
// ---------------------------------------------------------------------------

void MetricsCollector::reset()
{
    std::lock_guard lock(mutex_);

    searches_total_.store(0, std::memory_order_relaxed);
    search_errors_.store(0, std::memory_order_relaxed);
    search_incomplete_.store(0, std::memory_order_relaxed);
    writes_total_.store(0, std::memory_order_relaxed);
    write_errors_.store(0, std::memory_order_relaxed);
    retries_total_.store(0, std::memory_order_relaxed);
    circuit_open_events_.store(0, std::memory_order_relaxed);
    circuit_close_events_.store(0, std::memory_order_relaxed);

    latency_index_ = 0;
    latency_count_ = 0;
    std::fill(latency_buffer_.begin(), latency_buffer_.end(), 0.0);

    per_node_.clear();
}

// ---------------------------------------------------------------------------
// Latency buffer (private)
// ---------------------------------------------------------------------------

void MetricsCollector::push_latency(double latency_ms)
{
    std::lock_guard lock(mutex_);
    latency_buffer_[latency_index_] = latency_ms;
    latency_index_ = (latency_index_ + 1) % latency_buffer_size_;
    if (latency_count_ < latency_buffer_size_) {
        latency_count_++;
    }
}

LatencyStats MetricsCollector::compute_latency_stats() const
{
    std::lock_guard lock(mutex_);

    LatencyStats stats;
    if (latency_count_ == 0) {
        return stats;
    }

    stats.sample_count = latency_count_;

    // Compute average.
    double sum = 0.0;
    for (std::size_t i = 0; i < latency_count_; ++i) {
        sum += latency_buffer_[i];
    }
    stats.average_ms = sum / static_cast<double>(latency_count_);

    // Compute p99: sort a copy, take the 99th percentile.
    std::vector<double> sorted(latency_buffer_.begin(),
                               latency_buffer_.begin() + latency_count_);
    std::sort(sorted.begin(), sorted.end());

    // P99 index: 99th percentile using floor(0.99 * (n-1)).
    // For n=100 sorted [1..100]: index=floor(0.99*99)=98, value=99.
    // For n=10 sorted [1..10]: index=floor(0.99*9)=8, value=9.
    const std::size_t p99_index =
        static_cast<std::size_t>(0.99 * static_cast<double>(sorted.size() - 1));
    stats.p99_ms = sorted[p99_index];

    return stats;
}

} // namespace dse
