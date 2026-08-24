// Distributed Search Engine - Remote Node (Phase 12).
//
// NodeClient implementation that communicates with a remote NodeServer
// over HTTP/JSON. The coordinator uses this exactly like LocalNode —
// it cannot tell the difference.
//
// Every NodeClient method:
//   1. Serializes the request to JSON (via node_wire.h)
//   2. Sends an HTTP POST to the NodeServer
//   3. Deserializes the JSON response
//   4. Returns the NodeClient-level response
//
// Network failures become NodeClient-level errors (is_error = true).
// The coordinator's fail-fast behavior handles them.
//
// Thread safety:
//   All methods are safe for concurrent use. httplib::Client is safe
//   for concurrent requests (each request opens its own connection).

#pragma once

#include <cstddef>
#include <cstdint>
#include <memory>
#include <string>
#include <vector>

#include "circuit_breaker.h"
#include "metrics.h"
#include "node_client.h"
#include "retry_policy.h"

namespace httplib {
class Client;
}

namespace dse {

class RemoteNode : public NodeClient {
public:
    // Create a remote node client.
    //   node_id  — stable identity matching the remote NodeServer
    //   host     — hostname or IP (e.g., "127.0.0.1")
    //   port     — port the NodeServer is listening on
    //   timeout_seconds — connection/read/write timeout (default 30s)
    //   retry_policy — policy for retrying transient failures (default: no retries)
    //   circuit_breaker — circuit breaker for failure isolation (default: no breaking)
    // Constructor with default circuit breaker (no breaking).
    RemoteNode(std::size_t node_id,
               std::string host,
               int port,
               int timeout_seconds = 30,
               RetryPolicy retry_policy = RetryPolicy::no_retries());

    // Constructor with explicit circuit breaker.
    RemoteNode(std::size_t node_id,
               std::string host,
               int port,
               int timeout_seconds,
               RetryPolicy retry_policy,
               CircuitBreaker circuit_breaker);

    // Set an optional metrics collector for observability.
    // Pass nullptr to disable metrics (default).
    void set_metrics(MetricsCollector* metrics);

    ~RemoteNode() override;

    RemoteNode(const RemoteNode&) = delete;
    RemoteNode& operator=(const RemoteNode&) = delete;
    RemoteNode(RemoteNode&&) = delete;
    RemoteNode& operator=(RemoteNode&&) = delete;

    // --- NodeClient interface ---

    std::size_t node_id() const override;

    ShardSearchResponse search(const ShardSearchRequest& request) override;
    ShardWriteResponse add_document(const ShardWriteRequest& request) override;
    ShardWriteResponse update_document(const ShardWriteRequest& request) override;
    ShardRemoveResponse remove_document(const ShardRemoveRequest& request) override;

    ShardGetResponse get_document(const ShardGetRequest& request) override;
    ShardCountResponse document_count(const ShardCountRequest& request) override;

    bool save_shard(std::size_t shard_id) override;
    bool load_shard(std::size_t shard_id) override;

private:
    // Create a fresh httplib::Client for each request (safe for concurrency).
    std::unique_ptr<httplib::Client> make_client() const;

    // Check if an error response should be retried.
    bool should_retry(const std::string& error_message) const;

    // Check if an error is a transport-level failure (for circuit breaker).
    bool is_transport_failure(const std::string& error_message) const;

    // Sleep for the specified duration (milliseconds).
    static void sleep_ms(std::size_t milliseconds);

    // Helper to compute elapsed milliseconds since a time point.
    static double elapsed_ms(std::chrono::steady_clock::time_point start);

    // Observe circuit breaker state and record transitions in metrics.
    void observe_circuit_breaker();

    std::size_t node_id_;
    std::string host_;
    int port_;
    int timeout_seconds_;
    RetryPolicy retry_policy_;
    std::unique_ptr<CircuitBreaker> circuit_breaker_;
    MetricsCollector* metrics_ = nullptr;  // optional, not owned
};

} // namespace dse
