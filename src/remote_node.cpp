// Distributed Search Engine - Remote Node (Phase 12).
//
// Implements NodeClient over HTTP/JSON transport. Each method creates
// a fresh httplib::Client, sends a POST request, and deserializes
// the response. Network failures are mapped to NodeClient errors.

#include "remote_node.h"

#include <httplib.h>
#include <nlohmann/json.hpp>

#include <chrono>
#include <cstddef>
#include <memory>
#include <string>
#include <thread>
#include <utility>

#include "circuit_breaker.h"
#include "node_client.h"
#include "node_wire.h"
#include "retry_policy.h"

namespace dse {

// ---------------------------------------------------------------------------
// Construction
// ---------------------------------------------------------------------------

RemoteNode::RemoteNode(std::size_t node_id,
                       std::string host,
                       int port,
                       int timeout_seconds,
                       RetryPolicy retry_policy)
    : node_id_(node_id)
    , host_(std::move(host))
    , port_(port)
    , timeout_seconds_(timeout_seconds)
    , retry_policy_(std::move(retry_policy))
    , circuit_breaker_(std::make_unique<CircuitBreaker>())
{
}

RemoteNode::RemoteNode(std::size_t node_id,
                       std::string host,
                       int port,
                       int timeout_seconds,
                       RetryPolicy retry_policy,
                       CircuitBreaker circuit_breaker)
    : node_id_(node_id)
    , host_(std::move(host))
    , port_(port)
    , timeout_seconds_(timeout_seconds)
    , retry_policy_(std::move(retry_policy))
    , circuit_breaker_(std::make_unique<CircuitBreaker>(std::move(circuit_breaker)))
{
}

RemoteNode::~RemoteNode() = default;

std::size_t RemoteNode::node_id() const
{
    return node_id_;
}

std::unique_ptr<httplib::Client> RemoteNode::make_client() const
{
    auto client = std::make_unique<httplib::Client>(host_.c_str(), port_);
    client->set_connection_timeout(timeout_seconds_);
    client->set_read_timeout(timeout_seconds_);
    client->set_write_timeout(timeout_seconds_);
    return client;
}

bool RemoteNode::should_retry(const std::string& error_message) const
{
    auto category = RetryPolicy::classify_error(error_message);
    return retry_policy_.is_retryable(category);
}

bool RemoteNode::is_transport_failure(const std::string& error_message) const
{
    auto category = RetryPolicy::classify_error(error_message);
    return category == ErrorCategory::ConnectionRefused ||
           category == ErrorCategory::ConnectionReset ||
           category == ErrorCategory::Timeout ||
           category == ErrorCategory::TransportError;
}

void RemoteNode::sleep_ms(std::size_t milliseconds)
{
    std::this_thread::sleep_for(std::chrono::milliseconds(milliseconds));
}

// ---------------------------------------------------------------------------
// Search
// ---------------------------------------------------------------------------

ShardSearchResponse RemoteNode::search(const ShardSearchRequest& request)
{
    ShardSearchResponse response;
    response.shard_id = request.shard_id;

    // Check circuit breaker before attempting request.
    if (!circuit_breaker_->should_allow_request()) {
        response.is_error = true;
        response.error_message = "Circuit breaker OPEN: node " +
            std::to_string(node_id_) + " is unavailable";
        return response;
    }

    bool last_attempt_succeeded = false;

    for (std::size_t attempt = 0; attempt < retry_policy_.max_attempts; ++attempt) {
        // Sleep before retry (not on first attempt).
        if (attempt > 0) {
            auto delay = retry_policy_.delay_for_attempt(attempt);
            if (delay > 0) {
                sleep_ms(delay);
            }
        }

        try {
            auto client = make_client();
            auto body = to_json(request).dump();
            auto res = client->Post("/node/search", body, "application/json");

            if (!res) {
                response.is_error = true;
                response.error_message =
                    "Connection failed to node " + std::to_string(node_id_) +
                    " (host " + host_ + ":" + std::to_string(port_) + ")";
                if (should_retry(response.error_message) &&
                    attempt + 1 < retry_policy_.max_attempts) {
                    continue;  // Retry
                }
                // Record transport failure in circuit breaker.
                circuit_breaker_->record_failure();
                return response;
            }

            if (res->status != 200) {
                try {
                    auto err = nlohmann::json::parse(res->body);
                    response.is_error = true;
                    response.error_message = err.value("error", "Server error");
                } catch (...) {
                    response.is_error = true;
                    response.error_message = "Server returned status " +
                        std::to_string(res->status);
                }
                // HTTP errors are application-level — do not retry.
                // Do NOT record in circuit breaker (not a transport failure).
                return response;
            }

            auto j = nlohmann::json::parse(res->body);
            last_attempt_succeeded = true;
            return shard_search_response_from_json(j);
        } catch (const std::exception& e) {
            response.is_error = true;
            response.error_message = std::string("Request failed: ") + e.what();
            if (should_retry(response.error_message) &&
                attempt + 1 < retry_policy_.max_attempts) {
                continue;  // Retry
            }
            // Record transport failure in circuit breaker.
            if (is_transport_failure(response.error_message)) {
                circuit_breaker_->record_failure();
            }
            return response;
        } catch (...) {
            response.is_error = true;
            response.error_message = "Unknown error communicating with node";
            circuit_breaker_->record_failure();
            return response;
        }
    }

    // Record failure if all retries exhausted.
    if (!last_attempt_succeeded) {
        circuit_breaker_->record_failure();
    }
    return response;
}

// ---------------------------------------------------------------------------
// Write operations
// ---------------------------------------------------------------------------

ShardWriteResponse RemoteNode::add_document(const ShardWriteRequest& request)
{
    ShardWriteResponse response;
    response.shard_id = request.shard_id;
    response.document_id = request.document_id;

    // Check circuit breaker before attempting request.
    if (!circuit_breaker_->should_allow_request()) {
        response.is_error = true;
        response.error_message = "Circuit breaker OPEN: node " +
            std::to_string(node_id_) + " is unavailable";
        return response;
    }

    try {
        auto client = make_client();
        auto body = to_json(request).dump();
        auto res = client->Post("/node/add", body, "application/json");

        if (!res) {
            response.is_error = true;
            response.error_message =
                "Connection failed to node " + std::to_string(node_id_);
            circuit_breaker_->record_failure();
            return response;
        }

        auto j = nlohmann::json::parse(res->body);
        circuit_breaker_->record_success();
        return shard_write_response_from_json(j);
    } catch (const std::exception& e) {
        response.is_error = true;
        response.error_message = std::string("Request failed: ") + e.what();
        circuit_breaker_->record_failure();
        return response;
    } catch (...) {
        response.is_error = true;
        response.error_message = "Unknown error communicating with node";
        circuit_breaker_->record_failure();
        return response;
    }
}

ShardWriteResponse RemoteNode::update_document(const ShardWriteRequest& request)
{
    ShardWriteResponse response;
    response.shard_id = request.shard_id;
    response.document_id = request.document_id;

    // Check circuit breaker before attempting request.
    if (!circuit_breaker_->should_allow_request()) {
        response.is_error = true;
        response.error_message = "Circuit breaker OPEN: node " +
            std::to_string(node_id_) + " is unavailable";
        return response;
    }

    try {
        auto client = make_client();
        auto body = to_json(request).dump();
        auto res = client->Post("/node/update", body, "application/json");

        if (!res) {
            response.is_error = true;
            response.error_message =
                "Connection failed to node " + std::to_string(node_id_);
            circuit_breaker_->record_failure();
            return response;
        }

        auto j = nlohmann::json::parse(res->body);
        circuit_breaker_->record_success();
        return shard_write_response_from_json(j);
    } catch (const std::exception& e) {
        response.is_error = true;
        response.error_message = std::string("Request failed: ") + e.what();
        circuit_breaker_->record_failure();
        return response;
    } catch (...) {
        response.is_error = true;
        response.error_message = "Unknown error communicating with node";
        circuit_breaker_->record_failure();
        return response;
    }
}

ShardRemoveResponse RemoteNode::remove_document(const ShardRemoveRequest& request)
{
    ShardRemoveResponse response;
    response.shard_id = request.shard_id;

    // Check circuit breaker before attempting request.
    if (!circuit_breaker_->should_allow_request()) {
        response.is_error = true;
        response.error_message = "Circuit breaker OPEN: node " +
            std::to_string(node_id_) + " is unavailable";
        return response;
    }

    try {
        auto client = make_client();
        auto body = to_json(request).dump();
        auto res = client->Post("/node/remove", body, "application/json");

        if (!res) {
            response.is_error = true;
            response.error_message =
                "Connection failed to node " + std::to_string(node_id_);
            circuit_breaker_->record_failure();
            return response;
        }

        auto j = nlohmann::json::parse(res->body);
        circuit_breaker_->record_success();
        return shard_remove_response_from_json(j);
    } catch (const std::exception& e) {
        response.is_error = true;
        response.error_message = std::string("Request failed: ") + e.what();
        circuit_breaker_->record_failure();
        return response;
    } catch (...) {
        response.is_error = true;
        response.error_message = "Unknown error communicating with node";
        circuit_breaker_->record_failure();
        return response;
    }
}

// ---------------------------------------------------------------------------
// Read operations
// ---------------------------------------------------------------------------

ShardGetResponse RemoteNode::get_document(const ShardGetRequest& request)
{
    ShardGetResponse response;
    response.shard_id = request.shard_id;
    response.document_id = request.document_id;

    // Check circuit breaker before attempting request.
    if (!circuit_breaker_->should_allow_request()) {
        response.found = false;
        return response;
    }

    for (std::size_t attempt = 0; attempt < retry_policy_.max_attempts; ++attempt) {
        if (attempt > 0) {
            auto delay = retry_policy_.delay_for_attempt(attempt);
            if (delay > 0) {
                sleep_ms(delay);
            }
        }

        try {
            auto client = make_client();
            auto body = to_json(request).dump();
            auto res = client->Post("/node/get", body, "application/json");

            if (!res) {
                response.found = false;
                // Network failure — retry if allowed.
                if (attempt + 1 < retry_policy_.max_attempts) {
                    continue;
                }
                circuit_breaker_->record_failure();
                return response;
            }

            auto j = nlohmann::json::parse(res->body);
            circuit_breaker_->record_success();
            return shard_get_response_from_json(j);
        } catch (...) {
            response.found = false;
            if (attempt + 1 < retry_policy_.max_attempts) {
                continue;
            }
            circuit_breaker_->record_failure();
            return response;
        }
    }

    circuit_breaker_->record_failure();
    return response;
}

ShardCountResponse RemoteNode::document_count(const ShardCountRequest& request)
{
    ShardCountResponse response;
    response.shard_id = request.shard_id;

    // Check circuit breaker before attempting request.
    if (!circuit_breaker_->should_allow_request()) {
        response.document_count = 0;
        response.is_error = true;
        response.error_message = "Circuit breaker OPEN: node " +
            std::to_string(node_id_) + " is unavailable";
        return response;
    }

    for (std::size_t attempt = 0; attempt < retry_policy_.max_attempts; ++attempt) {
        if (attempt > 0) {
            auto delay = retry_policy_.delay_for_attempt(attempt);
            if (delay > 0) {
                sleep_ms(delay);
            }
        }

        try {
            auto client = make_client();
            auto body = to_json(request).dump();
            auto res = client->Post("/node/count", body, "application/json");

            if (!res) {
                response.document_count = 0;
                response.is_error = true;
                response.error_message =
                    "Connection failed to node " + std::to_string(node_id_);
                if (should_retry(response.error_message) &&
                    attempt + 1 < retry_policy_.max_attempts) {
                    continue;
                }
                circuit_breaker_->record_failure();
                return response;
            }

            auto j = nlohmann::json::parse(res->body);
            circuit_breaker_->record_success();
            return shard_count_response_from_json(j);
        } catch (...) {
            response.document_count = 0;
            response.is_error = true;
            response.error_message = "Unknown error communicating with node";
            if (attempt + 1 < retry_policy_.max_attempts) {
                continue;
            }
            circuit_breaker_->record_failure();
            return response;
        }
    }

    circuit_breaker_->record_failure();
    return response;
}

// ---------------------------------------------------------------------------
// Persistence
// ---------------------------------------------------------------------------

bool RemoteNode::save_shard(std::size_t shard_id)
{
    // Check circuit breaker before attempting request.
    if (!circuit_breaker_->should_allow_request()) {
        return false;
    }

    try {
        auto client = make_client();
        auto body = shard_persistence_request_to_json(shard_id).dump();
        auto res = client->Post("/node/save", body, "application/json");

        if (!res) {
            circuit_breaker_->record_failure();
            return false;
        }

        auto j = nlohmann::json::parse(res->body);
        circuit_breaker_->record_success();
        return shard_persistence_response_from_json(j);
    } catch (...) {
        circuit_breaker_->record_failure();
        return false;
    }
}

bool RemoteNode::load_shard(std::size_t shard_id)
{
    // Check circuit breaker before attempting request.
    if (!circuit_breaker_->should_allow_request()) {
        return false;
    }

    try {
        auto client = make_client();
        auto body = shard_persistence_request_to_json(shard_id).dump();
        auto res = client->Post("/node/load", body, "application/json");

        if (!res) {
            circuit_breaker_->record_failure();
            return false;
        }

        auto j = nlohmann::json::parse(res->body);
        circuit_breaker_->record_success();
        return shard_persistence_response_from_json(j);
    } catch (...) {
        circuit_breaker_->record_failure();
        return false;
    }
}

} // namespace dse
