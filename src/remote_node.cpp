// Distributed Search Engine - Remote Node (Phase 12).
//
// Implements NodeClient over HTTP/JSON transport. Each method creates
// a fresh httplib::Client, sends a POST request, and deserializes
// the response. Network failures are mapped to NodeClient errors.

#include "remote_node.h"

#include <httplib.h>
#include <nlohmann/json.hpp>

#include <cstddef>
#include <memory>
#include <string>
#include <utility>

#include "node_client.h"
#include "node_wire.h"

namespace dse {

// ---------------------------------------------------------------------------
// Construction
// ---------------------------------------------------------------------------

RemoteNode::RemoteNode(std::size_t node_id,
                       std::string host,
                       int port,
                       int timeout_seconds)
    : node_id_(node_id)
    , host_(std::move(host))
    , port_(port)
    , timeout_seconds_(timeout_seconds)
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

// ---------------------------------------------------------------------------
// Search
// ---------------------------------------------------------------------------

ShardSearchResponse RemoteNode::search(const ShardSearchRequest& request)
{
    ShardSearchResponse response;
    response.shard_id = request.shard_id;

    try {
        auto client = make_client();
        auto body = to_json(request).dump();
        auto res = client->Post("/node/search", body, "application/json");

        if (!res) {
            response.is_error = true;
            response.error_message =
                "Connection failed to node " + std::to_string(node_id_) +
                " (host " + host_ + ":" + std::to_string(port_) + ")";
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
            return response;
        }

        auto j = nlohmann::json::parse(res->body);
        return shard_search_response_from_json(j);
    } catch (const std::exception& e) {
        response.is_error = true;
        response.error_message = std::string("Request failed: ") + e.what();
        return response;
    } catch (...) {
        response.is_error = true;
        response.error_message = "Unknown error communicating with node";
        return response;
    }
}

// ---------------------------------------------------------------------------
// Write operations
// ---------------------------------------------------------------------------

ShardWriteResponse RemoteNode::add_document(const ShardWriteRequest& request)
{
    ShardWriteResponse response;
    response.shard_id = request.shard_id;
    response.document_id = request.document_id;

    try {
        auto client = make_client();
        auto body = to_json(request).dump();
        auto res = client->Post("/node/add", body, "application/json");

        if (!res) {
            response.is_error = true;
            response.error_message =
                "Connection failed to node " + std::to_string(node_id_);
            return response;
        }

        auto j = nlohmann::json::parse(res->body);
        return shard_write_response_from_json(j);
    } catch (const std::exception& e) {
        response.is_error = true;
        response.error_message = std::string("Request failed: ") + e.what();
        return response;
    } catch (...) {
        response.is_error = true;
        response.error_message = "Unknown error communicating with node";
        return response;
    }
}

ShardWriteResponse RemoteNode::update_document(const ShardWriteRequest& request)
{
    ShardWriteResponse response;
    response.shard_id = request.shard_id;
    response.document_id = request.document_id;

    try {
        auto client = make_client();
        auto body = to_json(request).dump();
        auto res = client->Post("/node/update", body, "application/json");

        if (!res) {
            response.is_error = true;
            response.error_message =
                "Connection failed to node " + std::to_string(node_id_);
            return response;
        }

        auto j = nlohmann::json::parse(res->body);
        return shard_write_response_from_json(j);
    } catch (const std::exception& e) {
        response.is_error = true;
        response.error_message = std::string("Request failed: ") + e.what();
        return response;
    } catch (...) {
        response.is_error = true;
        response.error_message = "Unknown error communicating with node";
        return response;
    }
}

ShardRemoveResponse RemoteNode::remove_document(const ShardRemoveRequest& request)
{
    ShardRemoveResponse response;
    response.shard_id = request.shard_id;

    try {
        auto client = make_client();
        auto body = to_json(request).dump();
        auto res = client->Post("/node/remove", body, "application/json");

        if (!res) {
            response.is_error = true;
            response.error_message =
                "Connection failed to node " + std::to_string(node_id_);
            return response;
        }

        auto j = nlohmann::json::parse(res->body);
        return shard_remove_response_from_json(j);
    } catch (const std::exception& e) {
        response.is_error = true;
        response.error_message = std::string("Request failed: ") + e.what();
        return response;
    } catch (...) {
        response.is_error = true;
        response.error_message = "Unknown error communicating with node";
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

    try {
        auto client = make_client();
        auto body = to_json(request).dump();
        auto res = client->Post("/node/get", body, "application/json");

        if (!res) {
            response.found = false;
            return response;
        }

        auto j = nlohmann::json::parse(res->body);
        return shard_get_response_from_json(j);
    } catch (...) {
        response.found = false;
        return response;
    }
}

ShardCountResponse RemoteNode::document_count(const ShardCountRequest& request)
{
    ShardCountResponse response;
    response.shard_id = request.shard_id;

    try {
        auto client = make_client();
        auto body = to_json(request).dump();
        auto res = client->Post("/node/count", body, "application/json");

        if (!res) {
            response.document_count = 0;
            response.is_error = true;
            response.error_message =
                "Connection failed to node " + std::to_string(node_id_);
            return response;
        }

        auto j = nlohmann::json::parse(res->body);
        return shard_count_response_from_json(j);
    } catch (...) {
        response.document_count = 0;
        response.is_error = true;
        response.error_message = "Unknown error communicating with node";
        return response;
    }
}

// ---------------------------------------------------------------------------
// Persistence
// ---------------------------------------------------------------------------

bool RemoteNode::save_shard(std::size_t shard_id)
{
    try {
        auto client = make_client();
        auto body = shard_persistence_request_to_json(shard_id).dump();
        auto res = client->Post("/node/save", body, "application/json");

        if (!res) {
            return false;
        }

        auto j = nlohmann::json::parse(res->body);
        return shard_persistence_response_from_json(j);
    } catch (...) {
        return false;
    }
}

bool RemoteNode::load_shard(std::size_t shard_id)
{
    try {
        auto client = make_client();
        auto body = shard_persistence_request_to_json(shard_id).dump();
        auto res = client->Post("/node/load", body, "application/json");

        if (!res) {
            return false;
        }

        auto j = nlohmann::json::parse(res->body);
        return shard_persistence_response_from_json(j);
    } catch (...) {
        return false;
    }
}

} // namespace dse
