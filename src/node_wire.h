// Distributed Search Engine - Node Wire Format (Phase 12).
//
// JSON serialization for all NodeClient request/response types.
// Used by RemoteNode (client) and NodeServer (server) to translate
// between in-memory C++ structs and the HTTP/JSON transport.
//
// All functions are free functions in namespace dse.
// No state, no side effects beyond serialization.

#pragma once

#include <nlohmann/json.hpp>

#include "node_client.h"

namespace dse {

// ---------------------------------------------------------------------------
// Request serialization
// ---------------------------------------------------------------------------

nlohmann::json to_json(const ShardSearchRequest& req);
nlohmann::json to_json(const ShardWriteRequest& req);
nlohmann::json to_json(const ShardRemoveRequest& req);
nlohmann::json to_json(const ShardGetRequest& req);
nlohmann::json to_json(const ShardCountRequest& req);
nlohmann::json shard_persistence_request_to_json(std::size_t shard_id);

// ---------------------------------------------------------------------------
// Request deserialization
// ---------------------------------------------------------------------------

ShardSearchRequest shard_search_request_from_json(const nlohmann::json& j);
ShardWriteRequest shard_write_request_from_json(const nlohmann::json& j);
ShardRemoveRequest shard_remove_request_from_json(const nlohmann::json& j);
ShardGetRequest shard_get_request_from_json(const nlohmann::json& j);
ShardCountRequest shard_count_request_from_json(const nlohmann::json& j);
std::size_t shard_persistence_request_from_json(const nlohmann::json& j);

// ---------------------------------------------------------------------------
// Response serialization
// ---------------------------------------------------------------------------

nlohmann::json to_json(const ShardSearchResponse& resp);
nlohmann::json to_json(const ShardWriteResponse& resp);
nlohmann::json to_json(const ShardRemoveResponse& resp);
nlohmann::json to_json(const ShardGetResponse& resp);
nlohmann::json to_json(const ShardCountResponse& resp);
nlohmann::json shard_persistence_response_to_json(bool success);

// ---------------------------------------------------------------------------
// Response deserialization
// ---------------------------------------------------------------------------

ShardSearchResponse shard_search_response_from_json(const nlohmann::json& j);
ShardWriteResponse shard_write_response_from_json(const nlohmann::json& j);
ShardRemoveResponse shard_remove_response_from_json(const nlohmann::json& j);
ShardGetResponse shard_get_response_from_json(const nlohmann::json& j);
ShardCountResponse shard_count_response_from_json(const nlohmann::json& j);
bool shard_persistence_response_from_json(const nlohmann::json& j);

} // namespace dse
