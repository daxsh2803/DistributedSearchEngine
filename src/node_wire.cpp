// Distributed Search Engine - Node Wire Format (Phase 12).
//
// JSON serialization for all NodeClient request/response types.
// Used by RemoteNode and NodeServer for HTTP/JSON transport.

#include "node_wire.h"

#include <cstddef>
#include <cstdint>
#include <string>
#include <vector>

#include "inverted_index.h"  // for doc_id

namespace dse {

// ---------------------------------------------------------------------------
// Request serialization
// ---------------------------------------------------------------------------

nlohmann::json to_json(const ShardSearchRequest& req)
{
    nlohmann::json j;
    j["shard_id"] = req.shard_id;
    j["terms"] = req.terms;
    return j;
}

nlohmann::json to_json(const ShardWriteRequest& req)
{
    nlohmann::json j;
    j["shard_id"] = req.shard_id;
    j["document_id"] = req.document_id;
    j["content"] = req.content;
    return j;
}

nlohmann::json to_json(const ShardRemoveRequest& req)
{
    nlohmann::json j;
    j["shard_id"] = req.shard_id;
    j["document_id"] = req.document_id;
    return j;
}

nlohmann::json to_json(const ShardGetRequest& req)
{
    nlohmann::json j;
    j["shard_id"] = req.shard_id;
    j["document_id"] = req.document_id;
    return j;
}

nlohmann::json to_json(const ShardCountRequest& req)
{
    nlohmann::json j;
    j["shard_id"] = req.shard_id;
    return j;
}

nlohmann::json shard_persistence_request_to_json(std::size_t shard_id)
{
    nlohmann::json j;
    j["shard_id"] = shard_id;
    return j;
}

// ---------------------------------------------------------------------------
// Request deserialization
// ---------------------------------------------------------------------------

ShardSearchRequest shard_search_request_from_json(const nlohmann::json& j)
{
    ShardSearchRequest req;
    req.shard_id = j.at("shard_id").get<std::size_t>();
    req.terms = j.at("terms").get<std::vector<std::string>>();
    return req;
}

ShardWriteRequest shard_write_request_from_json(const nlohmann::json& j)
{
    ShardWriteRequest req;
    req.shard_id = j.at("shard_id").get<std::size_t>();
    req.document_id = j.at("document_id").get<doc_id>();
    req.content = j.at("content").get<std::string>();
    return req;
}

ShardRemoveRequest shard_remove_request_from_json(const nlohmann::json& j)
{
    ShardRemoveRequest req;
    req.shard_id = j.at("shard_id").get<std::size_t>();
    req.document_id = j.at("document_id").get<doc_id>();
    return req;
}

ShardGetRequest shard_get_request_from_json(const nlohmann::json& j)
{
    ShardGetRequest req;
    req.shard_id = j.at("shard_id").get<std::size_t>();
    req.document_id = j.at("document_id").get<doc_id>();
    return req;
}

ShardCountRequest shard_count_request_from_json(const nlohmann::json& j)
{
    ShardCountRequest req;
    req.shard_id = j.at("shard_id").get<std::size_t>();
    return req;
}

std::size_t shard_persistence_request_from_json(const nlohmann::json& j)
{
    return j.at("shard_id").get<std::size_t>();
}

// ---------------------------------------------------------------------------
// Response serialization
// ---------------------------------------------------------------------------

nlohmann::json to_json(const ShardSearchResponse& resp)
{
    nlohmann::json j;
    j["shard_id"] = resp.shard_id;
    j["local_document_count"] = resp.local_document_count;
    j["is_error"] = resp.is_error;
    j["error_message"] = resp.error_message;

    nlohmann::json postings = nlohmann::json::array();
    for (const auto& term_postings : resp.terms_postings) {
        nlohmann::json arr = nlohmann::json::array();
        for (const auto& p : term_postings) {
            arr.push_back({
                {"document_id", p.document_id},
                {"term_frequency", p.term_frequency}
            });
        }
        postings.push_back(arr);
    }
    j["terms_postings"] = postings;

    return j;
}

nlohmann::json to_json(const ShardWriteResponse& resp)
{
    nlohmann::json j;
    j["shard_id"] = resp.shard_id;
    j["document_id"] = resp.document_id;
    j["terms_indexed"] = resp.terms_indexed;
    j["is_error"] = resp.is_error;
    j["error_message"] = resp.error_message;
    return j;
}

nlohmann::json to_json(const ShardRemoveResponse& resp)
{
    nlohmann::json j;
    j["shard_id"] = resp.shard_id;
    j["is_error"] = resp.is_error;
    j["error_message"] = resp.error_message;
    return j;
}

nlohmann::json to_json(const ShardGetResponse& resp)
{
    nlohmann::json j;
    j["shard_id"] = resp.shard_id;
    j["document_id"] = resp.document_id;
    j["content"] = resp.content;
    j["found"] = resp.found;
    return j;
}

nlohmann::json to_json(const ShardCountResponse& resp)
{
    nlohmann::json j;
    j["shard_id"] = resp.shard_id;
    j["document_count"] = resp.document_count;
    return j;
}

nlohmann::json shard_persistence_response_to_json(bool success)
{
    nlohmann::json j;
    j["success"] = success;
    return j;
}

// ---------------------------------------------------------------------------
// Response deserialization
// ---------------------------------------------------------------------------

ShardSearchResponse shard_search_response_from_json(const nlohmann::json& j)
{
    ShardSearchResponse resp;
    resp.shard_id = j.at("shard_id").get<std::size_t>();
    resp.local_document_count = j.at("local_document_count").get<std::size_t>();
    resp.is_error = j.at("is_error").get<bool>();
    resp.error_message = j.at("error_message").get<std::string>();

    resp.terms_postings.clear();
    if (j.contains("terms_postings") && j["terms_postings"].is_array()) {
        for (const auto& term_arr : j["terms_postings"]) {
            std::vector<NodeTermPosting> term_postings;
            for (const auto& p : term_arr) {
                term_postings.push_back({
                    p.at("document_id").get<doc_id>(),
                    p.at("term_frequency").get<std::uint32_t>()
                });
            }
            resp.terms_postings.push_back(std::move(term_postings));
        }
    }

    return resp;
}

ShardWriteResponse shard_write_response_from_json(const nlohmann::json& j)
{
    ShardWriteResponse resp;
    resp.shard_id = j.at("shard_id").get<std::size_t>();
    resp.document_id = j.at("document_id").get<doc_id>();
    resp.terms_indexed = j.at("terms_indexed").get<std::size_t>();
    resp.is_error = j.at("is_error").get<bool>();
    resp.error_message = j.at("error_message").get<std::string>();
    return resp;
}

ShardRemoveResponse shard_remove_response_from_json(const nlohmann::json& j)
{
    ShardRemoveResponse resp;
    resp.shard_id = j.at("shard_id").get<std::size_t>();
    resp.is_error = j.at("is_error").get<bool>();
    resp.error_message = j.at("error_message").get<std::string>();
    return resp;
}

ShardGetResponse shard_get_response_from_json(const nlohmann::json& j)
{
    ShardGetResponse resp;
    resp.shard_id = j.at("shard_id").get<std::size_t>();
    resp.document_id = j.at("document_id").get<doc_id>();
    resp.content = j.at("content").get<std::string>();
    resp.found = j.at("found").get<bool>();
    return resp;
}

ShardCountResponse shard_count_response_from_json(const nlohmann::json& j)
{
    ShardCountResponse resp;
    resp.shard_id = j.at("shard_id").get<std::size_t>();
    resp.document_count = j.at("document_count").get<std::size_t>();
    return resp;
}

bool shard_persistence_response_from_json(const nlohmann::json& j)
{
    return j.at("success").get<bool>();
}

} // namespace dse
