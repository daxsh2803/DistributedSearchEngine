// Distributed Search Engine - Document Events (Phase 18C/18E).
//
// JSON serialization for domain events published after successful
// document mutations. Includes event_id for reliable delivery tracking.

#include "document_event.h"

#include <nlohmann/json.hpp>

namespace dse {

namespace event_json {

std::string to_json(const DocumentIndexedEvent& event)
{
    nlohmann::json j;
    j["event_id"]        = event.event_id;
    j["event_type"]      = "document_indexed";
    j["document_id"]     = event.document_id;
    j["shard_id"]        = event.shard_id;
    j["source_node_id"]  = event.source_node_id;
    j["document_content"] = event.document_content;
    return j.dump();
}

std::string to_json(const DocumentUpdatedEvent& event)
{
    nlohmann::json j;
    j["event_id"]        = event.event_id;
    j["event_type"]      = "document_updated";
    j["document_id"]     = event.document_id;
    j["shard_id"]        = event.shard_id;
    j["source_node_id"]  = event.source_node_id;
    j["document_content"] = event.document_content;
    return j.dump();
}

std::string to_json(const DocumentRemovedEvent& event)
{
    nlohmann::json j;
    j["event_id"]        = event.event_id;
    j["event_type"]      = "document_removed";
    j["document_id"]     = event.document_id;
    j["shard_id"]        = event.shard_id;
    j["source_node_id"]  = event.source_node_id;
    return j.dump();
}

// ---------------------------------------------------------------------------
// Deserialization (Phase 19E)
// ---------------------------------------------------------------------------

bool from_json(const std::string& json_str, DocumentIndexedEvent& event)
{
    try {
        auto j = nlohmann::json::parse(json_str);
        // Require event_id, document_id, and shard_id.
        if (!j.contains("event_id") || !j.contains("document_id") ||
            !j.contains("shard_id")) {
            return false;
        }
        event.event_id         = j["event_id"].get<EventId>();
        event.document_id      = j["document_id"].get<doc_id>();
        event.shard_id         = j["shard_id"].get<std::size_t>();
        event.source_node_id   = j.value("source_node_id", std::size_t{0});
        event.document_content = j.value("document_content", std::string{});
        return true;
    } catch (...) {
        return false;
    }
}

bool from_json(const std::string& json_str, DocumentUpdatedEvent& event)
{
    try {
        auto j = nlohmann::json::parse(json_str);
        if (!j.contains("event_id") || !j.contains("document_id") ||
            !j.contains("shard_id")) {
            return false;
        }
        event.event_id         = j["event_id"].get<EventId>();
        event.document_id      = j["document_id"].get<doc_id>();
        event.shard_id         = j["shard_id"].get<std::size_t>();
        event.source_node_id   = j.value("source_node_id", std::size_t{0});
        event.document_content = j.value("document_content", std::string{});
        return true;
    } catch (...) {
        return false;
    }
}

bool from_json(const std::string& json_str, DocumentRemovedEvent& event)
{
    try {
        auto j = nlohmann::json::parse(json_str);
        if (!j.contains("event_id") || !j.contains("document_id") ||
            !j.contains("shard_id")) {
            return false;
        }
        event.event_id       = j["event_id"].get<EventId>();
        event.document_id    = j["document_id"].get<doc_id>();
        event.shard_id       = j["shard_id"].get<std::size_t>();
        event.source_node_id = j.value("source_node_id", std::size_t{0});
        return true;
    } catch (...) {
        return false;
    }
}

} // namespace event_json

} // namespace dse
