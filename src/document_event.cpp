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
    j["event_id"]    = event.event_id;
    j["event_type"]  = "document_indexed";
    j["document_id"] = event.document_id;
    j["shard_id"]    = event.shard_id;
    return j.dump();
}

std::string to_json(const DocumentUpdatedEvent& event)
{
    nlohmann::json j;
    j["event_id"]    = event.event_id;
    j["event_type"]  = "document_updated";
    j["document_id"] = event.document_id;
    j["shard_id"]    = event.shard_id;
    return j.dump();
}

std::string to_json(const DocumentRemovedEvent& event)
{
    nlohmann::json j;
    j["event_id"]    = event.event_id;
    j["event_type"]  = "document_removed";
    j["document_id"] = event.document_id;
    j["shard_id"]    = event.shard_id;
    return j.dump();
}

} // namespace event_json

} // namespace dse
