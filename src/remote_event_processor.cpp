// Distributed Search Engine - Remote Event Processor (Phase 19E).
//
// Applies document mutations received from Kafka consumer events.
// Uses LocalNode's apply_remote_*() methods for feedback-loop-safe
// mutations that bypass event publication and replication.

#include "remote_event_processor.h"

#include <string>

#include "document_event.h"
#include "local_node.h"
#include "message.h"

#include <nlohmann/json.hpp>

namespace dse {

// ---------------------------------------------------------------------------
// Construction
// ---------------------------------------------------------------------------

RemoteEventProcessor::RemoteEventProcessor(
    std::unordered_map<std::size_t, LocalNode*> nodes,
    std::size_t own_node_id)
    : nodes_(std::move(nodes))
    , own_node_id_(own_node_id)
{
}

// Backwards-compatible constructor: own_node_id defaults to 0.
RemoteEventProcessor::RemoteEventProcessor(
    std::unordered_map<std::size_t, LocalNode*> nodes)
    : RemoteEventProcessor(std::move(nodes), std::size_t{0})
{
}

// ---------------------------------------------------------------------------
// Primary entry point
// ---------------------------------------------------------------------------

bool RemoteEventProcessor::process(const std::string& topic,
                                    const std::string& payload)
{
    ++events_received_;

    Outcome outcome = Outcome::Failed;

    if (topic != topics::kDocumentMutations) {
        // Unknown topic — skipped. Return true so offset is committed.
        ++events_skipped_;
        return true;
    }

    nlohmann::json j;
    try {
        j = nlohmann::json::parse(payload);
    } catch (...) {
        ++malformed_events_;
        return true;
    }

    if (!j.contains("event_type")) {
        ++malformed_events_;
        return true;
    }

    std::string event_type = j["event_type"].get<std::string>();

    if (event_type == "document_indexed") {
        outcome = process_indexed(payload);
    } else if (event_type == "document_updated") {
        outcome = process_updated(payload);
    } else if (event_type == "document_removed") {
        outcome = process_removed(payload);
    } else {
        ++events_skipped_;
        return true;
    }

    switch (outcome) {
        case Outcome::Applied:
            ++events_processed_;
            return true;
        case Outcome::Skipped:
            // Events acknowledged without local mutation (e.g. self-events,
            // wrong shard, or duplicate no-ops) return true so Kafka commits
            // the offset, but are not counted as processed mutations.
            return true;
        case Outcome::Failed:
            ++events_failed_;
            return false;
    }

    return false;
}

// ---------------------------------------------------------------------------
// Indexed event
// ---------------------------------------------------------------------------

RemoteEventProcessor::Outcome RemoteEventProcessor::process_indexed(
    const std::string& payload)
{
    DocumentIndexedEvent event;
    if (!event_json::from_json(payload, event)) {
        ++malformed_events_;
        return Outcome::Skipped;
    }

    // Self-event check: if this event originated from this node,
    // skip it. The mutation was already applied locally when the
    // event was published. Acknowledged to commit offset.
    if (event.source_node_id == own_node_id_) {
        ++events_self_skipped_;
        ++events_skipped_;
        return Outcome::Skipped;
    }

    // Find a node that hosts this shard.
    LocalNode* node = find_node_for_shard(event.shard_id);
    if (!node) {
        // Shard not hosted by this node — skip.
        ++events_skipped_;
        return Outcome::Skipped;
    }

    // Apply the remote mutation directly to the shard.
    // This bypasses event publication and replication (feedback-loop safe).
    bool applied = false;
    if (node->apply_remote_indexed(event.shard_id, event.document_id,
                                    event.document_content, &applied)) {
        if (applied) {
            if (!node->save_shard(event.shard_id)) {
                return Outcome::Failed; // Transient local failure
            }
            ++index_operations_;
            return Outcome::Applied;
        } else {
            // Idempotent duplicate with identical content — no-op skipped.
            ++events_skipped_;
            return Outcome::Skipped;
        }
    }

    // Mutation failed (e.g., content conflict or invalid content).
    // Do NOT commit offset — Kafka will redeliver.
    return Outcome::Failed;
}

// ---------------------------------------------------------------------------
// Updated event
// ---------------------------------------------------------------------------

RemoteEventProcessor::Outcome RemoteEventProcessor::process_updated(
    const std::string& payload)
{
    DocumentUpdatedEvent event;
    if (!event_json::from_json(payload, event)) {
        ++malformed_events_;
        return Outcome::Skipped;
    }

    // Self-event check: skip events originating from this node.
    if (event.source_node_id == own_node_id_) {
        ++events_self_skipped_;
        ++events_skipped_;
        return Outcome::Skipped;
    }

    LocalNode* node = find_node_for_shard(event.shard_id);
    if (!node) {
        ++events_skipped_;
        return Outcome::Skipped;
    }

    bool applied = false;
    if (node->apply_remote_updated(event.shard_id, event.document_id,
                                    event.document_content, &applied)) {
        if (applied) {
            if (!node->save_shard(event.shard_id)) {
                return Outcome::Failed; // Transient local failure
            }
            ++update_operations_;
            return Outcome::Applied;
        } else {
            // Idempotent duplicate update with identical content.
            ++events_skipped_;
            return Outcome::Skipped;
        }
    }

    return Outcome::Failed;
}

// ---------------------------------------------------------------------------
// Removed event
// ---------------------------------------------------------------------------

RemoteEventProcessor::Outcome RemoteEventProcessor::process_removed(
    const std::string& payload)
{
    DocumentRemovedEvent event;
    if (!event_json::from_json(payload, event)) {
        ++malformed_events_;
        return Outcome::Skipped;
    }

    // Self-event check: skip events originating from this node.
    if (event.source_node_id == own_node_id_) {
        ++events_self_skipped_;
        ++events_skipped_;
        return Outcome::Skipped;
    }

    LocalNode* node = find_node_for_shard(event.shard_id);
    if (!node) {
        ++events_skipped_;
        return Outcome::Skipped;
    }

    bool applied = false;
    if (node->apply_remote_removed(event.shard_id, event.document_id, &applied)) {
        if (applied) {
            if (!node->save_shard(event.shard_id)) {
                return Outcome::Failed; // Transient local failure
            }
            ++remove_operations_;
            return Outcome::Applied;
        } else {
            // Already absent — idempotent no-op.
            ++events_skipped_;
            return Outcome::Skipped;
        }
    }

    return Outcome::Failed;
}

// ---------------------------------------------------------------------------
// Shard routing
// ---------------------------------------------------------------------------

LocalNode* RemoteEventProcessor::find_node_for_shard(std::size_t shard_id)
{
    for (auto& [node_id, node] : nodes_) {
        if (node->has_shard(shard_id)) {
            return node;
        }
    }
    return nullptr;
}

// ---------------------------------------------------------------------------
// Statistics
// ---------------------------------------------------------------------------

RemoteEventProcessor::Stats RemoteEventProcessor::stats() const
{
    Stats s;
    s.events_received    = events_received_.load();
    s.events_processed   = events_processed_.load();
    s.events_failed      = events_failed_.load();
    s.events_skipped     = events_skipped_.load();
    s.events_self_skipped = events_self_skipped_.load();
    s.malformed_events   = malformed_events_.load();
    s.index_operations   = index_operations_.load();
    s.update_operations  = update_operations_.load();
    s.remove_operations  = remove_operations_.load();
    return s;
}

void RemoteEventProcessor::reset_stats()
{
    events_received_    = 0;
    events_processed_   = 0;
    events_failed_      = 0;
    events_skipped_     = 0;
    events_self_skipped_ = 0;
    malformed_events_   = 0;
    index_operations_   = 0;
    update_operations_  = 0;
    remove_operations_  = 0;
}

} // namespace dse
