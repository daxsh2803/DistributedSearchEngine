// Distributed Search Engine - Document Events (Phase 18C/18E).
//
// Domain events published after successful document mutations.
// These events flow through the EventStore -> EventDispatcher ->
// MessageBroker and can be consumed by downstream services.
//
// Event lifecycle:
//   Document mutation succeeds
//       -> EventStore.create_event() assigns stable event_id
//       -> event serialized to JSON payload (includes event_id)
//       -> published to the appropriate topic
//
// One event per LOGICAL user operation, regardless of replication factor.
// A successful R=2 ingest produces ONE DocumentIndexed event, not two.
//
// Events are only published on SUCCESS. Failed mutations produce no events.

#pragma once

#include <cstddef>
#include <cstdint>
#include <string>

#include "event_store.h"   // for EventId
#include "inverted_index.h"  // for doc_id

namespace dse {

// ---------------------------------------------------------------------------
// Event types
// ---------------------------------------------------------------------------

// Document successfully indexed (new document ingested).
struct DocumentIndexedEvent {
    EventId event_id = 0;
    doc_id document_id = 0;
    std::size_t shard_id = 0;
};

// Document successfully updated.
struct DocumentUpdatedEvent {
    EventId event_id = 0;
    doc_id document_id = 0;
    std::size_t shard_id = 0;
};

// Document successfully removed.
struct DocumentRemovedEvent {
    EventId event_id = 0;
    doc_id document_id = 0;
    std::size_t shard_id = 0;
};

// ---------------------------------------------------------------------------
// Topics (centralized naming)
// ---------------------------------------------------------------------------

namespace topics {
    inline const std::string kDocumentIndexed  = "documents.indexed";
    inline const std::string kDocumentUpdated  = "documents.updated";
    inline const std::string kDocumentRemoved  = "documents.removed";
} // namespace topics

// ---------------------------------------------------------------------------
// JSON serialization
// ---------------------------------------------------------------------------

namespace event_json {

// Serialize DocumentIndexedEvent to a JSON payload string.
std::string to_json(const DocumentIndexedEvent& event);

// Serialize DocumentUpdatedEvent to a JSON payload string.
std::string to_json(const DocumentUpdatedEvent& event);

// Serialize DocumentRemovedEvent to a JSON payload string.
std::string to_json(const DocumentRemovedEvent& event);

} // namespace event_json

} // namespace dse
