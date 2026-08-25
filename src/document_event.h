// Distributed Search Engine - Document Events (Phase 18C).
//
// Domain events published after successful document mutations.
// These events flow through the MessageBroker abstraction and can
// be consumed by downstream services (search indexing, analytics,
// auditing, etc.).
//
// Event lifecycle:
//   Document mutation succeeds
//       → event constructed with document_id + shard_id
//       → event serialized to JSON payload
//       → published to the appropriate topic
//
// One event per LOGICAL user operation, regardless of replication factor.
// A successful R=2 ingest produces ONE DocumentIndexed event, not two.
//
// Events are only published on SUCCESS. Failed mutations produce no events.

#pragma once

#include <cstddef>
#include <cstdint>
#include <string>

#include "inverted_index.h"  // for doc_id

namespace dse {

// ---------------------------------------------------------------------------
// Event types
// ---------------------------------------------------------------------------

// Document successfully indexed (new document ingested).
struct DocumentIndexedEvent {
    doc_id document_id;
    std::size_t shard_id;
};

// Document successfully updated.
struct DocumentUpdatedEvent {
    doc_id document_id;
    std::size_t shard_id;
};

// Document successfully removed.
struct DocumentRemovedEvent {
    doc_id document_id;
    std::size_t shard_id;
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
