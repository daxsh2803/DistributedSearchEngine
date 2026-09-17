// Distributed Search Engine - Ingestion Service (Phase 6B-2, 7A-2, 9).
//
// Business-logic boundary for the write path. Coordinates between
// DocumentStore (raw text) and InvertedIndex (term→postings), validating
// requests and ensuring consistency between the two storage components.
//
// This mirrors SearchService (Phase 5B-1) for the read path:
//   SearchService → reads from InvertedIndex
//   IngestionService → writes to DocumentStore + InvertedIndex
//
// The IngestionService owns no storage. It borrows both the
// InvertedIndex (mutable) and DocumentStore (mutable).
//
// Phase 7A-2 adds persistence: if a data_path is configured, each
// successful ingest/update/delete call persists the DocumentStore to disk.
//
// Phase 9 adds update() and delete() for document lifecycle. Service-level
// mutation_coordination_mutex_ ensures create/update/delete operations are
// serialized, preventing interleaving of multi-component mutations.

#pragma once

#include <cstddef>
#include <cstdint>
#include <memory>
#include <mutex>
#include <string>
#include <string_view>

#include "document_store.h"
#include "inverted_index.h"
#include "shared_mutex.h"

namespace dse {

// ---------------------------------------------------------------------------
// Request / response data structures
// ---------------------------------------------------------------------------

// A structured ingestion request from a client.
struct IngestDocumentRequest {
    doc_id id;
    std::string content;
};

// A structured ingestion response, ready for serialization.
struct IngestDocumentResponse {
    doc_id document_id = 0;
    std::size_t terms_indexed = 0;
    bool is_error = false;
    std::string error_message;
};

// A structured update request.
struct UpdateDocumentRequest {
    doc_id id;
    std::string content;
};

// A structured update response.
struct UpdateDocumentResponse {
    doc_id document_id = 0;
    std::size_t terms_indexed = 0;
    bool is_error = false;
    std::string error_message;
};

// A structured delete response.
struct DeleteDocumentResponse {
    bool is_error = false;
    std::string error_message;
};

// ---------------------------------------------------------------------------
// Ingestion service
// ---------------------------------------------------------------------------

// Ingestion service with optional persistence. Borrows InvertedIndex and
// DocumentStore (both mutable).
//
// Contract (Phase 6B-2 + 7A-2 + 8B + 9):
//   - the index and store must outlive this service;
//   - every call to ingest/update/delete is independent (no side effects on
//     failure);
//   - on success, the document is stored/updated/removed in BOTH
//     DocumentStore AND InvertedIndex consistently;
//   - if data_path_ is non-empty, the DocumentStore is persisted to disk
//     after each successful operation; if persistence fails, the response
//     reports an error but the in-memory mutation may have succeeded;
//   - validation errors are returned as response structs with
//     is_error == true rather than throwing exceptions;
//   - empty or whitespace-only content is rejected for create/update;
//   - duplicate document IDs return a 409-style conflict error;
//   - terms_indexed reflects the number of distinct terms added/changed;
//   - thread-safe: mutation operations (ingest, update, delete) are
//     serialized by mutation_mutex_ to prevent interleaving of multi-
//     component operations. Read operations (search) remain concurrent.
class IngestionService {
public:
    // Construct without persistence (data_path empty = no persistence).
    IngestionService(InvertedIndex& index, DocumentStore& store);

    // Construct with persistence. If data_path is non-empty, each
    // successful operation persists the DocumentStore to that file.
    IngestionService(InvertedIndex& index, DocumentStore& store,
                     const std::string& data_path);

    // Create a new document. On success, returns a response with
    // is_error == false and the number of distinct terms indexed.
    // On failure, returns is_error == true with a description.
    IngestDocumentResponse ingest(const IngestDocumentRequest& request);

    // Update an existing document's content. On success, removes old index
    // representation and adds new one, then persists.
    UpdateDocumentResponse update(const UpdateDocumentRequest& request);

    // Delete an existing document. Removes from index and store, then
    // persists.
    DeleteDocumentResponse remove(doc_id id);

    // Validate a request without executing it.
    static bool validate_request(const IngestDocumentRequest& request);

private:
    // Persist the DocumentStore to disk if data_path_ is configured.
    // Returns true on success or if persistence is not configured.
    bool persist_if_configured();

    InvertedIndex& index_;
    DocumentStore& store_;
    std::string data_path_;  // empty = no persistence

    // Service-level mutation coordination mutex.
    // Serializes create/update/delete operations to prevent interleaving
    // of multi-component mutations. Search operations do NOT acquire this
    // mutex, so concurrent reads remain unblocked.
    mutable std::unique_ptr<SharedMutex> mutation_mutex_;
};

} // namespace dse
