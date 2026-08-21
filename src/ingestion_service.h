// Distributed Search Engine - Ingestion Service (Phase 6B-2, 7A-2).
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
// successful ingest() call persists the DocumentStore to disk.

#pragma once

#include <cstddef>
#include <cstdint>
#include <string>
#include <string_view>

#include "document_store.h"
#include "inverted_index.h"

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

// ---------------------------------------------------------------------------
// Ingestion service
// ---------------------------------------------------------------------------

// Ingestion service with optional persistence. Borrows InvertedIndex and
// DocumentStore (both mutable).
//
// Contract (Phase 6B-2 + 7A-2 + 8B):
//   - the index and store must outlive this service;
//   - every call to ingest() is independent (no side effects on failure);
//   - on success, the document is stored in BOTH DocumentStore AND
//     InvertedIndex atomically (if one fails, neither is modified);
//   - if data_path_ is non-empty, the DocumentStore is persisted to disk
//     after each successful ingest; if persistence fails, the response
//     reports an error and the in-memory state is unchanged;
//   - validation errors are returned as IngestDocumentResponse with
//     is_error == true rather than throwing exceptions;
//   - empty or whitespace-only content is rejected;
//   - duplicate document IDs return a 409-style conflict error;
//   - terms_indexed reflects the number of distinct terms added to
//     the index (not total token occurrences);
//   - thread-safe: concurrent ingest() calls are safe. The duplicate
//     check uses store_.add() as an atomic check-and-claim operation,
//     eliminating the TOCTOU race that existed when contains() and
//     add() were separate operations (Phase 8B).
class IngestionService {
public:
    // Construct without persistence (data_path empty = no persistence).
    IngestionService(InvertedIndex& index, DocumentStore& store);

    // Construct with persistence. If data_path is non-empty, each
    // successful ingest persists the DocumentStore to that file.
    IngestionService(InvertedIndex& index, DocumentStore& store,
                     const std::string& data_path);

    // Ingest a document. On success, returns a response with
    // is_error == false and the number of distinct terms indexed.
    // On failure, returns is_error == true with a description.
    IngestDocumentResponse ingest(const IngestDocumentRequest& request);

    // Validate a request without executing it.
    static bool validate_request(const IngestDocumentRequest& request);

private:
    InvertedIndex& index_;
    DocumentStore& store_;
    std::string data_path_;  // empty = no persistence
};

} // namespace dse
