// Distributed Search Engine - Ingestion Service (Phase 6B-2).
//
// Implementation of the contract in src/ingestion_service.h:
//   - validate request fields (id, content);
//   - check for duplicate IDs via DocumentStore;
//   - reject empty/whitespace-only content;
//   - store raw text in DocumentStore;
//   - index tokens via InvertedIndex;
//   - report the number of distinct terms indexed.

#include "ingestion_service.h"

#include <cstddef>
#include <string>
#include <string_view>

#include "document_store.h"
#include "inverted_index.h"
#include "tokenizer.h"

namespace dse {

namespace {

// Check whether a string is empty or contains only whitespace.
bool is_blank(const std::string& s)
{
    for (const char c : s) {
        if (c != ' ' && c != '\t' && c != '\n' && c != '\r') {
            return false;
        }
    }
    return true;
}

} // namespace

IngestionService::IngestionService(InvertedIndex& index, DocumentStore& store)
    : index_(index)
    , store_(store)
{
}

bool IngestionService::validate_request(const IngestDocumentRequest& request)
{
    // Content must be non-empty and non-blank.
    if (request.content.empty() || is_blank(request.content)) {
        return false;
    }
    return true;
}

IngestDocumentResponse IngestionService::ingest(
    const IngestDocumentRequest& request)
{
    IngestDocumentResponse response;
    response.document_id = request.id;

    // Validate content.
    if (!validate_request(request)) {
        response.is_error = true;
        response.error_message =
            "Invalid request: content must be non-empty and non-whitespace";
        return response;
    }

    // Check for duplicate ID BEFORE storing anything.
    // This ensures atomicity: if the document already exists, neither
    // DocumentStore nor InvertedIndex is modified.
    if (store_.contains(request.id)) {
        response.is_error = true;
        response.error_message =
            "Document with id " + std::to_string(request.id) +
            " already exists";
        return response;
    }

    // Count distinct terms that will be indexed (for the response).
    // We count BEFORE adding to the index so we can report accurately.
    // The tokenizer produces the same tokens as InvertedIndex::add_document
    // will use, so the distinct count is the same.
    const auto tokens = dse::tokenize(request.content);

    // Use a set to count distinct terms (matching InvertedIndex behavior).
    std::unordered_set<std::string> distinct_terms;
    for (const auto& token : tokens) {
        distinct_terms.insert(token);
    }

    // Store the raw document text.
    store_.add(Document{request.id, request.content});

    // Index the document in the InvertedIndex.
    index_.add_document(request.id, request.content);

    response.terms_indexed = distinct_terms.size();
    return response;
}

} // namespace dse
