// Distributed Search Engine - Ingestion Service (Phase 6B-2, 7A-2, 9).
//
// Implementation of the contract in src/ingestion_service.h:
//   - validate request fields (id, content);
//   - check for duplicate IDs via DocumentStore;
//   - reject empty/whitespace-only content;
//   - store raw text in DocumentStore;
//   - index tokens via InvertedIndex;
//   - persist DocumentStore to disk if data_path_ is configured;
//   - report the number of distinct terms indexed;
//   - update existing documents (Phase 9);
//   - delete existing documents (Phase 9).

#include "ingestion_service.h"

#include <algorithm>
#include <cstddef>
#include <string>
#include <string_view>
#include <unordered_set>

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
    , mutation_mutex_(std::make_unique<SharedMutex>())
{
}

IngestionService::IngestionService(InvertedIndex& index, DocumentStore& store,
                                   const std::string& data_path)
    : index_(index)
    , store_(store)
    , data_path_(data_path)
    , mutation_mutex_(std::make_unique<SharedMutex>())
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

bool IngestionService::persist_if_configured()
{
    if (data_path_.empty()) {
        return true;  // No persistence configured.
    }
    return store_.save(data_path_);
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

    // Service-level mutation coordination: serialize all create/update/delete
    // operations to prevent interleaving of multi-component mutations.
    std::unique_lock lock(*mutation_mutex_);

    // Atomic "check and claim": store_.add() returns false if a document
    // with this ID already exists.
    if (!store_.add(Document{request.id, request.content})) {
        response.is_error = true;
        response.error_message =
            "Document with id " + std::to_string(request.id) +
            " already exists";
        return response;
    }

    // Count distinct terms that will be indexed (for the response).
    const auto tokens = dse::tokenize(request.content);
    std::unordered_set<std::string> distinct_terms;
    for (const auto& token : tokens) {
        distinct_terms.insert(token);
    }

    // Index the document in the InvertedIndex.
    index_.add_document(request.id, request.content);

    // Persist to disk if persistence is configured.
    if (!persist_if_configured()) {
        response.is_error = true;
        response.error_message =
            "Document indexed but failed to persist to disk";
        return response;
    }

    response.terms_indexed = distinct_terms.size();
    return response;
}

UpdateDocumentResponse IngestionService::update(
    const UpdateDocumentRequest& request)
{
    UpdateDocumentResponse response;
    response.document_id = request.id;

    // Validate content.
    if (request.content.empty() ||
        std::all_of(request.content.begin(), request.content.end(),
                    [](char c) { return c == ' ' || c == '\t' ||
                                       c == '\n' || c == '\r'; })) {
        response.is_error = true;
        response.error_message =
            "Invalid request: content must be non-empty and non-whitespace";
        return response;
    }

    // Service-level mutation coordination.
    std::unique_lock lock(*mutation_mutex_);

    // Verify the document exists in the DocumentStore.
    if (!store_.contains(request.id)) {
        response.is_error = true;
        response.error_message =
            "Document with id " + std::to_string(request.id) +
            " not found";
        return response;
    }

    // Remove the old index representation.
    index_.remove_document(request.id);

    // Replace the content in DocumentStore.
    store_.update(Document{request.id, request.content});

    // Add the new index representation.
    index_.add_document(request.id, request.content);

    // Count distinct terms for the response.
    const auto tokens = dse::tokenize(request.content);
    std::unordered_set<std::string> distinct_terms;
    for (const auto& token : tokens) {
        distinct_terms.insert(token);
    }

    // Persist to disk if persistence is configured.
    if (!persist_if_configured()) {
        response.is_error = true;
        response.error_message =
            "Document updated but failed to persist to disk";
        return response;
    }

    response.terms_indexed = distinct_terms.size();
    return response;
}

DeleteDocumentResponse IngestionService::remove(doc_id id)
{
    DeleteDocumentResponse response;

    // Service-level mutation coordination.
    std::unique_lock lock(*mutation_mutex_);

    // Verify the document exists in the DocumentStore.
    if (!store_.contains(id)) {
        response.is_error = true;
        response.error_message =
            "Document with id " + std::to_string(id) + " not found";
        return response;
    }

    // Remove from the InvertedIndex.
    index_.remove_document(id);

    // Remove from the DocumentStore.
    store_.remove(id);

    // Persist to disk if persistence is configured.
    if (!persist_if_configured()) {
        response.is_error = true;
        response.error_message =
            "Document deleted but failed to persist to disk";
        return response;
    }

    return response;
}

} // namespace dse
