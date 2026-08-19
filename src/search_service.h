// Distributed Search Engine - Search Service (Phase 5B-1).
//
// Business-logic boundary between the search core (Ranker, InvertedIndex)
// and the transport layer (HTTP, to be added in Phase 5B-2).
//
// The SearchService owns no state beyond a borrowed reference to the
// InvertedIndex. It composes the Ranker for query execution, validates
// requests, applies result limits, and returns structured responses.
//
// This separation ensures the search logic is testable without HTTP
// and reusable across transport layers.

#pragma once

#include <cstddef>
#include <cstdint>
#include <string>
#include <string_view>
#include <vector>

#include "inverted_index.h"
#include "ranker.h"

namespace dse {

// ---------------------------------------------------------------------------
// Request / response data structures
// ---------------------------------------------------------------------------

// Search mode: "and" (all terms must match) or "or" (any term may match).
enum class SearchMode {
    And,
    Or,
};

// A single search result: one document with its relevance score.
struct SearchResult {
    doc_id document_id;
    double score;

    friend bool operator==(const SearchResult&, const SearchResult&) = default;
};

// A structured search request from a client.
struct SearchRequest {
    std::string query;
    SearchMode mode = SearchMode::Or;
    std::size_t limit = 10;
};

// A structured search response, ready for serialization.
struct SearchResponse {
    std::string query;           // echo of the original query
    std::string mode;            // "and" or "or"
    std::size_t total = 0;       // number of matches before applying limit
    std::size_t limit = 0;       // the requested limit
    std::vector<SearchResult> results;
    bool is_error = false;       // true if the request was invalid
    std::string error_message;   // description of the error (if is_error)
};

// ---------------------------------------------------------------------------
// Search service
// ---------------------------------------------------------------------------

// Stateless search service. Owns nothing; borrows the InvertedIndex.
//
// Contract (ADR-005):
//   - the index must outlive this service;
//   - every call to search() is independent (no side effects);
//   - validation errors are returned as SearchResponse with an error flag
//     (is_error == true) rather than throwing exceptions;
//   - results are always sorted by score descending (via the Ranker);
//   - the limit is applied after ranking, truncating the result list;
//   - an empty query always returns an error response;
//   - invalid mode or limit values return error responses.
class SearchService {
public:
    explicit SearchService(const InvertedIndex& index);

    // Execute a search request. Returns a SearchResponse with is_error == true
    // if the request is invalid.
    SearchResponse search(const SearchRequest& request) const;

    // Validate a request without executing it. Returns true if the request
    // is valid for this service.
    static bool validate_request(const SearchRequest& request);

private:
    const InvertedIndex& index_;
};

// ---------------------------------------------------------------------------
// JSON serialization (intended format for Phase 5B-2 HTTP layer)
//
// The HTTP handler will convert SearchResponse to JSON using this structure:
//
// {
//   "query": "<string>",
//   "mode": "<and|or>",
//   "total": <number>,
//   "limit": <number>,
//   "results": [
//     { "document_id": <number>, "score": <number> },
//     ...
//   ]
// }
//
// Error responses:
// {
//   "error": "<description>"
// }
//
// This header does not depend on any JSON library. The HTTP layer
// (Phase 5B-2) will handle the actual serialization.
// ---------------------------------------------------------------------------

} // namespace dse
