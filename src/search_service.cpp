// Distributed Search Engine - Search Service (Phase 5B-1).
//
// Implementation of the contract in ADR-005:
//   - validate request fields (query, mode, limit);
//   - normalize the query (trim whitespace);
//   - delegate to Ranker for AND/OR scoring;
//   - apply result limit after ranking;
//   - return structured SearchResponse.

#include "search_service.h"

#include <algorithm>
#include <cctype>
#include <cstddef>
#include <string>
#include <string_view>
#include <vector>

namespace dse {

namespace {

// Trim leading and trailing whitespace from a string.
std::string trim(const std::string& s)
{
    const auto start = s.find_first_not_of(" \t\n\r");
    if (start == std::string::npos) {
        return {};
    }
    const auto end = s.find_last_not_of(" \t\n\r");
    return s.substr(start, end - start + 1);
}

// Convert a SearchMode enum to its string representation.
const char* mode_to_string(SearchMode mode)
{
    return mode == SearchMode::And ? "and" : "or";
}

} // namespace

SearchService::SearchService(const InvertedIndex& index)
    : index_(index)
{
}

bool SearchService::validate_request(const SearchRequest& request)
{
    // Query must be non-empty after trimming.
    if (trim(request.query).empty()) {
        return false;
    }

    // Limit must be at least 1.
    if (request.limit < 1) {
        return false;
    }

    // Mode is an enum, so it is always valid by construction.
    // This check exists for future-proofing if mode becomes a string.
    return true;
}

SearchResponse SearchService::search(const SearchRequest& request) const
{
    SearchResponse response;
    response.query = request.query;
    response.mode = mode_to_string(request.mode);
    response.limit = request.limit;

    // Validate the request.
    if (!validate_request(request)) {
        response.is_error = true;
        response.error_message = "Invalid request: empty query or limit < 1";
        return response;
    }

    // Normalize the query for the ranker.
    const std::string normalized_query = trim(request.query);

    // Invoke the ranker.
    const Ranker ranker(index_);
    std::vector<RankedResult> ranked;

    if (request.mode == SearchMode::And) {
        ranked = ranker.ranked_and(normalized_query);
    } else {
        ranked = ranker.ranked_or(normalized_query);
    }

    // Total matches before applying limit.
    response.total = ranked.size();

    // Apply limit: take the first N results (already sorted by score desc).
    const std::size_t count = std::min(response.limit, ranked.size());
    response.results.reserve(count);

    for (std::size_t i = 0; i < count; ++i) {
        response.results.push_back({
            ranked[i].document_id,
            ranked[i].score
        });
    }

    return response;
}

} // namespace dse
