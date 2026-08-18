// Distributed Search Engine - Query Processor (Phase 3B).
//
// Public API only. Design per docs/decisions/ADR-003-query-processing-design.md:
// two pure two-pointer merge primitives over sorted postings lists, plus a
// QueryProcessor class that composes tokenization, index lookup, and merging
// into AND/OR Boolean queries.

#pragma once

#include <cstdint>
#include <span>
#include <string_view>
#include <vector>

#include "inverted_index.h"

namespace dse {

// Pure two-pointer merge primitives over sorted postings lists.
//
// Precondition: both inputs sorted by document_id (the Phase 2 invariant).
// Output: an owning vector of docIDs, sorted ascending, deduplicated.
// Complexity: O(lhs.size() + rhs.size()).
std::vector<doc_id> intersect(std::span<const Posting> lhs,
                              std::span<const Posting> rhs);

std::vector<doc_id> merge_union(std::span<const Posting> lhs,
                                std::span<const Posting> rhs);

// Boolean query processor.
//
// Tokenizes a query with dse::tokenize (Phase 1), looks up each distinct
// term in the borrowed index (Phase 2), and combines the postings lists
// with the two-pointer primitives above.
//
// Contract (ADR-003):
//   - the index is borrowed and must outlive this processor;
//   - queries are read during the call only and never stored;
//   - results are owning, sorted ascending, deduplicated;
//   - an empty query (zero terms) returns an empty result;
//   - a missing term makes AND empty and is ignored by OR.
class QueryProcessor {
public:
    explicit QueryProcessor(const InvertedIndex& index);

    // Documents containing ALL query terms, sorted by docID.
    // A missing term poisons the AND (returns empty).
    std::vector<doc_id> and_query(std::string_view query) const;

    // Documents containing AT LEAST ONE query term, sorted by docID.
    // Missing terms are simply ignored.
    std::vector<doc_id> or_query(std::string_view query) const;

private:
    const InvertedIndex* index_;  // borrowed; non-null, see constructor
};

} // namespace dse
