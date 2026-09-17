// Distributed Search Engine - Ranker (Phase 4).
//
// Public API only. Design per docs/decisions/ADR-004-ranking-design.md:
// TF-IDF scoring over the inverted index, returning documents sorted
// by relevance score.

#pragma once

#include <cmath>
#include <cstdint>
#include <string_view>
#include <unordered_map>
#include <vector>

#include "inverted_index.h"

namespace dse {

// A document paired with its TF-IDF relevance score for a query.
struct RankedResult {
    doc_id document_id;
    double score;

    friend bool operator==(const RankedResult&, const RankedResult&) = default;
};

// TF-IDF ranker.
//
// Borrows an InvertedIndex and scores documents against a query using
// standard TF-IDF (term frequency × inverse document frequency).
//
// Contract (ADR-004):
//   - the index is borrowed and must outlive this ranker;
//   - queries are tokenized with dse::tokenize (Phase 1) and deduplicated;
//   - results are owning, sorted by score descending;
//   - tie-breaking: doc_id ascending (deterministic);
//   - an empty query returns an empty result;
//   - a missing term in AND poisons the result; in OR it is ignored;
//   - scoring formula: tfidf(t,d) = tf(t,d) × ln(N / df(t)).
class Ranker {
public:
    explicit Ranker(const InvertedIndex& index);

    // Documents containing ALL query terms, scored by TF-IDF, sorted by
    // score descending. A missing term returns an empty result.
    std::vector<RankedResult> ranked_and(std::string_view query) const;

    // Documents containing AT LEAST ONE query term, scored by TF-IDF,
    // sorted by score descending. Missing terms are ignored.
    std::vector<RankedResult> ranked_or(std::string_view query) const;

private:
    const InvertedIndex* index_;  // borrowed; non-null
};

} // namespace dse
