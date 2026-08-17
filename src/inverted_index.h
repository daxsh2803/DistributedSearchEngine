// Distributed Search Engine - Inverted Index (Phase 2B).
//
// Public API only. Design per docs/decisions/ADR-002-inverted-index-design.md:
// an in-memory, deterministic map from terms to sorted postings lists,
// consuming dse::tokenize output from Phase 1.

#pragma once

#include <cstddef>
#include <cstdint>
#include <span>
#include <string>
#include <string_view>
#include <unordered_map>
#include <unordered_set>
#include <vector>

namespace dse {

// Uniquely identifies a document in the index. Compact, comparable, and
// abstract: the index tracks relationships between IDs, not document content.
using doc_id = std::uint32_t;

// One (document, term-frequency) pair inside a postings list: the term occurs
// `term_frequency` times in document `document_id`.
struct Posting {
    doc_id document_id;
    std::uint32_t term_frequency;

    friend bool operator==(const Posting&, const Posting&) = default;
};

// In-memory inverted index.
//
// Contract (ADR-002):
//   - a term is exactly a token produced by dse::tokenize (lowercased, ASCII);
//   - each document ID may be added at most once (precondition; re-adding is
//     a contract violation, asserted in debug builds);
//   - postings lists are always sorted by document ID;
//   - duplicates within a document are aggregated into term frequencies;
//   - postings(term) returns an empty span for unknown terms (a postings
//     list is never empty for a term that exists);
//   - a span returned by postings() is valid only until the next modification
//     of the index (any add_document call) or its destruction;
//   - identical insertion sequences produce identical indices (deterministic);
//   - vocabulary iteration order is unspecified (do not depend on it).
//
// Complexity: O(1) average lookup; O(T) amortized per add_document for a
// document with T tokens; O(distinct term-document pairs) space.
class InvertedIndex {
public:
    // Tokenizes `text` with dse::tokenize, counts term frequencies, and
    // merges (id, count) postings into the index, keeping every postings
    // list sorted by document ID.
    void add_document(doc_id id, std::string_view text);

    // Postings list for `term`, sorted by document ID, or empty if the term
    // is unknown. O(1) average. The returned view is non-owning: it borrows
    // the index's storage and is valid until the next modification.
    std::span<const Posting> postings(std::string_view term) const;

    // Number of documents added so far (including documents with no tokens).
    std::size_t document_count() const;

    // Number of distinct terms in the vocabulary.
    std::size_t term_count() const;

    // Whether `term` is in the vocabulary. O(1) average.
    bool contains(std::string_view term) const;

private:
    std::unordered_map<std::string, std::vector<Posting>> postings_by_term_;
    // Tracks added document IDs so the unique-docID precondition (ADR-002)
    // can be enforced.
    std::unordered_set<doc_id> documents_;
    std::size_t document_count_ = 0;
};

} // namespace dse
