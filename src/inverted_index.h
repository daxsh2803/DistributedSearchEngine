// Distributed Search Engine - Inverted Index (Phase 2B, Phase 8A-2).
//
// Public API only. Design per docs/decisions/ADR-002-inverted-index-design.md:
// an in-memory, deterministic map from terms to sorted postings lists,
// consuming dse::tokenize output from Phase 1.
//
// Phase 8A-2: Thread-safe via internal SharedMutex. All public methods are
// safe for concurrent access. postings() returns an owning vector snapshot
// instead of a non-owning span to ensure lifetime safety under mutation.

#pragma once

#include <cstddef>
#include <cstdint>
#include <memory>
#include <string>
#include <string_view>
#include <unordered_map>
#include <unordered_set>
#include <vector>

#include "shared_mutex.h"

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
// Contract (ADR-002 + 008):
//   - a term is exactly a token produced by dse::tokenize (lowercased, ASCII);
//   - each document ID may be added at most once (precondition; re-adding is
//     a contract violation, asserted in debug builds);
//   - postings lists are always sorted by document ID;
//   - duplicates within a document are aggregated into term frequencies;
//   - postings(term) returns an empty vector for unknown terms;
//   - identical insertion sequences produce identical indices (deterministic);
//   - vocabulary iteration order is unspecified (do not depend on it);
//   - remove_document(id) removes all postings for that document and cleans
//     up empty posting lists (Phase 9).
//
// Thread safety (Phase 8A-2 + 9):
//   - All public methods are safe for concurrent access.
//   - Read operations (postings, document_count, term_count, contains) use
//     shared locks allowing concurrent readers.
//   - Write operations (add_document, remove_document) use exclusive locks.
//   - postings() returns an owning vector snapshot, not a borrowed span,
//     to ensure lifetime safety when the index is mutated concurrently.
//
// Complexity: O(1) average lookup; O(T) amortized per add_document for a
// document with T tokens; O(T) for remove_document; O(distinct
// term-document pairs) space.
class InvertedIndex {
public:
    InvertedIndex() = default;
    ~InvertedIndex();

    InvertedIndex(const InvertedIndex&) = delete;
    InvertedIndex& operator=(const InvertedIndex&) = delete;
    InvertedIndex(InvertedIndex&&) = default;
    InvertedIndex& operator=(InvertedIndex&&) = default;

    // Tokenizes `text` with dse::tokenize, counts term frequencies, and
    // merges (id, count) postings into the index, keeping every postings
    // list sorted by document ID. Thread-safe: acquires exclusive lock.
    void add_document(doc_id id, std::string_view text);

    // Remove a document from the index. Removes that document's posting from
    // each relevant posting list. Erases posting lists that become empty.
    // Returns true if the document was removed, false if it did not exist.
    // Thread-safe: acquires exclusive lock.
    bool remove_document(doc_id id);

    // Postings list for `term`, sorted by document ID, or empty if the term
    // is unknown. Returns an OWNING vector snapshot that is safe to use
    // after the lock is released. O(1) average lookup + O(k) copy where
    // k is the postings list length.
    std::vector<Posting> postings(std::string_view term) const;

    // Number of documents added so far (including documents with no tokens).
    // Thread-safe: acquires shared lock.
    std::size_t document_count() const;

    // Number of distinct terms in the vocabulary.
    // Thread-safe: acquires shared lock.
    std::size_t term_count() const;

    // Whether `term` is in the vocabulary. O(1) average.
    // Thread-safe: acquires shared lock.
    bool contains(std::string_view term) const;

private:
    std::unordered_map<std::string, std::vector<Posting>> postings_by_term_;
    // Tracks added document IDs so the unique-docID precondition (ADR-002)
    // can be enforced.
    std::unordered_set<doc_id> documents_;
    // Reverse mapping: document ID -> set of terms that document contains.
    // Used by remove_document() to efficiently find which posting lists to
    // update without scanning the entire vocabulary.
    std::unordered_map<doc_id, std::unordered_set<std::string>> doc_terms_;
    std::size_t document_count_ = 0;

    // Portable SharedMutex (see shared_mutex.h) to avoid MinGW bugs with
    // std::shared_mutex. Wrapped in unique_ptr to preserve movability.
    mutable std::unique_ptr<SharedMutex> mutex_ = std::make_unique<SharedMutex>();
};

} // namespace dse
