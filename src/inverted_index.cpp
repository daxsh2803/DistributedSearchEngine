// Distributed Search Engine - Inverted Index (Phase 2B, Phase 8A-2).
//
// Implementation of the contract in docs/decisions/ADR-002-inverted-index-design.md:
//
//   - add_document tokenizes with dse::tokenize (Phase 1), aggregates
//     duplicate tokens into per-term counts, and merges (docID, count)
//     postings into the index;
//   - postings lists are always sorted by document ID (append in the common
//     in-order case, binary-search + insert otherwise);
//   - each document ID may be added at most once (asserted in debug builds);
//   - postings() returns an owning vector snapshot, safe for concurrent use.
//
// Phase 8A-2: Thread safety via internal SharedMutex.

#include "inverted_index.h"

#include "tokenizer.h"

#include <algorithm>
#include <cassert>
#include <cstdint>
#include <shared_mutex>
#include <string>
#include <string_view>
#include <unordered_map>
#include <utility>
#include <vector>

namespace dse {

InvertedIndex::~InvertedIndex() = default;

void InvertedIndex::add_document(doc_id id, std::string_view text)
{
    // Acquire exclusive lock for the entire mutation.
    std::unique_lock lock(*mutex_);

    // Precondition (ADR-002): each document ID may be added at most once.
    // Re-adding is a contract violation - undefined behavior in release
    // builds, caught here in debug builds.
    // NOTE: We must call insert() unconditionally (not inside assert())
    // because assert() is compiled out in Release builds, which would
    // skip the side effect of adding to documents_.
    auto [iter, inserted] = documents_.insert(id);
    assert(inserted && "Document ID already exists in index");

    // Tokenize once and aggregate duplicate tokens into per-term counts.
    // This is where the duplicate-preserving output of Phase 1's tokenizer
    // becomes term-frequency information.
    std::unordered_map<std::string, std::uint32_t> counts;
    for (const std::string& token : dse::tokenize(text)) {
        ++counts[token];
    }

    // Merge (id, tf) postings into each term's list, preserving the sorted
    // invariant. std::lower_bound locates the first posting with a larger
    // document ID: appending when `id` is beyond the back (the common
    // in-order case, O(1) amortized), a shifted insertion otherwise
    // (O(log k + k) for a list of length k).
    for (const auto& [term, tf] : counts) {
        auto& list = postings_by_term_[term];
        const auto pos = std::lower_bound(
            list.begin(), list.end(), id,
            [](const Posting& posting, doc_id value) {
                return posting.document_id < value;
            });
        list.insert(pos, Posting{id, tf});

        // Update reverse mapping: this document contains this term.
        doc_terms_[id].insert(term);
    }

    ++document_count_;
}

bool InvertedIndex::remove_document(doc_id id)
{
    // Acquire exclusive lock for the entire mutation.
    std::unique_lock lock(*mutex_);

    // Check if the document exists in the documents set.
    // (doc_terms_ may not have an entry for docs with no tokens.)
    if (!documents_.contains(id)) {
        return false;
    }

    // For each term in this document, remove its posting from the posting list.
    auto terms_it = doc_terms_.find(id);
    if (terms_it != doc_terms_.end()) {
        for (const std::string& term : terms_it->second) {
            auto term_it = postings_by_term_.find(term);
            if (term_it == postings_by_term_.end()) {
                continue;  // Should not happen, but be defensive.
            }

            auto& list = term_it->second;

            // Find and remove the posting for this document.
            auto pos = std::lower_bound(
                list.begin(), list.end(), id,
                [](const Posting& posting, doc_id value) {
                    return posting.document_id < value;
                });

            if (pos != list.end() && pos->document_id == id) {
                list.erase(pos);
            }

            // Clean up empty posting lists to keep the vocabulary clean.
            if (list.empty()) {
                postings_by_term_.erase(term_it);
            }
        }

        doc_terms_.erase(terms_it);
    }

    // Remove from documents set and decrement count.
    documents_.erase(id);
    --document_count_;

    return true;
}

std::vector<Posting> InvertedIndex::postings(std::string_view term) const
{
    // Acquire shared lock for read-only access.
    std::shared_lock lock(*mutex_);

    // ADR-002 accepts a temporary std::string key: hashing already reads
    // every byte of the term, so the copy costs the same O(|term|) as the
    // lookup itself (heterogeneous lookup needs a transparent hash).
    const auto it = postings_by_term_.find(std::string(term));
    if (it == postings_by_term_.end()) {
        return {};  // Unknown term: return empty vector.
    }

    // Return an owning copy of the postings list. This is safe to use
    // after the lock is released because the caller owns the data.
    // The copy costs O(k) where k is the list length, which is acceptable
    // for thread safety. Callers that need to iterate over postings can
    // do so without holding any lock on the index.
    return it->second;
}

std::size_t InvertedIndex::document_count() const
{
    // Acquire shared lock for read-only access.
    std::shared_lock lock(*mutex_);
    return document_count_;
}

std::size_t InvertedIndex::term_count() const
{
    // Acquire shared lock for read-only access.
    std::shared_lock lock(*mutex_);
    return postings_by_term_.size();
}

bool InvertedIndex::contains(std::string_view term) const
{
    // Acquire shared lock for read-only access.
    std::shared_lock lock(*mutex_);
    return postings_by_term_.contains(std::string(term));
}

} // namespace dse
