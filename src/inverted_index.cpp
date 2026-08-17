// Distributed Search Engine - Inverted Index (Phase 2B).
//
// Implementation of the contract in docs/decisions/ADR-002-inverted-index-design.md:
//
//   - add_document tokenizes with dse::tokenize (Phase 1), aggregates
//     duplicate tokens into per-term counts, and merges (docID, count)
//     postings into the index;
//   - postings lists are always sorted by document ID (append in the common
//     in-order case, binary-search + insert otherwise);
//   - each document ID may be added at most once (asserted in debug builds);
//   - postings() returns a non-owning std::span<const Posting>, empty for
//     unknown terms, valid until the next modification of the index.

#include "inverted_index.h"

#include "tokenizer.h"

#include <algorithm>
#include <cassert>
#include <cstdint>
#include <string>
#include <string_view>
#include <unordered_map>
#include <utility>
#include <span>

namespace dse {

void InvertedIndex::add_document(doc_id id, std::string_view text)
{
    // Precondition (ADR-002): each document ID may be added at most once.
    // Re-adding is a contract violation - undefined behavior in release
    // builds, caught here in debug builds.
    assert(documents_.insert(id).second);

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
    }

    ++document_count_;
}

std::span<const Posting> InvertedIndex::postings(std::string_view term) const
{
    // ADR-002 accepts a temporary std::string key: hashing already reads
    // every byte of the term, so the copy costs the same O(|term|) as the
    // lookup itself (heterogeneous lookup needs a transparent hash).
    const auto it = postings_by_term_.find(std::string(term));
    if (it == postings_by_term_.end()) {
        return {};
    }

    // Borrow the list's heap buffer. The span stays valid across map rehashes
    // (the buffer does not move); it is invalidated by a later push_back into
    // this same list or by the index's destruction - hence the documented
    // "valid until the next modification" contract.
    const auto& list = it->second;
    return std::span<const Posting>(list.data(), list.size());
}

std::size_t InvertedIndex::document_count() const
{
    return document_count_;
}

std::size_t InvertedIndex::term_count() const
{
    return postings_by_term_.size();
}

bool InvertedIndex::contains(std::string_view term) const
{
    return postings_by_term_.contains(std::string(term));
}

} // namespace dse
