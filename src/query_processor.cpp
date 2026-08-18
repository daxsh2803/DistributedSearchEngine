// Distributed Search Engine - Query Processor (Phase 3B).
//
// Implementation of the contract in docs/decisions/ADR-003-query-processing-design.md:
//
//   - Two pure two-pointer merge primitives (intersect, merge_union) over
//     sorted postings lists, projected to docIDs;
//   - QueryProcessor composes dse::tokenize, index.postings() lookups, and
//     the merge primitives into AND/OR Boolean queries;
//   - Query terms are deduplicated (sort + unique) before evaluation;
//   - AND: pairwise intersection, smallest-list-first fold;
//   - OR: pairwise union;
//   - Results: owning, sorted ascending, deduplicated, deterministic;
//   - Missing term: AND -> empty; OR -> ignored;
//   - Zero query terms -> empty result.
//
// Complexity: O(a + b) per two-pointer merge; O(Q + V log V) query
// preparation; O(result) output space.

#include "query_processor.h"

#include "tokenizer.h"

#include <algorithm>
#include <cassert>
#include <cstddef>
#include <string>
#include <string_view>
#include <vector>

namespace dse {

namespace {

// ---------------------------------------------------------------------------
// Core two-pointer merge primitives on sorted docID spans.
//
// These do the actual merge work. The public intersect/merge_union functions
// project Postings to docIDs and call these.
// ---------------------------------------------------------------------------

// Two-pointer intersection: emit a docID only when it appears in both sorted
// ranges. O(a.size() + b.size()).
std::vector<doc_id> two_intersect(std::span<const doc_id> a,
                                  std::span<const doc_id> b)
{
    std::vector<doc_id> result;
    std::size_t i = 0;
    std::size_t j = 0;

    while (i < a.size() && j < b.size()) {
        if (a[i] == b[j]) {
            result.push_back(a[i]);
            ++i;
            ++j;
        } else if (a[i] < b[j]) {
            ++i;
        } else {
            ++j;
        }
    }

    return result;
}

// Two-pointer union: emit every docID from both sorted ranges, deduplicating
// shared elements. Drain whichever range still has elements at the end.
// O(a.size() + b.size()).
std::vector<doc_id> two_union(std::span<const doc_id> a,
                              std::span<const doc_id> b)
{
    std::vector<doc_id> result;
    result.reserve(a.size() + b.size());

    std::size_t i = 0;
    std::size_t j = 0;

    while (i < a.size() && j < b.size()) {
        if (a[i] == b[j]) {
            result.push_back(a[i]);
            ++i;
            ++j;
        } else if (a[i] < b[j]) {
            result.push_back(a[i]);
            ++i;
        } else {
            result.push_back(b[j]);
            ++j;
        }
    }

    while (i < a.size()) {
        result.push_back(a[i]);
        ++i;
    }
    while (j < b.size()) {
        result.push_back(b[j]);
        ++j;
    }

    return result;
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

// Project a span of Postings to an owning vector of docIDs.
std::vector<doc_id> project_doc_ids(std::span<const Posting> postings)
{
    std::vector<doc_id> ids(postings.size());
    for (std::size_t i = 0; i < postings.size(); ++i) {
        ids[i] = postings[i].document_id;
    }
    return ids;
}

// Deduplicate and sort query terms. After this call, the vector contains
// each distinct term exactly once, in ascending order.
std::vector<std::string> dedup_terms(const std::vector<std::string>& terms)
{
    std::vector<std::string> result = terms;
    std::sort(result.begin(), result.end());
    result.erase(std::unique(result.begin(), result.end()), result.end());
    return result;
}

} // namespace

// ---------------------------------------------------------------------------
// Public free functions (ADR-003 API)
// ---------------------------------------------------------------------------

std::vector<doc_id> intersect(std::span<const Posting> lhs,
                              std::span<const Posting> rhs)
{
    return two_intersect(project_doc_ids(lhs), project_doc_ids(rhs));
}

std::vector<doc_id> merge_union(std::span<const Posting> lhs,
                                std::span<const Posting> rhs)
{
    return two_union(project_doc_ids(lhs), project_doc_ids(rhs));
}

// ---------------------------------------------------------------------------
// QueryProcessor
// ---------------------------------------------------------------------------

QueryProcessor::QueryProcessor(const InvertedIndex& index)
    : index_(&index)
{
}

std::vector<doc_id> QueryProcessor::and_query(std::string_view query) const
{
    // 1. Tokenize the query using the Phase 1 tokenizer.
    const auto tokens = dse::tokenize(query);

    // 2. Zero terms -> empty result.
    if (tokens.empty()) {
        return {};
    }

    // 3. Deduplicate terms (sort + unique).
    const auto terms = dedup_terms(tokens);

    // 4. Look up postings for each distinct term.
    //    Sort by postings-list size ascending (smallest-list-first fold)
    //    to minimize work. The fold order does not affect the result
    //    because intersection is commutative and associative.
    std::vector<std::pair<std::size_t, std::string_view>> ranked;
    ranked.reserve(terms.size());
    for (const auto& term : terms) {
        const auto span = index_->postings(term);
        ranked.emplace_back(span.size(), std::string_view(term));
    }
    std::sort(ranked.begin(), ranked.end());

    // 5. Fold pairwise intersections, smallest list first.
    //    Start with the smallest list projected to docIDs.
    const auto first_span = index_->postings(ranked[0].second);
    std::vector<doc_id> result = project_doc_ids(first_span);

    //    If the first list is empty, the intersection is empty regardless.
    if (result.empty()) {
        return {};
    }

    for (std::size_t k = 1; k < ranked.size(); ++k) {
        const auto next_span = index_->postings(ranked[k].second);
        result = two_intersect(result, project_doc_ids(next_span));

        // Early termination: intersection only shrinks; if empty, done.
        if (result.empty()) {
            return {};
        }
    }

    return result;
}

std::vector<doc_id> QueryProcessor::or_query(std::string_view query) const
{
    // 1. Tokenize the query using the Phase 1 tokenizer.
    const auto tokens = dse::tokenize(query);

    // 2. Zero terms -> empty result.
    if (tokens.empty()) {
        return {};
    }

    // 3. Deduplicate terms.
    const auto terms = dedup_terms(tokens);

    // 4. Look up postings for each distinct term and fold pairwise unions.
    //    Order does not matter for union (commutative, associative), but we
    //    process in sorted term order for determinism.
    std::vector<doc_id> result = project_doc_ids(index_->postings(terms[0]));

    for (std::size_t k = 1; k < terms.size(); ++k) {
        const auto span = index_->postings(terms[k]);
        result = two_union(result, project_doc_ids(span));
    }

    return result;
}

} // namespace dse
