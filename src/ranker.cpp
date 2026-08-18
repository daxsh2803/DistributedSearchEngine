// Distributed Search Engine - Ranker (Phase 4B).
//
// Implementation of the contract in docs/decisions/ADR-004-ranking-design.md:
//
//   - TF-IDF scoring: tfidf(t,d) = tf(t,d) × ln(N / df(t))
//   - ranked_and: documents matching ALL query terms, scored and sorted
//   - ranked_or: documents matching ANY query term, scored and sorted
//   - Results sorted by score descending, tie-broken by doc_id ascending
//   - Query terms deduplicated before scoring
//   - Missing term: AND -> empty; OR -> ignored
//
// Complexity: O(Q + V log V + Σ Lᵢ + C log C) where Q = query length,
// V = unique terms, Lᵢ = posting list lengths, C = candidates.

#include "ranker.h"

#include "tokenizer.h"

#include <algorithm>
#include <cmath>
#include <cstddef>
#include <string>
#include <string_view>
#include <unordered_map>
#include <vector>

namespace dse {

namespace {

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

Ranker::Ranker(const InvertedIndex& index)
    : index_(&index)
{
}

std::vector<RankedResult> Ranker::ranked_and(std::string_view query) const
{
    // 1. Tokenize the query.
    const auto tokens = dse::tokenize(query);
    if (tokens.empty()) {
        return {};
    }

    // 2. Deduplicate terms.
    const auto terms = dedup_terms(tokens);

    // 3. Collect postings for each term, computing IDF.
    //    Also build the intersection candidate set: documents that appear
    //    in ALL posting lists.
    struct TermInfo {
        std::string_view term;
        std::span<const Posting> postings_span;
        double idf;
    };

    std::vector<TermInfo> term_infos;
    term_infos.reserve(terms.size());

    const double n = static_cast<double>(index_->document_count());
    if (n == 0.0) {
        return {};
    }

    for (const auto& term : terms) {
        const auto span = index_->postings(term);
        if (span.empty()) {
            // Missing term poisons AND.
            return {};
        }
        const double df = static_cast<double>(span.size());
        const double idf = std::log(n / df);
        term_infos.push_back({std::string_view(term), span, idf});
    }

    // 4. Build candidate set: documents appearing in every posting list.
    //    Use the smallest posting list as the starting set and intersect.
    //    Sort by posting list size to minimize work.
    std::sort(term_infos.begin(), term_infos.end(),
              [](const TermInfo& a, const TermInfo& b) {
                  return a.postings_span.size() < b.postings_span.size();
              });

    // Start with the smallest list's docIDs.
    std::unordered_map<doc_id, double> scores;
    for (const auto& posting : term_infos[0].postings_span) {
        scores[posting.document_id] = 0.0;
    }

    // Intersect with each subsequent list.
    for (std::size_t k = 1; k < term_infos.size(); ++k) {
        std::unordered_map<doc_id, double> next_scores;
        for (const auto& posting : term_infos[k].postings_span) {
            if (scores.count(posting.document_id)) {
                next_scores[posting.document_id] = 0.0;
            }
        }
        scores = std::move(next_scores);
        if (scores.empty()) {
            return {};
        }
    }

    // 5. Score each candidate document.
    for (const auto& info : term_infos) {
        for (const auto& posting : info.postings_span) {
            auto it = scores.find(posting.document_id);
            if (it != scores.end()) {
                it->second += static_cast<double>(posting.term_frequency)
                              * info.idf;
            }
        }
    }

    // 6. Convert to vector and sort by score descending, doc_id ascending.
    std::vector<RankedResult> results;
    results.reserve(scores.size());
    for (const auto& [doc, score] : scores) {
        results.push_back({doc, score});
    }

    std::sort(results.begin(), results.end(),
              [](const RankedResult& a, const RankedResult& b) {
                  if (a.score != b.score) {
                      return a.score > b.score;  // higher score first
                  }
                  return a.document_id < b.document_id;  // tie-break by doc_id
              });

    return results;
}

std::vector<RankedResult> Ranker::ranked_or(std::string_view query) const
{
    // 1. Tokenize the query.
    const auto tokens = dse::tokenize(query);
    if (tokens.empty()) {
        return {};
    }

    // 2. Deduplicate terms.
    const auto terms = dedup_terms(tokens);

    // 3. Accumulate TF-IDF scores across all matching documents.
    const double n = static_cast<double>(index_->document_count());
    if (n == 0.0) {
        return {};
    }

    std::unordered_map<doc_id, double> scores;

    for (const auto& term : terms) {
        const auto span = index_->postings(term);
        if (span.empty()) {
            continue;  // Missing term: ignore in OR.
        }
        const double df = static_cast<double>(span.size());
        const double idf = std::log(n / df);

        for (const auto& posting : span) {
            scores[posting.document_id] +=
                static_cast<double>(posting.term_frequency) * idf;
        }
    }

    // 4. Convert to vector and sort by score descending, doc_id ascending.
    std::vector<RankedResult> results;
    results.reserve(scores.size());
    for (const auto& [doc, score] : scores) {
        results.push_back({doc, score});
    }

    std::sort(results.begin(), results.end(),
              [](const RankedResult& a, const RankedResult& b) {
                  if (a.score != b.score) {
                      return a.score > b.score;
                  }
                  return a.document_id < b.document_id;
              });

    return results;
}

} // namespace dse
