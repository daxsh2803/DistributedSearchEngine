// Distributed Search Engine - Shard Coordinator (Phase 10).
//
// Implementation of the contract in src/shard_coordinator.h.
// Routes document operations to the owning shard via ShardRouter.
// Performs cross-shard search with global TF-IDF statistics.

#include "shard_coordinator.h"

#include <algorithm>
#include <cmath>
#include <cstddef>
#include <string>
#include <string_view>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>

#include "inverted_index.h"
#include "search_service.h"
#include "shard.h"
#include "shard_router.h"
#include "tokenizer.h"

namespace dse {

ShardCoordinator::ShardCoordinator(
    std::unique_ptr<ShardRouter> router,
    std::vector<std::unique_ptr<Shard>> shards)
    : router_(std::move(router))
    , shards_(std::move(shards))
{
}

// ---------------------------------------------------------------------------
// Search: cross-shard with global TF-IDF
// ---------------------------------------------------------------------------

std::vector<Posting> ShardCoordinator::collect_postings(
    std::string_view term) const
{
    std::vector<Posting> all;

    for (const auto& s : shards_) {
        const auto local = s->index().postings(term);
        all.insert(all.end(), local.begin(), local.end());
    }

    return all;
}

std::size_t ShardCoordinator::compute_global_n() const
{
    std::size_t total = 0;
    for (const auto& s : shards_) {
        total += s->document_count();
    }
    return total;
}

SearchResponse ShardCoordinator::search(const SearchRequest& request) const
{
    SearchResponse response;
    response.query = request.query;
    response.mode = (request.mode == SearchMode::And) ? "and" : "or";
    response.limit = request.limit;

    // Validate.
    if (!SearchService::validate_request(request)) {
        response.is_error = true;
        response.error_message = "Invalid request: empty query or limit < 1";
        return response;
    }

    // Tokenize and deduplicate query terms.
    const auto tokens = tokenize(request.query);
    if (tokens.empty()) {
        return response;
    }

    std::vector<std::string> terms(tokens.begin(), tokens.end());
    std::sort(terms.begin(), terms.end());
    terms.erase(std::unique(terms.begin(), terms.end()), terms.end());

    // Global N.
    const double global_n = static_cast<double>(compute_global_n());
    if (global_n == 0.0) {
        return response;
    }

    if (request.mode == SearchMode::And) {
        // --- AND mode: collect candidates from all shards, then intersect ---
        struct TermInfo {
            std::string term;
            std::vector<Posting> postings_list;
            double idf;
        };

        std::vector<TermInfo> term_infos;
        term_infos.reserve(terms.size());

        for (const auto& term : terms) {
            auto postings_list = collect_postings(term);

            if (postings_list.empty()) {
                // Missing term poisons AND.
                return response;
            }

            const double df = static_cast<double>(postings_list.size());
            const double idf = std::log(global_n / df);
            term_infos.push_back({term, std::move(postings_list), idf});
        }

        // Sort by posting list size ascending (smallest-list-first).
        std::sort(term_infos.begin(), term_infos.end(),
                  [](const TermInfo& a, const TermInfo& b) {
                      return a.postings_list.size() < b.postings_list.size();
                  });

        // Build candidate set via intersection.
        std::unordered_map<doc_id, double> scores;
        for (const auto& posting : term_infos[0].postings_list) {
            scores[posting.document_id] = 0.0;
        }

        for (std::size_t k = 1; k < term_infos.size(); ++k) {
            std::unordered_map<doc_id, double> next_scores;
            for (const auto& posting : term_infos[k].postings_list) {
                if (scores.count(posting.document_id)) {
                    next_scores[posting.document_id] = 0.0;
                }
            }
            scores = std::move(next_scores);
            if (scores.empty()) {
                return response;
            }
        }

        // Score candidates with global TF-IDF.
        for (const auto& info : term_infos) {
            for (const auto& posting : info.postings_list) {
                auto it = scores.find(posting.document_id);
                if (it != scores.end()) {
                    it->second += static_cast<double>(posting.term_frequency)
                                  * info.idf;
                }
            }
        }

        // Convert to results.
        std::vector<SearchResult> results;
        results.reserve(scores.size());
        for (const auto& [doc, score] : scores) {
            results.push_back({doc, score});
        }

        // Sort by score descending, tie-break by doc_id ascending.
        std::sort(results.begin(), results.end(),
                  [](const SearchResult& a, const SearchResult& b) {
                      if (a.score != b.score) {
                          return a.score > b.score;
                      }
                      return a.document_id < b.document_id;
                  });

        response.total = results.size();
        const std::size_t count = std::min(response.limit, results.size());
        response.results.reserve(count);
        for (std::size_t i = 0; i < count; ++i) {
            response.results.push_back(results[i]);
        }

    } else {
        // --- OR mode: union of all matching documents across shards ---
        std::unordered_map<doc_id, double> scores;

        for (const auto& term : terms) {
            const auto postings_list = collect_postings(term);

            if (postings_list.empty()) {
                continue;  // Missing term: ignore in OR.
            }

            const double df = static_cast<double>(postings_list.size());
            const double idf = std::log(global_n / df);

            for (const auto& posting : postings_list) {
                scores[posting.document_id] +=
                    static_cast<double>(posting.term_frequency) * idf;
            }
        }

        // Convert to results.
        std::vector<SearchResult> results;
        results.reserve(scores.size());
        for (const auto& [doc, score] : scores) {
            results.push_back({doc, score});
        }

        // Sort by score descending, tie-break by doc_id ascending.
        std::sort(results.begin(), results.end(),
                  [](const SearchResult& a, const SearchResult& b) {
                      if (a.score != b.score) {
                          return a.score > b.score;
                      }
                      return a.document_id < b.document_id;
                  });

        response.total = results.size();
        const std::size_t count = std::min(response.limit, results.size());
        response.results.reserve(count);
        for (std::size_t i = 0; i < count; ++i) {
            response.results.push_back(results[i]);
        }
    }

    return response;
}

// ---------------------------------------------------------------------------
// Write operations
// ---------------------------------------------------------------------------

CoordinatorIngestResponse ShardCoordinator::ingest(
    const CoordinatorIngestRequest& request)
{
    CoordinatorIngestResponse response;
    response.document_id = request.id;

    // Validate content.
    if (request.content.empty()) {
        response.is_error = true;
        response.error_message = "Invalid request: content must be non-empty";
        return response;
    }

    // Check for whitespace-only content.
    bool all_blank = true;
    for (const char c : request.content) {
        if (c != ' ' && c != '\t' && c != '\n' && c != '\r') {
            all_blank = false;
            break;
        }
    }
    if (all_blank) {
        response.is_error = true;
        response.error_message = "Invalid request: content must be non-whitespace";
        return response;
    }

    // Route to owning shard.
    const std::size_t idx = router_->route(request.id);
    auto& target_shard = *shards_[idx];

    if (!target_shard.add_document(request.id, request.content)) {
        response.is_error = true;
        response.error_message =
            "Document with id " + std::to_string(request.id) + " already exists";
        return response;
    }

    // Persist the shard.
    target_shard.save();

    // Count distinct terms for response.
    const auto tokens = tokenize(request.content);
    std::unordered_set<std::string> distinct;
    for (const auto& t : tokens) {
        distinct.insert(t);
    }
    response.terms_indexed = distinct.size();

    return response;
}

CoordinatorUpdateResponse ShardCoordinator::update(
    const CoordinatorUpdateRequest& request)
{
    CoordinatorUpdateResponse response;
    response.document_id = request.id;

    // Validate content.
    if (request.content.empty()) {
        response.is_error = true;
        response.error_message = "Invalid request: content must be non-empty";
        return response;
    }

    bool all_blank = true;
    for (const char c : request.content) {
        if (c != ' ' && c != '\t' && c != '\n' && c != '\r') {
            all_blank = false;
            break;
        }
    }
    if (all_blank) {
        response.is_error = true;
        response.error_message = "Invalid request: content must be non-whitespace";
        return response;
    }

    // Route to owning shard.
    const std::size_t idx = router_->route(request.id);
    auto& target_shard = *shards_[idx];

    if (!target_shard.update_document(request.id, request.content)) {
        response.is_error = true;
        response.error_message =
            "Document with id " + std::to_string(request.id) + " not found";
        return response;
    }

    // Persist the shard.
    target_shard.save();

    // Count distinct terms.
    const auto tokens = tokenize(request.content);
    std::unordered_set<std::string> distinct;
    for (const auto& t : tokens) {
        distinct.insert(t);
    }
    response.terms_indexed = distinct.size();

    return response;
}

CoordinatorDeleteResponse ShardCoordinator::remove(doc_id id)
{
    CoordinatorDeleteResponse response;

    // Route to owning shard.
    const std::size_t idx = router_->route(id);
    auto& target_shard = *shards_[idx];

    if (!target_shard.remove_document(id)) {
        response.is_error = true;
        response.error_message =
            "Document with id " + std::to_string(id) + " not found";
        return response;
    }

    // Persist the shard.
    target_shard.save();

    return response;
}

// ---------------------------------------------------------------------------
// Read operations
// ---------------------------------------------------------------------------

std::optional<Document> ShardCoordinator::get_document(doc_id id) const
{
    const std::size_t idx = router_->route(id);
    return shards_[idx]->get_document(id);
}

std::size_t ShardCoordinator::total_document_count() const
{
    return compute_global_n();
}

// ---------------------------------------------------------------------------
// Persistence
// ---------------------------------------------------------------------------

bool ShardCoordinator::save_all() const
{
    for (const auto& s : shards_) {
        if (!s->save()) {
            return false;
        }
    }
    return true;
}

bool ShardCoordinator::load_all()
{
    for (auto& s : shards_) {
        s->load();  // Ignore per-shard failures; they are handled internally.
    }
    return true;
}

// ---------------------------------------------------------------------------
// Accessors
// ---------------------------------------------------------------------------

std::size_t ShardCoordinator::shard_count() const
{
    return shards_.size();
}

const Shard& ShardCoordinator::shard(std::size_t index) const
{
    return *shards_[index];
}

} // namespace dse
