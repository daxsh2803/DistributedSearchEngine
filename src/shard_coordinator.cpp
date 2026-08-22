// Distributed Search Engine - Shard Coordinator (Phase 11).
//
// Implementation of the contract in src/shard_coordinator.h.
// Routes operations through NodeClient, using ShardRouter for
// doc_id → shard_id and ShardPlacement for shard_id → node_id.

#include "shard_coordinator.h"

#include <algorithm>
#include <cmath>
#include <cstddef>
#include <functional>
#include <future>
#include <string>
#include <string_view>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>

#include "inverted_index.h"
#include "node_client.h"
#include "node_config.h"
#include "search_service.h"
#include "shard_router.h"
#include "tokenizer.h"

namespace dse {

ShardCoordinator::ShardCoordinator(
    std::unique_ptr<ShardRouter> router,
    std::unique_ptr<ShardPlacement> placement,
    std::vector<std::unique_ptr<NodeClient>> nodes)
    : router_(std::move(router))
    , placement_(std::move(placement))
    , nodes_(std::move(nodes))
{
}

// ---------------------------------------------------------------------------
// Private helpers
// ---------------------------------------------------------------------------

NodeClient& ShardCoordinator::node_for_shard(std::size_t shard_id) const
{
    const std::size_t node_id = placement_->node_of(shard_id);
    return *nodes_[node_id];
}

std::vector<Posting> ShardCoordinator::collect_postings(
    std::string_view term) const
{
    const std::size_t n = router_->shard_count();

    // Parallel fan-out: search all shards concurrently.
    std::vector<std::future<ShardSearchResponse>> futures;
    futures.reserve(n);

    for (std::size_t sid = 0; sid < n; ++sid) {
        futures.push_back(std::async(std::launch::async,
            [this, sid, term]() {
                NodeClient& nc = node_for_shard(sid);
                ShardSearchRequest req;
                req.shard_id = sid;
                req.terms = {std::string(term)};
                return nc.search(req);
            }));
    }

    // Collect results.
    std::vector<Posting> all;
    for (auto& f : futures) {
        auto resp = f.get();
        if (resp.is_error || resp.terms_postings.empty()) {
            continue;
        }
        const auto& postings = resp.terms_postings[0];
        for (const auto& p : postings) {
            all.push_back({p.document_id, p.term_frequency});
        }
    }

    return all;
}

std::size_t ShardCoordinator::compute_global_n() const
{
    std::size_t total = 0;
    for (std::size_t sid = 0; sid < router_->shard_count(); ++sid) {
        NodeClient& nc = node_for_shard(sid);
        ShardCountRequest req;
        req.shard_id = sid;
        total += nc.document_count(req).document_count;
    }
    return total;
}

// ---------------------------------------------------------------------------
// Search: cross-shard with global TF-IDF via NodeClient
// ---------------------------------------------------------------------------

SearchResponse ShardCoordinator::search(const SearchRequest& request) const
{
    SearchResponse response;
    response.query = request.query;
    response.mode = (request.mode == SearchMode::And) ? "and" : "or";
    response.limit = request.limit;

    if (!SearchService::validate_request(request)) {
        response.is_error = true;
        response.error_message = "Invalid request: empty query or limit < 1";
        return response;
    }

    const auto tokens = tokenize(request.query);
    if (tokens.empty()) {
        return response;
    }

    std::vector<std::string> terms(tokens.begin(), tokens.end());
    std::sort(terms.begin(), terms.end());
    terms.erase(std::unique(terms.begin(), terms.end()), terms.end());

    const double global_n = static_cast<double>(compute_global_n());
    if (global_n == 0.0) {
        return response;
    }

    if (request.mode == SearchMode::And) {
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
                return response;  // Missing term poisons AND.
            }

            const double df = static_cast<double>(postings_list.size());
            const double idf = std::log(global_n / df);
            term_infos.push_back({term, std::move(postings_list), idf});
        }

        // Smallest-list-first intersection.
        std::sort(term_infos.begin(), term_infos.end(),
                  [](const TermInfo& a, const TermInfo& b) {
                      return a.postings_list.size() < b.postings_list.size();
                  });

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

        for (const auto& info : term_infos) {
            for (const auto& posting : info.postings_list) {
                auto it = scores.find(posting.document_id);
                if (it != scores.end()) {
                    it->second += static_cast<double>(posting.term_frequency)
                                  * info.idf;
                }
            }
        }

        std::vector<SearchResult> results;
        results.reserve(scores.size());
        for (const auto& [doc, score] : scores) {
            results.push_back({doc, score});
        }

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
        // OR mode: union across all shards.
        std::unordered_map<doc_id, double> scores;

        for (const auto& term : terms) {
            const auto postings_list = collect_postings(term);

            if (postings_list.empty()) {
                continue;
            }

            const double df = static_cast<double>(postings_list.size());
            const double idf = std::log(global_n / df);

            for (const auto& posting : postings_list) {
                scores[posting.document_id] +=
                    static_cast<double>(posting.term_frequency) * idf;
            }
        }

        std::vector<SearchResult> results;
        results.reserve(scores.size());
        for (const auto& [doc, score] : scores) {
            results.push_back({doc, score});
        }

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

    const std::size_t shard_id = router_->route(request.id);
    NodeClient& nc = node_for_shard(shard_id);

    ShardWriteRequest req;
    req.shard_id = shard_id;
    req.document_id = request.id;
    req.content = request.content;

    auto resp = nc.add_document(req);

    if (resp.is_error) {
        response.is_error = true;
        response.error_message = std::move(resp.error_message);
        return response;
    }

    response.terms_indexed = resp.terms_indexed;
    return response;
}

CoordinatorUpdateResponse ShardCoordinator::update(
    const CoordinatorUpdateRequest& request)
{
    CoordinatorUpdateResponse response;
    response.document_id = request.id;

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

    const std::size_t shard_id = router_->route(request.id);
    NodeClient& nc = node_for_shard(shard_id);

    ShardWriteRequest req;
    req.shard_id = shard_id;
    req.document_id = request.id;
    req.content = request.content;

    auto resp = nc.update_document(req);

    if (resp.is_error) {
        response.is_error = true;
        response.error_message = std::move(resp.error_message);
        return response;
    }

    response.terms_indexed = resp.terms_indexed;
    return response;
}

CoordinatorDeleteResponse ShardCoordinator::remove(doc_id id)
{
    CoordinatorDeleteResponse response;

    const std::size_t shard_id = router_->route(id);
    NodeClient& nc = node_for_shard(shard_id);

    ShardRemoveRequest req;
    req.shard_id = shard_id;
    req.document_id = id;

    auto resp = nc.remove_document(req);

    if (resp.is_error) {
        response.is_error = true;
        response.error_message = std::move(resp.error_message);
        return response;
    }

    return response;
}

// ---------------------------------------------------------------------------
// Read operations
// ---------------------------------------------------------------------------

std::optional<Document> ShardCoordinator::get_document(doc_id id) const
{
    const std::size_t shard_id = router_->route(id);
    NodeClient& nc = node_for_shard(shard_id);

    ShardGetRequest req;
    req.shard_id = shard_id;
    req.document_id = id;

    auto resp = nc.get_document(req);

    if (!resp.found) {
        return std::nullopt;
    }

    return Document{id, resp.content};
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
    for (std::size_t sid = 0; sid < router_->shard_count(); ++sid) {
        NodeClient& nc = node_for_shard(sid);
        if (!nc.save_shard(sid)) {
            return false;
        }
    }
    return true;
}

bool ShardCoordinator::load_all()
{
    for (std::size_t sid = 0; sid < router_->shard_count(); ++sid) {
        NodeClient& nc = node_for_shard(sid);
        nc.load_shard(sid);  // Ignore per-shard failures.
    }
    return true;
}

// ---------------------------------------------------------------------------
// Accessors
// ---------------------------------------------------------------------------

std::size_t ShardCoordinator::shard_count() const
{
    return router_->shard_count();
}

std::size_t ShardCoordinator::node_count() const
{
    return nodes_.size();
}

const NodeClient& ShardCoordinator::node(std::size_t index) const
{
    return *nodes_[index];
}

} // namespace dse
