// Distributed Search Engine - Shard Coordinator (Phase 11).
//
// Implementation of the contract in src/shard_coordinator.h.
// Routes operations through NodeClient, using ShardRouter for
// doc_id → shard_id and ShardPlacement for shard_id → node_id.

#include "shard_coordinator.h"

#include <algorithm>
#include <chrono>
#include <cmath>
#include <cstddef>
#include <functional>
#include <future>
#include <string>
#include <string_view>
#include <unordered_map>
#include <utility>
#include <vector>

#include "inverted_index.h"
#include "metrics.h"
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

void ShardCoordinator::set_metrics(MetricsCollector* metrics)
{
    metrics_ = metrics;
}

// ---------------------------------------------------------------------------
// Private helpers
// ---------------------------------------------------------------------------

NodeClient& ShardCoordinator::node_for_shard(std::size_t shard_id) const
{
    const std::size_t node_id = placement_->node_of(shard_id);
    return *nodes_[node_id];
}

ShardCoordinator::PostingsResult ShardCoordinator::collect_postings(
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

    // Collect results, recording failures.
    PostingsResult result;
    for (auto& f : futures) {
        auto resp = f.get();
        if (resp.is_error || resp.terms_postings.empty()) {
            if (resp.is_error) {
                NodeFailureInfo fail;
                fail.shard_id = resp.shard_id;
                fail.node_id = placement_->node_of(resp.shard_id);
                fail.category = "search_failure";
                fail.message = resp.error_message;
                result.failures.push_back(std::move(fail));
            }
            continue;
        }
        const auto& postings = resp.terms_postings[0];
        for (const auto& p : postings) {
            result.postings.push_back({p.document_id, p.term_frequency});
        }
    }

    return result;
}

ShardCoordinator::GlobalNResult ShardCoordinator::compute_global_n() const
{
    GlobalNResult result;
    for (std::size_t sid = 0; sid < router_->shard_count(); ++sid) {
        NodeClient& nc = node_for_shard(sid);
        ShardCountRequest req;
        req.shard_id = sid;
        auto resp = nc.document_count(req);
        if (resp.is_error) {
            result.complete = false;
            NodeFailureInfo fail;
            fail.shard_id = sid;
            fail.node_id = placement_->node_of(sid);
            fail.category = "count_failure";
            fail.message = resp.error_message;
            result.failures.push_back(std::move(fail));
        } else {
            result.total += resp.document_count;
        }
    }
    return result;
}

// ---------------------------------------------------------------------------
// Search: cross-shard with global TF-IDF via NodeClient
// ---------------------------------------------------------------------------

SearchResponse ShardCoordinator::search(const SearchRequest& request) const
{
    const auto start_time = std::chrono::steady_clock::now();
    SearchResponse response;
    response.query = request.query;
    response.mode = (request.mode == SearchMode::And) ? "and" : "or";
    response.limit = request.limit;

    if (!SearchService::validate_request(request)) {
        response.is_error = true;
        response.error_message = "Invalid request: empty query or limit < 1";
        if (metrics_) {
            const auto elapsed = std::chrono::duration_cast<std::chrono::microseconds>(
                std::chrono::steady_clock::now() - start_time).count();
            metrics_->record_coordinator_search(
                static_cast<double>(elapsed) / 1000.0, false, true);
        }
        return response;
    }

    const auto tokens = tokenize(request.query);
    if (tokens.empty()) {
        if (metrics_) {
            const auto elapsed = std::chrono::duration_cast<std::chrono::microseconds>(
                std::chrono::steady_clock::now() - start_time).count();
            metrics_->record_coordinator_search(
                static_cast<double>(elapsed) / 1000.0, true, true);
        }
        return response;
    }

    std::vector<std::string> terms(tokens.begin(), tokens.end());
    std::sort(terms.begin(), terms.end());
    terms.erase(std::unique(terms.begin(), terms.end()), terms.end());

    // Compute global document count, tracking failures.
    const auto gn = compute_global_n();
    const double global_n = static_cast<double>(gn.total);
    response.complete = gn.complete;
    for (auto& f : gn.failures) {
        response.errors.push_back(std::move(f));
    }

    if (global_n == 0.0) {
        if (!response.complete) {
            response.total = 0;
        }
        if (metrics_) {
            const auto elapsed = std::chrono::duration_cast<std::chrono::microseconds>(
                std::chrono::steady_clock::now() - start_time).count();
            metrics_->record_coordinator_search(
                static_cast<double>(elapsed) / 1000.0, true, response.complete);
        }
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
            auto pr = collect_postings(term);

            // Merge per-term failures into response.
            for (auto& f : pr.failures) {
                response.complete = false;
                response.errors.push_back(std::move(f));
            }

            if (pr.postings.empty()) {
                if (metrics_) {
                    const auto elapsed = std::chrono::duration_cast<std::chrono::microseconds>(
                        std::chrono::steady_clock::now() - start_time).count();
                    metrics_->record_coordinator_search(
                        static_cast<double>(elapsed) / 1000.0, true, response.complete);
                }
                return response;  // Missing term poisons AND.
            }

            const double df = static_cast<double>(pr.postings.size());
            const double idf = std::log(global_n / df);
            term_infos.push_back({term, std::move(pr.postings), idf});
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
                if (metrics_) {
                    const auto elapsed = std::chrono::duration_cast<std::chrono::microseconds>(
                        std::chrono::steady_clock::now() - start_time).count();
                    metrics_->record_coordinator_search(
                        static_cast<double>(elapsed) / 1000.0, true, response.complete);
                }
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

        if (metrics_) {
            const auto elapsed = std::chrono::duration_cast<std::chrono::microseconds>(
                std::chrono::steady_clock::now() - start_time).count();
            metrics_->record_coordinator_search(
                static_cast<double>(elapsed) / 1000.0, true, response.complete);
        }
        return response;

    } else {
        // OR mode: union across all shards.
        std::unordered_map<doc_id, double> scores;

        for (const auto& term : terms) {
            auto pr = collect_postings(term);

            // Merge per-term failures into response.
            for (auto& f : pr.failures) {
                response.complete = false;
                response.errors.push_back(std::move(f));
            }

            if (pr.postings.empty()) {
                continue;
            }

            const double df = static_cast<double>(pr.postings.size());
            const double idf = std::log(global_n / df);

            for (const auto& posting : pr.postings) {
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

    if (metrics_) {
        const auto elapsed = std::chrono::duration_cast<std::chrono::microseconds>(
            std::chrono::steady_clock::now() - start_time).count();
        metrics_->record_coordinator_search(
            static_cast<double>(elapsed) / 1000.0, true, response.complete);
    }
    return response;
}

// ---------------------------------------------------------------------------
// Write operations
// ---------------------------------------------------------------------------

CoordinatorIngestResponse ShardCoordinator::ingest(
    const CoordinatorIngestRequest& request)
{
    const auto start_time = std::chrono::steady_clock::now();
    CoordinatorIngestResponse response;
    response.document_id = request.id;

    if (request.content.empty()) {
        response.is_error = true;
        response.error_message = "Invalid request: content must be non-empty";
        if (metrics_) {
            const auto elapsed = std::chrono::duration_cast<std::chrono::microseconds>(
                std::chrono::steady_clock::now() - start_time).count();
            metrics_->record_coordinator_write(
                static_cast<double>(elapsed) / 1000.0, false);
        }
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
        if (metrics_) {
            const auto elapsed = std::chrono::duration_cast<std::chrono::microseconds>(
                std::chrono::steady_clock::now() - start_time).count();
            metrics_->record_coordinator_write(
                static_cast<double>(elapsed) / 1000.0, false);
        }
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
        if (metrics_) {
            const auto elapsed = std::chrono::duration_cast<std::chrono::microseconds>(
                std::chrono::steady_clock::now() - start_time).count();
            metrics_->record_coordinator_write(
                static_cast<double>(elapsed) / 1000.0, false);
        }
        return response;
    }

    response.terms_indexed = resp.terms_indexed;
    if (metrics_) {
        const auto elapsed = std::chrono::duration_cast<std::chrono::microseconds>(
            std::chrono::steady_clock::now() - start_time).count();
        metrics_->record_coordinator_write(
            static_cast<double>(elapsed) / 1000.0, true);
    }
    return response;
}

CoordinatorUpdateResponse ShardCoordinator::update(
    const CoordinatorUpdateRequest& request)
{
    const auto start_time = std::chrono::steady_clock::now();
    CoordinatorUpdateResponse response;
    response.document_id = request.id;

    if (request.content.empty()) {
        response.is_error = true;
        response.error_message = "Invalid request: content must be non-empty";
        if (metrics_) {
            const auto elapsed = std::chrono::duration_cast<std::chrono::microseconds>(
                std::chrono::steady_clock::now() - start_time).count();
            metrics_->record_coordinator_write(
                static_cast<double>(elapsed) / 1000.0, false);
        }
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
        if (metrics_) {
            const auto elapsed = std::chrono::duration_cast<std::chrono::microseconds>(
                std::chrono::steady_clock::now() - start_time).count();
            metrics_->record_coordinator_write(
                static_cast<double>(elapsed) / 1000.0, false);
        }
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
        if (metrics_) {
            const auto elapsed = std::chrono::duration_cast<std::chrono::microseconds>(
                std::chrono::steady_clock::now() - start_time).count();
            metrics_->record_coordinator_write(
                static_cast<double>(elapsed) / 1000.0, false);
        }
        return response;
    }

    response.terms_indexed = resp.terms_indexed;
    if (metrics_) {
        const auto elapsed = std::chrono::duration_cast<std::chrono::microseconds>(
            std::chrono::steady_clock::now() - start_time).count();
        metrics_->record_coordinator_write(
            static_cast<double>(elapsed) / 1000.0, true);
    }
    return response;
}

CoordinatorDeleteResponse ShardCoordinator::remove(doc_id id)
{
    const auto start_time = std::chrono::steady_clock::now();
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
        if (metrics_) {
            const auto elapsed = std::chrono::duration_cast<std::chrono::microseconds>(
                std::chrono::steady_clock::now() - start_time).count();
            metrics_->record_coordinator_write(
                static_cast<double>(elapsed) / 1000.0, false);
        }
        return response;
    }

    if (metrics_) {
        const auto elapsed = std::chrono::duration_cast<std::chrono::microseconds>(
            std::chrono::steady_clock::now() - start_time).count();
        metrics_->record_coordinator_write(
            static_cast<double>(elapsed) / 1000.0, true);
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
    return compute_global_n().total;
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
