// Distributed Search Engine - Shard Coordinator (Phase 10).
//
// The ShardCoordinator manages multiple in-process shards and routes
// document operations to the correct shard based on the ShardRouter.
//
// Responsibilities:
//   - Write routing: create/update/delete/get all route to the owning shard.
//   - Cross-shard search: collects postings from all shards, computes
//     global TF-IDF statistics, and returns globally ranked results.
//   - Persistence: delegates to each shard's own save/load.
//
// The coordinator must NOT hold a global mutation mutex. Mutations on
// different shards proceed independently. The coordinator only performs
// read-only operations across shards during search.
//
// Thread safety:
//   - search() is safe for concurrent use (read-only across shards).
//   - ingest/update/remove are safe for concurrent use when targeting
//     different shards. Same-shard mutations are serialized by the
//     shard's internal mutex.

#pragma once

#include <cstddef>
#include <memory>
#include <string>
#include <vector>

#include "inverted_index.h"  // for doc_id, Posting
#include "search_service.h"  // for SearchRequest, SearchResponse, etc.
#include "shard.h"
#include "shard_router.h"

namespace dse {

// Request/response types for ingestion via coordinator.
// Mirrors the IngestionService types for API compatibility.
struct CoordinatorIngestRequest {
    doc_id id;
    std::string content;
};

struct CoordinatorIngestResponse {
    doc_id document_id = 0;
    std::size_t terms_indexed = 0;
    bool is_error = false;
    std::string error_message;
};

struct CoordinatorUpdateRequest {
    doc_id id;
    std::string content;
};

struct CoordinatorUpdateResponse {
    doc_id document_id = 0;
    std::size_t terms_indexed = 0;
    bool is_error = false;
    std::string error_message;
};

struct CoordinatorDeleteResponse {
    bool is_error = false;
    std::string error_message;
};

class ShardCoordinator {
public:
    // Construct a coordinator with the given shards and router.
    // The coordinator takes ownership of both via unique_ptr.
    ShardCoordinator(std::unique_ptr<ShardRouter> router,
                     std::vector<std::unique_ptr<Shard>> shards);

    // --- Search ---
    // Cross-shard search with global TF-IDF scoring.
    SearchResponse search(const SearchRequest& request) const;

    // --- Write operations ---
    CoordinatorIngestResponse ingest(const CoordinatorIngestRequest& request);
    CoordinatorUpdateResponse update(const CoordinatorUpdateRequest& request);
    CoordinatorDeleteResponse remove(doc_id id);

    // --- Read operations ---
    // Get a document by routing to the owning shard.
    std::optional<Document> get_document(doc_id id) const;

    // Total document count across all shards.
    std::size_t total_document_count() const;

    // --- Persistence ---
    // Save all shards.
    bool save_all() const;

    // Load all shards (startup recovery).
    bool load_all();

    // --- Accessors ---
    std::size_t shard_count() const;
    const Shard& shard(std::size_t index) const;

private:
    // Collect postings for a term across all shards.
    std::vector<Posting> collect_postings(std::string_view term) const;

    // Compute global document count.
    std::size_t compute_global_n() const;

    std::unique_ptr<ShardRouter> router_;
    std::vector<std::unique_ptr<Shard>> shards_;
};

} // namespace dse
