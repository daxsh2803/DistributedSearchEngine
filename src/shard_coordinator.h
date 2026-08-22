// Distributed Search Engine - Shard Coordinator (Phase 11).
//
// The ShardCoordinator manages multiple nodes and routes document
// operations to the correct shard via ShardRouter → ShardPlacement
// → NodeClient.
//
// Responsibilities:
//   - Write routing: create/update/delete/get all route to the owning
//     node via NodeClient.
//   - Cross-shard search: collects postings from all shards through
//     NodeClient, computes global TF-IDF statistics, and returns
//     globally ranked results.
//   - Persistence: delegates to each node's own save/load.
//
// The coordinator does NOT access Shard internals directly.
// All shard access goes through the NodeClient interface.
//
// Thread safety:
//   - search() issues parallel std::async fan-out to nodes and
//     computes global TF-IDF scoring.
//   - ingest/update/remove route to the owning node; same-shard
//     mutations are serialized by the shard's internal mutex.

#pragma once

#include <cstddef>
#include <memory>
#include <optional>
#include <string>
#include <unordered_map>
#include <vector>

#include "document_store.h"  // for Document
#include "inverted_index.h"   // for doc_id, Posting
#include "node_client.h"
#include "node_config.h"
#include "search_service.h"   // for SearchRequest, SearchResponse
#include "shard_router.h"

namespace dse {

// Request/response types for ingestion via coordinator.
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
    // Construct a coordinator with routing, placement, and nodes.
    // The coordinator takes ownership of all provided objects.
    //
    // nodes must be indexed by node_id and contain at least
    // placement.node_count() entries.
    ShardCoordinator(std::unique_ptr<ShardRouter> router,
                     std::unique_ptr<ShardPlacement> placement,
                     std::vector<std::unique_ptr<NodeClient>> nodes);

    // --- Search ---
    // Cross-shard search with global TF-IDF scoring.
    // Issues parallel fan-out to all nodes via std::async.
    SearchResponse search(const SearchRequest& request) const;

    // --- Write operations ---
    CoordinatorIngestResponse ingest(const CoordinatorIngestRequest& request);
    CoordinatorUpdateResponse update(const CoordinatorUpdateRequest& request);
    CoordinatorDeleteResponse remove(doc_id id);

    // --- Read operations ---
    std::optional<Document> get_document(doc_id id) const;

    // Total document count across all shards.
    std::size_t total_document_count() const;

    // --- Persistence ---
    bool save_all() const;
    bool load_all();

    // --- Accessors ---
    std::size_t shard_count() const;
    std::size_t node_count() const;

    // Get a node by index. For tests and diagnostics.
    const NodeClient& node(std::size_t index) const;

private:
    // Find the NodeClient that owns a given shard.
    NodeClient& node_for_shard(std::size_t shard_id) const;

    // Collect postings for a term across all shards.
    std::vector<Posting> collect_postings(std::string_view term) const;

    // Compute global document count.
    std::size_t compute_global_n() const;

    std::unique_ptr<ShardRouter> router_;
    std::unique_ptr<ShardPlacement> placement_;
    std::vector<std::unique_ptr<NodeClient>> nodes_;
};

} // namespace dse
