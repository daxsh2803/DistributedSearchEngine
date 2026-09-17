// Distributed Search Engine - Node Client (Phase 11).
//
// Transport-independent interface between ShardCoordinator and nodes.
// A node is a physical/logical entity that hosts one or more shards and
// serves search/mutation requests. This interface abstracts the boundary
// so that LocalNode (in-process) and a future RemoteNode (network) can
// be swapped without changing the coordinator.
//
// Thread safety:
//   All methods are safe for concurrent use by the implementing node.
//   LocalNode delegates to Shard, which has its own internal locking.

#pragma once

#include <cstddef>
#include <cstdint>
#include <string>
#include <utility>
#include <vector>

#include "inverted_index.h"  // for doc_id, Posting

namespace dse {

// ---------------------------------------------------------------------------
// Search: request one shard's local data for query terms
// ---------------------------------------------------------------------------

// Per-term posting data returned by a node for one shard.
struct NodeTermPosting {
    doc_id document_id;
    std::uint32_t term_frequency;
};

// Request: fetch postings for all given terms from one specific shard.
struct ShardSearchRequest {
    std::size_t shard_id;
    std::vector<std::string> terms;  // deduplicated query terms
};

// Response: local postings + metadata for one shard.
struct ShardSearchResponse {
    std::size_t shard_id;
    std::size_t local_document_count;
    // For each requested term, the matching postings from this shard.
    // terms_postings[i] corresponds to request.terms[i].
    std::vector<std::vector<NodeTermPosting>> terms_postings;
    bool is_error = false;
    std::string error_message;
};

// ---------------------------------------------------------------------------
// Write: add, update, remove a document
// ---------------------------------------------------------------------------

struct ShardWriteRequest {
    std::size_t shard_id;
    doc_id document_id;
    std::string content;
};

struct ShardWriteResponse {
    std::size_t shard_id;
    doc_id document_id;
    std::size_t terms_indexed = 0;  // for add/update
    bool is_error = false;
    std::string error_message;
};

struct ShardRemoveRequest {
    std::size_t shard_id;
    doc_id document_id;
};

struct ShardRemoveResponse {
    std::size_t shard_id;
    bool is_error = false;
    std::string error_message;
};

// ---------------------------------------------------------------------------
// Read: get document, document count
// ---------------------------------------------------------------------------

struct ShardGetRequest {
    std::size_t shard_id;
    doc_id document_id;
};

struct ShardGetResponse {
    std::size_t shard_id;
    doc_id document_id;
    std::string content;
    bool found = false;
    bool is_error = false;
    std::string error_message;
};

struct ShardCountRequest {
    std::size_t shard_id;
};

struct ShardCountResponse {
    std::size_t shard_id;
    std::size_t document_count = 0;
    bool is_error = false;
    std::string error_message;
};

// ---------------------------------------------------------------------------
// NodeClient: pure virtual transport-independent interface
// ---------------------------------------------------------------------------

class NodeClient {
public:
    virtual ~NodeClient() = default;

    // Stable identity for this node. Assigned at construction time.
    virtual std::size_t node_id() const = 0;

    // Search: return local postings for the requested terms from one shard.
    virtual ShardSearchResponse search(const ShardSearchRequest& request) = 0;

    // Write operations.
    virtual ShardWriteResponse add_document(const ShardWriteRequest& request) = 0;
    virtual ShardWriteResponse update_document(const ShardWriteRequest& request) = 0;
    virtual ShardRemoveResponse remove_document(const ShardRemoveRequest& request) = 0;

    // Read operations.
    virtual ShardGetResponse get_document(const ShardGetRequest& request) = 0;
    virtual ShardCountResponse document_count(const ShardCountRequest& request) = 0;

    // Persistence.
    virtual bool save_shard(std::size_t shard_id) = 0;
    virtual bool load_shard(std::size_t shard_id) = 0;
};

} // namespace dse
