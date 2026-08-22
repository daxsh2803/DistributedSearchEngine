// Distributed Search Engine - Local Node (Phase 11).
//
// Implementation of the contract in src/local_node.h.
// Wraps in-process Shards behind the NodeClient interface.

#include "local_node.h"

#include <cstddef>
#include <string>
#include <unordered_set>
#include <utility>
#include <vector>

#include "node_client.h"
#include "shard.h"
#include "tokenizer.h"

namespace dse {

LocalNode::LocalNode(std::size_t node_id)
    : node_id_(node_id)
{
}

void LocalNode::add_shard(std::size_t shard_id, std::unique_ptr<Shard> shard)
{
    shards_.emplace(shard_id, std::move(shard));
}

bool LocalNode::has_shard(std::size_t shard_id) const
{
    return shards_.count(shard_id) > 0;
}

std::size_t LocalNode::shard_count() const
{
    return shards_.size();
}

std::size_t LocalNode::node_id() const
{
    return node_id_;
}

// ---------------------------------------------------------------------------
// Private helpers
// ---------------------------------------------------------------------------

Shard* LocalNode::find_shard(std::size_t shard_id)
{
    auto it = shards_.find(shard_id);
    return (it != shards_.end()) ? it->second.get() : nullptr;
}

// ---------------------------------------------------------------------------
// Search
// ---------------------------------------------------------------------------

ShardSearchResponse LocalNode::search(const ShardSearchRequest& request)
{
    ShardSearchResponse response;
    response.shard_id = request.shard_id;

    Shard* shard = find_shard(request.shard_id);
    if (!shard) {
        response.is_error = true;
        response.error_message =
            "Shard " + std::to_string(request.shard_id) +
            " not found on node " + std::to_string(node_id_);
        return response;
    }

    response.local_document_count = shard->document_count();
    response.terms_postings.reserve(request.terms.size());

    for (const auto& term : request.terms) {
        const auto postings = shard->index().postings(term);
        std::vector<NodeTermPosting> local;
        local.reserve(postings.size());
        for (const auto& p : postings) {
            local.push_back({p.document_id, p.term_frequency});
        }
        response.terms_postings.push_back(std::move(local));
    }

    return response;
}

// ---------------------------------------------------------------------------
// Write operations
// ---------------------------------------------------------------------------

ShardWriteResponse LocalNode::add_document(const ShardWriteRequest& request)
{
    ShardWriteResponse response;
    response.shard_id = request.shard_id;
    response.document_id = request.document_id;

    Shard* shard = find_shard(request.shard_id);
    if (!shard) {
        response.is_error = true;
        response.error_message =
            "Shard " + std::to_string(request.shard_id) +
            " not found on node " + std::to_string(node_id_);
        return response;
    }

    if (!shard->add_document(request.document_id, request.content)) {
        response.is_error = true;
        response.error_message =
            "Document with id " + std::to_string(request.document_id) +
            " already exists or invalid content";
        return response;
    }

    shard->save();

    // Count distinct terms for response.
    const auto tokens = tokenize(request.content);
    std::unordered_set<std::string> distinct;
    for (const auto& t : tokens) {
        distinct.insert(t);
    }
    response.terms_indexed = distinct.size();

    return response;
}

ShardWriteResponse LocalNode::update_document(const ShardWriteRequest& request)
{
    ShardWriteResponse response;
    response.shard_id = request.shard_id;
    response.document_id = request.document_id;

    Shard* shard = find_shard(request.shard_id);
    if (!shard) {
        response.is_error = true;
        response.error_message =
            "Shard " + std::to_string(request.shard_id) +
            " not found on node " + std::to_string(node_id_);
        return response;
    }

    if (!shard->update_document(request.document_id, request.content)) {
        response.is_error = true;
        response.error_message =
            "Document with id " + std::to_string(request.document_id) +
            " not found";
        return response;
    }

    shard->save();

    const auto tokens = tokenize(request.content);
    std::unordered_set<std::string> distinct;
    for (const auto& t : tokens) {
        distinct.insert(t);
    }
    response.terms_indexed = distinct.size();

    return response;
}

ShardRemoveResponse LocalNode::remove_document(const ShardRemoveRequest& request)
{
    ShardRemoveResponse response;
    response.shard_id = request.shard_id;

    Shard* shard = find_shard(request.shard_id);
    if (!shard) {
        response.is_error = true;
        response.error_message =
            "Shard " + std::to_string(request.shard_id) +
            " not found on node " + std::to_string(node_id_);
        return response;
    }

    if (!shard->remove_document(request.document_id)) {
        response.is_error = true;
        response.error_message =
            "Document with id " + std::to_string(request.document_id) +
            " not found";
        return response;
    }

    shard->save();

    return response;
}

// ---------------------------------------------------------------------------
// Read operations
// ---------------------------------------------------------------------------

ShardGetResponse LocalNode::get_document(const ShardGetRequest& request)
{
    ShardGetResponse response;
    response.shard_id = request.shard_id;
    response.document_id = request.document_id;

    Shard* shard = find_shard(request.shard_id);
    if (!shard) {
        return response;
    }

    const auto doc = shard->get_document(request.document_id);
    if (doc.has_value()) {
        response.found = true;
        response.content = doc->content;
    }

    return response;
}

ShardCountResponse LocalNode::document_count(const ShardCountRequest& request)
{
    ShardCountResponse response;
    response.shard_id = request.shard_id;

    Shard* shard = find_shard(request.shard_id);
    if (shard) {
        response.document_count = shard->document_count();
    }

    return response;
}

// ---------------------------------------------------------------------------
// Persistence
// ---------------------------------------------------------------------------

bool LocalNode::save_shard(std::size_t shard_id)
{
    Shard* shard = find_shard(shard_id);
    if (!shard) {
        return false;
    }
    return shard->save();
}

bool LocalNode::load_shard(std::size_t shard_id)
{
    Shard* shard = find_shard(shard_id);
    if (!shard) {
        return false;
    }
    return shard->load();
}

} // namespace dse
