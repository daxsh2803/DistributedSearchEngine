// Distributed Search Engine - Local Node (Phase 11).
//
// LocalNode implements NodeClient for in-process shards.
// It owns one or more Shard instances, indexed by shard_id.
// The coordinator interacts with shards exclusively through NodeClient,
// making it possible to swap in RemoteNode later without changing
// the coordinator.

#pragma once

#include <cstddef>
#include <memory>
#include <unordered_map>

#include "node_client.h"
#include "shard.h"

namespace dse {

class LocalNode : public NodeClient {
public:
    // Create a node with a given identity.
    explicit LocalNode(std::size_t node_id);

    // Add a shard to this node. The node takes ownership.
    // shard_id must not already be present.
    void add_shard(std::size_t shard_id, std::unique_ptr<Shard> shard);

    // Whether this node hosts the given shard.
    bool has_shard(std::size_t shard_id) const;

    // Number of shards on this node.
    std::size_t shard_count() const;

    // --- NodeClient interface ---

    std::size_t node_id() const override;

    ShardSearchResponse search(const ShardSearchRequest& request) override;

    ShardWriteResponse add_document(const ShardWriteRequest& request) override;
    ShardWriteResponse update_document(const ShardWriteRequest& request) override;
    ShardRemoveResponse remove_document(const ShardRemoveRequest& request) override;

    ShardGetResponse get_document(const ShardGetRequest& request) override;
    ShardCountResponse document_count(const ShardCountRequest& request) override;

    bool save_shard(std::size_t shard_id) override;
    bool load_shard(std::size_t shard_id) override;

    // --- Remote mutation APIs (Phase 19E) ---
    // Apply document mutations received from Kafka without triggering
    // event publication, replication, or coordinator intervention.
    // If applied != nullptr, it is set to true if a local mutation was applied,
    // or false if the operation was an idempotent no-op.
    // Returns true on success (applied or no-op), false if the shard is not hosted
    // or the operation fails (e.g. conflict, missing document).
    bool apply_remote_indexed(std::size_t shard_id, doc_id document_id,
                              const std::string& content,
                              bool* applied = nullptr);
    bool apply_remote_updated(std::size_t shard_id, doc_id document_id,
                              const std::string& content,
                              bool* applied = nullptr);
    bool apply_remote_removed(std::size_t shard_id, doc_id document_id,
                              bool* applied = nullptr);

private:
    // Locate a shard by id, or return nullptr.
    Shard* find_shard(std::size_t shard_id);

    std::size_t node_id_;
    std::unordered_map<std::size_t, std::unique_ptr<Shard>> shards_;
};

} // namespace dse
