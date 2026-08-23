// Distributed Search Engine - Remote Node (Phase 12).
//
// NodeClient implementation that communicates with a remote NodeServer
// over HTTP/JSON. The coordinator uses this exactly like LocalNode —
// it cannot tell the difference.
//
// Every NodeClient method:
//   1. Serializes the request to JSON (via node_wire.h)
//   2. Sends an HTTP POST to the NodeServer
//   3. Deserializes the JSON response
//   4. Returns the NodeClient-level response
//
// Network failures become NodeClient-level errors (is_error = true).
// The coordinator's fail-fast behavior handles them.
//
// Thread safety:
//   All methods are safe for concurrent use. httplib::Client is safe
//   for concurrent requests (each request opens its own connection).

#pragma once

#include <cstddef>
#include <cstdint>
#include <memory>
#include <string>
#include <vector>

#include "node_client.h"

namespace httplib {
class Client;
}

namespace dse {

class RemoteNode : public NodeClient {
public:
    // Create a remote node client.
    //   node_id  — stable identity matching the remote NodeServer
    //   host     — hostname or IP (e.g., "127.0.0.1")
    //   port     — port the NodeServer is listening on
    //   timeout_seconds — connection/read/write timeout (default 30s)
    RemoteNode(std::size_t node_id,
               std::string host,
               int port,
               int timeout_seconds = 30);

    ~RemoteNode() override;

    RemoteNode(const RemoteNode&) = delete;
    RemoteNode& operator=(const RemoteNode&) = delete;
    RemoteNode(RemoteNode&&) = delete;
    RemoteNode& operator=(RemoteNode&&) = delete;

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

private:
    // Create a fresh httplib::Client for each request (safe for concurrency).
    std::unique_ptr<httplib::Client> make_client() const;

    std::size_t node_id_;
    std::string host_;
    int port_;
    int timeout_seconds_;
};

} // namespace dse
