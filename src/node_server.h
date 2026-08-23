// Distributed Search Engine - Node Server (Phase 12).
//
// HTTP server that wraps a LocalNode and exposes NodeClient operations
// as HTTP POST endpoints. RemoteNode on the coordinator side communicates
// with this server over HTTP/JSON.
//
// Endpoints (all POST, JSON bodies):
//   /node/search    — search a shard
//   /node/add       — add a document
//   /node/update    — update a document
//   /node/remove    — remove a document
//   /node/get       — get a document
//   /node/count     — document count
//   /node/save      — persist a shard
//   /node/load      — load a shard from persistence
//
// Lifecycle:
//   1. Construct NodeServer with a LocalNode.
//   2. Call listen(port) to start (blocks until stop()).
//   3. Call stop() from any thread to shut down.
//   4. Destructor calls stop() if still running.
//
// Thread safety:
//   - listen() blocks the calling thread.
//   - stop() is safe from a different thread.
//   - The server must not be moved or copied while running.
//   - Concurrent requests are handled by cpp-httplib's built-in threads.

#pragma once

#include <cstddef>
#include <memory>

namespace httplib {
class Server;
}

namespace dse {

class LocalNode;

class NodeServer {
public:
    // Construct a node server that delegates to the given LocalNode.
    // The NodeServer takes ownership of the LocalNode.
    NodeServer(std::size_t node_id, std::unique_ptr<LocalNode> local_node);
    ~NodeServer();

    NodeServer(const NodeServer&) = delete;
    NodeServer& operator=(const NodeServer&) = delete;
    NodeServer(NodeServer&&) = delete;
    NodeServer& operator=(NodeServer&&) = delete;

    // Start listening on the given port.
    // port=0 binds to an ephemeral port.
    // Blocks until stop() is called.
    bool listen(int port);

    // Stop the server. Safe to call from any thread.
    void stop();

    // Block until the server is ready to accept connections.
    void wait_until_ready() const;

    // The port the server is listening on (after listen()).
    int port() const;

    // The node_id of this server.
    std::size_t node_id() const;

private:
    void register_routes();

    std::size_t node_id_;
    std::unique_ptr<LocalNode> local_node_;
    std::unique_ptr<httplib::Server> server_;
    int port_ = 0;
};

} // namespace dse
