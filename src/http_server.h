// Distributed Search Engine - HTTP Server (Phase 5B-2, Phase 10).
//
// Thin HTTP transport layer over ShardCoordinator. Owns the httplib::Server
// and registers GET /search, POST /documents, PUT /documents/:id,
// DELETE /documents/:id. All logic flows through ShardCoordinator.
//
// Lifecycle:
//   1. Construct HttpServer with a reference to ShardCoordinator.
//   2. Call listen(port) to start the server (blocks until stop()).
//   3. Call stop() from any thread to shut down gracefully.
//   4. Destroy HttpServer — destructor calls stop() if still running.
//
// Thread safety:
//   - listen() blocks the calling thread.
//   - stop() is safe to call from a different thread.
//   - The server must not be moved or copied while running.

#pragma once

#include <memory>

// Forward-declare httplib to keep this header lightweight.
namespace httplib {
class Server;
}

namespace dse {

class ShardCoordinator;
class MetricsCollector;
class EventStore;

class HttpServer {
public:
    // Construct with optional MetricsCollector for /metrics endpoint.
    // When metrics is nullptr, /metrics returns HTTP 503.
    // When eventStore is nullptr, event metrics are omitted from /metrics.
    explicit HttpServer(ShardCoordinator& coordinator,
                        MetricsCollector* metrics = nullptr,
                        EventStore* eventStore = nullptr);
    ~HttpServer();

    HttpServer(const HttpServer&) = delete;
    HttpServer& operator=(const HttpServer&) = delete;
    HttpServer(HttpServer&&) = delete;
    HttpServer& operator=(HttpServer&&) = delete;

    bool listen(int port);
    void stop();
    void wait_until_ready() const;
    int port() const;

private:
    void register_routes();

    ShardCoordinator& coordinator_;
    MetricsCollector* metrics_ = nullptr;  // optional, not owned
    EventStore* eventStore_ = nullptr;      // optional, not owned
    std::unique_ptr<httplib::Server> server_;
    int port_ = 0;
};

} // namespace dse
