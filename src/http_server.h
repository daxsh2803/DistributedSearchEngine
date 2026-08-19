// Distributed Search Engine - HTTP Server (Phase 5B-2).
//
// Thin HTTP transport layer over SearchService. Owns the httplib::Server
// and registers a single GET /search endpoint. All search logic flows
// through SearchService — this class does nothing except translate
// between HTTP and the SearchRequest/SearchResponse data model.
//
// Lifecycle:
//   1. Construct HttpServer with a reference to SearchService.
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
// The full implementation is only in http_server.cpp.
namespace httplib {
class Server;
}

namespace dse {

class SearchService;

class HttpServer {
public:
    explicit HttpServer(const SearchService& service);
    ~HttpServer();

    HttpServer(const HttpServer&) = delete;
    HttpServer& operator=(const HttpServer&) = delete;
    HttpServer(HttpServer&&) = delete;
    HttpServer& operator=(HttpServer&&) = delete;

    // Start the server on the given port. Blocks until stop() is called.
    bool listen(int port);

    // Request a graceful shutdown. Safe to call from any thread.
    void stop();

    // Block until the server is accepting connections.
    void wait_until_ready() const;

    // Returns the port the server is actually listening on.
    // Useful when port 0 was passed to listen() (OS-assigned port).
    int port() const;

private:
    void register_routes();

    const SearchService& service_;
    std::unique_ptr<httplib::Server> server_;
    int port_ = 0;
};

} // namespace dse
