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

#include <atomic>
#include <cstddef>
#include <cstdint>
#include <memory>

// Forward-declare httplib to keep this header lightweight.
namespace httplib {
class Server;
}

namespace dse {

class ShardCoordinator;
class MetricsCollector;
class EventStore;
class EventDispatcher;
class MessageBroker;

class HttpServer {
public:
    // Default application-level concurrency limit (Phase 27).
    // Conservative limit justified by cpp-httplib's worker thread pool model.
    static constexpr std::size_t kDefaultMaxConcurrentRequests = 64;

    // Construct with optional MetricsCollector for /metrics endpoint.
    // When metrics is nullptr, /metrics returns HTTP 503.
    // When eventStore is nullptr, event metrics are omitted from /metrics.
    // When dispatcher is nullptr, dispatcher metrics are omitted from /metrics.
    // When broker is nullptr, consumer metrics are omitted from /metrics.
    // When max_concurrent_requests is 0, resolves from DSE_MAX_CONCURRENT_REQUESTS
    // environment variable or defaults to kDefaultMaxConcurrentRequests.
    explicit HttpServer(ShardCoordinator& coordinator,
                        MetricsCollector* metrics = nullptr,
                        EventStore* eventStore = nullptr,
                        EventDispatcher* dispatcher = nullptr,
                        MessageBroker* broker = nullptr,
                        std::size_t max_concurrent_requests = 0);
    ~HttpServer();

    HttpServer(const HttpServer&) = delete;
    HttpServer& operator=(const HttpServer&) = delete;
    HttpServer(HttpServer&&) = delete;
    HttpServer& operator=(HttpServer&&) = delete;

    void set_event_dispatcher(EventDispatcher* dispatcher) { dispatcher_ = dispatcher; }
    void set_message_broker(MessageBroker* broker) { broker_ = broker; }

    void set_max_concurrent_requests(std::size_t limit) { max_concurrent_requests_ = limit; }
    std::size_t max_concurrent_requests() const { return max_concurrent_requests_; }
    std::size_t active_requests() const { return active_requests_.load(std::memory_order_relaxed); }
    std::uint64_t load_shed_rejections() const { return load_shed_rejections_total_.load(std::memory_order_relaxed); }

    bool listen(int port);
    void stop();
    void wait_until_ready() const;
    int port() const;

private:
    void register_routes();
    void record_load_shed_rejection();

    ShardCoordinator& coordinator_;
    MetricsCollector* metrics_ = nullptr;     // optional, not owned
    EventStore* eventStore_ = nullptr;         // optional, not owned
    EventDispatcher* dispatcher_ = nullptr;    // optional, not owned
    MessageBroker* broker_ = nullptr;          // optional, not owned
    std::unique_ptr<httplib::Server> server_;
    int port_ = 0;

    std::size_t max_concurrent_requests_ = kDefaultMaxConcurrentRequests;
    std::atomic<std::size_t> active_requests_{0};
    std::atomic<std::uint64_t> load_shed_rejections_total_{0};
};

} // namespace dse
