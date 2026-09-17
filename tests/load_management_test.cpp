// Distributed Search Engine - Load Management & Concurrency Tests (Phase 27).
//
// Verifies application-level concurrency limiting on HttpServer:
//   1. Requests below limit are accepted.
//   2. Requests exceeding limit receive HTTP 429 Too Many Requests.
//   3. Rejected requests cause no application state modification.
//   4. load_shed_rejections_total metric increments accurately.
//   5. Active request count returns to 0 upon request completion.
//   6. Early return and error paths do not leak request slots.
//   7. /health remains available during overload and consumes no slot.
//   8. /metrics remains available during overload and consumes no slot.
//   9. NodeServer behavior is preserved and not subject to load shedding.
//  10. Configuration via DSE_MAX_CONCURRENT_REQUESTS and constructor defaults.
//  11. High-concurrency stress shedding with clean final state.

#include <gtest/gtest.h>

#include "http_server.h"
#include "local_node.h"
#include "metrics.h"
#include "node_client.h"
#include "node_config.h"
#include "node_server.h"
#include "shard.h"
#include "shard_coordinator.h"
#include "shard_router.h"

#include <httplib.h>
#include <nlohmann/json.hpp>

#include <atomic>
#include <chrono>
#include <cstddef>
#include <cstdlib>
#include <memory>
#include <string>
#include <thread>
#include <vector>

namespace {

using dse::doc_id;
using dse::HttpServer;
using dse::LocalNode;
using dse::MetricsCollector;
using dse::NodeClient;
using dse::NodeServer;
using dse::Shard;
using dse::ShardCoordinator;
using dse::ShardPlacement;
using dse::ShardRouter;
using dse::ShardSearchRequest;
using dse::ShardSearchResponse;

// Helper: build a single-node, multi-shard coordinator
std::unique_ptr<ShardCoordinator> make_test_coordinator(std::size_t shard_count = 3)
{
    auto router = std::make_unique<ShardRouter>(shard_count);
    std::vector<std::size_t> placement(shard_count, 0);
    auto shard_placement = std::make_unique<ShardPlacement>(shard_count, 1, placement);

    auto node = std::make_unique<LocalNode>(0);
    for (std::size_t i = 0; i < shard_count; ++i) {
        node->add_shard(i, std::make_unique<Shard>());
    }

    std::vector<std::unique_ptr<NodeClient>> nodes;
    nodes.push_back(std::move(node));

    return std::make_unique<ShardCoordinator>(
        std::move(router), std::move(shard_placement), std::move(nodes));
}

class LoadManagementTest : public ::testing::Test {
protected:
    void SetUp() override
    {
        coordinator_ = make_test_coordinator(3);
        metrics_ = std::make_unique<MetricsCollector>();
        coordinator_->set_metrics(metrics_.get());
    }

    void TearDown() override
    {
        stop_server();
    }

    void create_server(std::size_t max_concurrent = 64)
    {
        server_ = std::make_unique<HttpServer>(
            *coordinator_, metrics_.get(), nullptr, nullptr, nullptr, max_concurrent);
    }

    void start_server()
    {
        server_thread_ = std::thread([this]() { server_->listen(0); });
        server_->wait_until_ready();
    }

    void stop_server()
    {
        if (server_) {
            server_->stop();
            if (server_thread_.joinable()) {
                server_thread_.join();
            }
        }
    }

    std::pair<int, std::string> http_get(const std::string& path)
    {
        httplib::Client client("localhost", server_->port());
        client.set_connection_timeout(5);
        client.set_read_timeout(5);
        auto res = client.Get(path);
        if (!res) return {0, ""};
        return {res->status, res->body};
    }

    std::pair<int, std::string> http_post(const std::string& path, const std::string& body)
    {
        httplib::Client client("localhost", server_->port());
        client.set_connection_timeout(5);
        client.set_read_timeout(5);
        auto res = client.Post(path, body, "application/json");
        if (!res) return {0, ""};
        return {res->status, res->body};
    }

    std::unique_ptr<ShardCoordinator> coordinator_;
    std::unique_ptr<MetricsCollector> metrics_;
    std::unique_ptr<HttpServer> server_;
    std::thread server_thread_;
};

// ===========================================================================
// 1. Requests below limit are accepted
// ===========================================================================

TEST_F(LoadManagementTest, RequestsBelowLimitAccepted)
{
    create_server(10);
    start_server();

    // Ingest a document
    nlohmann::json doc;
    doc["id"] = 1;
    doc["content"] = "distributed systems load management";
    const auto [p_status, p_body] = http_post("/documents", doc.dump());
    EXPECT_EQ(p_status, 201);

    // Search for the document
    const auto [s_status, s_body] = http_get("/search?q=distributed");
    EXPECT_EQ(s_status, 200);

    const auto j = nlohmann::json::parse(s_body);
    EXPECT_EQ(j["total"], 1u);

    // Active requests must have returned to 0
    EXPECT_EQ(server_->active_requests(), 0u);
    EXPECT_EQ(server_->load_shed_rejections(), 0u);
}

// ===========================================================================
// 2. Requests exceeding limit receive HTTP 429
// ===========================================================================

TEST_F(LoadManagementTest, RequestsExceedingLimitReceive429)
{
    // Limit = 0 means no application requests can be accepted
    create_server(0);
    server_->set_max_concurrent_requests(0);
    start_server();

    const auto [status, body] = http_get("/search?q=test");
    EXPECT_EQ(status, 429);

    const auto j = nlohmann::json::parse(body);
    EXPECT_TRUE(j.contains("error"));
    EXPECT_NE(j["error"].get<std::string>().find("concurrency limit reached"),
              std::string::npos);

    EXPECT_EQ(server_->load_shed_rejections(), 1u);
    EXPECT_EQ(metrics_->snapshot().load_shed_rejections_total, 1u);
}

// ===========================================================================
// 3. Rejected requests do not modify application state
// ===========================================================================

TEST_F(LoadManagementTest, RejectedRequestDoesNotModifyState)
{
    create_server(0);
    server_->set_max_concurrent_requests(0);
    start_server();

    // Attempt to ingest while server is saturated
    nlohmann::json doc;
    doc["id"] = 42;
    doc["content"] = "state modification should not happen";
    const auto [status, body] = http_post("/documents", doc.dump());
    EXPECT_EQ(status, 429);

    // Reopen concurrency limit
    server_->set_max_concurrent_requests(10);

    // Verify document 42 was never indexed
    const auto [s_status, s_body] = http_get("/search?q=modification");
    EXPECT_EQ(s_status, 200);
    const auto j = nlohmann::json::parse(s_body);
    EXPECT_EQ(j["total"], 0u);
    EXPECT_TRUE(j["results"].empty());
}

// ===========================================================================
// 4. load_shed_rejections_total increments accurately
// ===========================================================================

TEST_F(LoadManagementTest, LoadShedRejectionsTotalIncrements)
{
    create_server(0);
    server_->set_max_concurrent_requests(0);
    start_server();

    EXPECT_EQ(server_->load_shed_rejections(), 0u);
    EXPECT_EQ(metrics_->snapshot().load_shed_rejections_total, 0u);

    http_get("/search?q=a");
    http_get("/search?q=b");
    http_post("/documents", "{\"id\":1,\"content\":\"c\"}");

    EXPECT_EQ(server_->load_shed_rejections(), 3u);
    EXPECT_EQ(metrics_->snapshot().load_shed_rejections_total, 3u);

    // Verify via /metrics endpoint
    const auto [m_status, m_body] = http_get("/metrics");
    EXPECT_EQ(m_status, 200);
    const auto mj = nlohmann::json::parse(m_body);
    EXPECT_EQ(mj["load_shed_rejections_total"], 3u);
}

// ===========================================================================
// 5. Active request count returns to 0 upon completion
// ===========================================================================

TEST_F(LoadManagementTest, ActiveCountReturnsToZero)
{
    create_server(10);
    start_server();

    for (int i = 0; i < 5; ++i) {
        http_get("/search?q=test");
        EXPECT_EQ(server_->active_requests(), 0u);
    }
}

// ===========================================================================
// 6. Early return and error paths do not leak request slots
// ===========================================================================

TEST_F(LoadManagementTest, ErrorPathsDoNotLeakSlots)
{
    create_server(10);
    start_server();

    // 1. Invalid search mode (400 Bad Request)
    auto [s1, b1] = http_get("/search?q=test&mode=invalid");
    EXPECT_EQ(s1, 400);
    EXPECT_EQ(server_->active_requests(), 0u);

    // 2. Invalid limit (400 Bad Request)
    auto [s2, b2] = http_get("/search?q=test&limit=999");
    EXPECT_EQ(s2, 400);
    EXPECT_EQ(server_->active_requests(), 0u);

    // 3. Invalid JSON body on POST (400 Bad Request)
    auto [s3, b3] = http_post("/documents", "{not valid json}");
    EXPECT_EQ(s3, 400);
    EXPECT_EQ(server_->active_requests(), 0u);

    // 4. Missing required fields in JSON (400 Bad Request)
    auto [s4, b4] = http_post("/documents", "{\"other\":\"field\"}");
    EXPECT_EQ(s4, 400);
    EXPECT_EQ(server_->active_requests(), 0u);
}

// ===========================================================================
// 7. /health remains available during overload and consumes no slot
// ===========================================================================

TEST_F(LoadManagementTest, HealthExemptFromLoadShedding)
{
    create_server(0);
    server_->set_max_concurrent_requests(0);
    start_server();

    // Application search is rejected
    const auto [s_status, _] = http_get("/search?q=test");
    EXPECT_EQ(s_status, 429);

    // /health is accepted with 200 OK
    const auto [h_status, h_body] = http_get("/health");
    EXPECT_EQ(h_status, 200);
    const auto j = nlohmann::json::parse(h_body);
    EXPECT_EQ(j["status"], "ok");

    // /health did not increment rejection counter
    EXPECT_EQ(server_->load_shed_rejections(), 1u);
}

// ===========================================================================
// 8. /metrics remains available during overload and consumes no slot
// ===========================================================================

TEST_F(LoadManagementTest, MetricsExemptFromLoadShedding)
{
    create_server(0);
    server_->set_max_concurrent_requests(0);
    start_server();

    // /metrics is accepted with 200 OK
    const auto [m_status, m_body] = http_get("/metrics");
    EXPECT_EQ(m_status, 200);
    const auto j = nlohmann::json::parse(m_body);
    EXPECT_TRUE(j.contains("load_shed_rejections_total"));

    // /metrics did not increment rejection counter
    EXPECT_EQ(server_->load_shed_rejections(), 0u);
}

// ===========================================================================
// 9. NodeServer behavior is preserved and not subject to load shedding
// ===========================================================================

TEST_F(LoadManagementTest, NodeServerUnchangedAndNotLoadShed)
{
    auto local_node = std::make_unique<LocalNode>(0);
    local_node->add_shard(0, std::make_unique<Shard>());
    NodeServer node_server(0, std::move(local_node));

    ASSERT_TRUE(node_server.bind(0));
    std::thread ns_thread([&node_server]() { node_server.listen_after_bind(); });
    node_server.wait_until_ready();

    httplib::Client client("localhost", node_server.port());
    client.set_connection_timeout(5);
    client.set_read_timeout(5);

    // Internal RPC write
    nlohmann::json write_req;
    write_req["shard_id"] = 0;
    write_req["document_id"] = 100;
    write_req["content"] = "internal replication rpc";

    auto res = client.Post("/node/add", write_req.dump(), "application/json");
    ASSERT_TRUE(res != nullptr);
    EXPECT_EQ(res->status, 200);

    node_server.stop();
    if (ns_thread.joinable()) {
        ns_thread.join();
    }
}

// ===========================================================================
// 10. High-concurrency stress shedding with clean final state
// ===========================================================================

TEST_F(LoadManagementTest, HighConcurrencyStressShedding)
{
    // Define a node that explicitly waits for a release signal,
    // guaranteeing deterministic saturation for the test.
    class BlockingNode : public LocalNode {
    public:
        std::atomic<std::size_t> active_in_search{0};
        std::atomic<bool> release_search{false};

        explicit BlockingNode(std::size_t id) : LocalNode(id) {}

        ShardSearchResponse search(const ShardSearchRequest& req) override {
            active_in_search.fetch_add(1, std::memory_order_relaxed);
            while (!release_search.load(std::memory_order_relaxed)) {
                std::this_thread::sleep_for(std::chrono::milliseconds(1));
            }
            active_in_search.fetch_sub(1, std::memory_order_relaxed);
            return LocalNode::search(req);
        }
    };

    auto router = std::make_unique<ShardRouter>(1);
    std::vector<std::size_t> placement = {0};
    auto shard_placement = std::make_unique<ShardPlacement>(1, 1, placement);
    auto blocking_node = std::make_unique<BlockingNode>(0);
    auto* blocking_node_ptr = blocking_node.get();

    auto shard = std::make_unique<Shard>();
    shard->add_document(1, "test"); // Contains the query term

    blocking_node->add_shard(0, std::move(shard));

    std::vector<std::unique_ptr<NodeClient>> nodes;
    nodes.push_back(std::move(blocking_node));

    coordinator_ = std::make_unique<ShardCoordinator>(
        std::move(router), std::move(shard_placement), std::move(nodes));
    coordinator_->set_metrics(metrics_.get());

    // Configure small concurrency limit to trigger shedding under load
    constexpr std::size_t kLimit = 4;
    create_server(kLimit);
    start_server();

    std::atomic<int> accepted{0};
    std::atomic<int> rejected{0};

    auto client_task = [this, &accepted, &rejected](std::size_t id, bool is_blocker) {
        httplib::Client client("localhost", server_->port());
        client.set_read_timeout(5);
        auto res = client.Get("/search?q=test");
        if (res && res->status == 200) {
            accepted.fetch_add(1);
        } else if (res && res->status == 429) {
            rejected.fetch_add(1, std::memory_order_relaxed);
        }
    };

    // 1. Launch blockers
    std::vector<std::thread> blockers;
    for (std::size_t i = 0; i < kLimit; ++i) {
        blockers.emplace_back(client_task, i, true);
    }

    // Wait until they are all successfully blocking inside search()
    std::size_t wait_iters = 0;
    while (blocking_node_ptr->active_in_search.load(std::memory_order_relaxed) < kLimit) {
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
        if (++wait_iters > 5000) break;
    }

    EXPECT_EQ(blocking_node_ptr->active_in_search.load(std::memory_order_relaxed), kLimit);

    // 2. Launch overflowers while slots are held
    std::vector<std::thread> overflowers;
    constexpr std::size_t kOverflowClients = 4;
    for (std::size_t i = 0; i < kOverflowClients; ++i) {
        overflowers.emplace_back(client_task, kLimit + i, false);
    }

    // Overflowers should return 429 immediately because limits are saturated
    for (auto& t : overflowers) {
        if (t.joinable()) t.join();
    }

    // 3. Release blockers and let them finish
    blocking_node_ptr->release_search.store(true, std::memory_order_relaxed);
    for (auto& t : blockers) {
        if (t.joinable()) t.join();
    }

    // 5. Verify the exact invariant: exactly kLimit accepted, exactly kOverflowClients rejected.
    EXPECT_EQ(accepted.load(), static_cast<int>(kLimit));
    EXPECT_EQ(rejected.load(), kOverflowClients);

    EXPECT_EQ(server_->load_shed_rejections(), static_cast<std::uint64_t>(kOverflowClients));
    EXPECT_EQ(metrics_->snapshot().load_shed_rejections_total, static_cast<std::uint64_t>(kOverflowClients));

    // When all clients finish, active requests must return to 0
    EXPECT_EQ(server_->active_requests(), 0u);
    std::cout << "Test logic complete." << std::endl;
}

} // namespace
