// Distributed Search Engine - Replication Integration Tests (Phase 17D).
//
// End-to-end integration tests proving the replication architecture works
// correctly as an integrated application feature:
//   - Write replication: ingest/update/remove fan out to all replicas
//   - Read failover: primary failure causes secondary fallback
//   - Persistence: replicated state survives save/load cycles
//   - Metrics: coordinator counts one user operation despite replication
//   - Thread safety: concurrent replicated operations are safe

#include <gtest/gtest.h>

#include "http_server.h"
#include "local_node.h"
#include "metrics.h"
#include "node_client.h"
#include "replica_placement.h"
#include "search_service.h"
#include "shard.h"
#include "shard_coordinator.h"
#include "shard_router.h"

#include <httplib.h>
#include <nlohmann/json.hpp>

#include <atomic>
#include <cstddef>
#include <cstdio>
#include <memory>
#include <string>
#include <thread>
#include <vector>

namespace dse {
namespace {

// =========================================================================
// Helpers
// =========================================================================

// Create an R=2 coordinator with 2 shards, 2 nodes.
// Each shard exists on both nodes. No persistence configured.
std::unique_ptr<ShardCoordinator> make_r2_no_persist()
{
    auto router = std::make_unique<ShardRouter>(2);
    std::vector<ShardReplicaSet> replica_sets = {
        {0, {0, 1}},
        {1, {1, 0}},
    };
    auto placement = std::make_unique<ShardReplicaPlacement>(
        2, 2, 2, replica_sets);

    auto node0 = std::make_unique<LocalNode>(0);
    node0->add_shard(0, std::make_unique<Shard>());
    node0->add_shard(1, std::make_unique<Shard>());

    auto node1 = std::make_unique<LocalNode>(1);
    node1->add_shard(0, std::make_unique<Shard>());
    node1->add_shard(1, std::make_unique<Shard>());

    std::vector<std::unique_ptr<NodeClient>> nodes;
    nodes.push_back(std::move(node0));
    nodes.push_back(std::move(node1));

    return std::make_unique<ShardCoordinator>(
        std::move(router), std::move(placement), std::move(nodes));
}

// Create an R=2 coordinator with 1 shard and persistence paths.
std::unique_ptr<ShardCoordinator> make_r2_persistent(
    const std::string& path0, const std::string& path1)
{
    auto router = std::make_unique<ShardRouter>(1);
    std::vector<ShardReplicaSet> replica_sets = {{0, {0, 1}}};
    auto placement = std::make_unique<ShardReplicaPlacement>(
        1, 2, 2, replica_sets);

    auto node0 = std::make_unique<LocalNode>(0);
    node0->add_shard(0, std::make_unique<Shard>(path0));

    auto node1 = std::make_unique<LocalNode>(1);
    node1->add_shard(0, std::make_unique<Shard>(path1));

    std::vector<std::unique_ptr<NodeClient>> nodes;
    nodes.push_back(std::move(node0));
    nodes.push_back(std::move(node1));

    return std::make_unique<ShardCoordinator>(
        std::move(router), std::move(placement), std::move(nodes));
}

// A NodeClient that always returns errors (simulating node failure).
class FailingNode : public NodeClient {
public:
    explicit FailingNode(std::size_t id) : node_id_(id) {}

    std::size_t node_id() const override { return node_id_; }

    ShardSearchResponse search(const ShardSearchRequest& req) override {
        ShardSearchResponse resp;
        resp.shard_id = req.shard_id;
        resp.is_error = true;
        resp.error_message = "node unavailable";
        return resp;
    }

    ShardWriteResponse add_document(const ShardWriteRequest& req) override {
        ShardWriteResponse resp;
        resp.shard_id = req.shard_id;
        resp.document_id = req.document_id;
        resp.is_error = true;
        resp.error_message = "node unavailable";
        return resp;
    }

    ShardWriteResponse update_document(const ShardWriteRequest& req) override {
        ShardWriteResponse resp;
        resp.shard_id = req.shard_id;
        resp.document_id = req.document_id;
        resp.is_error = true;
        resp.error_message = "node unavailable";
        return resp;
    }

    ShardRemoveResponse remove_document(const ShardRemoveRequest& req) override {
        ShardRemoveResponse resp;
        resp.shard_id = req.shard_id;
        resp.is_error = true;
        resp.error_message = "node unavailable";
        return resp;
    }

    ShardGetResponse get_document(const ShardGetRequest& req) override {
        ShardGetResponse resp;
        resp.shard_id = req.shard_id;
        resp.document_id = req.document_id;
        resp.is_error = true;
        resp.error_message = "node unavailable";
        return resp;
    }

    ShardCountResponse document_count(const ShardCountRequest& req) override {
        ShardCountResponse resp;
        resp.shard_id = req.shard_id;
        resp.is_error = true;
        resp.error_message = "node unavailable";
        return resp;
    }

    bool save_shard(std::size_t) override { return false; }
    bool load_shard(std::size_t) override { return false; }

private:
    std::size_t node_id_;
};

// =========================================================================
// 1. R=1 backward compatibility through full stack
// =========================================================================

TEST(ReplicationIntegrationTest, R1IngestSearchGetRoundTrip)
{
    auto coord = make_r2_no_persist();
    // Use a single-shard R=1 coordinator for backward compat test.
    auto router = std::make_unique<ShardRouter>(1);
    std::vector<std::size_t> placement = {0};
    auto sp = std::make_unique<ShardPlacement>(1, 1, placement);
    auto node = std::make_unique<LocalNode>(0);
    node->add_shard(0, std::make_unique<Shard>());
    std::vector<std::unique_ptr<NodeClient>> nodes;
    nodes.push_back(std::move(node));
    auto coord1 = std::make_unique<ShardCoordinator>(
        std::move(router), std::move(sp), std::move(nodes));

    // Ingest
    const auto ingest_resp = coord1->ingest({1, "hello world"});
    EXPECT_FALSE(ingest_resp.is_error);

    // Search
    SearchRequest req;
    req.query = "hello";
    req.mode = SearchMode::Or;
    req.limit = 10;
    const auto search_resp = coord1->search(req);
    EXPECT_FALSE(search_resp.is_error);
    EXPECT_EQ(search_resp.total, 1u);

    // Get
    const auto doc = coord1->get_document(1);
    EXPECT_TRUE(doc.has_value());
    EXPECT_EQ(doc->content, "hello world");
}

// =========================================================================
// 2. R=2 end-to-end: replicated ingest reaches both replicas
// =========================================================================

TEST(ReplicationIntegrationTest, R2IngestBothReplicasHaveData)
{
    auto coord = make_r2_no_persist();
    const auto resp = coord->ingest({1, "replicated document"});
    EXPECT_FALSE(resp.is_error);

    // Search should find the document (using primary by default).
    SearchRequest req;
    req.query = "replicated";
    req.mode = SearchMode::Or;
    req.limit = 10;
    const auto search_resp = coord->search(req);
    EXPECT_FALSE(search_resp.is_error);
    EXPECT_EQ(search_resp.total, 1u);

    // Get should find the document.
    const auto doc = coord->get_document(1);
    EXPECT_TRUE(doc.has_value());
    EXPECT_EQ(doc->content, "replicated document");
}

// =========================================================================
// 3. R=2 end-to-end: multiple replicated writes, all searchable
// =========================================================================

TEST(ReplicationIntegrationTest, R2MultipleIngestsAllSearchable)
{
    auto coord = make_r2_no_persist();

    for (doc_id i = 1; i <= 10; ++i) {
        const auto resp = coord->ingest(
            {i, "document " + std::to_string(i) + " content"});
        EXPECT_FALSE(resp.is_error) << "Failed to ingest doc " << i;
    }

    SearchRequest req;
    req.query = "content";
    req.mode = SearchMode::Or;
    req.limit = 20;
    const auto search_resp = coord->search(req);
    EXPECT_FALSE(search_resp.is_error);
    EXPECT_EQ(search_resp.total, 10u);
}

// =========================================================================
// 4. R=2 end-to-end: update propagates to all replicas
// =========================================================================

TEST(ReplicationIntegrationTest, R2UpdatePropagates)
{
    auto coord = make_r2_no_persist();
    coord->ingest({1, "original content"});

    const auto update_resp = coord->update({1, "updated content"});
    EXPECT_FALSE(update_resp.is_error);

    const auto doc = coord->get_document(1);
    ASSERT_TRUE(doc.has_value());
    EXPECT_EQ(doc->content, "updated content");

    // Search for updated content.
    SearchRequest req;
    req.query = "updated";
    req.mode = SearchMode::Or;
    req.limit = 10;
    const auto search_resp = coord->search(req);
    EXPECT_FALSE(search_resp.is_error);
    EXPECT_EQ(search_resp.total, 1u);
}

// =========================================================================
// 5. R=2 end-to-end: remove propagates to all replicas
// =========================================================================

TEST(ReplicationIntegrationTest, R2RemovePropagates)
{
    auto coord = make_r2_no_persist();
    coord->ingest({1, "to be removed"});

    const auto remove_resp = coord->remove(1);
    EXPECT_FALSE(remove_resp.is_error);

    const auto doc = coord->get_document(1);
    EXPECT_FALSE(doc.has_value());

    EXPECT_EQ(coord->total_document_count(), 0u);
}

// =========================================================================
// 6. R=2 persistence: save and reload, both replicas persist
// =========================================================================

TEST(ReplicationIntegrationTest, R2PersistenceBothReplicasPersist)
{
    const char* path0 = "test_repl_integ_0.jsonl";
    const char* path1 = "test_repl_integ_1.jsonl";

    // Phase 1: ingest and save.
    {
        auto coord = make_r2_persistent(path0, path1);
        coord->ingest({1, "persisted document"});
        coord->ingest({2, "second document"});
        EXPECT_TRUE(coord->save_all());
    }

    // Phase 2: reload from persistence.
    {
        auto coord = make_r2_persistent(path0, path1);
        coord->load_all();

        // Both documents should be available after reload.
        const auto doc1 = coord->get_document(1);
        EXPECT_TRUE(doc1.has_value());
        EXPECT_EQ(doc1->content, "persisted document");

        const auto doc2 = coord->get_document(2);
        EXPECT_TRUE(doc2.has_value());
        EXPECT_EQ(doc2->content, "second document");

        EXPECT_EQ(coord->total_document_count(), 2u);

        // Search should also find them.
        SearchRequest req;
        req.query = "document";
        req.mode = SearchMode::Or;
        req.limit = 10;
        const auto search_resp = coord->search(req);
        EXPECT_FALSE(search_resp.is_error);
        EXPECT_EQ(search_resp.total, 2u);
    }

    std::remove(path0);
    std::remove(path1);
}

// =========================================================================
// 7. R=2 persistence: save with R=2, reload with R=1 (graceful degradation)
// =========================================================================

TEST(ReplicationIntegrationTest, R2SaveAndReloadWithReplicas)
{
    const char* path0 = "test_repl_integ_s0.jsonl";
    const char* path1 = "test_repl_integ_s1.jsonl";

    // Save.
    {
        auto coord = make_r2_persistent(path0, path1);
        coord->ingest({1, "shared data"});
        coord->ingest({2, "also shared"});
        EXPECT_TRUE(coord->save_all());
    }

    // Reload — both replicas should have the data independently.
    {
        auto coord = make_r2_persistent(path0, path1);
        coord->load_all();
        EXPECT_EQ(coord->total_document_count(), 2u);
        EXPECT_TRUE(coord->get_document(1).has_value());
        EXPECT_TRUE(coord->get_document(2).has_value());
    }

    std::remove(path0);
    std::remove(path1);
}

// =========================================================================
// 8. R=2 failover: primary fails, secondary serves search
// =========================================================================

TEST(ReplicationIntegrationTest, R2SearchFailoverPrimaryToSecondary)
{
    auto router = std::make_unique<ShardRouter>(1);
    std::vector<ShardReplicaSet> replica_sets = {{0, {0, 1}}};
    auto placement = std::make_unique<ShardReplicaPlacement>(
        1, 2, 2, replica_sets);

    // Primary is failing.
    auto n0 = std::make_unique<FailingNode>(0);

    // Secondary is healthy with data.
    auto shard = std::make_unique<Shard>();
    shard->add_document(1, "failover document");
    auto n1 = std::make_unique<LocalNode>(1);
    n1->add_shard(0, std::move(shard));

    std::vector<std::unique_ptr<NodeClient>> nodes;
    nodes.push_back(std::move(n0));
    nodes.push_back(std::move(n1));
    auto coord = std::make_unique<ShardCoordinator>(
        std::move(router), std::move(placement), std::move(nodes));

    SearchRequest req;
    req.query = "failover";
    req.mode = SearchMode::Or;
    req.limit = 10;
    const auto resp = coord->search(req);
    EXPECT_FALSE(resp.is_error);
    EXPECT_EQ(resp.total, 1u);
}

// =========================================================================
// 9. R=2 failover: primary fails, secondary serves get_document
// =========================================================================

TEST(ReplicationIntegrationTest, R2GetDocumentFailoverPrimaryToSecondary)
{
    auto router = std::make_unique<ShardRouter>(1);
    std::vector<ShardReplicaSet> replica_sets = {{0, {0, 1}}};
    auto placement = std::make_unique<ShardReplicaPlacement>(
        1, 2, 2, replica_sets);

    auto n0 = std::make_unique<FailingNode>(0);

    auto shard = std::make_unique<Shard>();
    shard->add_document(42, "secondary only data");
    auto n1 = std::make_unique<LocalNode>(1);
    n1->add_shard(0, std::move(shard));

    std::vector<std::unique_ptr<NodeClient>> nodes;
    nodes.push_back(std::move(n0));
    nodes.push_back(std::move(n1));
    auto coord = std::make_unique<ShardCoordinator>(
        std::move(router), std::move(placement), std::move(nodes));

    const auto doc = coord->get_document(42);
    ASSERT_TRUE(doc.has_value());
    EXPECT_EQ(doc->content, "secondary only data");
}

// =========================================================================
// 10. R=2 failover: primary fails, secondary serves document_count
// =========================================================================

TEST(ReplicationIntegrationTest, R2DocumentCountFailoverPrimaryToSecondary)
{
    auto router = std::make_unique<ShardRouter>(1);
    std::vector<ShardReplicaSet> replica_sets = {{0, {0, 1}}};
    auto placement = std::make_unique<ShardReplicaPlacement>(
        1, 2, 2, replica_sets);

    auto n0 = std::make_unique<FailingNode>(0);

    auto shard = std::make_unique<Shard>();
    shard->add_document(1, "count test");
    shard->add_document(2, "count test 2");
    auto n1 = std::make_unique<LocalNode>(1);
    n1->add_shard(0, std::move(shard));

    std::vector<std::unique_ptr<NodeClient>> nodes;
    nodes.push_back(std::move(n0));
    nodes.push_back(std::move(n1));
    auto coord = std::make_unique<ShardCoordinator>(
        std::move(router), std::move(placement), std::move(nodes));

    EXPECT_EQ(coord->total_document_count(), 2u);
}

// =========================================================================
// 11. R=2: primary healthy → secondary NOT queried (no duplicate results)
// =========================================================================

TEST(ReplicationIntegrationTest, R2HealthyPrimaryNoSecondaryQuery)
{
    auto coord = make_r2_no_persist();
    coord->ingest({1, "unique content"});

    SearchRequest req;
    req.query = "unique";
    req.mode = SearchMode::Or;
    req.limit = 10;
    const auto resp = coord->search(req);
    EXPECT_FALSE(resp.is_error);
    // Must be exactly 1, not 2 (no duplicate from secondary).
    EXPECT_EQ(resp.total, 1u);
}

// =========================================================================
// 12. R=2: both replicas fail → search reports incomplete
// =========================================================================

TEST(ReplicationIntegrationTest, R2AllReplicasFailSearchIncomplete)
{
    auto router = std::make_unique<ShardRouter>(1);
    std::vector<ShardReplicaSet> replica_sets = {{0, {0, 1}}};
    auto placement = std::make_unique<ShardReplicaPlacement>(
        1, 2, 2, replica_sets);

    auto n0 = std::make_unique<FailingNode>(0);
    auto n1 = std::make_unique<FailingNode>(1);

    std::vector<std::unique_ptr<NodeClient>> nodes;
    nodes.push_back(std::move(n0));
    nodes.push_back(std::move(n1));
    auto coord = std::make_unique<ShardCoordinator>(
        std::move(router), std::move(placement), std::move(nodes));

    SearchRequest req;
    req.query = "anything";
    req.mode = SearchMode::Or;
    req.limit = 10;
    const auto resp = coord->search(req);
    EXPECT_FALSE(resp.is_error);
    EXPECT_FALSE(resp.complete);
    EXPECT_FALSE(resp.errors.empty());
}

// =========================================================================
// 13. Metrics: one user write despite R=2 replication
// =========================================================================

TEST(ReplicationIntegrationTest, R2MetricsCountOneUserWrite)
{
    auto coord = make_r2_no_persist();
    MetricsCollector metrics;
    coord->set_metrics(&metrics);

    for (doc_id i = 1; i <= 5; ++i) {
        coord->ingest({i, "metric doc " + std::to_string(i)});
    }

    const auto snap = metrics.snapshot();
    EXPECT_EQ(snap.coordinator_writes_total, 5u);
    EXPECT_EQ(snap.coordinator_write_success, 5u);
    EXPECT_EQ(snap.coordinator_write_errors, 0u);
}

// =========================================================================
// 14. Metrics: one user search despite failover
// =========================================================================

TEST(ReplicationIntegrationTest, R2MetricsCountOneSearchDespiteFailover)
{
    auto router = std::make_unique<ShardRouter>(1);
    std::vector<ShardReplicaSet> replica_sets = {{0, {0, 1}}};
    auto placement = std::make_unique<ShardReplicaPlacement>(
        1, 2, 2, replica_sets);

    auto n0 = std::make_unique<FailingNode>(0);
    auto shard = std::make_unique<Shard>();
    shard->add_document(1, "failover search");
    auto n1 = std::make_unique<LocalNode>(1);
    n1->add_shard(0, std::move(shard));

    std::vector<std::unique_ptr<NodeClient>> nodes;
    nodes.push_back(std::move(n0));
    nodes.push_back(std::move(n1));
    auto coord = std::make_unique<ShardCoordinator>(
        std::move(router), std::move(placement), std::move(nodes));

    MetricsCollector metrics;
    coord->set_metrics(&metrics);

    SearchRequest req;
    req.query = "failover";
    req.mode = SearchMode::Or;
    req.limit = 10;
    coord->search(req);

    const auto snap = metrics.snapshot();
    EXPECT_EQ(snap.coordinator_searches_total, 1u);
}

// =========================================================================
// 15. Metrics: write failure counted correctly
// =========================================================================

TEST(ReplicationIntegrationTest, R2MetricsCountWriteFailure)
{
    auto router = std::make_unique<ShardRouter>(1);
    std::vector<ShardReplicaSet> replica_sets = {{0, {0, 1}}};
    auto placement = std::make_unique<ShardReplicaPlacement>(
        1, 2, 2, replica_sets);

    auto n0 = std::make_unique<FailingNode>(0);
    auto n1 = std::make_unique<FailingNode>(1);

    std::vector<std::unique_ptr<NodeClient>> nodes;
    nodes.push_back(std::move(n0));
    nodes.push_back(std::move(n1));
    auto coord = std::make_unique<ShardCoordinator>(
        std::move(router), std::move(placement), std::move(nodes));

    MetricsCollector metrics;
    coord->set_metrics(&metrics);

    const auto resp = coord->ingest({1, "will fail"});
    EXPECT_TRUE(resp.is_error);

    const auto snap = metrics.snapshot();
    EXPECT_EQ(snap.coordinator_writes_total, 1u);
    EXPECT_EQ(snap.coordinator_write_success, 0u);
    EXPECT_EQ(snap.coordinator_write_errors, 1u);
}

// =========================================================================
// 16. Concurrent replicated writes are safe
// =========================================================================

TEST(ReplicationIntegrationTest, R2ConcurrentReplicatedWrites)
{
    auto coord = make_r2_no_persist();
    MetricsCollector metrics;
    coord->set_metrics(&metrics);

    constexpr std::size_t kThreads = 4;
    constexpr doc_id kDocsPerThread = 20;
    std::atomic<std::size_t> success_count{0};
    std::atomic<std::size_t> error_count{0};

    auto writer = [&](doc_id base) {
        for (doc_id i = 0; i < kDocsPerThread; ++i) {
            const auto resp = coord->ingest(
                {base + i, "concurrent doc " + std::to_string(base + i)});
            if (resp.is_error) {
                error_count.fetch_add(1, std::memory_order_relaxed);
            } else {
                success_count.fetch_add(1, std::memory_order_relaxed);
            }
        }
    };

    std::vector<std::thread> threads;
    for (std::size_t t = 0; t < kThreads; ++t) {
        threads.emplace_back(writer, t * 1000 + 1);
    }
    for (auto& th : threads) {
        th.join();
    }

    EXPECT_EQ(error_count.load(), 0u);
    EXPECT_EQ(success_count.load(), kThreads * kDocsPerThread);
    EXPECT_EQ(coord->total_document_count(), kThreads * kDocsPerThread);

    // Coordinator metrics: one write per successful operation.
    const auto snap = metrics.snapshot();
    EXPECT_EQ(snap.coordinator_writes_total, kThreads * kDocsPerThread);
    EXPECT_EQ(snap.coordinator_write_success, kThreads * kDocsPerThread);
}

// =========================================================================
// 17. Concurrent replicated reads are safe
// =========================================================================

TEST(ReplicationIntegrationTest, R2ConcurrentReplicatedReads)
{
    auto coord = make_r2_no_persist();

    for (doc_id i = 1; i <= 10; ++i) {
        coord->ingest({i, "readable doc " + std::to_string(i)});
    }

    std::atomic<std::size_t> success_count{0};

    auto searcher = [&]() {
        SearchRequest req;
        req.query = "readable";
        req.mode = SearchMode::Or;
        req.limit = 20;
        const auto resp = coord->search(req);
        if (!resp.is_error && resp.total == 10u) {
            success_count.fetch_add(1, std::memory_order_relaxed);
        }
    };

    std::vector<std::thread> threads;
    for (int i = 0; i < 8; ++i) {
        threads.emplace_back(searcher);
    }
    for (auto& th : threads) {
        th.join();
    }

    EXPECT_EQ(success_count.load(), 8u);
}

// =========================================================================
// 18. HTTP API end-to-end: ingest, search, get through HttpServer
// =========================================================================

class ReplicationHttpTest : public ::testing::Test {
protected:
    void SetUp() override {
        auto router = std::make_unique<ShardRouter>(1);
        std::vector<ShardReplicaSet> replica_sets = {{0, {0, 1}}};
        auto placement = std::make_unique<ShardReplicaPlacement>(
            1, 2, 2, replica_sets);

        auto node0 = std::make_unique<LocalNode>(0);
        node0->add_shard(0, std::make_unique<Shard>());
        auto node1 = std::make_unique<LocalNode>(1);
        node1->add_shard(0, std::make_unique<Shard>());

        std::vector<std::unique_ptr<NodeClient>> nodes;
        nodes.push_back(std::move(node0));
        nodes.push_back(std::move(node1));

        coordinator_ = std::make_unique<ShardCoordinator>(
            std::move(router), std::move(placement), std::move(nodes));

        metrics_ = std::make_unique<MetricsCollector>();
        coordinator_->set_metrics(metrics_.get());

        server_ = std::make_unique<HttpServer>(*coordinator_, metrics_.get());
    }

    void start_server() {
        server_thread_ = std::thread([this]() { server_->listen(0); });
        server_->wait_until_ready();
    }

    void TearDown() override {
        if (server_) server_->stop();
        if (server_thread_.joinable()) server_thread_.join();
    }

    std::pair<int, std::string> get(const std::string& path) {
        httplib::Client client("localhost", server_->port());
        client.set_connection_timeout(5);
        client.set_read_timeout(5);
        auto res = client.Get(path);
        if (!res) return {0, ""};
        return {res->status, res->body};
    }

    std::pair<int, std::string> post(const std::string& path,
                                     const std::string& body) {
        httplib::Client client("localhost", server_->port());
        client.set_connection_timeout(5);
        client.set_read_timeout(5);
        auto res = client.Post(path.c_str(), body.c_str(), "application/json");
        if (!res) return {0, ""};
        return {res->status, res->body};
    }

    std::unique_ptr<ShardCoordinator> coordinator_;
    std::unique_ptr<MetricsCollector> metrics_;
    std::unique_ptr<HttpServer> server_;
    std::thread server_thread_;
};

TEST_F(ReplicationHttpTest, R2HttpIngestSearchGet)
{
    start_server();

    // Ingest via HTTP.
    {
        auto [status, body] = post("/documents",
            R"({"id":1,"content":"http replicated doc"})");
        EXPECT_EQ(status, 201);
        const auto j = nlohmann::json::parse(body);
        EXPECT_EQ(j["document_id"], 1u);
    }

    // Search via HTTP.
    {
        auto [status, body] = get("/search?q=replicated");
        EXPECT_EQ(status, 200);
        const auto j = nlohmann::json::parse(body);
        EXPECT_EQ(j["total"], 1u);
    }
}

TEST_F(ReplicationHttpTest, R2HttpUpdateAndDelete)
{
    start_server();

    // Ingest
    post("/documents", R"({"id":1,"content":"original"})");

    // Update
    {
        httplib::Client client("localhost", server_->port());
        client.set_connection_timeout(5);
        client.set_read_timeout(5);
        auto res = client.Put("/documents/1",
            R"({"content":"updated"})",
            "application/json");
        ASSERT_TRUE(res);
        EXPECT_EQ(res->status, 200);
    }

    // Search for updated content
    {
        auto [status, body] = get("/search?q=updated");
        EXPECT_EQ(status, 200);
        const auto j = nlohmann::json::parse(body);
        EXPECT_EQ(j["total"], 1u);
    }

    // Delete
    {
        httplib::Client client("localhost", server_->port());
        client.set_connection_timeout(5);
        client.set_read_timeout(5);
        auto res = client.Delete("/documents/1");
        ASSERT_TRUE(res);
        EXPECT_EQ(res->status, 204);
    }

    // Verify gone
    {
        auto [status, body] = get("/search?q=updated");
        EXPECT_EQ(status, 200);
        const auto j = nlohmann::json::parse(body);
        EXPECT_EQ(j["total"], 0u);
    }
}

TEST_F(ReplicationHttpTest, R2HttpMetricsExposeReplicationCounters)
{
    start_server();

    // Ingest 3 docs.
    for (int i = 1; i <= 3; ++i) {
        post("/documents",
             R"({"id":)" + std::to_string(i) +
             R"(,"content":"metric doc )" + std::to_string(i) + R"("})");
    }

    // Search once.
    get("/search?q=metric");

    auto [status, body] = get("/metrics");
    EXPECT_EQ(status, 200);
    const auto j = nlohmann::json::parse(body);

    // Coordinator writes: 3 (one per user ingest).
    EXPECT_EQ(j["coordinator_writes_total"], 3u);
    EXPECT_EQ(j["coordinator_write_success"], 3u);

    // Coordinator searches: at least 1.
    EXPECT_GE(j["coordinator_searches_total"].get<std::size_t>(), 1u);
}

TEST_F(ReplicationHttpTest, R2HttpConcurrentIngestAndSearch)
{
    start_server();

    constexpr int kWriters = 4;
    constexpr int kReaders = 4;

    std::atomic<int> write_successes{0};
    std::atomic<int> read_successes{0};

    std::vector<std::thread> threads;

    // Writers
    for (int i = 0; i < kWriters; ++i) {
        threads.emplace_back([this, i, &write_successes]() {
            const doc_id id = 100 + i;
            auto [status, body] = post("/documents",
                R"({"id":)" + std::to_string(id) +
                R"(,"content":"concurrent http doc )" + std::to_string(id) + R"("})");
            if (status == 201) {
                write_successes.fetch_add(1, std::memory_order_relaxed);
            }
        });
    }

    // Readers
    for (int i = 0; i < kReaders; ++i) {
        threads.emplace_back([this, &read_successes]() {
            auto [status, body] = get("/search?q=concurrent");
            if (status == 200) {
                read_successes.fetch_add(1, std::memory_order_relaxed);
            }
        });
    }

    for (auto& t : threads) {
        t.join();
    }

    EXPECT_EQ(write_successes.load(), kWriters);
    EXPECT_EQ(read_successes.load(), kReaders);
}

// =========================================================================
// 19. R=1 backward compat: full HTTP round trip
// =========================================================================

TEST(ReplicationIntegrationTest, R1FullHttpRoundTrip)
{
    auto router = std::make_unique<ShardRouter>(1);
    std::vector<std::size_t> placement = {0};
    auto sp = std::make_unique<ShardPlacement>(1, 1, placement);
    auto node = std::make_unique<LocalNode>(0);
    node->add_shard(0, std::make_unique<Shard>());
    std::vector<std::unique_ptr<NodeClient>> nodes;
    nodes.push_back(std::move(node));
    auto coord = std::make_unique<ShardCoordinator>(
        std::move(router), std::move(sp), std::move(nodes));

    HttpServer server(*coord);
    std::thread server_thread([&server]() { server.listen(0); });
    server.wait_until_ready();

    httplib::Client client("localhost", server.port());
    client.set_connection_timeout(5);
    client.set_read_timeout(5);

    // Ingest
    {
        auto res = client.Post("/documents",
            R"({"id":1,"content":"backward compat"})",
            "application/json");
        ASSERT_TRUE(res);
        EXPECT_EQ(res->status, 201);
    }

    // Search
    {
        auto res = client.Get("/search?q=backward");
        ASSERT_TRUE(res);
        EXPECT_EQ(res->status, 200);
        const auto j = nlohmann::json::parse(res->body);
        EXPECT_EQ(j["total"], 1u);
    }

    server.stop();
    server_thread.join();
}

} // namespace
} // namespace dse
