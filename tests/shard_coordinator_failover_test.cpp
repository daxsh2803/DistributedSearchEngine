// Distributed Search Engine - Shard Coordinator Failover Tests (Phase 17C).
//
// Tests for ShardCoordinator with replica-aware read failover.
// Verifies that search, get_document, and document_count can fail over
// from primary to secondary replicas when the primary is unavailable.

#include "shard_coordinator.h"

#include <gtest/gtest.h>

#include <atomic>
#include <cstddef>
#include <memory>
#include <string>
#include <thread>
#include <vector>

#include "inverted_index.h"
#include "local_node.h"
#include "metrics.h"
#include "node_client.h"
#include "replica_placement.h"
#include "search_service.h"
#include "shard.h"
#include "shard_router.h"

namespace dse {
namespace {

// ---------------------------------------------------------------------------
// A NodeClient that always returns errors (for simulating node failure).
// ---------------------------------------------------------------------------
class FailingNode : public NodeClient {
public:
    explicit FailingNode(std::size_t id) : node_id_(id) {}

    std::size_t node_id() const override { return node_id_; }

    ShardSearchResponse search(const ShardSearchRequest& req) override {
        ShardSearchResponse resp;
        resp.shard_id = req.shard_id;
        resp.is_error = true;
        resp.error_message = "node " + std::to_string(node_id_) + " unavailable";
        return resp;
    }

    ShardWriteResponse add_document(const ShardWriteRequest& req) override {
        ShardWriteResponse resp;
        resp.shard_id = req.shard_id;
        resp.document_id = req.document_id;
        resp.is_error = true;
        resp.error_message = "node " + std::to_string(node_id_) + " unavailable";
        return resp;
    }

    ShardWriteResponse update_document(const ShardWriteRequest& req) override {
        ShardWriteResponse resp;
        resp.shard_id = req.shard_id;
        resp.document_id = req.document_id;
        resp.is_error = true;
        resp.error_message = "node " + std::to_string(node_id_) + " unavailable";
        return resp;
    }

    ShardRemoveResponse remove_document(const ShardRemoveRequest& req) override {
        ShardRemoveResponse resp;
        resp.shard_id = req.shard_id;
        resp.is_error = true;
        resp.error_message = "node " + std::to_string(node_id_) + " unavailable";
        return resp;
    }

    ShardGetResponse get_document(const ShardGetRequest& req) override {
        ShardGetResponse resp;
        resp.shard_id = req.shard_id;
        resp.document_id = req.document_id;
        resp.is_error = true;
        resp.error_message = "node " + std::to_string(node_id_) + " unavailable";
        return resp;
    }

    ShardCountResponse document_count(const ShardCountRequest& req) override {
        ShardCountResponse resp;
        resp.shard_id = req.shard_id;
        resp.is_error = true;
        resp.error_message = "node " + std::to_string(node_id_) + " unavailable";
        return resp;
    }

    bool save_shard(std::size_t) override { return false; }
    bool load_shard(std::size_t) override { return false; }

private:
    std::size_t node_id_;
};

// ---------------------------------------------------------------------------
// Helper: R=2 coordinator with primary healthy
// ---------------------------------------------------------------------------
std::unique_ptr<ShardCoordinator> make_r2_healthy()
{
    auto router = std::make_unique<ShardRouter>(1);
    std::vector<ShardReplicaSet> replica_sets = {{0, {0, 1}}};
    auto placement = std::make_unique<ShardReplicaPlacement>(1, 2, 2, replica_sets);

    auto node0 = std::make_unique<LocalNode>(0);
    node0->add_shard(0, std::make_unique<Shard>());
    auto node1 = std::make_unique<LocalNode>(1);
    node1->add_shard(0, std::make_unique<Shard>());

    std::vector<std::unique_ptr<NodeClient>> nodes;
    nodes.push_back(std::move(node0));
    nodes.push_back(std::move(node1));

    return std::make_unique<ShardCoordinator>(
        std::move(router), std::move(placement), std::move(nodes));
}

// ---------------------------------------------------------------------------
// Helper: R=2 coordinator where both nodes are FailingNodes
// ---------------------------------------------------------------------------
std::unique_ptr<ShardCoordinator> make_r2_all_failing()
{
    auto router = std::make_unique<ShardRouter>(1);
    std::vector<ShardReplicaSet> replica_sets = {{0, {0, 1}}};
    auto placement = std::make_unique<ShardReplicaPlacement>(1, 2, 2, replica_sets);

    auto node0 = std::make_unique<FailingNode>(0);
    auto node1 = std::make_unique<FailingNode>(1);

    std::vector<std::unique_ptr<NodeClient>> nodes;
    nodes.push_back(std::move(node0));
    nodes.push_back(std::move(node1));

    return std::make_unique<ShardCoordinator>(
        std::move(router), std::move(placement), std::move(nodes));
}

// =========================================================================
// 1. R=1 backward compatibility
// =========================================================================

TEST(CoordinatorFailoverTest, R1PrimarySuccess)
{
    auto router = std::make_unique<ShardRouter>(1);
    std::vector<std::size_t> placement(1, 0);
    auto sp = std::make_unique<ShardPlacement>(1, 1, placement);
    auto node = std::make_unique<LocalNode>(0);
    node->add_shard(0, std::make_unique<Shard>());
    std::vector<std::unique_ptr<NodeClient>> nodes;
    nodes.push_back(std::move(node));
    auto coord = std::make_unique<ShardCoordinator>(
        std::move(router), std::move(sp), std::move(nodes));

    coord->ingest({1, "hello"});
    const auto doc = coord->get_document(1);
    EXPECT_TRUE(doc.has_value());
    EXPECT_EQ(doc->content, "hello");
}

TEST(CoordinatorFailoverTest, R1PrimaryFailure)
{
    auto router = std::make_unique<ShardRouter>(1);
    std::vector<std::size_t> placement(1, 0);
    auto sp = std::make_unique<ShardPlacement>(1, 1, placement);
    auto node = std::make_unique<FailingNode>(0);
    std::vector<std::unique_ptr<NodeClient>> nodes;
    nodes.push_back(std::move(node));
    auto coord = std::make_unique<ShardCoordinator>(
        std::move(router), std::move(sp), std::move(nodes));

    // Search with failing node: Phase 13 semantics use complete=false + errors
    // rather than is_error=true.
    SearchRequest req;
    req.query = "hello";
    req.mode = SearchMode::Or;
    req.limit = 10;
    const auto resp = coord->search(req);
    EXPECT_FALSE(resp.is_error);  // search request itself succeeded
    EXPECT_FALSE(resp.complete);  // but shard was unavailable
    EXPECT_FALSE(resp.errors.empty());  // error is recorded
}

// =========================================================================
// 2. R=2 search: primary success
// =========================================================================

TEST(CoordinatorFailoverTest, R2SearchPrimarySuccess)
{
    auto coord = make_r2_healthy();
    coord->ingest({1, "quick brown fox"});

    SearchRequest req;
    req.query = "fox";
    req.mode = SearchMode::Or;
    req.limit = 10;
    const auto resp = coord->search(req);
    EXPECT_FALSE(resp.is_error);
    EXPECT_EQ(resp.total, 1u);
}

// =========================================================================
// 3. R=2 search: primary failure → secondary success
// =========================================================================

TEST(CoordinatorFailoverTest, R2SearchPrimaryFailsFallbackToSecondary)
{
    // 1 shard, R=2: primary=failing, secondary=healthy with data.
    auto router = std::make_unique<ShardRouter>(1);
    std::vector<ShardReplicaSet> replica_sets = {{0, {0, 1}}};
    auto placement = std::make_unique<ShardReplicaPlacement>(1, 2, 2, replica_sets);

    auto n0 = std::make_unique<FailingNode>(0);
    auto shard1 = std::make_unique<Shard>();
    shard1->add_document(1, "fallback doc");
    auto n1 = std::make_unique<LocalNode>(1);
    n1->add_shard(0, std::move(shard1));

    std::vector<std::unique_ptr<NodeClient>> nodes;
    nodes.push_back(std::move(n0));
    nodes.push_back(std::move(n1));
    auto coord = std::make_unique<ShardCoordinator>(
        std::move(router), std::move(placement), std::move(nodes));

    SearchRequest req;
    req.query = "fallback";
    req.mode = SearchMode::Or;
    req.limit = 10;
    const auto resp = coord->search(req);
    EXPECT_FALSE(resp.is_error);
    EXPECT_EQ(resp.total, 1u);
}

// =========================================================================
// 4. R=3 search: first two fail → third succeeds
// =========================================================================

TEST(CoordinatorFailoverTest, R3SearchTwoFailsThirdSucceeds)
{
    auto router = std::make_unique<ShardRouter>(1);
    std::vector<ShardReplicaSet> replica_sets = {{0, {0, 1, 2}}};
    auto placement = std::make_unique<ShardReplicaPlacement>(1, 3, 3, replica_sets);

    auto n0 = std::make_unique<FailingNode>(0);
    auto n1 = std::make_unique<FailingNode>(1);
    auto shard2 = std::make_unique<Shard>();
    shard2->add_document(1, "third replica doc");
    auto n2 = std::make_unique<LocalNode>(2);
    n2->add_shard(0, std::move(shard2));

    std::vector<std::unique_ptr<NodeClient>> nodes;
    nodes.push_back(std::move(n0));
    nodes.push_back(std::move(n1));
    nodes.push_back(std::move(n2));
    auto coord = std::make_unique<ShardCoordinator>(
        std::move(router), std::move(placement), std::move(nodes));

    SearchRequest req;
    req.query = "third";
    req.mode = SearchMode::Or;
    req.limit = 10;
    const auto resp = coord->search(req);
    EXPECT_FALSE(resp.is_error);
    EXPECT_EQ(resp.total, 1u);
}

// =========================================================================
// 5. All replicas fail → search reports error
// =========================================================================

TEST(CoordinatorFailoverTest, R2SearchAllReplicasFail)
{
    auto coord = make_r2_all_failing();
    SearchRequest req;
    req.query = "hello";
    req.mode = SearchMode::Or;
    req.limit = 10;
    const auto resp = coord->search(req);
    // Phase 13 semantics: search succeeds but is degraded.
    EXPECT_FALSE(resp.is_error);
    EXPECT_FALSE(resp.complete);
    EXPECT_FALSE(resp.errors.empty());
}

// =========================================================================
// 6. Primary search returns empty result → NO failover
// =========================================================================

TEST(CoordinatorFailoverTest, R2SearchEmptyResultNoFailover)
{
    // With R=2, both replicas have an empty shard.
    // Search should succeed with 0 results, not fail over.
    auto coord = make_r2_healthy();
    SearchRequest req;
    req.query = "nonexistent";
    req.mode = SearchMode::Or;
    req.limit = 10;
    const auto resp = coord->search(req);
    EXPECT_FALSE(resp.is_error);
    EXPECT_EQ(resp.total, 0u);
}

// =========================================================================
// 7. Multi-shard search with failover on one shard
// =========================================================================

TEST(CoordinatorFailoverTest, MultiShardSearchFailoverOneShard)
{
    // 2 shards, each R=2.
    // Shard 0: primary=healthy, secondary=healthy.
    // Shard 1: primary=failing, secondary=healthy.
    auto router = std::make_unique<ShardRouter>(2);
    std::vector<ShardReplicaSet> replica_sets = {
        {0, {0, 1}},  // shard 0: primary=node0(healthy)
        {1, {2, 3}},  // shard 1: primary=node2(fail), secondary=node3(healthy)
    };
    auto placement = std::make_unique<ShardReplicaPlacement>(2, 4, 2, replica_sets);

    // Pre-populate shards with data before adding to nodes.
    auto s0_0 = std::make_unique<Shard>();
    s0_0->add_document(1, "shard zero doc");
    auto n0 = std::make_unique<LocalNode>(0);
    n0->add_shard(0, std::move(s0_0));
    n0->add_shard(1, std::make_unique<Shard>());
    auto n1 = std::make_unique<LocalNode>(1);
    n1->add_shard(0, std::make_unique<Shard>());
    n1->add_shard(1, std::make_unique<Shard>());
    auto n2 = std::make_unique<FailingNode>(2);
    auto s3_1 = std::make_unique<Shard>();
    s3_1->add_document(2, "shard one doc");
    auto n3 = std::make_unique<LocalNode>(3);
    n3->add_shard(0, std::make_unique<Shard>());
    n3->add_shard(1, std::move(s3_1));

    std::vector<std::unique_ptr<NodeClient>> nodes;
    nodes.push_back(std::move(n0));
    nodes.push_back(std::move(n1));
    nodes.push_back(std::move(n2));
    nodes.push_back(std::move(n3));
    auto coord = std::make_unique<ShardCoordinator>(
        std::move(router), std::move(placement), std::move(nodes));

    SearchRequest req;
    req.query = "doc";
    req.mode = SearchMode::Or;
    req.limit = 10;
    const auto resp = coord->search(req);
    EXPECT_FALSE(resp.is_error);
    EXPECT_EQ(resp.total, 2u);
}

// =========================================================================
// 8. get_document: primary failure → secondary success
// =========================================================================

TEST(CoordinatorFailoverTest, R2GetDocumentPrimaryFailsFallback)
{
    // 1 shard, R=2, primary=failing, secondary=healthy with data.
    auto router = std::make_unique<ShardRouter>(1);
    std::vector<ShardReplicaSet> replica_sets = {{0, {0, 1}}};
    auto placement = std::make_unique<ShardReplicaPlacement>(1, 2, 2, replica_sets);

    auto n0 = std::make_unique<FailingNode>(0);
    auto shard1 = std::make_unique<Shard>();
    shard1->add_document(42, "secondary data");
    auto n1 = std::make_unique<LocalNode>(1);
    n1->add_shard(0, std::move(shard1));

    std::vector<std::unique_ptr<NodeClient>> nodes;
    nodes.push_back(std::move(n0));
    nodes.push_back(std::move(n1));
    auto coord = std::make_unique<ShardCoordinator>(
        std::move(router), std::move(placement), std::move(nodes));

    const auto doc = coord->get_document(42);
    ASSERT_TRUE(doc.has_value());
    EXPECT_EQ(doc->content, "secondary data");
}

// =========================================================================
// 9. get_document: successful not-found does not trigger failover
// =========================================================================

TEST(CoordinatorFailoverTest, R2GetDocumentNotFoundNoFailover)
{
    auto coord = make_r2_healthy();
    // Document doesn't exist on any replica.
    const auto doc = coord->get_document(999);
    EXPECT_FALSE(doc.has_value());
}

// =========================================================================
// 10. document_count: primary failure → secondary success
// =========================================================================

TEST(CoordinatorFailoverTest, R2DocumentCountPrimaryFailsFallback)
{
    auto router = std::make_unique<ShardRouter>(1);
    std::vector<ShardReplicaSet> replica_sets = {{0, {0, 1}}};
    auto placement = std::make_unique<ShardReplicaPlacement>(1, 2, 2, replica_sets);

    auto n0 = std::make_unique<FailingNode>(0);
    auto shard1 = std::make_unique<Shard>();
    shard1->add_document(1, "test");
    auto n1 = std::make_unique<LocalNode>(1);
    n1->add_shard(0, std::move(shard1));

    std::vector<std::unique_ptr<NodeClient>> nodes;
    nodes.push_back(std::move(n0));
    nodes.push_back(std::move(n1));
    auto coord = std::make_unique<ShardCoordinator>(
        std::move(router), std::move(placement), std::move(nodes));

    EXPECT_EQ(coord->total_document_count(), 1u);
}

// =========================================================================
// 11. document_count: not summed across replicas
// =========================================================================

TEST(CoordinatorFailoverTest, R2DocumentCountNotSummed)
{
    // Both replicas healthy, same data. Count should be 1, not 2.
    auto coord = make_r2_healthy();
    coord->ingest({1, "hello"});
    EXPECT_EQ(coord->total_document_count(), 1u);
}

// =========================================================================
// 12. coordinator metrics: one search despite failover
// =========================================================================

TEST(CoordinatorFailoverTest, R2MetricsOneSearchDespiteFailover)
{
    // Shard 0: primary=fail, secondary=healthy with data.
    auto router = std::make_unique<ShardRouter>(1);
    std::vector<ShardReplicaSet> replica_sets = {{0, {0, 1}}};
    auto placement = std::make_unique<ShardReplicaPlacement>(1, 2, 2, replica_sets);

    auto n0 = std::make_unique<FailingNode>(0);
    auto shard1 = std::make_unique<Shard>();
    shard1->add_document(1, "test");
    auto n1 = std::make_unique<LocalNode>(1);
    n1->add_shard(0, std::move(shard1));

    std::vector<std::unique_ptr<NodeClient>> nodes;
    nodes.push_back(std::move(n0));
    nodes.push_back(std::move(n1));
    auto coord = std::make_unique<ShardCoordinator>(
        std::move(router), std::move(placement), std::move(nodes));

    MetricsCollector metrics;
    coord->set_metrics(&metrics);

    SearchRequest req;
    req.query = "test";
    req.mode = SearchMode::Or;
    req.limit = 10;
    coord->search(req);

    const auto snap = metrics.snapshot();
    // Exactly one coordinator search, not two (one per attempted replica).
    EXPECT_EQ(snap.coordinator_searches_total, 1u);
}

// =========================================================================
// 13. concurrent failover searches are safe
// =========================================================================

TEST(CoordinatorFailoverTest, R2ConcurrentFailoverSearches)
{
    auto router = std::make_unique<ShardRouter>(1);
    std::vector<ShardReplicaSet> replica_sets = {{0, {0, 1}}};
    auto placement = std::make_unique<ShardReplicaPlacement>(1, 2, 2, replica_sets);

    auto n0 = std::make_unique<FailingNode>(0);
    auto shard1 = std::make_unique<Shard>();
    shard1->add_document(1, "concurrent");
    auto n1 = std::make_unique<LocalNode>(1);
    n1->add_shard(0, std::move(shard1));

    std::vector<std::unique_ptr<NodeClient>> nodes;
    nodes.push_back(std::move(n0));
    nodes.push_back(std::move(n1));
    auto coord = std::make_unique<ShardCoordinator>(
        std::move(router), std::move(placement), std::move(nodes));

    std::atomic<std::size_t> success_count{0};

    auto searcher = [&]() {
        SearchRequest req;
        req.query = "concurrent";
        req.mode = SearchMode::Or;
        req.limit = 10;
        const auto resp = coord->search(req);
        if (!resp.is_error && resp.total > 0) {
            success_count.fetch_add(1, std::memory_order_relaxed);
        }
    };

    std::vector<std::thread> threads;
    for (int i = 0; i < 8; ++i) {
        threads.emplace_back(searcher);
    }
    for (auto& t : threads) {
        t.join();
    }

    EXPECT_EQ(success_count.load(), 8u);
}

// =========================================================================
// 14. R=1 backward compat: search with single healthy node
// =========================================================================

TEST(CoordinatorFailoverTest, R1SearchWorks)
{
    auto router = std::make_unique<ShardRouter>(1);
    std::vector<std::size_t> placement(1, 0);
    auto sp = std::make_unique<ShardPlacement>(1, 1, placement);
    auto node = std::make_unique<LocalNode>(0);
    node->add_shard(0, std::make_unique<Shard>());
    std::vector<std::unique_ptr<NodeClient>> nodes;
    nodes.push_back(std::move(node));
    auto coord = std::make_unique<ShardCoordinator>(
        std::move(router), std::move(sp), std::move(nodes));

    coord->ingest({1, "hello world"});

    SearchRequest req;
    req.query = "hello";
    req.mode = SearchMode::Or;
    req.limit = 10;
    const auto resp = coord->search(req);
    EXPECT_FALSE(resp.is_error);
    EXPECT_EQ(resp.total, 1u);
}

// =========================================================================
// 15. get_document: all replicas fail → returns nullopt
// =========================================================================

TEST(CoordinatorFailoverTest, R2GetDocumentAllReplicasFail)
{
    auto coord = make_r2_all_failing();
    const auto doc = coord->get_document(1);
    EXPECT_FALSE(doc.has_value());
}

// =========================================================================
// 16. document_count: all replicas fail → total is 0, incomplete
// =========================================================================

TEST(CoordinatorFailoverTest, R2DocumentCountAllReplicasFail)
{
    auto coord = make_r2_all_failing();
    // total_document_count uses compute_global_n which reports 0 when
    // all shards fail (incomplete).
    EXPECT_EQ(coord->total_document_count(), 0u);
}

// =========================================================================
// 17. R=2 get_document: primary found → returns without failover
// =========================================================================

TEST(CoordinatorFailoverTest, R2GetDocumentPrimaryFound)
{
    auto coord = make_r2_healthy();
    coord->ingest({1, "primary data"});

    const auto doc = coord->get_document(1);
    ASSERT_TRUE(doc.has_value());
    EXPECT_EQ(doc->content, "primary data");
}

// =========================================================================
// 18. R=2 search: fallback results are not duplicated
// =========================================================================

TEST(CoordinatorFailoverTest, R2SearchFallbackNotDuplicated)
{
    // 1 shard, R=2. Primary fails, secondary has data.
    // Search should return results from secondary only, not both.
    auto router = std::make_unique<ShardRouter>(1);
    std::vector<ShardReplicaSet> replica_sets = {{0, {0, 1}}};
    auto placement = std::make_unique<ShardReplicaPlacement>(1, 2, 2, replica_sets);

    auto n0 = std::make_unique<FailingNode>(0);
    auto shard1 = std::make_unique<Shard>();
    for (doc_id i = 1; i <= 3; ++i) {
        shard1->add_document(i, "unique_" + std::to_string(i));
    }
    auto n1 = std::make_unique<LocalNode>(1);
    n1->add_shard(0, std::move(shard1));

    std::vector<std::unique_ptr<NodeClient>> nodes;
    nodes.push_back(std::move(n0));
    nodes.push_back(std::move(n1));
    auto coord = std::make_unique<ShardCoordinator>(
        std::move(router), std::move(placement), std::move(nodes));

    SearchRequest req;
    req.query = "unique";
    req.mode = SearchMode::Or;
    req.limit = 10;
    const auto resp = coord->search(req);
    EXPECT_FALSE(resp.is_error);
    // Must be exactly 3, not 6 (duplicated).
    EXPECT_EQ(resp.total, 3u);
}

} // namespace
} // namespace dse
