// Distributed Search Engine - Shard Coordinator Failover Tests (Phase 17C).
//
// Tests for ShardCoordinator with replica-aware read failover.
// Verifies that search, get_document, and document_count can fail over
// from primary to secondary replicas when the primary is unavailable.

#include "shard_coordinator.h"

#include <gtest/gtest.h>

#include <atomic>
#include <cmath>
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
// A NodeClient wrapper that delegates to a LocalNode, but can fail search
// after count succeeds, and tracks how many times each method was called.
// ---------------------------------------------------------------------------
class InspectableNode : public NodeClient {
public:
    InspectableNode(std::size_t id, std::unique_ptr<LocalNode> delegate)
        : node_id_(id), delegate_(std::move(delegate)) {}

    std::size_t node_id() const override { return node_id_; }

    ShardSearchResponse search(const ShardSearchRequest& req) override {
        search_calls.fetch_add(1, std::memory_order_relaxed);
        if (fail_search_) {
            ShardSearchResponse resp;
            resp.shard_id = req.shard_id;
            resp.is_error = true;
            resp.error_message = "simulated search failure on node " + std::to_string(node_id_);
            return resp;
        }
        return delegate_->search(req);
    }

    ShardWriteResponse add_document(const ShardWriteRequest& req) override {
        return delegate_->add_document(req);
    }

    ShardWriteResponse update_document(const ShardWriteRequest& req) override {
        return delegate_->update_document(req);
    }

    ShardRemoveResponse remove_document(const ShardRemoveRequest& req) override {
        return delegate_->remove_document(req);
    }

    ShardGetResponse get_document(const ShardGetRequest& req) override {
        get_calls.fetch_add(1, std::memory_order_relaxed);
        if (fail_get_) {
            ShardGetResponse resp;
            resp.shard_id = req.shard_id;
            resp.document_id = req.document_id;
            resp.is_error = true;
            resp.error_message = "simulated get failure on node " + std::to_string(node_id_);
            return resp;
        }
        return delegate_->get_document(req);
    }

    ShardCountResponse document_count(const ShardCountRequest& req) override {
        count_calls.fetch_add(1, std::memory_order_relaxed);
        if (fail_count_) {
            ShardCountResponse resp;
            resp.shard_id = req.shard_id;
            resp.is_error = true;
            resp.error_message = "simulated count failure on node " + std::to_string(node_id_);
            return resp;
        }
        return delegate_->document_count(req);
    }

    bool save_shard(std::size_t sid) override { return delegate_->save_shard(sid); }
    bool load_shard(std::size_t sid) override { return delegate_->load_shard(sid); }

    void set_fail_search(bool fail) { fail_search_ = fail; }
    void set_fail_count(bool fail) { fail_count_ = fail; }
    void set_fail_get(bool fail) { fail_get_ = fail; }

    std::atomic<std::size_t> search_calls{0};
    std::atomic<std::size_t> count_calls{0};
    std::atomic<std::size_t> get_calls{0};

private:
    std::size_t node_id_;
    std::unique_ptr<LocalNode> delegate_;
    bool fail_search_ = false;
    bool fail_count_ = false;
    bool fail_get_ = false;
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

// =========================================================================
// Phase 24: Distributed Read Resilience & Query Failover Tests
// =========================================================================

// 1. Primary read succeeds: no failover metric, primary remains selected.
TEST(CoordinatorFailoverTest, Phase24_PrimaryReadSucceedsNoFailoverMetric)
{
    auto router = std::make_unique<ShardRouter>(1);
    std::vector<ShardReplicaSet> replica_sets = {{0, {0, 1}}};
    auto placement = std::make_unique<ShardReplicaPlacement>(1, 2, 2, replica_sets);

    auto local0 = std::make_unique<LocalNode>(0);
    auto shard0 = std::make_unique<Shard>();
    shard0->add_document(1, "test document");
    local0->add_shard(0, std::move(shard0));

    auto local1 = std::make_unique<LocalNode>(1);
    auto shard1 = std::make_unique<Shard>();
    shard1->add_document(1, "test document");
    local1->add_shard(0, std::move(shard1));

    auto n0 = std::make_unique<InspectableNode>(0, std::move(local0));
    auto n1 = std::make_unique<InspectableNode>(1, std::move(local1));

    auto* p0 = n0.get();
    auto* p1 = n1.get();

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
    const auto resp = coord->search(req);

    EXPECT_FALSE(resp.is_error);
    EXPECT_TRUE(resp.complete);
    EXPECT_EQ(resp.total, 1u);

    // Primary was selected and served both count and search.
    EXPECT_EQ(p0->count_calls.load(), 1u);
    EXPECT_EQ(p0->search_calls.load(), 1u);
    // Secondary was never touched.
    EXPECT_EQ(p1->count_calls.load(), 0u);
    EXPECT_EQ(p1->search_calls.load(), 0u);

    // No read failovers recorded.
    const auto snap = metrics.snapshot();
    EXPECT_EQ(snap.read_failovers_total, 0u);
}

// 2. Primary read fails and secondary succeeds:
//    - search succeeds
//    - failover metric increments exactly once for that shard selection
//    - secondary is pinned
TEST(CoordinatorFailoverTest, Phase24_PrimaryFailsSecondarySucceedsFailoverMetricIncrementsOnce)
{
    auto router = std::make_unique<ShardRouter>(1);
    std::vector<ShardReplicaSet> replica_sets = {{0, {0, 1}}};
    auto placement = std::make_unique<ShardReplicaPlacement>(1, 2, 2, replica_sets);

    auto n0 = std::make_unique<FailingNode>(0);

    auto local1 = std::make_unique<LocalNode>(1);
    auto shard1 = std::make_unique<Shard>();
    shard1->add_document(1, "resilient content");
    local1->add_shard(0, std::move(shard1));
    auto n1 = std::make_unique<InspectableNode>(1, std::move(local1));
    auto* p1 = n1.get();

    std::vector<std::unique_ptr<NodeClient>> nodes;
    nodes.push_back(std::move(n0));
    nodes.push_back(std::move(n1));
    auto coord = std::make_unique<ShardCoordinator>(
        std::move(router), std::move(placement), std::move(nodes));

    MetricsCollector metrics;
    coord->set_metrics(&metrics);

    SearchRequest req;
    req.query = "resilient";
    req.mode = SearchMode::Or;
    req.limit = 10;
    const auto resp = coord->search(req);

    EXPECT_FALSE(resp.is_error);
    EXPECT_TRUE(resp.complete);
    EXPECT_EQ(resp.total, 1u);

    // Secondary was pinned: served count and search.
    EXPECT_EQ(p1->count_calls.load(), 1u);
    EXPECT_EQ(p1->search_calls.load(), 1u);

    // Failover metric incremented exactly once for this shard selection.
    const auto snap = metrics.snapshot();
    EXPECT_EQ(snap.read_failovers_total, 1u);
}

// 3. Replica pinning across multiple terms:
//    - compute_global_n selects secondary
//    - all terms in collect_postings use that same secondary
//    - failover metric increments exactly once despite multiple terms
//    - tertiary replica is not touched
TEST(CoordinatorFailoverTest, Phase24_ReplicaPinningMultiTermNoIndependentSelection)
{
    auto router = std::make_unique<ShardRouter>(1);
    std::vector<ShardReplicaSet> replica_sets = {{0, {0, 1, 2}}};
    auto placement = std::make_unique<ShardReplicaPlacement>(1, 3, 3, replica_sets);

    auto n0 = std::make_unique<FailingNode>(0);

    auto local1 = std::make_unique<LocalNode>(1);
    auto shard1 = std::make_unique<Shard>();
    shard1->add_document(1, "alpha beta gamma");
    local1->add_shard(0, std::move(shard1));
    auto n1 = std::make_unique<InspectableNode>(1, std::move(local1));
    auto* p1 = n1.get();

    auto local2 = std::make_unique<LocalNode>(2);
    auto shard2 = std::make_unique<Shard>();
    shard2->add_document(1, "alpha beta gamma");
    local2->add_shard(0, std::move(shard2));
    auto n2 = std::make_unique<InspectableNode>(2, std::move(local2));
    auto* p2 = n2.get();

    std::vector<std::unique_ptr<NodeClient>> nodes;
    nodes.push_back(std::move(n0));
    nodes.push_back(std::move(n1));
    nodes.push_back(std::move(n2));
    auto coord = std::make_unique<ShardCoordinator>(
        std::move(router), std::move(placement), std::move(nodes));

    MetricsCollector metrics;
    coord->set_metrics(&metrics);

    SearchRequest req;
    req.query = "alpha beta gamma";
    req.mode = SearchMode::And;
    req.limit = 10;
    const auto resp = coord->search(req);

    EXPECT_FALSE(resp.is_error);
    EXPECT_TRUE(resp.complete);
    EXPECT_EQ(resp.total, 1u);

    // Secondary pinned: 1 count call, 3 search calls (1 per term).
    EXPECT_EQ(p1->count_calls.load(), 1u);
    EXPECT_EQ(p1->search_calls.load(), 3u);

    // Tertiary was never selected or touched during postings.
    EXPECT_EQ(p2->count_calls.load(), 0u);
    EXPECT_EQ(p2->search_calls.load(), 0u);

    // Exactly 1 failover recorded for the shard selection.
    const auto snap = metrics.snapshot();
    EXPECT_EQ(snap.read_failovers_total, 1u);
}

// 4. Pinned replica fails after Global N:
//    - shard becomes incomplete
//    - existing error reporting is preserved
//    - NO mid-query failover to another replica occurs
TEST(CoordinatorFailoverTest, Phase24_PinnedReplicaFailsAfterGlobalNMarkedIncomplete)
{
    auto router = std::make_unique<ShardRouter>(1);
    std::vector<ShardReplicaSet> replica_sets = {{0, {0, 1}}};
    auto placement = std::make_unique<ShardReplicaPlacement>(1, 2, 2, replica_sets);

    // Primary node succeeds on count, but will fail on search.
    auto local0 = std::make_unique<LocalNode>(0);
    auto shard0 = std::make_unique<Shard>();
    shard0->add_document(1, "data on both");
    local0->add_shard(0, std::move(shard0));
    auto n0 = std::make_unique<InspectableNode>(0, std::move(local0));
    n0->set_fail_search(true);  // Fails during postings!
    auto* p0 = n0.get();

    // Secondary node is healthy with data.
    auto local1 = std::make_unique<LocalNode>(1);
    auto shard1 = std::make_unique<Shard>();
    shard1->add_document(1, "data on both");
    local1->add_shard(0, std::move(shard1));
    auto n1 = std::make_unique<InspectableNode>(1, std::move(local1));
    auto* p1 = n1.get();

    std::vector<std::unique_ptr<NodeClient>> nodes;
    nodes.push_back(std::move(n0));
    nodes.push_back(std::move(n1));
    auto coord = std::make_unique<ShardCoordinator>(
        std::move(router), std::move(placement), std::move(nodes));

    MetricsCollector metrics;
    coord->set_metrics(&metrics);

    SearchRequest req;
    req.query = "data";
    req.mode = SearchMode::Or;
    req.limit = 10;
    const auto resp = coord->search(req);

    // Search request itself succeeds but result is incomplete.
    EXPECT_FALSE(resp.is_error);
    EXPECT_FALSE(resp.complete);
    EXPECT_FALSE(resp.errors.empty());

    // Primary was pinned: received count and search.
    EXPECT_EQ(p0->count_calls.load(), 1u);
    EXPECT_EQ(p0->search_calls.load(), 1u);

    // CRITICAL: secondary was NOT queried during postings (no mid-query failover!).
    EXPECT_EQ(p1->search_calls.load(), 0u);
}

// 5. Multi-shard search:
//    - pinning is independent per shard
//    - one shard uses primary while another uses secondary
TEST(CoordinatorFailoverTest, Phase24_MultiShardIndependentPinning)
{
    // 2 shards, R=2.
    // Shard 0: primary(0) healthy, secondary(1) healthy.
    // Shard 1: primary(2) failing, secondary(3) healthy.
    auto router = std::make_unique<ShardRouter>(2);
    std::vector<ShardReplicaSet> replica_sets = {
        {0, {0, 1}},
        {1, {2, 3}},
    };
    auto placement = std::make_unique<ShardReplicaPlacement>(2, 4, 2, replica_sets);

    // Shard 0 nodes
    auto local0 = std::make_unique<LocalNode>(0);
    auto s0_0 = std::make_unique<Shard>();
    s0_0->add_document(0, "multi shard match");
    local0->add_shard(0, std::move(s0_0));
    local0->add_shard(1, std::make_unique<Shard>());
    auto n0 = std::make_unique<InspectableNode>(0, std::move(local0));
    auto* p0 = n0.get();

    auto local1 = std::make_unique<LocalNode>(1);
    local1->add_shard(0, std::make_unique<Shard>());
    local1->add_shard(1, std::make_unique<Shard>());
    auto n1 = std::make_unique<InspectableNode>(1, std::move(local1));
    auto* p1 = n1.get();

    // Shard 1 nodes (primary 2 fails, secondary 3 succeeds)
    auto n2 = std::make_unique<FailingNode>(2);

    auto local3 = std::make_unique<LocalNode>(3);
    local3->add_shard(0, std::make_unique<Shard>());
    auto s3_1 = std::make_unique<Shard>();
    s3_1->add_document(1, "multi shard match");
    local3->add_shard(1, std::move(s3_1));
    auto n3 = std::make_unique<InspectableNode>(3, std::move(local3));
    auto* p3 = n3.get();

    std::vector<std::unique_ptr<NodeClient>> nodes;
    nodes.push_back(std::move(n0));
    nodes.push_back(std::move(n1));
    nodes.push_back(std::move(n2));
    nodes.push_back(std::move(n3));
    auto coord = std::make_unique<ShardCoordinator>(
        std::move(router), std::move(placement), std::move(nodes));

    MetricsCollector metrics;
    coord->set_metrics(&metrics);

    SearchRequest req;
    req.query = "match";
    req.mode = SearchMode::Or;
    req.limit = 10;
    const auto resp = coord->search(req);

    EXPECT_FALSE(resp.is_error);
    EXPECT_TRUE(resp.complete);
    EXPECT_EQ(resp.total, 2u);

    // Shard 0 used primary (Node 0). Secondary (Node 1) was not queried.
    EXPECT_EQ(p0->search_calls.load(), 1u);
    EXPECT_EQ(p1->search_calls.load(), 0u);

    // Shard 1 used secondary (Node 3).
    EXPECT_EQ(p3->search_calls.load(), 1u);

    // Exactly 1 failover recorded across the whole search (from shard 1).
    const auto snap = metrics.snapshot();
    EXPECT_EQ(snap.read_failovers_total, 1u);
}

// 6. get_document: primary failure -> secondary success increments failover metric
TEST(CoordinatorFailoverTest, Phase24_GetDocumentFailoverMetric)
{
    auto router = std::make_unique<ShardRouter>(1);
    std::vector<ShardReplicaSet> replica_sets = {{0, {0, 1}}};
    auto placement = std::make_unique<ShardReplicaPlacement>(1, 2, 2, replica_sets);

    auto n0 = std::make_unique<FailingNode>(0);

    auto local1 = std::make_unique<LocalNode>(1);
    auto shard1 = std::make_unique<Shard>();
    shard1->add_document(42, "failover document");
    local1->add_shard(0, std::move(shard1));
    auto n1 = std::make_unique<InspectableNode>(1, std::move(local1));

    std::vector<std::unique_ptr<NodeClient>> nodes;
    nodes.push_back(std::move(n0));
    nodes.push_back(std::move(n1));
    auto coord = std::make_unique<ShardCoordinator>(
        std::move(router), std::move(placement), std::move(nodes));

    MetricsCollector metrics;
    coord->set_metrics(&metrics);

    // Failover to secondary where document exists.
    const auto doc = coord->get_document(42);
    ASSERT_TRUE(doc.has_value());
    EXPECT_EQ(doc->content, "failover document");

    auto snap = metrics.snapshot();
    EXPECT_EQ(snap.read_failovers_total, 1u);

    // Failover to secondary where document does NOT exist (not found).
    const auto missing = coord->get_document(999);
    EXPECT_FALSE(missing.has_value());

    snap = metrics.snapshot();
    EXPECT_EQ(snap.read_failovers_total, 2u);
}

// 7. All replicas fail: failover metric does NOT increment (no replica served successfully).
TEST(CoordinatorFailoverTest, Phase24_AllReplicasFailNoFailoverMetric)
{
    auto coord = make_r2_all_failing();

    MetricsCollector metrics;
    coord->set_metrics(&metrics);

    SearchRequest req;
    req.query = "hello";
    req.mode = SearchMode::Or;
    req.limit = 10;
    const auto resp = coord->search(req);

    EXPECT_FALSE(resp.is_error);
    EXPECT_FALSE(resp.complete);

    const auto snap = metrics.snapshot();
    EXPECT_EQ(snap.read_failovers_total, 0u);
}

// ===========================================================================
// Phase 25: Search Result Correctness & Partial-Availability Semantics
// ===========================================================================

// 1. AND Query Partial Availability:
// Controlled multi-shard scenario where one shard is unavailable.
// Verifies:
// - Search does not become an unexpected transport/application error
// - response.complete == false
// - response.errors identifies the failed shard
// - Available shard results are handled according to existing semantics
// - AND processing remains correct over the available search data
// Note: Partial results are NOT claimed to be equivalent to a complete-cluster query.
TEST(CoordinatorFailoverTest, Phase25_AndQueryPartialAvailability)
{
    // 2 shards: Shard 0 (healthy), Shard 1 (failing).
    auto router = std::make_unique<ShardRouter>(2);
    std::vector<ShardReplicaSet> replica_sets = {
        {0, {0}},
        {1, {1}},
    };
    auto placement = std::make_unique<ShardReplicaPlacement>(2, 2, 1, replica_sets);

    // Shard 0: LocalNode with documents
    auto local0 = std::make_unique<LocalNode>(0);
    auto s0 = std::make_unique<Shard>();
    // doc 10 contains both "apple" and "orange" (and "banana")
    s0->add_document(10, "apple orange banana");
    // doc 20 contains both "apple" and "orange"
    s0->add_document(20, "apple orange");
    // doc 30 contains only "apple"
    s0->add_document(30, "apple mango");
    local0->add_shard(0, std::move(s0));
    local0->add_shard(1, std::make_unique<Shard>());
    auto n0 = std::make_unique<InspectableNode>(0, std::move(local0));

    // Shard 1: FailingNode (simulating total unavailability of Shard 1)
    auto n1 = std::make_unique<FailingNode>(1);

    std::vector<std::unique_ptr<NodeClient>> nodes;
    nodes.push_back(std::move(n0));
    nodes.push_back(std::move(n1));
    auto coord = std::make_unique<ShardCoordinator>(
        std::move(router), std::move(placement), std::move(nodes));

    SearchRequest req;
    req.query = "apple orange";
    req.mode = SearchMode::And;
    req.limit = 10;
    const auto resp = coord->search(req);

    // Search request succeeds at coordinator level without unhandled crash.
    EXPECT_FALSE(resp.is_error);

    // Marked incomplete because Shard 1 operations failed.
    EXPECT_FALSE(resp.complete);

    // Errors list identifies the failed shard.
    ASSERT_FALSE(resp.errors.empty());
    bool found_shard1_error = false;
    for (const auto& err : resp.errors) {
        if (err.shard_id == 1) {
            found_shard1_error = true;
            break;
        }
    }
    EXPECT_TRUE(found_shard1_error);

    // AND processing correctly intersects available postings:
    // doc 10 and doc 20 contain both "apple" and "orange".
    // doc 30 contains only "apple" and is excluded by AND intersection.
    EXPECT_EQ(resp.total, 2u);
    ASSERT_EQ(resp.results.size(), 2u);
    EXPECT_TRUE(resp.results[0].document_id == 10 || resp.results[0].document_id == 20);
    EXPECT_TRUE(resp.results[1].document_id == 10 || resp.results[1].document_id == 20);
    EXPECT_NE(resp.results[0].document_id, resp.results[1].document_id);
    for (const auto& r : resp.results) {
        EXPECT_NE(r.document_id, 30u);
        EXPECT_GT(r.score, 0.0);
    }
}

// 2. OR Query Partial Availability:
// Controlled multi-shard scenario where one shard is unavailable.
// Verifies:
// - response.complete == false
// - response.errors identifies the failed shard
// - Available shard results are still returned according to existing semantics
// - OR processing correctly aggregates available postings
// Note: Partial results are NOT claimed to be equivalent to a complete-cluster query.
TEST(CoordinatorFailoverTest, Phase25_OrQueryPartialAvailability)
{
    // 2 shards: Shard 0 (healthy), Shard 1 (failing).
    auto router = std::make_unique<ShardRouter>(2);
    std::vector<ShardReplicaSet> replica_sets = {
        {0, {0}},
        {1, {1}},
    };
    auto placement = std::make_unique<ShardReplicaPlacement>(2, 2, 1, replica_sets);

    // Shard 0: LocalNode with documents
    auto local0 = std::make_unique<LocalNode>(0);
    auto s0 = std::make_unique<Shard>();
    s0->add_document(10, "apple grape");
    s0->add_document(20, "orange peach");
    s0->add_document(30, "banana kiwi");
    local0->add_shard(0, std::move(s0));
    local0->add_shard(1, std::make_unique<Shard>());
    auto n0 = std::make_unique<InspectableNode>(0, std::move(local0));

    // Shard 1: FailingNode (simulating total unavailability of Shard 1)
    auto n1 = std::make_unique<FailingNode>(1);

    std::vector<std::unique_ptr<NodeClient>> nodes;
    nodes.push_back(std::move(n0));
    nodes.push_back(std::move(n1));
    auto coord = std::make_unique<ShardCoordinator>(
        std::move(router), std::move(placement), std::move(nodes));

    SearchRequest req;
    req.query = "apple orange";
    req.mode = SearchMode::Or;
    req.limit = 10;
    const auto resp = coord->search(req);

    EXPECT_FALSE(resp.is_error);
    EXPECT_FALSE(resp.complete);

    ASSERT_FALSE(resp.errors.empty());
    bool found_shard1_error = false;
    for (const auto& err : resp.errors) {
        if (err.shard_id == 1) {
            found_shard1_error = true;
            break;
        }
    }
    EXPECT_TRUE(found_shard1_error);

    // OR processing correctly unions available postings:
    // doc 10 matches "apple", doc 20 matches "orange", doc 30 matches neither.
    EXPECT_EQ(resp.total, 2u);
    ASSERT_EQ(resp.results.size(), 2u);
    EXPECT_TRUE(resp.results[0].document_id == 10 || resp.results[0].document_id == 20);
    EXPECT_TRUE(resp.results[1].document_id == 10 || resp.results[1].document_id == 20);
    EXPECT_NE(resp.results[0].document_id, resp.results[1].document_id);
    for (const auto& r : resp.results) {
        EXPECT_NE(r.document_id, 30u);
        EXPECT_GT(r.score, 0.0);
    }
}

// 3. TF-IDF Failure Window:
// Controlled scenario where:
//   compute_global_n() -> target shard succeeds
//   collect_postings() -> the same pinned shard fails
//
// Verifies:
// 1. response.complete == false
// 2. The failure is represented in response.errors
// 3. The returned result/scoring reflects the ACTUAL CURRENT implementation:
//    score = tf * log(global_n / df) = 1.0 * log(3.0 / 1.0) = log(3.0)
// 4. Documents that Global N includes documents from a shard whose postings
//    subsequently became unavailable.
// 5. Does NOT describe the resulting score as mathematically equivalent to the
//    available subset (which would be log(1.0 / 1.0) = 0.0).
// 6. Does NOT change the TF-IDF implementation, locking in existing partial semantics.
TEST(CoordinatorFailoverTest, Phase25_TfIdfScoringUnderPartialAvailability)
{
    // 2 shards: Shard 0 (healthy), Shard 1 (succeeds count, fails search).
    auto router = std::make_unique<ShardRouter>(2);
    std::vector<ShardReplicaSet> replica_sets = {
        {0, {0}},
        {1, {1}},
    };
    auto placement = std::make_unique<ShardReplicaPlacement>(2, 2, 1, replica_sets);

    // Shard 0: 1 document containing "quantum"
    auto local0 = std::make_unique<LocalNode>(0);
    auto s0 = std::make_unique<Shard>();
    s0->add_document(100, "quantum computing");
    local0->add_shard(0, std::move(s0));
    local0->add_shard(1, std::make_unique<Shard>());
    auto n0 = std::make_unique<InspectableNode>(0, std::move(local0));

    // Shard 1: 2 documents containing data
    auto local1 = std::make_unique<LocalNode>(1);
    local1->add_shard(0, std::make_unique<Shard>());
    auto s1 = std::make_unique<Shard>();
    s1->add_document(201, "quantum mechanics");
    s1->add_document(202, "classical mechanics");
    local1->add_shard(1, std::move(s1));
    auto n1 = std::make_unique<InspectableNode>(1, std::move(local1));

    // Node 1 configuration: succeeds document_count (N=2), but fails search!
    n1->set_fail_count(false);
    n1->set_fail_search(true);

    auto* p1 = n1.get();

    std::vector<std::unique_ptr<NodeClient>> nodes;
    nodes.push_back(std::move(n0));
    nodes.push_back(std::move(n1));
    auto coord = std::make_unique<ShardCoordinator>(
        std::move(router), std::move(placement), std::move(nodes));

    SearchRequest req;
    req.query = "quantum";
    req.mode = SearchMode::Or;
    req.limit = 10;
    const auto resp = coord->search(req);

    // 1. Search request completed at coordinator level, but marked incomplete.
    EXPECT_FALSE(resp.is_error);
    EXPECT_FALSE(resp.complete);

    // 2. Node 1 served count successfully, but failed search.
    EXPECT_EQ(p1->count_calls.load(), 1u);
    EXPECT_EQ(p1->search_calls.load(), 1u);

    // 3. Failure is recorded in errors with shard_id == 1 and category == "search_failure".
    ASSERT_FALSE(resp.errors.empty());
    bool found_search_failure = false;
    for (const auto& err : resp.errors) {
        if (err.shard_id == 1 && err.category == "search_failure") {
            found_search_failure = true;
            break;
        }
    }
    EXPECT_TRUE(found_search_failure);

    // 4. Exactly 1 result returned (doc 100 from Shard 0).
    EXPECT_EQ(resp.total, 1u);
    ASSERT_EQ(resp.results.size(), 1u);
    EXPECT_EQ(resp.results[0].document_id, 100u);

    // 5. Score reflects the ACTUAL CURRENT implementation:
    //    global_n = 1 (Shard 0) + 2 (Shard 1) = 3.0
    //    df = 1.0 (only Shard 0 postings collected; Shard 1 failed)
    //    idf = std::log(3.0 / 1.0) = std::log(3.0)
    //    tf = 1.0
    //    score = 1.0 * std::log(3.0) ≈ 1.098612...
    const double expected_actual_score = 1.0 * std::log(3.0 / 1.0);
    EXPECT_NEAR(resp.results[0].score, expected_actual_score, 1e-6);

    // 6. Documents that Global N includes documents from a shard whose postings
    //    subsequently became unavailable.
    //    If Global N had been calculated only over shards whose postings succeeded,
    //    N would be 1.0, df would be 1.0, and idf would be log(1.0 / 1.0) = 0.0.
    const double score_if_recalculated = 1.0 * std::log(1.0 / 1.0);
    EXPECT_DOUBLE_EQ(score_if_recalculated, 0.0);
    EXPECT_NE(resp.results[0].score, score_if_recalculated);
}

} // namespace
} // namespace dse
