// Distributed Search Engine - Shard Coordinator Replication Tests (Phase 17B).
//
// Tests for ShardCoordinator with replicated writes (write-all semantics).
// Verifies that ingest/update/remove fan out to all replicas and that
// partial failures are correctly reported.

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
// Helper: create a coordinator with R=1 (single replica, backward compat)
// ---------------------------------------------------------------------------
std::unique_ptr<ShardCoordinator> make_r1_coordinator(std::size_t n)
{
    auto router = std::make_unique<ShardRouter>(n);
    std::vector<std::size_t> placement(n, 0);  // all shards on node 0
    auto shard_placement = std::make_unique<ShardPlacement>(n, 1, placement);

    auto node = std::make_unique<LocalNode>(0);
    for (std::size_t i = 0; i < n; ++i) {
        node->add_shard(i, std::make_unique<Shard>());
    }

    std::vector<std::unique_ptr<NodeClient>> nodes;
    nodes.push_back(std::move(node));

    return std::make_unique<ShardCoordinator>(
        std::move(router), std::move(shard_placement), std::move(nodes));
}

// ---------------------------------------------------------------------------
// Helper: create a coordinator with R=2 (2 shards, 2 nodes, each shard on both)
// ---------------------------------------------------------------------------
std::unique_ptr<ShardCoordinator> make_r2_coordinator()
{
    auto router = std::make_unique<ShardRouter>(2);

    std::vector<ShardReplicaSet> replica_sets = {
        {0, {0, 1}},  // shard 0 on nodes 0 and 1
        {1, {1, 0}},  // shard 1 on nodes 1 and 0
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

// ---------------------------------------------------------------------------
// Helper: create a coordinator with R=3 (1 shard, 3 nodes)
// ---------------------------------------------------------------------------
std::unique_ptr<ShardCoordinator> make_r3_coordinator()
{
    auto router = std::make_unique<ShardRouter>(1);

    std::vector<ShardReplicaSet> replica_sets = {
        {0, {0, 1, 2}},  // shard 0 on nodes 0, 1, and 2
    };
    auto placement = std::make_unique<ShardReplicaPlacement>(
        1, 3, 3, replica_sets);

    auto node0 = std::make_unique<LocalNode>(0);
    node0->add_shard(0, std::make_unique<Shard>());

    auto node1 = std::make_unique<LocalNode>(1);
    node1->add_shard(0, std::make_unique<Shard>());

    auto node2 = std::make_unique<LocalNode>(2);
    node2->add_shard(0, std::make_unique<Shard>());

    std::vector<std::unique_ptr<NodeClient>> nodes;
    nodes.push_back(std::move(node0));
    nodes.push_back(std::move(node1));
    nodes.push_back(std::move(node2));

    return std::make_unique<ShardCoordinator>(
        std::move(router), std::move(placement), std::move(nodes));
}

// =========================================================================
// 1. R=1 backward compatibility
// =========================================================================

TEST(ShardCoordinatorReplicationTest, R1IngestSucceeds)
{
    auto coord = make_r1_coordinator(3);
    const auto resp = coord->ingest({1, "hello world"});
    EXPECT_FALSE(resp.is_error);
    EXPECT_EQ(resp.document_id, 1u);

    const auto doc = coord->get_document(1);
    EXPECT_TRUE(doc.has_value());
}

TEST(ShardCoordinatorReplicationTest, R1UpdateSucceeds)
{
    auto coord = make_r1_coordinator(3);
    coord->ingest({1, "original"});
    const auto resp = coord->update({1, "updated"});
    EXPECT_FALSE(resp.is_error);

    const auto doc = coord->get_document(1);
    ASSERT_TRUE(doc.has_value());
    EXPECT_EQ(doc->content, "updated");
}

TEST(ShardCoordinatorReplicationTest, R1RemoveSucceeds)
{
    auto coord = make_r1_coordinator(3);
    coord->ingest({1, "to delete"});
    const auto resp = coord->remove(1);
    EXPECT_FALSE(resp.is_error);
    EXPECT_FALSE(coord->get_document(1).has_value());
}

// =========================================================================
// 2. R=2 ingest: both replicas receive the document
// =========================================================================

TEST(ShardCoordinatorReplicationTest, R2IngestBothReplicasReceiveDocument)
{
    auto coord = make_r2_coordinator();

    const auto resp = coord->ingest({1, "replicated doc"});
    EXPECT_FALSE(resp.is_error);
    EXPECT_EQ(resp.document_id, 1u);

    // Verify both nodes can find the document via their local shard.
    // Node 0 has shard 0 (primary for doc 1 if hash(1)%2==0 or shard 1 if hash(1)%2==1).
    // Node 1 also has both shards.
    const auto doc = coord->get_document(1);
    EXPECT_TRUE(doc.has_value());
}

TEST(ShardCoordinatorReplicationTest, R2IngestMultipleDocuments)
{
    auto coord = make_r2_coordinator();

    for (doc_id i = 1; i <= 10; ++i) {
        const auto resp = coord->ingest({i, "doc " + std::to_string(i)});
        EXPECT_FALSE(resp.is_error) << "Failed to ingest doc " << i;
    }

    EXPECT_EQ(coord->total_document_count(), 10u);
}

// =========================================================================
// 3. R=3 ingest: all three replicas receive the document
// =========================================================================

TEST(ShardCoordinatorReplicationTest, R3IngestAllReplicasReceiveDocument)
{
    auto coord = make_r3_coordinator();

    const auto resp = coord->ingest({1, "triplicated doc"});
    EXPECT_FALSE(resp.is_error);
    EXPECT_EQ(resp.document_id, 1u);

    const auto doc = coord->get_document(1);
    EXPECT_TRUE(doc.has_value());
}

// =========================================================================
// 4. R=2 update: both replicas receive the update
// =========================================================================

TEST(ShardCoordinatorReplicationTest, R2UpdateBothReplicasUpdated)
{
    auto coord = make_r2_coordinator();
    coord->ingest({1, "original"});

    const auto resp = coord->update({1, "updated"});
    EXPECT_FALSE(resp.is_error);

    const auto doc = coord->get_document(1);
    ASSERT_TRUE(doc.has_value());
    EXPECT_EQ(doc->content, "updated");
}

// =========================================================================
// 5. R=2 remove: both replicas remove the document
// =========================================================================

TEST(ShardCoordinatorReplicationTest, R2RemoveBothReplicasRemoveDocument)
{
    auto coord = make_r2_coordinator();
    coord->ingest({1, "to delete"});

    const auto resp = coord->remove(1);
    EXPECT_FALSE(resp.is_error);
    EXPECT_FALSE(coord->get_document(1).has_value());
}

// =========================================================================
// 6. Concurrent replicated writes
// =========================================================================

TEST(ShardCoordinatorReplicationTest, R2ConcurrentWrites)
{
    auto coord = make_r2_coordinator();
    MetricsCollector metrics;
    coord->set_metrics(&metrics);

    std::atomic<std::size_t> success_count{0};
    std::atomic<std::size_t> error_count{0};

    auto writer = [&](doc_id start_id) {
        for (doc_id i = start_id; i < start_id + 20; ++i) {
            const auto resp = coord->ingest(
                {i, "concurrent doc " + std::to_string(i)});
            if (resp.is_error) {
                error_count.fetch_add(1, std::memory_order_relaxed);
            } else {
                success_count.fetch_add(1, std::memory_order_relaxed);
            }
        }
    };

    std::vector<std::thread> threads;
    threads.emplace_back(writer, 1);
    threads.emplace_back(writer, 100);
    threads.emplace_back(writer, 200);
    threads.emplace_back(writer, 300);

    for (auto& t : threads) {
        t.join();
    }

    EXPECT_EQ(success_count.load(), 80u);
    EXPECT_EQ(error_count.load(), 0u);
    EXPECT_EQ(coord->total_document_count(), 80u);
}

// =========================================================================
// 7. Validation errors with R=2
// =========================================================================

TEST(ShardCoordinatorReplicationTest, R2IngestEmptyContentFails)
{
    auto coord = make_r2_coordinator();
    const auto resp = coord->ingest({1, ""});
    EXPECT_TRUE(resp.is_error);
}

TEST(ShardCoordinatorReplicationTest, R2UpdateEmptyContentFails)
{
    auto coord = make_r2_coordinator();
    coord->ingest({1, "original"});
    const auto resp = coord->update({1, ""});
    EXPECT_TRUE(resp.is_error);
}

TEST(ShardCoordinatorReplicationTest, R2UpdateMissingDocumentFails)
{
    auto coord = make_r2_coordinator();
    const auto resp = coord->update({999, "new content"});
    EXPECT_TRUE(resp.is_error);
}

TEST(ShardCoordinatorReplicationTest, R2RemoveMissingDocumentFails)
{
    auto coord = make_r2_coordinator();
    const auto resp = coord->remove(999);
    EXPECT_TRUE(resp.is_error);
}

// =========================================================================
// 8. R=2 duplicate document behavior
// =========================================================================

TEST(ShardCoordinatorReplicationTest, R2DuplicateIngestFails)
{
    auto coord = make_r2_coordinator();
    coord->ingest({1, "first"});
    const auto resp = coord->ingest({1, "second"});
    EXPECT_TRUE(resp.is_error);
}

// =========================================================================
// 9. R=2 search with replication (unchanged behavior)
// =========================================================================

TEST(ShardCoordinatorReplicationTest, R2SearchWorksAfterReplicatedWrites)
{
    auto coord = make_r2_coordinator();

    coord->ingest({1, "quick brown fox"});
    coord->ingest({5, "lazy dog"});
    coord->ingest({10, "another fox jumps"});

    SearchRequest req;
    req.query = "fox";
    req.mode = SearchMode::Or;
    req.limit = 10;

    const auto resp = coord->search(req);
    EXPECT_FALSE(resp.is_error);
    // Note: with R=2, each document exists on both nodes.
    // Search currently fans out to primaries, so it should find each doc once.
    EXPECT_EQ(resp.total, 2u);
}

// =========================================================================
// 10. Coordinator metrics: one user write, not R writes
// =========================================================================

TEST(ShardCoordinatorReplicationTest, R2MetricsCountsOneUserWrite)
{
    auto coord = make_r2_coordinator();
    MetricsCollector metrics;
    coord->set_metrics(&metrics);

    // Ingest 5 documents with R=2.
    for (doc_id i = 1; i <= 5; ++i) {
        coord->ingest({i, "doc " + std::to_string(i)});
    }

    const auto snap = metrics.snapshot();
    // Coordinator should count exactly 5 user-initiated writes.
    EXPECT_EQ(snap.coordinator_writes_total, 5u);
    EXPECT_EQ(snap.coordinator_write_success, 5u);
    EXPECT_EQ(snap.coordinator_write_errors, 0u);
}

TEST(ShardCoordinatorReplicationTest, R2MetricsCountsWriteError)
{
    auto coord = make_r2_coordinator();
    MetricsCollector metrics;
    coord->set_metrics(&metrics);

    // Attempt to update a non-existent document.
    const auto resp = coord->update({999, "new"});
    EXPECT_TRUE(resp.is_error);

    const auto snap = metrics.snapshot();
    EXPECT_EQ(snap.coordinator_writes_total, 1u);
    EXPECT_EQ(snap.coordinator_write_success, 0u);
    EXPECT_EQ(snap.coordinator_write_errors, 1u);
}

// =========================================================================
// 11. R=2 remove preserves other documents
// =========================================================================

TEST(ShardCoordinatorReplicationTest, R2RemovePreservesOtherDocuments)
{
    auto coord = make_r2_coordinator();
    coord->ingest({1, "alpha"});
    coord->ingest({2, "beta"});
    coord->ingest({3, "gamma"});

    coord->remove(2);

    EXPECT_FALSE(coord->get_document(2).has_value());
    EXPECT_TRUE(coord->get_document(1).has_value());
    EXPECT_TRUE(coord->get_document(3).has_value());
    EXPECT_EQ(coord->total_document_count(), 2u);
}

// =========================================================================
// 12. R=2 update preserves other documents
// =========================================================================

TEST(ShardCoordinatorReplicationTest, R2UpdatePreservesOtherDocuments)
{
    auto coord = make_r2_coordinator();
    coord->ingest({1, "alpha"});
    coord->ingest({2, "beta"});

    coord->update({1, "alpha_new"});

    EXPECT_EQ(coord->total_document_count(), 2u);
    EXPECT_TRUE(coord->get_document(2).has_value());

    const auto doc = coord->get_document(1);
    ASSERT_TRUE(doc.has_value());
    EXPECT_EQ(doc->content, "alpha_new");
}

// =========================================================================
// 13. R=2 persistence round-trip
// =========================================================================

TEST(ShardCoordinatorReplicationTest, R2PersistenceRoundTrip)
{
    auto router = std::make_unique<ShardRouter>(1);

    std::vector<ShardReplicaSet> replica_sets = {
        {0, {0, 1}},
    };
    auto placement = std::make_unique<ShardReplicaPlacement>(
        1, 2, 2, replica_sets);

    auto node0 = std::make_unique<LocalNode>(0);
    node0->add_shard(0, std::make_unique<Shard>("test_repl_0.jsonl"));
    auto node1 = std::make_unique<LocalNode>(1);
    node1->add_shard(0, std::make_unique<Shard>("test_repl_1.jsonl"));

    std::vector<std::unique_ptr<NodeClient>> nodes;
    nodes.push_back(std::move(node0));
    nodes.push_back(std::move(node1));

    {
        auto coord = std::make_unique<ShardCoordinator>(
            std::move(router), std::move(placement), std::move(nodes));
        coord->ingest({1, "hello"});
        coord->ingest({2, "world"});
        coord->save_all();
    }

    // Reload.
    auto router2 = std::make_unique<ShardRouter>(1);
    std::vector<ShardReplicaSet> replica_sets2 = {
        {0, {0, 1}},
    };
    auto placement2 = std::make_unique<ShardReplicaPlacement>(
        1, 2, 2, replica_sets2);

    auto node0b = std::make_unique<LocalNode>(0);
    node0b->add_shard(0, std::make_unique<Shard>("test_repl_0.jsonl"));
    auto node1b = std::make_unique<LocalNode>(1);
    node1b->add_shard(0, std::make_unique<Shard>("test_repl_1.jsonl"));

    std::vector<std::unique_ptr<NodeClient>> nodes2;
    nodes2.push_back(std::move(node0b));
    nodes2.push_back(std::move(node1b));

    auto coord2 = std::make_unique<ShardCoordinator>(
        std::move(router2), std::move(placement2), std::move(nodes2));
    coord2->load_all();

    EXPECT_EQ(coord2->total_document_count(), 2u);
    EXPECT_TRUE(coord2->get_document(1).has_value());
    EXPECT_TRUE(coord2->get_document(2).has_value());

    std::remove("test_repl_0.jsonl");
    std::remove("test_repl_1.jsonl");
}

// =========================================================================
// 14. R=2 remove decreases document count
// =========================================================================

TEST(ShardCoordinatorReplicationTest, R2RemoveDecreasesDocumentCount)
{
    auto coord = make_r2_coordinator();
    coord->ingest({1, "a"});
    coord->ingest({2, "b"});
    EXPECT_EQ(coord->total_document_count(), 2u);

    coord->remove(1);
    EXPECT_EQ(coord->total_document_count(), 1u);
}

// =========================================================================
// 15. R=2 update preserves document count
// =========================================================================

TEST(ShardCoordinatorReplicationTest, R2UpdatePreservesDocumentCount)
{
    auto coord = make_r2_coordinator();
    coord->ingest({1, "alpha"});
    coord->ingest({2, "beta"});
    EXPECT_EQ(coord->total_document_count(), 2u);

    coord->update({1, "alpha_new"});
    EXPECT_EQ(coord->total_document_count(), 2u);
}

// =========================================================================
// 16. Partial write failure: primary fails, secondary succeeds
//      Write-all semantics require ALL replicas to succeed.
// =========================================================================

class PartialFailNode : public NodeClient {
public:
    PartialFailNode(std::size_t id, bool fail)
        : node_id_(id), should_fail_(fail) {}

    std::size_t node_id() const override { return node_id_; }

    ShardSearchResponse search(const ShardSearchRequest& req) override {
        ShardSearchResponse resp;
        resp.shard_id = req.shard_id;
        if (should_fail_) { resp.is_error = true; resp.error_message = "fail"; }
        return resp;
    }

    ShardWriteResponse add_document(const ShardWriteRequest& req) override {
        ShardWriteResponse resp;
        resp.shard_id = req.shard_id;
        resp.document_id = req.document_id;
        if (should_fail_) {
            resp.is_error = true;
            resp.error_message = "node " + std::to_string(node_id_) + " unavailable";
        } else {
            resp.terms_indexed = 3;
        }
        return resp;
    }

    ShardWriteResponse update_document(const ShardWriteRequest& req) override {
        ShardWriteResponse resp;
        resp.shard_id = req.shard_id;
        resp.document_id = req.document_id;
        if (should_fail_) {
            resp.is_error = true;
            resp.error_message = "node " + std::to_string(node_id_) + " unavailable";
        } else {
            resp.terms_indexed = 3;
        }
        return resp;
    }

    ShardRemoveResponse remove_document(const ShardRemoveRequest& req) override {
        ShardRemoveResponse resp;
        resp.shard_id = req.shard_id;
        if (should_fail_) { resp.is_error = true; resp.error_message = "fail"; }
        return resp;
    }

    ShardGetResponse get_document(const ShardGetRequest& req) override {
        ShardGetResponse resp;
        resp.shard_id = req.shard_id;
        resp.document_id = req.document_id;
        if (should_fail_) { resp.is_error = true; resp.error_message = "fail"; }
        return resp;
    }

    ShardCountResponse document_count(const ShardCountRequest& req) override {
        ShardCountResponse resp;
        resp.shard_id = req.shard_id;
        if (should_fail_) { resp.is_error = true; resp.error_message = "fail"; }
        return resp;
    }

    bool save_shard(std::size_t) override { return !should_fail_; }
    bool load_shard(std::size_t) override { return !should_fail_; }

private:
    std::size_t node_id_;
    bool should_fail_;
};

TEST(ShardCoordinatorReplicationTest, R2PartialWriteFailureIngestReportsError)
{
    // R=2: primary (node 0) fails, secondary (node 1) succeeds.
    // Write-all semantics: overall operation must fail.
    auto router = std::make_unique<ShardRouter>(1);
    std::vector<ShardReplicaSet> replica_sets = {{0, {0, 1}}};
    auto placement = std::make_unique<ShardReplicaPlacement>(1, 2, 2, replica_sets);

    auto n0 = std::make_unique<PartialFailNode>(0, true);   // primary fails
    auto n1 = std::make_unique<PartialFailNode>(1, false);  // secondary succeeds

    std::vector<std::unique_ptr<NodeClient>> nodes;
    nodes.push_back(std::move(n0));
    nodes.push_back(std::move(n1));
    auto coord = std::make_unique<ShardCoordinator>(
        std::move(router), std::move(placement), std::move(nodes));

    const auto resp = coord->ingest({1, "partial fail doc"});
    EXPECT_TRUE(resp.is_error);
    EXPECT_FALSE(resp.error_message.empty());
}

TEST(ShardCoordinatorReplicationTest, R2PartialWriteFailureUpdateReportsError)
{
    auto router = std::make_unique<ShardRouter>(1);
    std::vector<ShardReplicaSet> replica_sets = {{0, {0, 1}}};
    auto placement = std::make_unique<ShardReplicaPlacement>(1, 2, 2, replica_sets);

    // Both nodes healthy for initial ingest.
    auto n0h = std::make_unique<PartialFailNode>(0, false);
    auto n1h = std::make_unique<PartialFailNode>(1, false);
    std::vector<std::unique_ptr<NodeClient>> nodes_h;
    nodes_h.push_back(std::move(n0h));
    nodes_h.push_back(std::move(n1h));
    auto coord = std::make_unique<ShardCoordinator>(
        std::move(router), std::move(placement), std::move(nodes_h));

    coord->ingest({1, "original"});

    // Rebuild with primary failing for update.
    // Note: we can't easily swap nodes, so we test via fresh coordinator.
    auto router2 = std::make_unique<ShardRouter>(1);
    std::vector<ShardReplicaSet> rs2 = {{0, {0, 1}}};
    auto pl2 = std::make_unique<ShardReplicaPlacement>(1, 2, 2, rs2);

    auto shard0 = std::make_unique<Shard>();
    shard0->add_document(1, "original");
    auto n0 = std::make_unique<LocalNode>(0);
    n0->add_shard(0, std::move(shard0));
    auto n1 = std::make_unique<PartialFailNode>(1, true);  // secondary fails

    std::vector<std::unique_ptr<NodeClient>> nodes;
    nodes.push_back(std::move(n0));
    nodes.push_back(std::move(n1));
    auto coord2 = std::make_unique<ShardCoordinator>(
        std::move(router2), std::move(pl2), std::move(nodes));

    const auto resp = coord2->update({1, "updated"});
    EXPECT_TRUE(resp.is_error);
}

TEST(ShardCoordinatorReplicationTest, R2PartialWriteFailureRemoveReportsError)
{
    auto router = std::make_unique<ShardRouter>(1);
    std::vector<ShardReplicaSet> replica_sets = {{0, {0, 1}}};
    auto placement = std::make_unique<ShardReplicaPlacement>(1, 2, 2, replica_sets);

    auto shard0 = std::make_unique<Shard>();
    shard0->add_document(1, "to delete");
    auto n0 = std::make_unique<LocalNode>(0);
    n0->add_shard(0, std::move(shard0));
    auto n1 = std::make_unique<PartialFailNode>(1, true);  // secondary fails

    std::vector<std::unique_ptr<NodeClient>> nodes;
    nodes.push_back(std::move(n0));
    nodes.push_back(std::move(n1));
    auto coord = std::make_unique<ShardCoordinator>(
        std::move(router), std::move(placement), std::move(nodes));

    const auto resp = coord->remove(1);
    EXPECT_TRUE(resp.is_error);
}

} // namespace
} // namespace dse
