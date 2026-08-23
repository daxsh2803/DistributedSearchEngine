// Distributed Search Engine - Remote Coordinator Integration Tests (Phase 12D).
//
// Tests ShardCoordinator with RemoteNode instances, verifying that the
// coordinator works identically whether nodes are local or remote.
// Each test runs NodeServer(s) in background threads.

#include <gtest/gtest.h>

#include <chrono>
#include <cstddef>
#include <memory>
#include <string>
#include <thread>
#include <vector>

#include "local_node.h"
#include "node_client.h"
#include "node_config.h"
#include "node_server.h"
#include "remote_node.h"
#include "shard.h"
#include "shard_coordinator.h"
#include "shard_router.h"

using namespace dse;

// ---------------------------------------------------------------------------
// Helper: start a NodeServer in a background thread
// ---------------------------------------------------------------------------

struct ServerHandle {
    std::unique_ptr<NodeServer> server;
    std::thread thread;

    void stop_and_join()
    {
        server->stop();
        if (thread.joinable()) {
            thread.join();
        }
    }
};

static ServerHandle start_server(std::size_t node_id,
                                 std::unique_ptr<LocalNode> node)
{
    ServerHandle h;
    h.server = std::make_unique<NodeServer>(node_id, std::move(node));
    h.thread = std::thread([&h]() { h.server->listen(0); });
    h.server->wait_until_ready();
    return h;
}

// ---------------------------------------------------------------------------
// Helper: create a coordinator with RemoteNode instances
// ---------------------------------------------------------------------------

static std::unique_ptr<ShardCoordinator> make_remote_coordinator(
    std::size_t shard_count,
    std::vector<ServerHandle>& handles)
{
    auto router = std::make_unique<ShardRouter>(shard_count);

    // Each shard on its own node.
    std::vector<std::size_t> placement;
    for (std::size_t i = 0; i < shard_count; ++i) {
        placement.push_back(i);
    }
    auto shard_placement = std::make_unique<ShardPlacement>(
        shard_count, shard_count, placement);

    std::vector<std::unique_ptr<NodeClient>> nodes;
    for (std::size_t i = 0; i < shard_count; ++i) {
        nodes.push_back(std::make_unique<RemoteNode>(
            i, "127.0.0.1", handles[i].server->port()));
    }

    return std::make_unique<ShardCoordinator>(
        std::move(router), std::move(shard_placement), std::move(nodes));
}

// ===========================================================================
// Single remote node: basic lifecycle
// ===========================================================================

TEST(RemoteCoordinatorTest, SingleNodeIngestAndSearch)
{
    auto node = std::make_unique<LocalNode>(0);
    node->add_shard(0, std::make_unique<Shard>());
    auto h = start_server(0, std::move(node));
    std::vector<ServerHandle> handles;
    handles.push_back(std::move(h));

    auto coord = make_remote_coordinator(1, handles);

    auto ingest_resp = coord->ingest({1, "the quick brown fox"});
    EXPECT_FALSE(ingest_resp.is_error) << ingest_resp.error_message;
    EXPECT_EQ(ingest_resp.document_id, 1u);

    auto search_resp = coord->search({"quick", SearchMode::Or, 10});
    EXPECT_FALSE(search_resp.is_error) << search_resp.error_message;
    EXPECT_EQ(search_resp.total, 1u);
    EXPECT_EQ(search_resp.results[0].document_id, 1u);

    for (auto& hd : handles) hd.stop_and_join();
}

TEST(RemoteCoordinatorTest, SingleNodeDuplicateIngest)
{
    auto node = std::make_unique<LocalNode>(0);
    node->add_shard(0, std::make_unique<Shard>());
    auto h = start_server(0, std::move(node));

    std::vector<ServerHandle> handles;
    handles.push_back(std::move(h));

    auto coord = make_remote_coordinator(1, handles);

    coord->ingest({1, "first"});
    auto resp = coord->ingest({1, "second"});
    EXPECT_TRUE(resp.is_error);

    for (auto& hd : handles) hd.stop_and_join();
}

TEST(RemoteCoordinatorTest, SingleNodeUpdate)
{
    auto node = std::make_unique<LocalNode>(0);
    node->add_shard(0, std::make_unique<Shard>());
    auto h = start_server(0, std::move(node));

    std::vector<ServerHandle> handles;
    handles.push_back(std::move(h));

    auto coord = make_remote_coordinator(1, handles);

    coord->ingest({1, "old content"});
    auto update_resp = coord->update({1, "new content"});
    EXPECT_FALSE(update_resp.is_error) << update_resp.error_message;

    // "old" should no longer appear.
    auto r1 = coord->search({"old", SearchMode::Or, 10});
    EXPECT_EQ(r1.total, 0u);

    // "new" should appear.
    auto r2 = coord->search({"new", SearchMode::Or, 10});
    EXPECT_EQ(r2.total, 1u);

    for (auto& hd : handles) hd.stop_and_join();
}

TEST(RemoteCoordinatorTest, SingleNodeDelete)
{
    auto node = std::make_unique<LocalNode>(0);
    node->add_shard(0, std::make_unique<Shard>());
    auto h = start_server(0, std::move(node));

    std::vector<ServerHandle> handles;
    handles.push_back(std::move(h));

    auto coord = make_remote_coordinator(1, handles);

    coord->ingest({1, "to be deleted"});
    auto del_resp = coord->remove(1);
    EXPECT_FALSE(del_resp.is_error) << del_resp.error_message;

    auto doc = coord->get_document(1);
    EXPECT_FALSE(doc.has_value());

    auto r = coord->search({"deleted", SearchMode::Or, 10});
    EXPECT_EQ(r.total, 0u);    for (auto& hd : handles) hd.stop_and_join();
}


// ===========================================================================
// Multiple remote nodes
// ===========================================================================

TEST(RemoteCoordinatorTest, MultiNodeIngestAndSearch)
{
    std::vector<ServerHandle> handles;
    for (std::size_t i = 0; i < 3; ++i) {
        auto node = std::make_unique<LocalNode>(i);
        node->add_shard(i, std::make_unique<Shard>());
        handles.push_back(start_server(i, std::move(node)));
    }

    auto coord = make_remote_coordinator(3, handles);

    coord->ingest({1, "quick brown fox"});
    coord->ingest({2, "lazy dog"});
    coord->ingest({3, "fast cat"});

    EXPECT_EQ(coord->total_document_count(), 3u);

    auto r = coord->search({"quick", SearchMode::Or, 10});
    EXPECT_EQ(r.total, 1u);
    EXPECT_EQ(r.results[0].document_id, 1u);

    for (auto& h : handles) h.stop_and_join();
}

// ===========================================================================
// Global TF-IDF across remote nodes
// ===========================================================================

TEST(RemoteCoordinatorTest, GlobalTfIdfAcrossRemoteNodes)
{
    std::vector<ServerHandle> handles;
    for (std::size_t i = 0; i < 3; ++i) {
        auto node = std::make_unique<LocalNode>(i);
        node->add_shard(i, std::make_unique<Shard>());
        handles.push_back(start_server(i, std::move(node)));
    }

    auto coord = make_remote_coordinator(3, handles);

    // Doc 1 (shard 0): "search engine" — "search" tf=1
    // Doc 5 (shard 1): "distributed search" — "search" tf=1
    // Doc 10 (shard 2): "search engine optimization" — "search" tf=1
    // global_df("search") = 3, global_N = 3
    // idf("search") = ln(3/3) = 0 → all scores are 0 → tie-break by doc_id

    coord->ingest({1, "search engine"});
    coord->ingest({5, "distributed search"});
    coord->ingest({10, "search engine optimization"});

    auto r = coord->search({"search", SearchMode::Or, 10});
    EXPECT_EQ(r.total, 3u);
    // All tie at score 0, ordered by doc_id ascending.
    EXPECT_EQ(r.results[0].document_id, 1u);
    EXPECT_EQ(r.results[1].document_id, 5u);
    EXPECT_EQ(r.results[2].document_id, 10u);

    for (auto& h : handles) h.stop_and_join();
}

// ===========================================================================
// AND search across remote nodes
// ===========================================================================

TEST(RemoteCoordinatorTest, AndSearchAcrossRemoteNodes)
{
    std::vector<ServerHandle> handles;
    for (std::size_t i = 0; i < 2; ++i) {
        auto node = std::make_unique<LocalNode>(i);
        node->add_shard(i, std::make_unique<Shard>());
        handles.push_back(start_server(i, std::move(node)));
    }

    auto coord = make_remote_coordinator(2, handles);

    coord->ingest({1, "quick brown fox"});
    coord->ingest({2, "lazy dog"});
    coord->ingest({3, "quick dog"});

    // AND: must have both "quick" and "dog".
    auto r = coord->search({"quick dog", SearchMode::And, 10});
    EXPECT_EQ(r.total, 1u);
    EXPECT_EQ(r.results[0].document_id, 3u);

    for (auto& h : handles) h.stop_and_join();
}

// ===========================================================================
// Cross-node update and delete
// ===========================================================================

TEST(RemoteCoordinatorTest, UpdateAcrossRemoteNodes)
{
    std::vector<ServerHandle> handles;
    for (std::size_t i = 0; i < 2; ++i) {
        auto node = std::make_unique<LocalNode>(i);
        node->add_shard(i, std::make_unique<Shard>());
        handles.push_back(start_server(i, std::move(node)));
    }

    auto coord = make_remote_coordinator(2, handles);

    coord->ingest({1, "old content"});
    coord->ingest({2, "other document"});

    auto update_resp = coord->update({1, "new content"});
    EXPECT_FALSE(update_resp.is_error) << update_resp.error_message;

    auto r = coord->search({"new", SearchMode::Or, 10});
    EXPECT_EQ(r.total, 1u);
    EXPECT_EQ(r.results[0].document_id, 1u);

    auto r2 = coord->search({"old", SearchMode::Or, 10});
    EXPECT_EQ(r2.total, 0u);

    // Unrelated doc still present.
    auto doc2 = coord->get_document(2);
    EXPECT_TRUE(doc2.has_value());

    for (auto& h : handles) h.stop_and_join();
}

TEST(RemoteCoordinatorTest, DeleteAcrossRemoteNodes)
{
    std::vector<ServerHandle> handles;
    for (std::size_t i = 0; i < 2; ++i) {
        auto node = std::make_unique<LocalNode>(i);
        node->add_shard(i, std::make_unique<Shard>());
        handles.push_back(start_server(i, std::move(node)));
    }

    auto coord = make_remote_coordinator(2, handles);

    coord->ingest({1, "to delete"});
    coord->ingest({2, "keep this"});

    auto del_resp = coord->remove(1);
    EXPECT_FALSE(del_resp.is_error) << del_resp.error_message;

    EXPECT_EQ(coord->total_document_count(), 1u);
    EXPECT_FALSE(coord->get_document(1).has_value());
    EXPECT_TRUE(coord->get_document(2).has_value());

    for (auto& h : handles) h.stop_and_join();
}

// ===========================================================================
// Fail-fast: network failure returns error
// ===========================================================================

TEST(RemoteCoordinatorTest, UnavailableNodeReturnsEmptyResults)
{
    // When a node is unreachable, the coordinator's collect_postings()
    // silently skips the error and returns empty results. This is the
    // existing Phase 11 fail-fast behavior: errors are not propagated
    // as search errors, they are treated as empty postings.
    auto router = std::make_unique<ShardRouter>(1);
    std::vector<std::size_t> placement = {0};
    auto sp = std::make_unique<ShardPlacement>(1, 1, placement);

    std::vector<std::unique_ptr<NodeClient>> nodes;
    nodes.push_back(std::make_unique<RemoteNode>(0, "127.0.0.1", 1));

    ShardCoordinator coord(std::move(router), std::move(sp), std::move(nodes));

    auto r = coord.search({"test", SearchMode::Or, 10});
    // No documents found because the node is unreachable.
    EXPECT_FALSE(r.is_error);
    EXPECT_EQ(r.total, 0u);

    // Write operations fail at the RemoteNode level.
    auto ingest = coord.ingest({1, "test content"});
    EXPECT_TRUE(ingest.is_error);
}

// ===========================================================================
// Persistence through remote nodes
// ===========================================================================

TEST(RemoteCoordinatorTest, PersistenceAcrossRemoteNodes)
{
    const std::string path0 = "test_remote_coord_0.jsonl";
    const std::string path1 = "test_remote_coord_1.jsonl";

    std::vector<ServerHandle> handles;

    {
        // Phase 1: ingest and save.
        auto n0 = std::make_unique<LocalNode>(0);
        n0->add_shard(0, std::make_unique<Shard>(path0));
        handles.push_back(start_server(0, std::move(n0)));

        auto n1 = std::make_unique<LocalNode>(1);
        n1->add_shard(1, std::make_unique<Shard>(path1));
        handles.push_back(start_server(1, std::move(n1)));

        auto coord = make_remote_coordinator(2, handles);

        coord->ingest({1, "persisted doc one"});
        coord->ingest({2, "persisted doc two"});
        coord->save_all();

        for (auto& h : handles) h.stop_and_join();
        handles.clear();
    }

    {
        // Phase 2: reload from persistence.
        auto n0 = std::make_unique<LocalNode>(0);
        n0->add_shard(0, std::make_unique<Shard>(path0));
        handles.push_back(start_server(0, std::move(n0)));

        auto n1 = std::make_unique<LocalNode>(1);
        n1->add_shard(1, std::make_unique<Shard>(path1));
        handles.push_back(start_server(1, std::move(n1)));

        auto coord = make_remote_coordinator(2, handles);
        coord->load_all();

        EXPECT_EQ(coord->total_document_count(), 2u);
        EXPECT_TRUE(coord->get_document(1).has_value());
        EXPECT_TRUE(coord->get_document(2).has_value());
    }

    for (auto& h : handles) h.stop_and_join();
    std::remove(path0.c_str());
    std::remove(path1.c_str());
}

// ===========================================================================
// Mixed LocalNode + RemoteNode
// ===========================================================================

TEST(RemoteCoordinatorTest, MixedLocalAndRemote)
{
    // Shard 0: local. Shard 1: remote.
    auto local_node = std::make_unique<LocalNode>(0);
    local_node->add_shard(0, std::make_unique<Shard>());

    auto remote_node_local = std::make_unique<LocalNode>(1);
    remote_node_local->add_shard(1, std::make_unique<Shard>());
    auto h = start_server(1, std::move(remote_node_local));

    auto router = std::make_unique<ShardRouter>(2);
    std::vector<std::size_t> placement = {0, 1};
    auto sp = std::make_unique<ShardPlacement>(2, 2, placement);

    std::vector<std::unique_ptr<NodeClient>> nodes;
    nodes.push_back(std::move(local_node));
    nodes.push_back(std::make_unique<RemoteNode>(1, "127.0.0.1",
                                                  h.server->port()));

    ShardCoordinator coord(std::move(router), std::move(sp), std::move(nodes));

    coord.ingest({1, "local document"});
    coord.ingest({2, "remote document"});

    EXPECT_EQ(coord.total_document_count(), 2u);

    auto r1 = coord.search({"local", SearchMode::Or, 10});
    EXPECT_EQ(r1.total, 1u);
    EXPECT_EQ(r1.results[0].document_id, 1u);

    auto r2 = coord.search({"remote", SearchMode::Or, 10});
    EXPECT_EQ(r2.total, 1u);
    EXPECT_EQ(r2.results[0].document_id, 2u);

    h.stop_and_join();
}

// ===========================================================================
// Concurrent searches across remote nodes
// ===========================================================================

TEST(RemoteCoordinatorTest, ConcurrentRemoteSearches)
{
    std::vector<ServerHandle> handles;
    for (std::size_t i = 0; i < 3; ++i) {
        auto node = std::make_unique<LocalNode>(i);
        node->add_shard(i, std::make_unique<Shard>());
        handles.push_back(start_server(i, std::move(node)));
    }

    auto coord = make_remote_coordinator(3, handles);

    for (int i = 0; i < 9; ++i) {
        coord->ingest({static_cast<doc_id>(i + 1),
                        "document " + std::to_string(i)});
    }

    std::vector<std::thread> threads;
    for (int i = 0; i < 5; ++i) {
        threads.emplace_back([&coord]() {
            for (int j = 0; j < 10; ++j) {
                auto r = coord->search({"document", SearchMode::Or, 10});
                EXPECT_FALSE(r.is_error);
            }
        });
    }

    for (auto& t : threads) {
        t.join();
    }

    for (auto& h : handles) h.stop_and_join();
}
