// Distributed Search Engine - System Validation Integration Test (Phase 29).
//
// Comprehensive end-to-end in-process validation of the 3-node distributed search engine:
// 1. 3-node topology (N=3, S=3, R=3) with full HTTP & RPC readiness
// 2. Synchronous all-replica replication across all logical shards
// 3. Complete document lifecycle (CREATE -> UPDATE -> DELETE)
// 4. Deterministic read failover upon primary replica shutdown
// 5. Graceful node restart & shard persistence recovery
// 6. Resilient write handling during Kafka broker outage
// 7. Durable event failure tracking in PersistentEventStore
// 8. Kafka recovery verification without automatic replay
// 9. Explicit replay via EventDispatcher::replay_failed() with identity stability
// 10. Final replica consistency & operational metrics verification
// 11. Compact 3-node search performance regression

#include <gtest/gtest.h>

#include <httplib.h>
#include <nlohmann/json.hpp>

#include <atomic>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <cstdlib>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <memory>
#include <string>
#include <thread>
#include <unordered_map>
#include <vector>

#include "circuit_breaker.h"
#include "document_event.h"
#include "event_dispatcher.h"
#include "event_store.h"
#include "http_server.h"
#include "local_node.h"
#include "metrics.h"
#include "node_client.h"
#include "node_config.h"
#include "node_server.h"
#include "persistent_event_store.h"
#include "remote_node.h"
#include "replica_placement.h"
#include "retry_policy.h"
#include "shard.h"
#include "shard_coordinator.h"
#include "shard_router.h"

#ifdef DSE_KAFKA_ENABLED
#include "kafka_message_broker.h"
#include "remote_event_processor.h"
#endif

namespace dse {
namespace {

// ---------------------------------------------------------------------------
// Configuration Constants
// ---------------------------------------------------------------------------
constexpr std::size_t kNodeCount = 3;
constexpr std::size_t kShardCount = 3;
constexpr std::size_t kReplicaFactor = 3;

constexpr int kHttpPortNode0 = 8081;
constexpr int kRpcPortNode0  = 9081;

constexpr int kHttpPortNode1 = 8082;
constexpr int kRpcPortNode1  = 9082;

constexpr int kHttpPortNode2 = 8083;
constexpr int kRpcPortNode2  = 9083;

// ---------------------------------------------------------------------------
// HTTP Client Helpers
// ---------------------------------------------------------------------------
struct HttpResponse {
    int status = 0;
    std::string body;
};

HttpResponse http_get(int port, const std::string& path, int timeout_sec = 5) {
    httplib::Client client("127.0.0.1", port);
    client.set_connection_timeout(std::chrono::seconds(timeout_sec));
    client.set_read_timeout(std::chrono::seconds(timeout_sec));
    auto res = client.Get(path.c_str());
    if (!res) return {0, ""};
    return {res->status, res->body};
}

HttpResponse http_post(int port, const std::string& path, const std::string& json_body, int timeout_sec = 5) {
    httplib::Client client("127.0.0.1", port);
    client.set_connection_timeout(std::chrono::seconds(timeout_sec));
    client.set_read_timeout(std::chrono::seconds(timeout_sec));
    auto res = client.Post(path.c_str(), json_body, "application/json");
    if (!res) return {0, ""};
    return {res->status, res->body};
}

HttpResponse http_put(int port, const std::string& path, const std::string& json_body, int timeout_sec = 5) {
    httplib::Client client("127.0.0.1", port);
    client.set_connection_timeout(std::chrono::seconds(timeout_sec));
    client.set_read_timeout(std::chrono::seconds(timeout_sec));
    auto res = client.Put(path.c_str(), json_body, "application/json");
    if (!res) return {0, ""};
    return {res->status, res->body};
}

HttpResponse http_delete(int port, const std::string& path, int timeout_sec = 5) {
    httplib::Client client("127.0.0.1", port);
    client.set_connection_timeout(std::chrono::seconds(timeout_sec));
    client.set_read_timeout(std::chrono::seconds(timeout_sec));
    auto res = client.Delete(path.c_str());
    if (!res) return {0, ""};
    return {res->status, res->body};
}

// ---------------------------------------------------------------------------
// IPC helper: communicates with tests/e2e_phase29_system_test.py
// ---------------------------------------------------------------------------
bool request_orchestrator(const std::string& request, const std::string& expected_ack) {
    std::cout << request << std::endl;
    std::string response;
    if (!std::getline(std::cin, response)) {
        return false;
    }
    if (!response.empty() && response.back() == '\r') {
        response.pop_back();
    }
    return response == expected_ack;
}

// ---------------------------------------------------------------------------
// NodeInstance: Manages lifecycle of one node in the 3-node cluster
// ---------------------------------------------------------------------------
struct NodeInstance {
    std::size_t node_id = 0;
    int http_port = 0;
    int rpc_port = 0;
    std::string data_dir;

    LocalNode* local_node_ptr = nullptr;
    std::unique_ptr<ShardCoordinator> coordinator;
    std::unique_ptr<MetricsCollector> metrics;
    std::unique_ptr<PersistentEventStore> event_store;

#ifdef DSE_KAFKA_ENABLED
    std::unique_ptr<KafkaMessageBroker> broker;
    std::unique_ptr<RemoteEventProcessor> remote_processor;
#endif
    std::unique_ptr<EventDispatcher> dispatcher;

    std::unique_ptr<NodeServer> node_server;
    std::thread node_server_thread;

    std::unique_ptr<HttpServer> http_server;
    std::thread http_server_thread;

    void stop_servers() {
        if (http_server) {
            http_server->stop();
        }
        if (http_server_thread.joinable()) {
            http_server_thread.join();
        }
        if (node_server) {
            node_server->stop();
        }
        if (node_server_thread.joinable()) {
            node_server_thread.join();
        }
    }

    void stop_all() {
        stop_servers();
        if (dispatcher) {
            dispatcher->stop();
        }
#ifdef DSE_KAFKA_ENABLED
        if (broker) {
            broker->stop();
        }
#endif
    }

    ~NodeInstance() {
        stop_all();
    }
};

// ---------------------------------------------------------------------------
// Helper: Builds a fully wired NodeInstance
// ---------------------------------------------------------------------------
std::unique_ptr<NodeInstance> build_node(
    std::size_t node_id,
    int http_port,
    int rpc_port,
    const std::string& base_data_dir,
    bool load_existing_persistence = false)
{
    auto node = std::make_unique<NodeInstance>();
    node->node_id = node_id;
    node->http_port = http_port;
    node->rpc_port = rpc_port;
    node->data_dir = base_data_dir + "/node_" + std::to_string(node_id);
    std::filesystem::create_directories(node->data_dir);

    // 1. Shard router
    auto router = std::make_unique<ShardRouter>(kShardCount);

    // 2. Replica placement
    const auto replica_sets = make_deterministic_replica_sets(
        kShardCount, kNodeCount, kReplicaFactor);
    auto placement = std::make_unique<ShardReplicaPlacement>(
        kShardCount, kNodeCount, kReplicaFactor, replica_sets);

    // 3. LocalNode with persistent shards
    auto local_node = std::make_unique<LocalNode>(node_id);
    for (std::size_t sid = 0; sid < kShardCount; ++sid) {
        const std::string shard_dir = node->data_dir + "/shard-" + std::to_string(sid);
        std::filesystem::create_directories(shard_dir);
        const std::string shard_file = shard_dir + "/documents.jsonl";
        auto shard = std::make_unique<Shard>(shard_file);
        if (load_existing_persistence) {
            shard->load();
        }
        local_node->add_shard(sid, std::move(shard));
    }
    node->local_node_ptr = local_node.get();

    // 4. RemoteNode clients for other peers
    std::vector<std::unique_ptr<NodeClient>> node_clients;
    node_clients.reserve(kNodeCount);
    for (std::size_t i = 0; i < kNodeCount; ++i) {
        if (i == node_id) {
            node_clients.push_back(std::move(local_node));
        } else {
            const int peer_rpc_port = 9081 + static_cast<int>(i);
            node_clients.push_back(std::make_unique<RemoteNode>(i, "127.0.0.1", peer_rpc_port));
        }
    }

    // 5. Shard coordinator
    node->coordinator = std::make_unique<ShardCoordinator>(
        std::move(router), std::move(placement), std::move(node_clients));

    // 6. Metrics collector
    node->metrics = std::make_unique<MetricsCollector>();
    node->coordinator->set_metrics(node->metrics.get());

    // 7. Persistent event store
    const std::string events_dir = node->data_dir + "/events";
    std::filesystem::create_directories(events_dir);
    node->event_store = std::make_unique<PersistentEventStore>(events_dir);
    node->coordinator->set_event_store(node->event_store.get());

    // 8. Broker & Dispatcher
#ifdef DSE_KAFKA_ENABLED
    static const std::string session_id = std::to_string(
        std::chrono::duration_cast<std::chrono::milliseconds>(
            std::chrono::system_clock::now().time_since_epoch()).count());
    KafkaBrokerConfig kcfg;
    kcfg.bootstrap_servers = "localhost:9094";
    kcfg.client_id = "dse-sysval-" + session_id + "-p-" + std::to_string(node_id);
    kcfg.group_id = "dse-sysval-" + session_id + "-g-" + std::to_string(node_id);
    kcfg.consumer_client_id = "dse-sysval-" + session_id + "-c-" + std::to_string(node_id);
    kcfg.auto_offset_reset = "latest";

    std::unordered_map<std::size_t, LocalNode*> node_map;
    node_map[node_id] = node->local_node_ptr;
    node->remote_processor = std::make_unique<RemoteEventProcessor>(std::move(node_map), node_id);

    node->broker = std::make_unique<KafkaMessageBroker>(kcfg);
    node->broker->subscribe(topics::kDocumentMutations,
        [proc = node->remote_processor.get()](const Message& msg) {
            return proc->process(msg.topic, msg.payload);
        });
    node->broker->start();

    EventDispatcher::Config dcfg;
    dcfg.max_retries = 1;
    dcfg.retry_delay_ms = 50;
    node->dispatcher = std::make_unique<EventDispatcher>(*node->broker, *node->event_store, dcfg);
    node->dispatcher->start();
    node->coordinator->set_event_dispatcher(node->dispatcher.get());
#endif

    // 9. NodeServer (RPC)
    node->node_server = std::make_unique<NodeServer>(node_id, node->local_node_ptr);
    node->node_server_thread = std::thread([ns = node->node_server.get(), rpc_port]() {
        ns->listen(rpc_port);
    });
    node->node_server->wait_until_ready();

    // 10. HttpServer
    node->http_server = std::make_unique<HttpServer>(
        *node->coordinator,
        node->metrics.get(),
        node->event_store.get(),
        node->dispatcher.get(),
#ifdef DSE_KAFKA_ENABLED
        node->broker.get()
#else
        nullptr
#endif
    );
    node->http_server_thread = std::thread([hs = node->http_server.get(), http_port]() {
        hs->listen(http_port);
    });
    node->http_server->wait_until_ready();

    return node;
}

} // namespace

// ===========================================================================
// Comprehensive Integrated System Validation Test
// ===========================================================================
TEST(Phase29SystemValidationTest, CompleteClusterLifecycleAndFailureVerification)
{
    const std::string base_data_dir = "test_phase29_data";
    std::filesystem::remove_all(base_data_dir);
    std::filesystem::create_directories(base_data_dir);

    std::cout << "[PHASE 29] Initializing 3-node in-process cluster (N=3, S=3, R=3)..." << std::endl;

    // -----------------------------------------------------------------------
    // STEP 1: Cluster Topology & Readiness Verification
    // -----------------------------------------------------------------------
    auto node0 = build_node(0, kHttpPortNode0, kRpcPortNode0, base_data_dir);
    auto node1 = build_node(1, kHttpPortNode1, kRpcPortNode1, base_data_dir);
    auto node2 = build_node(2, kHttpPortNode2, kRpcPortNode2, base_data_dir);

    // Bounded polling for HTTP readiness across all 3 nodes
    for (int port : {kHttpPortNode0, kHttpPortNode1, kHttpPortNode2}) {
        bool ready = false;
        const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(10);
        while (std::chrono::steady_clock::now() < deadline) {
            auto res = http_get(port, "/health");
            if (res.status == 200) {
                ready = true;
                break;
            }
            std::this_thread::sleep_for(std::chrono::milliseconds(100));
        }
        ASSERT_TRUE(ready) << "Node on HTTP port " << port << " failed readiness check";
    }

    // Bounded polling for RPC readiness across all 3 nodes
    for (int rpc_port : {kRpcPortNode0, kRpcPortNode1, kRpcPortNode2}) {
        RemoteNode remote_check(999, "127.0.0.1", rpc_port);
        ShardCountRequest req;
        req.shard_id = 0;
        auto count_res = remote_check.document_count(req);
        EXPECT_FALSE(count_res.is_error) << "Node on RPC port " << rpc_port << " failed RPC readiness";
    }

    std::cout << "[PHASE 29] All 3 nodes report healthy HTTP and RPC readiness." << std::endl;

    // -----------------------------------------------------------------------
    // STEP 2: Document Lifecycle & Synchronous Replication (CREATE -> UPDATE -> DELETE)
    // -----------------------------------------------------------------------
    std::cout << "[PHASE 29] Testing document lifecycle & synchronous replication..." << std::endl;

    // CREATE
    {
        const nlohmann::json create_doc = {
            {"id", 101},
            {"content", "distributed search system validation lifecycle"}
        };
        auto post_resp = http_post(kHttpPortNode0, "/documents", create_doc.dump());
        EXPECT_EQ(post_resp.status, 201);
        auto post_json = nlohmann::json::parse(post_resp.body);
        EXPECT_EQ(post_json["document_id"].get<doc_id>(), 101u);
        EXPECT_GT(post_json["terms_indexed"].get<std::size_t>(), 0u);

        // Verify immediately searchable across all 3 nodes via HTTP GET /search
        for (int http_port : {kHttpPortNode0, kHttpPortNode1, kHttpPortNode2}) {
            auto search_resp = http_get(http_port, "/search?q=lifecycle");
            EXPECT_EQ(search_resp.status, 200);
            auto s_json = nlohmann::json::parse(search_resp.body);
            EXPECT_TRUE(s_json["complete"].get<bool>());
            EXPECT_EQ(s_json["total"].get<std::size_t>(), 1u);
            ASSERT_FALSE(s_json["results"].empty());
            EXPECT_EQ(s_json["results"][0]["document_id"].get<doc_id>(), 101u);
        }

        // Verify content on all 3 nodes via direct RPC inspection
        ShardRouter doc_router(kShardCount);
        const std::size_t lifecycle_shard = doc_router.route(101);
        for (std::size_t nid = 0; nid < kNodeCount; ++nid) {
            RemoteNode rnode(nid, "127.0.0.1", 9081 + static_cast<int>(nid));
            ShardGetRequest greq;
            greq.shard_id = lifecycle_shard;
            greq.document_id = 101;
            auto gresp = rnode.get_document(greq);
            EXPECT_FALSE(gresp.is_error);
            EXPECT_TRUE(gresp.found);
            EXPECT_EQ(gresp.content, "distributed search system validation lifecycle");
        }
    }

    // UPDATE
    {
        const nlohmann::json update_doc = {
            {"content", "distributed search updated validation content"}
        };
        auto put_resp = http_put(kHttpPortNode0, "/documents/101", update_doc.dump());
        EXPECT_EQ(put_resp.status, 200);

        // Verify updated search term is visible and old term is removed across all nodes
        for (int http_port : {kHttpPortNode0, kHttpPortNode1, kHttpPortNode2}) {
            auto s_new = http_get(http_port, "/search?q=updated");
            EXPECT_EQ(s_new.status, 200);
            auto sj_new = nlohmann::json::parse(s_new.body);
            EXPECT_EQ(sj_new["total"].get<std::size_t>(), 1u);

            auto s_old = http_get(http_port, "/search?q=lifecycle");
            EXPECT_EQ(s_old.status, 200);
            auto sj_old = nlohmann::json::parse(s_old.body);
            EXPECT_EQ(sj_old["total"].get<std::size_t>(), 0u);
        }

        // Direct RPC verification of updated content on all replicas
        ShardRouter doc_router(kShardCount);
        const std::size_t lifecycle_shard = doc_router.route(101);
        for (std::size_t nid = 0; nid < kNodeCount; ++nid) {
            RemoteNode rnode(nid, "127.0.0.1", 9081 + static_cast<int>(nid));
            ShardGetRequest greq;
            greq.shard_id = lifecycle_shard;
            greq.document_id = 101;
            auto gresp = rnode.get_document(greq);
            EXPECT_TRUE(gresp.found);
            EXPECT_EQ(gresp.content, "distributed search updated validation content");
        }
    }

    // DELETE
    {
        auto del_resp = http_delete(kHttpPortNode0, "/documents/101");
        EXPECT_EQ(del_resp.status, 204);

        // Verify document is gone from all nodes
        for (int http_port : {kHttpPortNode0, kHttpPortNode1, kHttpPortNode2}) {
            auto s_resp = http_get(http_port, "/search?q=updated");
            EXPECT_EQ(s_resp.status, 200);
            auto s_json = nlohmann::json::parse(s_resp.body);
            EXPECT_EQ(s_json["total"].get<std::size_t>(), 0u);
        }

        // Direct RPC verification that document is removed on all replicas (bounded polling for async Kafka pipeline)
        ShardRouter doc_router(kShardCount);
        const std::size_t lifecycle_shard = doc_router.route(101);
        for (std::size_t nid = 0; nid < kNodeCount; ++nid) {
            RemoteNode rnode(nid, "127.0.0.1", 9081 + static_cast<int>(nid));
            ShardGetRequest greq;
            greq.shard_id = lifecycle_shard;
            greq.document_id = 101;
            bool doc_removed = false;
            const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(5);
            while (std::chrono::steady_clock::now() < deadline) {
                auto gresp = rnode.get_document(greq);
                if (!gresp.found) {
                    doc_removed = true;
                    break;
                }
                std::this_thread::sleep_for(std::chrono::milliseconds(50));
            }
            EXPECT_TRUE(doc_removed) << "Document 101 was not removed on node " << nid;
        }

        // Verify second DELETE returns 404 Not Found
        auto del_again = http_delete(kHttpPortNode0, "/documents/101");
        EXPECT_EQ(del_again.status, 404);
    }

    std::cout << "[PHASE 29] Document lifecycle (CREATE, UPDATE, DELETE) verified across all nodes." << std::endl;

    // -----------------------------------------------------------------------
    // STEP 3: Deterministic Read Failover
    // -----------------------------------------------------------------------
    std::cout << "[PHASE 29] Testing deterministic read failover..." << std::endl;

    // Find a document ID that routes specifically to Shard 1 (where Node 1 is primary)
    ShardRouter doc_router(kShardCount);
    doc_id failover_doc_id = 1;
    while (doc_router.route(failover_doc_id) != 1 || failover_doc_id == 101) {
        ++failover_doc_id;
    }

    const nlohmann::json failover_doc = {
        {"id", failover_doc_id},
        {"content", "resilient failover secondary target document"}
    };
    auto doc_failover_resp = http_post(kHttpPortNode0, "/documents", failover_doc.dump());
    EXPECT_EQ(doc_failover_resp.status, 201);

    // Save Node 1 shards before graceful stop
    for (std::size_t sid = 0; sid < kShardCount; ++sid) {
        EXPECT_TRUE(node1->local_node_ptr->save_shard(sid));
    }

    // Take metrics snapshot on Node 0 before failover
    const auto before_failover = node0->metrics->snapshot();

    // Gracefully stop Node 1 servers (HTTP 8082, RPC 9082)
    node1->stop_servers();

    // Issue exactly one deterministic query from Node 0 targeting the affected shard
    auto failover_search = http_get(kHttpPortNode0, "/search?q=target");
    EXPECT_EQ(failover_search.status, 200);
    auto fs_json = nlohmann::json::parse(failover_search.body);

    // Assert: request succeeds, complete is true, and failover_doc was retrieved from secondary
    EXPECT_TRUE(fs_json["complete"].get<bool>());
    EXPECT_EQ(fs_json["total"].get<std::size_t>(), 1u);
    ASSERT_FALSE(fs_json["results"].empty());
    EXPECT_EQ(fs_json["results"][0]["document_id"].get<doc_id>(), failover_doc_id);

    // Take metrics snapshot on Node 0 after failover
    const auto after_failover = node0->metrics->snapshot();

    // Assert exact failover delta: exactly one read failover occurred
    EXPECT_EQ(after_failover.read_failovers_total - before_failover.read_failovers_total, 1u)
        << "Expected exactly 1 read failover increment attributable to this query";

    std::cout << "[PHASE 29] Deterministic read failover passed. Metric delta: +1 failover." << std::endl;

    // -----------------------------------------------------------------------
    // STEP 4: Node Restart & Persistence Recovery
    // -----------------------------------------------------------------------
    std::cout << "[PHASE 29] Restarting Node 1 gracefully and verifying persistence recovery..." << std::endl;

    // Restart Node 1 using existing persistent shard files
    node1 = build_node(1, kHttpPortNode1, kRpcPortNode1, base_data_dir, true /* load persistence */);

    // Boundedly poll HTTP readiness of recovered Node 1
    {
        bool recovered_http = false;
        const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(10);
        while (std::chrono::steady_clock::now() < deadline) {
            auto res = http_get(kHttpPortNode1, "/health");
            if (res.status == 200) {
                recovered_http = true;
                break;
            }
            std::this_thread::sleep_for(std::chrono::milliseconds(100));
        }
        ASSERT_TRUE(recovered_http) << "Node 1 HTTP server failed to recover";
    }

    // Boundedly poll RPC readiness of recovered Node 1
    {
        bool recovered_rpc = false;
        const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(10);
        while (std::chrono::steady_clock::now() < deadline) {
            RemoteNode rnode(1, "127.0.0.1", kRpcPortNode1);
            ShardCountRequest creq;
            creq.shard_id = 1;
            auto cresp = rnode.document_count(creq);
            if (!cresp.is_error && cresp.document_count >= 1) {
                recovered_rpc = true;
                break;
            }
            std::this_thread::sleep_for(std::chrono::milliseconds(100));
        }
        ASSERT_TRUE(recovered_rpc) << "Node 1 RPC server failed to recover persistent state";
    }

    // Verify recovered document 202 is intact and searchable on Node 1
    auto n1_search = http_get(kHttpPortNode1, "/search?q=target");
    EXPECT_EQ(n1_search.status, 200);
    auto n1_json = nlohmann::json::parse(n1_search.body);
    EXPECT_EQ(n1_json["total"].get<std::size_t>(), 1u);

    // Direct RPC verification of document content on recovered Node 1
    {
        RemoteNode rnode(1, "127.0.0.1", kRpcPortNode1);
        ShardGetRequest greq;
        greq.shard_id = 1;
        greq.document_id = failover_doc_id;
        auto gresp = rnode.get_document(greq);
        EXPECT_TRUE(gresp.found);
        EXPECT_EQ(gresp.content, "resilient failover secondary target document");
    }

    std::cout << "[PHASE 29] Node 1 recovered persistence successfully." << std::endl;

    // -----------------------------------------------------------------------
    // STEP 5: Kafka Outage, Durable Event Failure, Recovery & Explicit Replay
    // -----------------------------------------------------------------------
    const bool orchestrator_enabled = (std::getenv("DSE_PHASE29_ORCHESTRATOR") != nullptr);
    if (!orchestrator_enabled) {
        std::cout << "[PHASE 29 NOTICE] DSE_PHASE29_ORCHESTRATOR not set. Skipping live Kafka outage test." << std::endl;
        std::cout << "Run via: python tests/e2e_phase29_system_test.py for full Dockerized Kafka testing." << std::endl;
    } else {
#ifdef DSE_KAFKA_ENABLED
        std::cout << "[PHASE 29] Testing Kafka outage fault-injection via orchestrator IPC..." << std::endl;

        // 1. Request Python orchestrator to stop Kafka container
        ASSERT_TRUE(request_orchestrator("REQUEST: STOP_KAFKA", "ACK: KAFKA_STOPPED"))
            << "Orchestrator failed to acknowledge STOP_KAFKA";
        std::cout << "[PHASE 29] Kafka is STOPPED." << std::endl;

        // Compute outage_doc_id dynamically
        doc_id outage_doc_id = failover_doc_id + 1;
        while (outage_doc_id == 101 || outage_doc_id == failover_doc_id) {
            ++outage_doc_id;
        }
        const std::string outage_key = std::to_string(outage_doc_id);

        // 2. Authoritative write to Node 0 during Kafka outage
        const nlohmann::json outage_doc = {
            {"id", outage_doc_id},
            {"content", "kafka outage resilient authoritative write"}
        };
        auto out_post = http_post(kHttpPortNode0, "/documents", outage_doc.dump());
        EXPECT_EQ(out_post.status, 201) << "Authoritative write must succeed even when Kafka is down";

        // 3. Verify synchronous replication path succeeds and document is immediately searchable
        for (int http_port : {kHttpPortNode0, kHttpPortNode1, kHttpPortNode2}) {
            auto s_res = http_get(http_port, "/search?q=authoritative");
            EXPECT_EQ(s_res.status, 200);
            auto sj = nlohmann::json::parse(s_res.body);
            EXPECT_TRUE(sj["complete"].get<bool>());
            EXPECT_EQ(sj["total"].get<std::size_t>(), 1u);
            ASSERT_FALSE(sj["results"].empty());
            EXPECT_EQ(sj["results"][0]["document_id"].get<doc_id>(), outage_doc_id);
        }

        // 4. Identify exact event created in Node 0's PersistentEventStore
        EventId target_event_id = 0;
        {
            auto pending_events = node0->event_store->get_by_status(EventStatus::PENDING);
            for (const auto& ev : pending_events) {
                if (ev.key == outage_key) {
                    target_event_id = ev.id;
                    break;
                }
            }
            if (target_event_id == 0) {
                auto disp_events = node0->event_store->get_by_status(EventStatus::DISPATCHING);
                for (const auto& ev : disp_events) {
                    if (ev.key == outage_key) {
                        target_event_id = ev.id;
                        break;
                    }
                }
            }
            if (target_event_id == 0) {
                auto failed_events = node0->event_store->get_by_status(EventStatus::FAILED);
                for (const auto& ev : failed_events) {
                    if (ev.key == outage_key) {
                        target_event_id = ev.id;
                        break;
                    }
                }
            }
        }
        ASSERT_GT(target_event_id, 0u) << "Could not locate event for document " << outage_doc_id << " in Node 0 EventStore";

        // 5. Boundedly poll Node 0's PersistentEventStore until target event reaches FAILED
        std::cout << "[PHASE 29] Polling Node 0 EventStore until event " << target_event_id << " reaches FAILED..." << std::endl;
        bool reached_failed = false;
        const auto fail_deadline = std::chrono::steady_clock::now() + std::chrono::seconds(20);
        while (std::chrono::steady_clock::now() < fail_deadline) {
            const auto* ev = node0->event_store->get(target_event_id);
            if (ev && ev->status == EventStatus::FAILED) {
                reached_failed = true;
                break;
            }
            std::this_thread::sleep_for(std::chrono::milliseconds(100));
        }
        ASSERT_TRUE(reached_failed) << "Event " << target_event_id << " did not reach FAILED within timeout";

        const auto* failed_ev = node0->event_store->get(target_event_id);
        ASSERT_NE(failed_ev, nullptr);
        EXPECT_EQ(failed_ev->status, EventStatus::FAILED);
        EXPECT_EQ(failed_ev->key, outage_key);
        EXPECT_FALSE(failed_ev->error_message.empty());
        std::cout << "[PHASE 29] Event " << target_event_id << " successfully reached durable FAILED state." << std::endl;

        // 6. Request Python orchestrator to restart Kafka container
        ASSERT_TRUE(request_orchestrator("REQUEST: START_KAFKA", "ACK: KAFKA_READY"))
            << "Orchestrator failed to acknowledge START_KAFKA / KAFKA_READY";
        std::cout << "[PHASE 29] Kafka is RESTORED and READY." << std::endl;

        // 7. Verify NO automatic replay occurs after Kafka recovery
        std::cout << "[PHASE 29] Verifying that event does NOT automatically replay..." << std::endl;
        for (int i = 0; i < 20; ++i) {
            std::this_thread::sleep_for(std::chrono::milliseconds(100));
            const auto* ev = node0->event_store->get(target_event_id);
            ASSERT_NE(ev, nullptr);
            EXPECT_EQ(ev->status, EventStatus::FAILED)
                << "Kafka recovery must NOT automatically transition event out of FAILED state";
        }

        // 8. Explicit replay: invoke EventDispatcher::replay_failed()
        std::cout << "[PHASE 29] Invoking explicit replay (EventDispatcher::replay_failed)..." << std::endl;
        std::size_t replayed = node0->dispatcher->replay_failed();
        EXPECT_GE(replayed, 1u) << "Expected at least 1 event to be replayed";

        // 9. Boundedly poll until target event transitions to PUBLISHED
        bool reached_published = false;
        const auto pub_deadline = std::chrono::steady_clock::now() + std::chrono::seconds(15);
        while (std::chrono::steady_clock::now() < pub_deadline) {
            const auto* ev = node0->event_store->get(target_event_id);
            if (ev && ev->status == EventStatus::PUBLISHED) {
                reached_published = true;
                break;
            }
            std::this_thread::sleep_for(std::chrono::milliseconds(100));
        }
        ASSERT_TRUE(reached_published) << "Replayed event did not reach PUBLISHED state";

        const auto* pub_ev = node0->event_store->get(target_event_id);
        ASSERT_NE(pub_ev, nullptr);
        EXPECT_EQ(pub_ev->status, EventStatus::PUBLISHED);
        EXPECT_EQ(pub_ev->id, target_event_id) << "Event ID must remain stable across replay";
        EXPECT_EQ(pub_ev->key, outage_key) << "Message key must remain stable across replay";

        std::cout << "[PHASE 29] Explicit replay verified: event published with stable ID "
                  << target_event_id << " and key '" << outage_key << "'." << std::endl;
#endif
    }

    // -----------------------------------------------------------------------
    // STEP 6: Final Replica Consistency Verification
    // -----------------------------------------------------------------------
    std::cout << "[PHASE 29] Verifying final replica consistency across all shards..." << std::endl;
    for (int http_port : {kHttpPortNode0, kHttpPortNode1, kHttpPortNode2}) {
        auto resp202 = http_get(http_port, "/search?q=target");
        EXPECT_EQ(resp202.status, 200);
        auto j202 = nlohmann::json::parse(resp202.body);
        EXPECT_EQ(j202["total"].get<std::size_t>(), 1u);

        if (orchestrator_enabled) {
            auto resp303 = http_get(http_port, "/search?q=authoritative");
            EXPECT_EQ(resp303.status, 200);
            auto j303 = nlohmann::json::parse(resp303.body);
            EXPECT_EQ(j303["total"].get<std::size_t>(), 1u);
        }
    }

    // -----------------------------------------------------------------------
    // STEP 7: Operational Metrics Verification
    // -----------------------------------------------------------------------
    std::cout << "[PHASE 29] Verifying operational metrics on GET /metrics..." << std::endl;
    auto metrics_resp = http_get(kHttpPortNode0, "/metrics");
    EXPECT_EQ(metrics_resp.status, 200);
    auto mj = nlohmann::json::parse(metrics_resp.body);

    EXPECT_TRUE(mj.contains("coordinator_searches_total"));
    EXPECT_TRUE(mj.contains("coordinator_writes_total"));
    EXPECT_TRUE(mj.contains("read_failovers_total"));
    EXPECT_GE(mj["read_failovers_total"].get<std::uint64_t>(), 1u);
    EXPECT_TRUE(mj.contains("events_total"));

    if (orchestrator_enabled) {
        EXPECT_GE(mj["events_total"].get<std::size_t>(), 1u);
        EXPECT_TRUE(mj.contains("events_published"));
        EXPECT_GE(mj["events_published"].get<std::size_t>(), 1u);
    }

    // -----------------------------------------------------------------------
    // STEP 8: Compact 3-Node Search Performance Regression
    // -----------------------------------------------------------------------
    std::cout << "[PHASE 29] Running compact 3-node search performance regression (50 queries)..." << std::endl;
    std::vector<double> latencies_ms;
    latencies_ms.reserve(50);

    for (int i = 0; i < 50; ++i) {
        const auto t0 = std::chrono::steady_clock::now();
        auto s_res = http_get(kHttpPortNode0, "/search?q=target");
        const auto t1 = std::chrono::steady_clock::now();

        EXPECT_EQ(s_res.status, 200);
        const double ms = std::chrono::duration<double, std::milli>(t1 - t0).count();
        latencies_ms.push_back(ms);
    }

    std::sort(latencies_ms.begin(), latencies_ms.end());
    const double p50 = latencies_ms[latencies_ms.size() * 50 / 100];
    const double p99 = latencies_ms[latencies_ms.size() * 99 / 100];

    std::cout << "[PHASE 29] Search regression results: p50 = " << p50
              << " ms, p99 = " << p99 << " ms." << std::endl;
    EXPECT_LT(p99, 100.0) << "Search p99 latency regression exceeded 100ms threshold";

    // -----------------------------------------------------------------------
    // STEP 9: Graceful Teardown
    // -----------------------------------------------------------------------
    std::cout << "[PHASE 29] Shutting down cluster gracefully..." << std::endl;
    node0->stop_all();
    node1->stop_all();
    node2->stop_all();

    std::cout << "[PHASE 29] System validation integration test PASSED." << std::endl;
}

} // namespace dse
