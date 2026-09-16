// Distributed Search Engine - Failure Testing & Fault Injection Integration Tests (Phase 28).
//
// Systematically validates existing distributed-search failure semantics under controlled failures.
// Scenarios verified:
//   A. One replica unavailable during synchronous write (test-only R=2 fixture)
//   B. Primary read replica unavailable (read failover, secondary fallback, complete=true, metrics)
//   C. All replicas of one shard unavailable (partial availability, complete=false, shard errors)
//   D. Kafka unavailable (authoritative synchronous write succeeds; event marked FAILED in EventStore)
//   E. Kafka recovery + explicit replay (no automatic replay; explicit replay_failed succeeds; stable ID/key)
//   F. Node shutdown and restart (graceful NodeServer shutdown and shard persistence reload)
//   G. RPC retry behavior (search retries on transport error; mutations do NOT retry; application errors not retried)

#include <gtest/gtest.h>

#include <atomic>
#include <chrono>
#include <cstddef>
#include <filesystem>
#include <memory>
#include <string>
#include <thread>
#include <vector>

#include "circuit_breaker.h"
#include "document_event.h"
#include "event_dispatcher.h"
#include "event_store.h"
#include "local_node.h"
#include "message.h"
#include "message_broker.h"
#include "metrics.h"
#include "node_client.h"
#include "node_server.h"
#include "remote_node.h"
#include "replica_placement.h"
#include "retry_policy.h"
#include "shard.h"
#include "shard_coordinator.h"
#include "shard_router.h"

namespace dse {
namespace {

// ---------------------------------------------------------------------------
// ServerHandle helper: manages NodeServer lifecycle in a dedicated thread
// ---------------------------------------------------------------------------
struct ServerHandle {
    std::unique_ptr<NodeServer> server;
    std::thread thread;

    void stop_and_join() {
        if (server) {
            server->stop();
        }
        if (thread.joinable()) {
            thread.join();
        }
    }
};

static ServerHandle start_server(std::size_t node_id, std::unique_ptr<LocalNode> node) {
    ServerHandle h;
    h.server = std::make_unique<NodeServer>(node_id, std::move(node));
    h.thread = std::thread([&h]() { h.server->listen(0); });
    h.server->wait_until_ready();
    return h;
}

// ---------------------------------------------------------------------------
// FailingNode: NodeClient test double that deterministically fails all RPCs
// ---------------------------------------------------------------------------
class FailingNode : public NodeClient {
public:
    explicit FailingNode(std::size_t id) : node_id_(id) {}

    std::size_t node_id() const override { return node_id_; }

    ShardSearchResponse search(const ShardSearchRequest& req) override {
        ShardSearchResponse resp;
        resp.shard_id = req.shard_id;
        resp.is_error = true;
        resp.error_message = "node " + std::to_string(node_id_) + " simulated unavailable";
        return resp;
    }

    ShardWriteResponse add_document(const ShardWriteRequest& req) override {
        ShardWriteResponse resp;
        resp.shard_id = req.shard_id;
        resp.document_id = req.document_id;
        resp.is_error = true;
        resp.error_message = "node " + std::to_string(node_id_) + " simulated unavailable";
        return resp;
    }

    ShardWriteResponse update_document(const ShardWriteRequest& req) override {
        ShardWriteResponse resp;
        resp.shard_id = req.shard_id;
        resp.document_id = req.document_id;
        resp.is_error = true;
        resp.error_message = "node " + std::to_string(node_id_) + " simulated unavailable";
        return resp;
    }

    ShardRemoveResponse remove_document(const ShardRemoveRequest& req) override {
        ShardRemoveResponse resp;
        resp.shard_id = req.shard_id;
        resp.is_error = true;
        resp.error_message = "node " + std::to_string(node_id_) + " simulated unavailable";
        return resp;
    }

    ShardGetResponse get_document(const ShardGetRequest& req) override {
        ShardGetResponse resp;
        resp.shard_id = req.shard_id;
        resp.document_id = req.document_id;
        resp.is_error = true;
        resp.error_message = "node " + std::to_string(node_id_) + " simulated unavailable";
        return resp;
    }

    ShardCountResponse document_count(const ShardCountRequest& req) override {
        ShardCountResponse resp;
        resp.shard_id = req.shard_id;
        resp.is_error = true;
        resp.error_message = "node " + std::to_string(node_id_) + " simulated unavailable";
        return resp;
    }

    bool save_shard(std::size_t) override { return false; }
    bool load_shard(std::size_t) override { return false; }

private:
    std::size_t node_id_;
};

// ---------------------------------------------------------------------------
// ControllableBroker: simulates Kafka unavailability and recovery
// ---------------------------------------------------------------------------
class ControllableBroker : public MessageBroker {
public:
    explicit ControllableBroker(bool fail_initially = true)
        : fail_(fail_initially) {}

    Offset publish(Message message) override {
        if (fail_.load()) {
            if (cb_) {
                cb_(message.id, false, "broker connection refused");
            }
            throw std::runtime_error("broker connection refused");
        }
        published_messages_.push_back(message);
        if (cb_) {
            cb_(message.id, true, "");
        }
        return next_offset_++;
    }

    std::optional<Offset> publish_with_timeout(Message message, std::size_t) override {
        if (fail_.load()) {
            return std::nullopt;
        }
        return publish(std::move(message));
    }

    void subscribe(const Topic&, MessageHandler) override {}
    void start() override {}
    void stop() override {}
    std::size_t queue_size(const Topic&) const override { return 0; }
    BrokerStats stats() const override { return {}; }
    std::vector<Message> dead_letters(const Topic&) const override { return {}; }
    bool was_processed(MessageId) const override { return false; }

    void set_delivery_callback(DeliveryCallback cb) override { cb_ = std::move(cb); }
    void set_fail(bool f) { fail_.store(f); }

    const std::vector<Message>& published() const { return published_messages_; }

private:
    std::atomic<bool> fail_{true};
    DeliveryCallback cb_;
    std::vector<Message> published_messages_;
    Offset next_offset_ = 0;
};

// ---------------------------------------------------------------------------
// InMemoryEventStore test implementation
// ---------------------------------------------------------------------------
class InMemoryEventStore : public EventStore {
public:
    EventId create_event(std::string topic, std::string key, std::string payload) override {
        std::lock_guard<std::mutex> lock(mutex_);
        EventId id = ++next_id_;
        StoredEvent ev;
        ev.id = id;
        ev.topic = std::move(topic);
        ev.key = std::move(key);
        ev.payload = std::move(payload);
        ev.status = EventStatus::PENDING;
        ev.attempt_count = 0;
        events_[id] = ev;
        return id;
    }

    void mark_dispatching(EventId id) override {
        std::lock_guard<std::mutex> lock(mutex_);
        auto it = events_.find(id);
        if (it != events_.end() && it->second.status == EventStatus::PENDING) {
            it->second.status = EventStatus::DISPATCHING;
        }
    }

    void record_attempt(EventId id) override {
        std::lock_guard<std::mutex> lock(mutex_);
        auto it = events_.find(id);
        if (it != events_.end()) {
            ++it->second.attempt_count;
        }
    }

    void mark_published(EventId id) override {
        std::lock_guard<std::mutex> lock(mutex_);
        auto it = events_.find(id);
        if (it != events_.end()) {
            it->second.status = EventStatus::PUBLISHED;
        }
    }

    void mark_failed(EventId id, const std::string& error) override {
        std::lock_guard<std::mutex> lock(mutex_);
        auto it = events_.find(id);
        if (it != events_.end()) {
            it->second.status = EventStatus::FAILED;
            it->second.error_message = error;
        }
    }

    bool requeue(EventId id) override {
        std::lock_guard<std::mutex> lock(mutex_);
        auto it = events_.find(id);
        if (it != events_.end() && it->second.status == EventStatus::FAILED) {
            it->second.status = EventStatus::PENDING;
            return true;
        }
        return false;
    }

    const StoredEvent* get(EventId id) const override {
        std::lock_guard<std::mutex> lock(mutex_);
        auto it = events_.find(id);
        if (it != events_.end()) {
            return &it->second;
        }
        return nullptr;
    }

    std::vector<StoredEvent> get_by_status(EventStatus status) const override {
        std::lock_guard<std::mutex> lock(mutex_);
        std::vector<StoredEvent> result;
        for (const auto& [_, ev] : events_) {
            if (ev.status == status) {
                result.push_back(ev);
            }
        }
        return result;
    }

    std::vector<StoredEvent> get_by_topic(const std::string& topic, EventStatus status) const override {
        std::lock_guard<std::mutex> lock(mutex_);
        std::vector<StoredEvent> result;
        for (const auto& [_, ev] : events_) {
            if (ev.topic == topic && ev.status == status) {
                result.push_back(ev);
            }
        }
        return result;
    }

    DeliveryStats stats() const override {
        std::lock_guard<std::mutex> lock(mutex_);
        DeliveryStats s;
        s.total = events_.size();
        for (const auto& [_, ev] : events_) {
            switch (ev.status) {
                case EventStatus::PENDING: ++s.pending; break;
                case EventStatus::DISPATCHING: ++s.dispatching; break;
                case EventStatus::PUBLISHED: ++s.published; break;
                case EventStatus::FAILED: ++s.failed; break;
            }
        }
        return s;
    }

private:
    mutable std::mutex mutex_;
    EventId next_id_{0};
    std::unordered_map<EventId, StoredEvent> events_;
};

} // namespace

// ===========================================================================
// SCENARIO A: One replica unavailable during synchronous write
// ===========================================================================
// Verifies Phase 17 authoritative synchronous replication semantics:
// - All-replica write fan-out requires every replica in the replica set to acknowledge.
// - If any replica fails (e.g. secondary gracefully stopped), the write fails synchronously.
// - No false success is reported (response.is_error == true).
// - There is no 2PC or distributed rollback: primary retains the written state (partial state).
// NOTE: This uses an R=2 fixture strictly as a deterministic test fixture.
TEST(FaultInjectionTest, ScenarioA_OneReplicaUnavailableDuringSynchronousWrite)
{
    // Node 0: Primary
    auto local0 = std::make_unique<LocalNode>(0);
    local0->add_shard(0, std::make_unique<Shard>());
    auto h0 = start_server(0, std::move(local0));

    // Node 1: Secondary
    auto local1 = std::make_unique<LocalNode>(1);
    local1->add_shard(0, std::make_unique<Shard>());
    auto h1 = start_server(1, std::move(local1));

    // Reduced deterministic R=2 test fixture (test-only)
    auto router = std::make_unique<ShardRouter>(1);
    std::vector<ShardReplicaSet> replica_sets = {{0, {0, 1}}};
    auto placement = std::make_unique<ShardReplicaPlacement>(1, 2, 2, replica_sets);

    std::vector<std::unique_ptr<NodeClient>> nodes;
    nodes.push_back(std::make_unique<RemoteNode>(0, "127.0.0.1", h0.server->port()));
    nodes.push_back(std::make_unique<RemoteNode>(1, "127.0.0.1", h1.server->port()));

    ShardCoordinator coord(std::move(router), std::move(placement), std::move(nodes));

    // Baseline write: both replicas healthy -> success
    auto initial_resp = coord.ingest({1, "both replicas healthy"});
    EXPECT_FALSE(initial_resp.is_error);

    // Fault injection: gracefully stop secondary NodeServer (Node 1)
    h1.stop_and_join();

    // Write attempt while secondary is unavailable
    auto fail_resp = coord.ingest({2, "secondary is offline"});

    // 1. Verify synchronous failure: write MUST fail to caller
    EXPECT_TRUE(fail_resp.is_error);
    EXPECT_FALSE(fail_resp.error_message.empty());

    // 2. Verify no false success was reported
    EXPECT_NE(fail_resp.is_error, false);

    // 3. Verify partial state semantics (no 2PC):
    // Primary node 0 is still alive and processed the write before secondary failed.
    RemoteNode direct_node0(0, "127.0.0.1", h0.server->port());
    ShardGetRequest get_req;
    get_req.shard_id = 0;
    get_req.document_id = 2;
    auto get_resp0 = direct_node0.get_document(get_req);
    EXPECT_TRUE(get_resp0.found) << "Primary node retains write in partial state (absence of 2PC)";
    EXPECT_EQ(get_resp0.content, "secondary is offline");

    // Clean up
    h0.stop_and_join();
}

// ===========================================================================
// SCENARIO B: Primary read replica unavailable
// ===========================================================================
// Verifies Phase 24 read failover semantics:
// - When primary replica fails, query execution falls back to healthy secondary.
// - Returns complete=true since all required shards succeeded.
// - Increments read_failovers_total metric exactly once per shard failover.
TEST(FaultInjectionTest, ScenarioB_PrimaryReadReplicaUnavailableFallback)
{
    auto router = std::make_unique<ShardRouter>(1);
    std::vector<ShardReplicaSet> replica_sets = {{0, {0, 1}}};
    auto placement = std::make_unique<ShardReplicaPlacement>(1, 2, 2, replica_sets);

    // Node 0: failing primary
    auto n0 = std::make_unique<FailingNode>(0);

    // Node 1: healthy secondary with pre-indexed content
    auto local1 = std::make_unique<LocalNode>(1);
    auto shard1 = std::make_unique<Shard>();
    shard1->add_document(10, "resilient distributed search failover document");
    local1->add_shard(0, std::move(shard1));

    std::vector<std::unique_ptr<NodeClient>> nodes;
    nodes.push_back(std::move(n0));
    nodes.push_back(std::move(local1));

    ShardCoordinator coord(std::move(router), std::move(placement), std::move(nodes));

    MetricsCollector metrics;
    coord.set_metrics(&metrics);

    // Search failover verification
    SearchRequest s_req;
    s_req.query = "resilient";
    s_req.mode = SearchMode::Or;
    s_req.limit = 10;
    auto s_resp = coord.search(s_req);

    EXPECT_FALSE(s_resp.is_error);
    EXPECT_TRUE(s_resp.complete);
    EXPECT_EQ(s_resp.total, 1u);
    ASSERT_FALSE(s_resp.results.empty());
    EXPECT_EQ(s_resp.results[0].document_id, 10u);

    // Document retrieval failover verification
    auto doc = coord.get_document(10);
    ASSERT_TRUE(doc.has_value());
    EXPECT_EQ(doc->content, "resilient distributed search failover document");

    // Verify read_failovers_total metric incremented
    const auto snap = metrics.snapshot();
    EXPECT_GE(snap.read_failovers_total, 1u);
}

// ===========================================================================
// SCENARIO C: All replicas of one shard unavailable
// ===========================================================================
// Verifies Phase 25 partial-availability search semantics:
// - Multi-shard fixture (Shard 0 and Shard 1).
// - All replicas of Shard 1 are unavailable, while Shard 0 remains available.
// - Search returns partial results from Shard 0 with complete=false.
// - Error list specifically identifies Shard 1 as unavailable.
// - Partial results are never described as complete cluster results.
TEST(FaultInjectionTest, ScenarioC_AllReplicasOfOneShardUnavailable)
{
    // 2 shards: Shard 0 and Shard 1
    auto router = std::make_unique<ShardRouter>(2);
    std::vector<ShardReplicaSet> replica_sets = {
        {0, {0}},  // Shard 0 on Node 0 (healthy)
        {1, {1}}   // Shard 1 on Node 1 (all replicas failing)
    };
    auto placement = std::make_unique<ShardReplicaPlacement>(2, 2, 1, replica_sets);

    // Pre-populate Shard 0 with a document matching term "common"
    // Document ID 0 routes to Shard 0 (0 % 2 == 0)
    auto local0 = std::make_unique<LocalNode>(0);
    auto shard0 = std::make_unique<Shard>();
    shard0->add_document(2, "common keyword in shard zero");
    local0->add_shard(0, std::move(shard0));

    // Node 1: FailingNode (represents complete failure of Shard 1)
    auto n1 = std::make_unique<FailingNode>(1);

    std::vector<std::unique_ptr<NodeClient>> nodes;
    nodes.push_back(std::move(local0));
    nodes.push_back(std::move(n1));

    ShardCoordinator coord(std::move(router), std::move(placement), std::move(nodes));

    SearchRequest req;
    req.query = "common";
    req.mode = SearchMode::Or;
    req.limit = 10;

    auto resp = coord.search(req);

    // Search request itself succeeds at the coordinator level
    EXPECT_FALSE(resp.is_error);

    // But availability is degraded: complete MUST be false
    EXPECT_FALSE(resp.complete);

    // Partial results from available Shard 0 are returned
    EXPECT_EQ(resp.total, 1u);
    ASSERT_EQ(resp.results.size(), 1u);
    EXPECT_EQ(resp.results[0].document_id, 2u);

    // Shard-specific errors are recorded for Shard 1
    EXPECT_FALSE(resp.errors.empty());
    bool found_shard_1_error = false;
    for (const auto& err : resp.errors) {
        if (err.shard_id == 1) {
            found_shard_1_error = true;
            EXPECT_FALSE(err.message.empty());
        }
    }
    EXPECT_TRUE(found_shard_1_error) << "Response errors must explicitly identify Shard 1";
}

// ===========================================================================
// SCENARIO D: Kafka unavailable
// ===========================================================================
// Verifies asynchronous decoupling of Kafka from synchronous replication:
// - Authoritative write succeeds independently of Kafka availability.
// - EventDispatcher fails to publish and exhausts retries.
// - EventStore retains the mutation with its actual documented failure state (FAILED).
// - Stable event ID and document key remain preserved for replay.
TEST(FaultInjectionTest, ScenarioD_KafkaUnavailableAuthoritativeWriteSucceeds)
{
    auto router = std::make_unique<ShardRouter>(1);
    std::vector<ShardReplicaSet> replica_sets = {{0, {0}}};
    auto placement = std::make_unique<ShardReplicaPlacement>(1, 1, 1, replica_sets);

    auto local0 = std::make_unique<LocalNode>(0);
    local0->add_shard(0, std::make_unique<Shard>());
    std::vector<std::unique_ptr<NodeClient>> nodes;
    nodes.push_back(std::move(local0));

    ShardCoordinator coord(std::move(router), std::move(placement), std::move(nodes));

    InMemoryEventStore store;
    ControllableBroker broker(true); // Broker connection is down / throws

    EventDispatcher::Config cfg;
    cfg.max_retries = 1;
    cfg.retry_delay_ms = 0; // Deterministic, no sleep
    EventDispatcher dispatcher(broker, store, cfg);
    dispatcher.start();

    coord.set_event_store(&store);
    coord.set_event_dispatcher(&dispatcher);

    // Authoritative synchronous write
    auto resp = coord.ingest({42, "authoritative data independent of kafka"});

    // 1. Authoritative write succeeds immediately
    EXPECT_FALSE(resp.is_error);

    // 2. Data is immediately queryable
    auto doc = coord.get_document(42);
    ASSERT_TRUE(doc.has_value());
    EXPECT_EQ(doc->content, "authoritative data independent of kafka");

    // Stop dispatcher to drain worker processing
    dispatcher.stop();

    // 3. Verify EventStore lifecycle state: event is durably recorded as FAILED
    auto failed_events = store.get_by_status(EventStatus::FAILED);
    ASSERT_EQ(failed_events.size(), 1u);
    EXPECT_EQ(failed_events[0].key, "42");
    EXPECT_EQ(failed_events[0].topic, topics::kDocumentMutations);
    EXPECT_GT(failed_events[0].id, 0u);
    EXPECT_FALSE(failed_events[0].error_message.empty());
}

// ===========================================================================
// SCENARIO E: Kafka recovery + explicit replay
// ===========================================================================
// Verifies Phase 19F async event recovery:
// - Broker recovers from outage.
// - Failed events do NOT magically become PUBLISHED without explicit replay.
// - Explicitly invoking EventDispatcher::replay_failed() re-enqueues failed events.
// - Successful publication preserves stable event ID and message key.
TEST(FaultInjectionTest, ScenarioE_KafkaRecoveryAndExplicitReplay)
{
    auto router = std::make_unique<ShardRouter>(1);
    std::vector<ShardReplicaSet> replica_sets = {{0, {0}}};
    auto placement = std::make_unique<ShardReplicaPlacement>(1, 1, 1, replica_sets);

    auto local0 = std::make_unique<LocalNode>(0);
    local0->add_shard(0, std::make_unique<Shard>());
    std::vector<std::unique_ptr<NodeClient>> nodes;
    nodes.push_back(std::move(local0));

    ShardCoordinator coord(std::move(router), std::move(placement), std::move(nodes));

    InMemoryEventStore store;
    ControllableBroker broker(true); // Broker down

    EventDispatcher::Config cfg;
    cfg.max_retries = 0;
    cfg.retry_delay_ms = 0;
    EventDispatcher dispatcher(broker, store, cfg);
    dispatcher.start();

    coord.set_event_store(&store);
    coord.set_event_dispatcher(&dispatcher);

    auto resp = coord.ingest({99, "replayable event payload"});
    EXPECT_FALSE(resp.is_error);

    dispatcher.stop();

    // Verify initial FAILED state
    auto failed_events = store.get_by_status(EventStatus::FAILED);
    ASSERT_EQ(failed_events.size(), 1u);
    const EventId original_id = failed_events[0].id;
    const std::string original_key = failed_events[0].key;

    // Step 1: Kafka recovery
    broker.set_fail(false);

    // Step 2: Verify NO automatic replay occurs merely because Kafka is healthy
    const auto* ev_unreplayed = store.get(original_id);
    ASSERT_NE(ev_unreplayed, nullptr);
    EXPECT_EQ(ev_unreplayed->status, EventStatus::FAILED)
        << "Events must not automatically change from FAILED to PUBLISHED without explicit replay";

    // Step 3: Explicitly invoke the existing replay mechanism (replay_failed)
    dispatcher.start();
    std::size_t replayed = dispatcher.replay_failed();
    EXPECT_EQ(replayed, 1u);
    dispatcher.stop();

    // Step 4: Verify event is now PUBLISHED and event identity is preserved
    const auto* ev_published = store.get(original_id);
    ASSERT_NE(ev_published, nullptr);
    EXPECT_EQ(ev_published->status, EventStatus::PUBLISHED);
    EXPECT_EQ(ev_published->id, original_id);
    EXPECT_EQ(ev_published->key, original_key);
    EXPECT_EQ(broker.published().size(), 1u);
}

// ===========================================================================
// SCENARIO F: Node shutdown/restart
// ===========================================================================
// Verifies graceful node shutdown and shard persistence recovery:
// - A NodeServer with persistent shard storage is shut down gracefully.
// - Restarting a new NodeServer on the same persistent shard data recovers all indexed documents.
// NOTE: Clearly documented as graceful restart (ServerHandle::stop_and_join), NOT process crash.
TEST(FaultInjectionTest, ScenarioF_GracefulNodeShutdownAndRestartPersistence)
{
    const std::string test_dir = "test_phase28_persist";
    std::filesystem::create_directories(test_dir);
    const std::string persist_file = test_dir + "/shard_0.jsonl";
    if (std::filesystem::exists(persist_file)) {
        std::filesystem::remove(persist_file);
    }

    // Phase 1: Start NodeServer, ingest document, save shard, and shut down gracefully
    int initial_port = 0;
    {
        auto node = std::make_unique<LocalNode>(0);
        node->add_shard(0, std::make_unique<Shard>(persist_file));
        auto handle = start_server(0, std::move(node));
        initial_port = handle.server->port();

        RemoteNode client(0, "127.0.0.1", initial_port);
        ShardWriteRequest write_req;
        write_req.shard_id = 0;
        write_req.document_id = 123;
        write_req.content = "survives graceful server restart";

        auto write_resp = client.add_document(write_req);
        EXPECT_FALSE(write_resp.is_error);

        EXPECT_TRUE(client.save_shard(0));

        // Graceful shutdown
        handle.stop_and_join();
    }

    // Phase 2: Start new NodeServer with the same persistent shard file
    {
        auto recovered_node = std::make_unique<LocalNode>(0);
        recovered_node->add_shard(0, std::make_unique<Shard>(persist_file));
        auto handle = start_server(0, std::move(recovered_node));

        RemoteNode client(0, "127.0.0.1", handle.server->port());
        EXPECT_TRUE(client.load_shard(0));

        // Verify document recovered
        ShardGetRequest get_req;
        get_req.shard_id = 0;
        get_req.document_id = 123;
        auto get_resp = client.get_document(get_req);

        EXPECT_TRUE(get_resp.found);
        EXPECT_EQ(get_resp.content, "survives graceful server restart");

        handle.stop_and_join();
    }

    std::filesystem::remove_all(test_dir);
}

// ===========================================================================
// SCENARIO G: RPC retry behavior (Search vs. Mutation)
// ===========================================================================
// Inspects and validates RemoteNode retry behavior:
// 1. Read operations (search) retry on transport errors up to max_attempts.
// 2. Read retries increment retries_total and per_node[id].retries metrics.
// 3. Application errors (e.g. document not found) return HTTP 200 and are NOT retried.
// 4. Mutation operations (add, update, delete) make a single attempt and do NOT retry.
TEST(FaultInjectionTest, ScenarioG_RpcRetryBehavior_SearchVsMutation)
{
    // Part 1: Search retries on connection refused
    {
        RetryPolicy policy;
        policy.max_attempts = 3;
        policy.initial_delay_ms = 0; // Deterministic, no sleep

        MetricsCollector metrics;
        RemoteNode node(0, "127.0.0.1", 59990, 1, policy);
        node.set_metrics(&metrics);

        ShardSearchRequest req;
        req.shard_id = 0;
        req.terms = {"test"};

        auto resp = node.search(req);
        EXPECT_TRUE(resp.is_error);

        // 3 total attempts = 1 initial + 2 retries
        auto snap = metrics.snapshot();
        EXPECT_EQ(snap.retries_total, 2u);
        EXPECT_EQ(snap.per_node[0].retries, 2u);
    }

    // Part 2: Mutations do NOT retry even on connection failure
    {
        RetryPolicy policy;
        policy.max_attempts = 3;
        policy.initial_delay_ms = 0;

        MetricsCollector metrics;
        RemoteNode node(0, "127.0.0.1", 59990, 1, policy);
        node.set_metrics(&metrics);

        ShardWriteRequest write_req;
        write_req.shard_id = 0;
        write_req.document_id = 55;
        write_req.content = "non retryable write";

        auto write_resp = node.add_document(write_req);
        EXPECT_TRUE(write_resp.is_error);

        // Mutations make a single attempt without retrying
        auto snap = metrics.snapshot();
        EXPECT_EQ(snap.retries_total, 0u);
        EXPECT_EQ(snap.per_node[0].retries, 0u);

        // Similarly for update and delete
        ShardWriteRequest update_req = write_req;
        auto update_resp = node.update_document(update_req);
        EXPECT_TRUE(update_resp.is_error);
        EXPECT_EQ(metrics.snapshot().retries_total, 0u);

        ShardRemoveRequest remove_req;
        remove_req.shard_id = 0;
        remove_req.document_id = 55;
        auto remove_resp = node.remove_document(remove_req);
        EXPECT_TRUE(remove_resp.is_error);
        EXPECT_EQ(metrics.snapshot().retries_total, 0u);
    }

    // Part 3: Application-level errors are NOT retried
    {
        auto local = std::make_unique<LocalNode>(0);
        local->add_shard(0, std::make_unique<Shard>());
        auto handle = start_server(0, std::move(local));

        RetryPolicy policy;
        policy.max_attempts = 3;
        policy.initial_delay_ms = 0;

        MetricsCollector metrics;
        RemoteNode node(0, "127.0.0.1", handle.server->port(), 5, policy);
        node.set_metrics(&metrics);

        // Request non-existent document -> application returns found=false without retrying
        ShardGetRequest req;
        req.shard_id = 0;
        req.document_id = 99999;
        auto resp = node.get_document(req);

        EXPECT_FALSE(resp.is_error);
        EXPECT_FALSE(resp.found);

        // No transport retries were triggered
        EXPECT_EQ(metrics.snapshot().retries_total, 0u);

        handle.stop_and_join();
    }
}

} // namespace dse
