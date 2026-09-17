// Distributed Search Engine - Document Event Tests (Phase 18C/18D/18E).
//
// Tests for domain event publication after successful document mutations.
// Verifies that:
//   - Successful mutations publish exactly one event per logical operation
//   - Failed mutations publish no events
//   - Event payloads are correct JSON (including event_id)
//   - No double-counting with replication factor R=2
//   - No dispatcher = no events (backward compatibility)
//   - Event topics are correct
//   - Asynchronous dispatch works correctly
//   - EventStore integration tracks event lifecycle

#include "shard_coordinator.h"

#include <gtest/gtest.h>

#include <atomic>
#include <nlohmann/json.hpp>
#include <cstddef>
#include <memory>
#include <string>
#include <vector>

#include "document_event.h"
#include "event_dispatcher.h"
#include "event_store.h"
#include "in_memory_message_broker.h"
#include "inverted_index.h"
#include "local_node.h"
#include "message.h"
#include "message_broker.h"
#include "node_client.h"
#include "replica_placement.h"
#include "shard.h"
#include "shard_router.h"

namespace dse {
namespace {

// ---------------------------------------------------------------------------
// Test helpers
// ---------------------------------------------------------------------------

// Create a coordinator with N shards, 1 node, and optional dispatcher/store.
std::unique_ptr<ShardCoordinator> make_coord(
    std::size_t n,
    EventDispatcher* dispatcher = nullptr,
    EventStore* store = nullptr)
{
    auto router = std::make_unique<ShardRouter>(n);
    std::vector<std::size_t> placement(n, 0);
    auto rp = std::make_unique<ShardPlacement>(n, 1, placement);

    auto node = std::make_unique<LocalNode>(0);
    for (std::size_t i = 0; i < n; ++i) {
        node->add_shard(i, std::make_unique<Shard>());
    }

    std::vector<std::unique_ptr<NodeClient>> nodes;
    nodes.push_back(std::move(node));

    auto coord = std::make_unique<ShardCoordinator>(
        std::move(router), std::move(rp), std::move(nodes));
    coord->set_event_dispatcher(dispatcher);
    if (store) coord->set_event_store(store);
    return coord;
}

// Create a coordinator with R=2 replication.
std::unique_ptr<ShardCoordinator> make_coord_r2(
    std::size_t n,
    EventDispatcher* dispatcher = nullptr,
    EventStore* store = nullptr)
{
    auto router = std::make_unique<ShardRouter>(n);
    std::vector<ShardReplicaSet> replica_sets;
    for (std::size_t sid = 0; sid < n; ++sid) {
        replica_sets.push_back({sid, {0, 1}});
    }
    auto rp = std::make_unique<ShardReplicaPlacement>(
        n, 2, 2, replica_sets);

    auto node0 = std::make_unique<LocalNode>(0);
    auto node1 = std::make_unique<LocalNode>(1);
    for (std::size_t i = 0; i < n; ++i) {
        node0->add_shard(i, std::make_unique<Shard>());
        node1->add_shard(i, std::make_unique<Shard>());
    }

    std::vector<std::unique_ptr<NodeClient>> nodes;
    nodes.push_back(std::move(node0));
    nodes.push_back(std::move(node1));

    auto coord = std::make_unique<ShardCoordinator>(
        std::move(router), std::move(rp), std::move(nodes));
    coord->set_event_dispatcher(dispatcher);
    if (store) coord->set_event_store(store);
    return coord;
}

// ---------------------------------------------------------------------------
// Event serialization tests
// ---------------------------------------------------------------------------

TEST(DocumentEventTest, IndexedEventSerialization)
{
    DocumentIndexedEvent event{42, 3, 0, 0, ""};
    const std::string json = event_json::to_json(event);

    auto j = nlohmann::json::parse(json);
    EXPECT_EQ(j["event_id"], 42u);
    EXPECT_EQ(j["event_type"], "document_indexed");
    EXPECT_EQ(j["document_id"], 3u);
    EXPECT_EQ(j["shard_id"], 0u);
}

TEST(DocumentEventTest, UpdatedEventSerialization)
{
    DocumentUpdatedEvent event{7, 1, 2, 0, ""};
    const std::string json = event_json::to_json(event);

    auto j = nlohmann::json::parse(json);
    EXPECT_EQ(j["event_id"], 7u);
    EXPECT_EQ(j["event_type"], "document_updated");
    EXPECT_EQ(j["document_id"], 1u);
    EXPECT_EQ(j["shard_id"], 2u);
}

TEST(DocumentEventTest, RemovedEventSerialization)
{
    DocumentRemovedEvent event{99, 5, 1, 0};
    const std::string json = event_json::to_json(event);

    auto j = nlohmann::json::parse(json);
    EXPECT_EQ(j["event_id"], 99u);
    EXPECT_EQ(j["event_type"], "document_removed");
    EXPECT_EQ(j["document_id"], 5u);
    EXPECT_EQ(j["shard_id"], 1u);
}

TEST(DocumentEventTest, TopicNamesAreCorrect)
{
    EXPECT_EQ(topics::kDocumentMutations, "documents.mutations");
}

// ---------------------------------------------------------------------------
// Successful operations produce events (async dispatch)
// ---------------------------------------------------------------------------

TEST(DocumentEventTest, SuccessfulIngestProducesIndexedEvent)
{
    InMemoryMessageBroker broker;
    broker.subscribe(topics::kDocumentMutations, [](const Message& msg) {
        auto j = nlohmann::json::parse(msg.payload);
        EXPECT_EQ(j["event_type"], "document_indexed");
        EXPECT_EQ(j["document_id"], 1u);
        return true;
    });
    broker.start();

    EventDispatcher dispatcher(broker);
    dispatcher.start();

    auto coord = make_coord(3, &dispatcher);
    const auto resp = coord->ingest({1, "hello world"});
    ASSERT_FALSE(resp.is_error);

    dispatcher.stop();
    broker.stop();
    EXPECT_EQ(broker.stats().messages_acknowledged, 1u);
}

TEST(DocumentEventTest, SuccessfulUpdateProducesUpdatedEvent)
{
    InMemoryMessageBroker broker;
    broker.subscribe(topics::kDocumentMutations, [](const Message& msg) {
        auto j = nlohmann::json::parse(msg.payload);
        if (j["event_type"] == "document_updated") {
            EXPECT_EQ(j["document_id"], 5u);
        }
        return true;
    });
    broker.start();

    EventDispatcher dispatcher(broker);
    dispatcher.start();

    auto coord = make_coord(3, &dispatcher);
    coord->ingest({5, "original"});
    const auto resp = coord->update({5, "updated content"});
    ASSERT_FALSE(resp.is_error);

    dispatcher.stop();
    broker.stop();
    EXPECT_EQ(broker.stats().messages_acknowledged, 2u);
}

TEST(DocumentEventTest, SuccessfulRemoveProducesRemovedEvent)
{
    InMemoryMessageBroker broker;
    broker.subscribe(topics::kDocumentMutations, [](const Message& msg) {
        auto j = nlohmann::json::parse(msg.payload);
        if (j["event_type"] == "document_removed") {
            EXPECT_EQ(j["document_id"], 10u);
        }
        return true;
    });
    broker.start();

    EventDispatcher dispatcher(broker);
    dispatcher.start();

    auto coord = make_coord(3, &dispatcher);
    coord->ingest({10, "to be removed"});
    const auto resp = coord->remove(10);
    ASSERT_FALSE(resp.is_error);

    dispatcher.stop();
    broker.stop();
    EXPECT_EQ(broker.stats().messages_acknowledged, 2u);
}

// ---------------------------------------------------------------------------
// Failed operations produce no events
// ---------------------------------------------------------------------------

TEST(DocumentEventTest, FailedIngestProducesNoEvent)
{
    InMemoryMessageBroker broker;
    std::atomic<int> event_count{0};
    broker.subscribe(topics::kDocumentMutations, [&](const Message& /*msg*/) {
        ++event_count;
        return true;
    });
    broker.start();

    EventDispatcher dispatcher(broker);
    dispatcher.start();

    auto coord = make_coord(3, &dispatcher);
    const auto resp = coord->ingest({1, ""});  // Empty content = failure.
    ASSERT_TRUE(resp.is_error);

    dispatcher.stop();
    broker.stop();
    EXPECT_EQ(event_count.load(), 0);
    EXPECT_EQ(broker.stats().messages_acknowledged, 0u);
}

TEST(DocumentEventTest, FailedUpdateProducesNoEvent)
{
    InMemoryMessageBroker broker;
    std::atomic<int> event_count{0};
    broker.subscribe(topics::kDocumentMutations, [&](const Message& /*msg*/) {
        ++event_count;
        return true;
    });
    broker.start();

    EventDispatcher dispatcher(broker);
    dispatcher.start();

    auto coord = make_coord(3, &dispatcher);
    const auto resp = coord->update({1, ""});  // Empty content = failure.
    ASSERT_TRUE(resp.is_error);

    dispatcher.stop();
    broker.stop();
    EXPECT_EQ(event_count.load(), 0);
    EXPECT_EQ(broker.stats().messages_acknowledged, 0u);
}

// ---------------------------------------------------------------------------
// No dispatcher = no events (backward compatibility)
// ---------------------------------------------------------------------------

TEST(DocumentEventTest, NoDispatcherMeansNoEvents)
{
    auto coord = make_coord(3, nullptr);  // No dispatcher.
    const auto resp = coord->ingest({1, "hello"});
    ASSERT_FALSE(resp.is_error);
    // Just verify it doesn't crash — no dispatcher means no event publishing.
}

TEST(DocumentEventTest, NoDispatcherUpdateDoesNotCrash)
{
    auto coord = make_coord(3, nullptr);
    coord->ingest({1, "hello"});
    const auto resp = coord->update({1, "world"});
    ASSERT_FALSE(resp.is_error);
}

TEST(DocumentEventTest, NoDispatcherRemoveDoesNotCrash)
{
    auto coord = make_coord(3, nullptr);
    coord->ingest({1, "hello"});
    const auto resp = coord->remove(1);
    ASSERT_FALSE(resp.is_error);
}

// ---------------------------------------------------------------------------
// Replication: one event per logical operation (not per replica)
// ---------------------------------------------------------------------------

TEST(DocumentEventTest, R2IngestProducesExactlyOneIndexedEvent)
{
    InMemoryMessageBroker broker;
    std::atomic<int> event_count{0};
    broker.subscribe(topics::kDocumentMutations, [&](const Message& msg) {
        auto j = nlohmann::json::parse(msg.payload);
        if (j["event_type"] == "document_indexed") {
            ++event_count;
            EXPECT_EQ(j["document_id"], 1u);
        }
        return true;
    });
    broker.start();

    EventDispatcher dispatcher(broker);
    dispatcher.start();

    auto coord = make_coord_r2(2, &dispatcher);
    const auto resp = coord->ingest({1, "replicated doc"});
    ASSERT_FALSE(resp.is_error);

    dispatcher.stop();
    broker.stop();
    EXPECT_EQ(event_count.load(), 1);  // One event, not two.
    EXPECT_EQ(broker.stats().messages_acknowledged, 1u);
}

TEST(DocumentEventTest, R2UpdateProducesExactlyOneUpdatedEvent)
{
    InMemoryMessageBroker broker;
    std::atomic<int> event_count{0};
    broker.subscribe(topics::kDocumentMutations, [&](const Message& msg) {
        auto j = nlohmann::json::parse(msg.payload);
        if (j["event_type"] == "document_updated") {
            ++event_count;
        }
        return true;
    });
    broker.start();

    EventDispatcher dispatcher(broker);
    dispatcher.start();

    auto coord = make_coord_r2(2, &dispatcher);
    coord->ingest({1, "original"});
    const auto resp = coord->update({1, "updated"});
    ASSERT_FALSE(resp.is_error);

    dispatcher.stop();
    broker.stop();
    EXPECT_EQ(event_count.load(), 1);  // One event, not two.
}

TEST(DocumentEventTest, R2RemoveProducesExactlyOneRemovedEvent)
{
    InMemoryMessageBroker broker;
    std::atomic<int> event_count{0};
    broker.subscribe(topics::kDocumentMutations, [&](const Message& msg) {
        auto j = nlohmann::json::parse(msg.payload);
        if (j["event_type"] == "document_removed") {
            ++event_count;
        }
        return true;
    });
    broker.start();

    EventDispatcher dispatcher(broker);
    dispatcher.start();

    auto coord = make_coord_r2(2, &dispatcher);
    coord->ingest({1, "to remove"});
    const auto resp = coord->remove(1);
    ASSERT_FALSE(resp.is_error);

    dispatcher.stop();
    broker.stop();
    EXPECT_EQ(event_count.load(), 1);  // One event, not two.
}

// ---------------------------------------------------------------------------
// Multiple operations produce multiple events
// ---------------------------------------------------------------------------

TEST(DocumentEventTest, MultipleIngestsProduceMultipleEvents)
{
    InMemoryMessageBroker broker;
    std::atomic<int> event_count{0};
    broker.subscribe(topics::kDocumentMutations, [&](const Message& /*msg*/) {
        ++event_count;
        return true;
    });
    broker.start();

    EventDispatcher dispatcher(broker);
    dispatcher.start();

    auto coord = make_coord(3, &dispatcher);
    for (doc_id i = 0; i < 5; ++i) {
        coord->ingest({i, "doc " + std::to_string(i)});
    }

    dispatcher.stop();
    broker.stop();
    EXPECT_EQ(event_count.load(), 5);
    EXPECT_EQ(broker.stats().messages_acknowledged, 5u);
}

// ---------------------------------------------------------------------------
// Event payload contains correct shard_id
// ---------------------------------------------------------------------------

TEST(DocumentEventTest, EventContainsCorrectShardId)
{
    InMemoryMessageBroker broker;
    std::size_t captured_shard = 999;
    broker.subscribe(topics::kDocumentMutations, [&](const Message& msg) {
        auto j = nlohmann::json::parse(msg.payload);
        captured_shard = j["shard_id"].get<std::size_t>();
        return true;
    });
    broker.start();

    EventDispatcher dispatcher(broker);
    dispatcher.start();

    auto coord = make_coord(4, &dispatcher);  // 4 shards
    coord->ingest({1, "hello"});

    dispatcher.stop();
    broker.stop();
    // Shard assignment depends on routing — just verify it's a valid shard.
    EXPECT_LT(captured_shard, 4u);
}

// ---------------------------------------------------------------------------
// All three event types in sequence
// ---------------------------------------------------------------------------

TEST(DocumentEventTest, IngestUpdateRemoveAllProduceEvents)
{
    InMemoryMessageBroker broker;
    std::atomic<int> indexed{0};
    std::atomic<int> updated{0};
    std::atomic<int> removed{0};

    broker.subscribe(topics::kDocumentMutations, [&](const Message& msg) {
        auto j = nlohmann::json::parse(msg.payload);
        if (j["event_type"] == "document_indexed") ++indexed;
        else if (j["event_type"] == "document_updated") ++updated;
        else if (j["event_type"] == "document_removed") ++removed;
        return true;
    });
    broker.start();

    EventDispatcher dispatcher(broker);
    dispatcher.start();

    auto coord = make_coord(3, &dispatcher);
    coord->ingest({1, "hello"});
    coord->update({1, "world"});
    coord->remove(1);

    dispatcher.stop();
    broker.stop();
    EXPECT_EQ(indexed.load(), 1);
    EXPECT_EQ(updated.load(), 1);
    EXPECT_EQ(removed.load(), 1);
}

// ---------------------------------------------------------------------------
// Phase 18E: EventStore integration
// ---------------------------------------------------------------------------

TEST(DocumentEventTest, EventStoreTracksIngestEvent)
{
    auto store = create_in_memory_event_store();
    InMemoryMessageBroker broker;
    broker.subscribe(topics::kDocumentMutations, [](const Message&) {
        return true;
    });
    broker.start();

    EventDispatcher dispatcher(broker, *store);
    dispatcher.start();

    auto coord = make_coord(3, &dispatcher, store.get());
    const auto resp = coord->ingest({42, "hello world"});
    ASSERT_FALSE(resp.is_error);

    dispatcher.stop();
    broker.stop();

    auto stats = store->stats();
    EXPECT_EQ(stats.total, 1u);
    EXPECT_EQ(stats.published, 1u);
    EXPECT_EQ(stats.pending, 0u);
    EXPECT_EQ(stats.dispatching, 0u);
    EXPECT_EQ(stats.failed, 0u);
}

TEST(DocumentEventTest, EventStoreTracksEventId)
{
    auto store = create_in_memory_event_store();
    InMemoryMessageBroker broker;

    std::atomic<doc_id> received_doc{0};
    std::atomic<EventId> received_event_id{0};

    broker.subscribe(topics::kDocumentMutations, [&](const Message& msg) {
        auto j = nlohmann::json::parse(msg.payload);
        received_doc = j["document_id"].get<doc_id>();
        received_event_id = j["event_id"].get<EventId>();
        return true;
    });
    broker.start();

    EventDispatcher dispatcher(broker, *store);
    dispatcher.start();

    auto coord = make_coord(3, &dispatcher, store.get());
    coord->ingest({99, "original"});
    const auto resp = coord->update({99, "updated"});
    ASSERT_FALSE(resp.is_error);

    dispatcher.stop();
    broker.stop();

    EXPECT_GT(received_event_id.load(), 0u);
    EXPECT_EQ(received_doc.load(), 99u);

    const auto* stored = store->get(received_event_id.load());
    ASSERT_NE(stored, nullptr);
    EXPECT_EQ(stored->status, EventStatus::PUBLISHED);
    EXPECT_EQ(stored->topic, topics::kDocumentMutations);
    EXPECT_GE(stored->attempt_count, 1u);
}

TEST(DocumentEventTest, FailedOperationCreatesNoTrackedEvent)
{
    auto store = create_in_memory_event_store();
    InMemoryMessageBroker broker;
    broker.subscribe(topics::kDocumentMutations, [](const Message&) {
        return true;
    });
    broker.start();

    EventDispatcher dispatcher(broker, *store);
    dispatcher.start();

    auto coord = make_coord(3, &dispatcher, store.get());
    const auto resp = coord->ingest({1, ""});  // Fail: empty content
    ASSERT_TRUE(resp.is_error);

    dispatcher.stop();
    broker.stop();

    auto stats = store->stats();
    EXPECT_EQ(stats.total, 0u);
}

TEST(DocumentEventTest, R2WithEventStoreProducesOneTrackedEvent)
{
    auto store = create_in_memory_event_store();
    InMemoryMessageBroker broker;
    std::atomic<int> event_count{0};
    broker.subscribe(topics::kDocumentMutations, [&](const Message&) {
        ++event_count;
        return true;
    });
    broker.start();

    EventDispatcher dispatcher(broker, *store);
    dispatcher.start();

    auto coord = make_coord_r2(2, &dispatcher, store.get());
    const auto resp = coord->ingest({1, "replicated"});
    ASSERT_FALSE(resp.is_error);

    dispatcher.stop();
    broker.stop();

    EXPECT_EQ(event_count.load(), 1);

    auto stats = store->stats();
    EXPECT_EQ(stats.total, 1u);
    EXPECT_EQ(stats.published, 1u);
}

TEST(DocumentEventTest, NoStoreBackwardCompatible)
{
    InMemoryMessageBroker broker;
    std::atomic<int> event_count{0};
    broker.subscribe(topics::kDocumentMutations, [&](const Message&) {
        ++event_count;
        return true;
    });
    broker.start();

    EventDispatcher dispatcher(broker);
    dispatcher.start();

    auto coord = make_coord(3, &dispatcher, nullptr);
    const auto resp = coord->ingest({1, "no store"});
    ASSERT_FALSE(resp.is_error);

    dispatcher.stop();
    broker.stop();

    EXPECT_EQ(event_count.load(), 1);
}

} // namespace
} // namespace dse
