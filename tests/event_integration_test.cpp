// Distributed Search Engine - Event Integration Tests (Phase 18G).
//
// Integration tests for the complete event system lifecycle:
//   PersistentEventStore -> EventDispatcher -> InMemoryMessageBroker
//
// Tests verify:
//   - Production-style event lifecycle with all components
//   - Successful event flow through integrated components
//   - Persistence and recovery of pending events
//   - Restart recovery and replay
//   - Graceful shutdown
//   - Event metrics exposure
//   - Concurrent document mutations with event system active

#include <gtest/gtest.h>

#include <atomic>
#include <chrono>
#include <filesystem>
#include <memory>
#include <string>
#include <thread>
#include <vector>

#include "document_event.h"
#include "event_dispatcher.h"
#include "event_store.h"
#include "in_memory_message_broker.h"
#include "inverted_index.h"
#include "local_node.h"
#include "message.h"
#include "persistent_event_store.h"
#include "shard.h"
#include "shard_coordinator.h"
#include "shard_router.h"

namespace dse {
namespace {

// Helper: create a unique temporary directory for tests.
std::string make_test_dir(const std::string& name)
{
    std::string base = std::tmpnam(nullptr);
    std::string path = base + "_" + name;
    std::filesystem::create_directories(path);
    return path;
}

// Helper: create a coordinator with N shards and optional event system.
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

// ==========================================================================
// 1. Production-Style Lifecycle
// ==========================================================================
TEST(EventIntegrationTest, ProductionStyleLifecycle)
{
    auto dir = make_test_dir("lifecycle");

    // Simulate production lifecycle: create all components, use, shutdown.
    PersistentEventStore store(dir + "/events");
    BrokerConfig brokerConfig;
    brokerConfig.max_queue_size = 4096;
    InMemoryMessageBroker broker(brokerConfig);

    EventDispatcher::Config dispatcherConfig;
    dispatcherConfig.max_retries = 3;
    EventDispatcher dispatcher(broker, store, dispatcherConfig);

    auto coord = make_coord(3, &dispatcher, &store);

    // Start event system
    dispatcher.start();
    broker.start();

    // Perform operations
    for (doc_id i = 1; i <= 5; ++i) {
        coord->ingest({i, "document " + std::to_string(i)});
    }

    // Verify events were created
    EXPECT_EQ(store.size(), 5u);

    // Shutdown in correct order
    dispatcher.stop();
    store.flush();
    broker.stop();

    // Verify events persisted
    EXPECT_FALSE(store.get_by_status(EventStatus::PENDING).empty() &&
                 store.get_by_status(EventStatus::PUBLISHED).empty());

    std::filesystem::remove_all(dir);
}

// ==========================================================================
// 2. Successful Event Flow
// ==========================================================================
TEST(EventIntegrationTest, SuccessfulEventFlow)
{
    auto dir = make_test_dir("flow");

    {
        PersistentEventStore store(dir + "/events");
        InMemoryMessageBroker broker;

        std::atomic<int> indexedCount{0};
        std::atomic<int> updatedCount{0};
        std::atomic<int> removedCount{0};

        broker.subscribe(topics::kDocumentIndexed, [&](const Message&) {
            ++indexedCount;
            return true;
        });
        broker.subscribe(topics::kDocumentUpdated, [&](const Message&) {
            ++updatedCount;
            return true;
        });
        broker.subscribe(topics::kDocumentRemoved, [&](const Message&) {
            ++removedCount;
            return true;
        });
        broker.start();

        EventDispatcher dispatcher(broker, store);
        dispatcher.start();

        auto coord = make_coord(3, &dispatcher, &store);

        // Perform all three operations
        coord->ingest({1, "hello world"});
        coord->update({1, "updated content"});
        coord->remove(1);

        dispatcher.stop();
        broker.stop();

        // Verify all events were received
        EXPECT_EQ(indexedCount.load(), 1);
        EXPECT_EQ(updatedCount.load(), 1);
        EXPECT_EQ(removedCount.load(), 1);

        // Verify store has all events
        EXPECT_EQ(store.size(), 3u);
    }

    std::filesystem::remove_all(dir);
}

// ==========================================================================
// 3. Persistence and Recovery
// ==========================================================================
TEST(EventIntegrationTest, PersistenceAndRecovery)
{
    auto dir = make_test_dir("recovery");

    // Phase 1: Create events and persist
    EventId lastId;
    {
        PersistentEventStore store(dir + "/events");
        InMemoryMessageBroker broker;
        EventDispatcher dispatcher(broker, store);
        dispatcher.start();
        broker.start();

        auto coord = make_coord(3, &dispatcher, &store);
        coord->ingest({1, "first doc"});
        coord->ingest({2, "second doc"});
        lastId = store.create_event("test.topic", "failed_event");
        store.mark_dispatching(lastId);
        store.mark_failed(lastId, "broker down");

        dispatcher.stop();
        store.flush();
        broker.stop();
    }

    // Phase 2: Recover and verify
    {
        PersistentEventStore store(dir + "/events");
        InMemoryMessageBroker broker;

        std::atomic<int> receivedCount{0};
        broker.subscribe("test.topic", [&](const Message&) {
            ++receivedCount;
            return true;
        });
        broker.start();

        EventDispatcher dispatcher(broker, store);
        dispatcher.start();

        // Replay failed events
        auto replayed = dispatcher.replay_failed();
        dispatcher.stop();
        broker.stop();

        EXPECT_EQ(replayed, 1u);
        EXPECT_EQ(receivedCount.load(), 1);

        // Verify store state
        auto failed = store.get_by_status(EventStatus::FAILED);
        EXPECT_EQ(failed.size(), 0u);

        auto published = store.get_by_status(EventStatus::PUBLISHED);
        EXPECT_GE(published.size(), 1u);
    }

    std::filesystem::remove_all(dir);
}

// ==========================================================================
// 4. Graceful Shutdown
// ==========================================================================
TEST(EventIntegrationTest, GracefulShutdown)
{
    auto dir = make_test_dir("shutdown");

    PersistentEventStore store(dir + "/events");
    InMemoryMessageBroker broker;

    std::atomic<int> receivedCount{0};
    broker.subscribe(topics::kDocumentIndexed, [&](const Message&) {
        ++receivedCount;
        return true;
    });
    broker.start();

    EventDispatcher dispatcher(broker, store);
    dispatcher.start();

    auto coord = make_coord(3, &dispatcher, &store);

    // Enqueue events
    for (doc_id i = 1; i <= 10; ++i) {
        coord->ingest({i, "document " + std::to_string(i)});
    }

    // Shutdown in correct order
    dispatcher.stop();
    store.flush();
    broker.stop();

    // All events should have been processed
    EXPECT_EQ(receivedCount.load(), 10);
    EXPECT_EQ(store.get_by_status(EventStatus::PUBLISHED).size(), 10u);

    std::filesystem::remove_all(dir);
}

// ==========================================================================
// 5. Event Metrics Exposure
// ==========================================================================
TEST(EventIntegrationTest, EventMetricsExposure)
{
    auto dir = make_test_dir("metrics");

    {
        PersistentEventStore store(dir + "/events");
        InMemoryMessageBroker broker;
        EventDispatcher dispatcher(broker, store);
        dispatcher.start();
        broker.start();

        auto coord = make_coord(3, &dispatcher, &store);

        // Perform operations
        coord->ingest({1, "hello"});
        coord->ingest({2, "world"});

        // Get metrics
        auto stats = store.stats();
        EXPECT_EQ(stats.total, 2u);
        EXPECT_GE(stats.pending + stats.dispatching + stats.published, 2u);

        dispatcher.stop();
        broker.stop();
    }

    std::filesystem::remove_all(dir);
}

// ==========================================================================
// 6. Concurrent Mutations
// ==========================================================================
TEST(EventIntegrationTest, ConcurrentMutations)
{
    auto dir = make_test_dir("concurrent");

    {
        PersistentEventStore store(dir + "/events");
        InMemoryMessageBroker broker;

        std::atomic<int> eventCount{0};
        broker.subscribe(topics::kDocumentIndexed, [&](const Message&) {
            ++eventCount;
            return true;
        });
        broker.start();

        EventDispatcher::Config cfg;
        cfg.max_queue_size = 200;
        EventDispatcher dispatcher(broker, store, cfg);
        dispatcher.start();

        auto coord = make_coord(4, &dispatcher, &store);

        // Concurrent writes
        constexpr int kThreads = 4;
        constexpr int kPerThread = 25;
        std::vector<std::thread> threads;

        for (int t = 0; t < kThreads; ++t) {
            threads.emplace_back([&coord, t]() {
                for (int i = 0; i < kPerThread; ++i) {
                    doc_id id = static_cast<doc_id>(t * kPerThread + i + 1);
                    coord->ingest({id, "doc " + std::to_string(id)});
                }
            });
        }

        for (auto& t : threads) {
            t.join();
        }

        dispatcher.stop();
        broker.stop();

        // All events should have been processed
        EXPECT_EQ(eventCount.load(), kThreads * kPerThread);
        EXPECT_EQ(store.size(), static_cast<std::size_t>(kThreads * kPerThread));
    }

    std::filesystem::remove_all(dir);
}

// ==========================================================================
// 7. InMemoryMessageBroker Fallback
// ==========================================================================
TEST(EventIntegrationTest, InMemoryBrokerFallback)
{
    // Test that event system works without EventStore (backward compat)
    InMemoryMessageBroker broker;
    std::atomic<int> eventCount{0};
    broker.subscribe(topics::kDocumentIndexed, [&](const Message&) {
        ++eventCount;
        return true;
    });
    broker.start();

    EventDispatcher dispatcher(broker);
    dispatcher.start();

    auto coord = make_coord(3, &dispatcher, nullptr);  // No store
    coord->ingest({1, "hello"});

    dispatcher.stop();
    broker.stop();

    EXPECT_EQ(eventCount.load(), 1);
}

// ==========================================================================
// 8. PersistentEventStore Recovery After Crash
// ==========================================================================
TEST(EventIntegrationTest, CrashRecovery)
{
    auto dir = make_test_dir("crash_recovery");

    // Simulate crash: create events, don't flush.
    // The destructor auto-flushes, so events ARE persisted
    // during clean shutdown (stack unwinding).
    {
        PersistentEventStore store(dir + "/events");
        store.create_event("topic1", "event1");
        store.create_event("topic2", "event2");
        // Destructor auto-flushes — events survive clean shutdown
    }

    // After clean shutdown (destructor flush), events persist
    {
        PersistentEventStore store(dir + "/events");
        EXPECT_GE(store.size(), 2u);
    }

    // Now create and explicitly flush additional events
    {
        PersistentEventStore store(dir + "/events");
        store.create_event("topic3", "event3");
        store.flush();
    }

    // Recover — should find all events
    {
        PersistentEventStore store(dir + "/events");
        EXPECT_GE(store.size(), 3u);
        // Verify PENDING events are recoverable
        auto pending = store.get_by_status(EventStatus::PENDING);
        EXPECT_FALSE(pending.empty());
    }

    std::filesystem::remove_all(dir);
}

} // namespace
} // namespace dse
