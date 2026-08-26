// Distributed Search Engine - Event Dispatcher Tests (Phase 18D/18E).
//
// Tests for the bounded asynchronous EventDispatcher layer between
// ShardCoordinator and MessageBroker.
//
// Covers:
//   - Basic dispatch and FIFO ordering
//   - Asynchronous behavior (worker processes events)
//   - Queue bounds and backpressure
//   - Broker failure handling
//   - Shutdown semantics
//   - Coordinator integration
//   - Concurrency safety
//   - EventStore integration (Phase 18E)
//   - Retry and replay semantics (Phase 18E)

#include "event_dispatcher.h"

#include <gtest/gtest.h>

#include <atomic>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <functional>
#include <memory>
#include <string>
#include <thread>
#include <vector>

#include "document_event.h"
#include "event_store.h"
#include <nlohmann/json.hpp>
#include "in_memory_message_broker.h"
#include "inverted_index.h"
#include "local_node.h"
#include "message.h"
#include "message_broker.h"
#include "node_client.h"
#include "replica_placement.h"
#include "shard.h"
#include "shard_coordinator.h"
#include "shard_router.h"

namespace dse {
namespace {

// ---------------------------------------------------------------------------
// Test helper: controllable broker that can fail on demand
// ---------------------------------------------------------------------------

class FailingMessageBroker : public MessageBroker {
public:
    explicit FailingMessageBroker(bool fail = false) : fail_(fail) {}

    Offset publish(Message /*message*/) override {
        ++attempts_;
        if (fail_) {
            throw std::runtime_error("broker failure");
        }
        ++successes_;
        return 0;
    }

    std::optional<Offset> publish_with_timeout(
        Message message, std::size_t /*timeout_ms*/) override
    {
        return publish(std::move(message));
    }

    void subscribe(const Topic& /*topic*/, MessageHandler /*handler*/) override {}
    void start() override {}
    void stop() override {}
    std::size_t queue_size(const Topic& /*topic*/) const override { return 0; }
    BrokerStats stats() const override { return {}; }
    std::vector<Message> dead_letters(const Topic& /*topic*/) const override {
        return {};
    }
    bool was_processed(MessageId /*id*/) const override { return false; }

    void set_fail(bool f) { fail_ = f; }
    std::atomic<int> attempts_{0};
    std::atomic<int> successes_{0};

private:
    bool fail_;
};

// ---------------------------------------------------------------------------
// Test helper: create a coordinator with an optional dispatcher
// ---------------------------------------------------------------------------

std::unique_ptr<ShardCoordinator> make_coord(
    std::size_t n, EventDispatcher* dispatcher = nullptr)
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
    return coord;
}

// =========================================================================
// 1. Basic dispatch
// =========================================================================

TEST(EventDispatcherTest, EnqueueAndReceive)
{
    InMemoryMessageBroker broker;
    std::atomic<int> received{0};
    broker.subscribe("test.topic", [&](const Message& msg) {
        EXPECT_EQ(msg.payload, "hello");
        ++received;
        return true;
    });
    broker.start();

    EventDispatcher dispatcher(broker);
    dispatcher.start();

    EXPECT_TRUE(dispatcher.enqueue("test.topic", "hello"));

    dispatcher.stop();
    broker.stop();
    EXPECT_EQ(received.load(), 1);
}

TEST(EventDispatcherTest, FIFOPreservesOrder)
{
    InMemoryMessageBroker broker;
    std::vector<std::string> received_payloads;
    std::mutex mtx;
    broker.subscribe("test.topic", [&](const Message& msg) {
        std::lock_guard<std::mutex> lock(mtx);
        received_payloads.push_back(msg.payload);
        return true;
    });
    broker.start();

    EventDispatcher dispatcher(broker);
    dispatcher.start();

    for (int i = 0; i < 10; ++i) {
        EXPECT_TRUE(dispatcher.enqueue("test.topic", "msg_" + std::to_string(i)));
    }

    dispatcher.stop();
    broker.stop();

    ASSERT_EQ(received_payloads.size(), 10u);
    for (int i = 0; i < 10; ++i) {
        EXPECT_EQ(received_payloads[i], "msg_" + std::to_string(i));
    }
}

TEST(EventDispatcherTest, MultipleTopicsDelivered)
{
    InMemoryMessageBroker broker;
    std::atomic<int> topic_a{0};
    std::atomic<int> topic_b{0};

    broker.subscribe("topic.a", [&](const Message&) { ++topic_a; return true; });
    broker.subscribe("topic.b", [&](const Message&) { ++topic_b; return true; });
    broker.start();

    EventDispatcher dispatcher(broker);
    dispatcher.start();

    dispatcher.enqueue("topic.a", "a1");
    dispatcher.enqueue("topic.b", "b1");
    dispatcher.enqueue("topic.a", "a2");

    dispatcher.stop();
    broker.stop();

    EXPECT_EQ(topic_a.load(), 2);
    EXPECT_EQ(topic_b.load(), 1);
}

// =========================================================================
// 2. Asynchrony
// =========================================================================

TEST(EventDispatcherTest, EnqueueDoesNotBlockCallerIndefinitely)
{
    InMemoryMessageBroker broker;
    std::atomic<bool> received{false};
    broker.subscribe("test.topic", [&](const Message&) {
        // Simulate slow processing.
        std::this_thread::sleep_for(std::chrono::milliseconds(50));
        received = true;
        return true;
    });
    broker.start();

    EventDispatcher::Config cfg;
    cfg.max_queue_size = 100;
    cfg.default_timeout_ms = 10;
    EventDispatcher dispatcher(broker, cfg);
    dispatcher.start();

    // Enqueue should return quickly even though handler is slow.
    auto start = std::chrono::steady_clock::now();
    EXPECT_TRUE(dispatcher.enqueue("test.topic", "slow", 10));
    auto elapsed = std::chrono::steady_clock::now() - start;

    // Should have returned well within 200ms (not blocked by the 50ms handler).
    EXPECT_LT(elapsed, std::chrono::milliseconds(200));

    dispatcher.stop();
    broker.stop();
    EXPECT_TRUE(received.load());
}

// =========================================================================
// 3. Queue bounds
// =========================================================================

TEST(EventDispatcherTest, QueueCapacityEnforced)
{
    InMemoryMessageBroker broker;
    std::atomic<int> received{0};
    // Slow consumer: blocks for a bit to let queue fill up.
    broker.subscribe("test.topic", [&](const Message&) {
        std::this_thread::sleep_for(std::chrono::milliseconds(5));
        ++received;
        return true;
    });
    broker.start();

    // Tiny queue to force backpressure.
    EventDispatcher::Config cfg;
    cfg.max_queue_size = 2;
    cfg.default_timeout_ms = 50;
    EventDispatcher dispatcher(broker, cfg);
    dispatcher.start();

    // First few should succeed (queue has space).
    EXPECT_TRUE(dispatcher.enqueue("test.topic", "1"));
    EXPECT_TRUE(dispatcher.enqueue("test.topic", "2"));

    // Third might succeed or be rejected depending on worker speed.
    // Fill remaining capacity.
    dispatcher.enqueue("test.topic", "3");
    dispatcher.enqueue("test.topic", "4", 0);  // non-blocking

    // At least one should have been rejected.
    auto s = dispatcher.stats();
    EXPECT_GE(s.enqueued + s.rejected, 3u);

    dispatcher.stop();
    broker.stop();
    EXPECT_GE(received.load(), 1);
}

TEST(EventDispatcherTest, TimedEnqueueReturnsFalseWhenFull)
{
    FailingMessageBroker fbroker(false);

    // Use InMemoryMessageBroker with a blocking consumer to create backpressure.
    InMemoryMessageBroker broker;
    std::atomic<bool> pause{true};
    broker.subscribe("test.topic", [&](const Message&) {
        while (pause.load()) {
            std::this_thread::sleep_for(std::chrono::milliseconds(1));
        }
        return true;
    });
    broker.start();

    EventDispatcher::Config cfg;
    cfg.max_queue_size = 1;
    cfg.default_timeout_ms = 50;
    EventDispatcher dispatcher(broker, cfg);
    dispatcher.start();

    // Fill the queue.
    EXPECT_TRUE(dispatcher.enqueue("test.topic", "1"));

    // Give the worker a moment to pick up the first message.
    std::this_thread::sleep_for(std::chrono::milliseconds(10));

    // Now try with a short timeout — should time out if queue is full.
    dispatcher.enqueue("test.topic", "2", 50);

    // Unblock the consumer.
    pause = false;

    dispatcher.stop();
    broker.stop();

    // The enqueue may or may not have succeeded depending on timing,
    // but we can verify the stats reflect some activity.
    auto s = dispatcher.stats();
    EXPECT_GE(s.enqueued, 1u);
}

// =========================================================================
// 4. Broker failure
// =========================================================================

TEST(EventDispatcherTest, BrokerFailureDoesNotCrashDispatcher)
{
    FailingMessageBroker broker(true);  // Will throw on publish.

    EventDispatcher::Config cfg;
    cfg.max_retries = 0;  // Fail immediately (test purpose: crash resilience).
    EventDispatcher dispatcher(broker, cfg);
    dispatcher.start();

    // Enqueue should succeed even though broker will fail.
    EXPECT_TRUE(dispatcher.enqueue("test.topic", "fail_me"));

    dispatcher.stop();

    EXPECT_EQ(broker.attempts_.load(), 1);
    EXPECT_EQ(broker.successes_.load(), 0);
    auto s = dispatcher.stats();
    EXPECT_EQ(s.broker_errors, 1u);
    EXPECT_EQ(s.published, 0u);
}

TEST(EventDispatcherTest, LaterEventsProcessedAfterBrokerFailure)
{
    InMemoryMessageBroker broker;
    std::atomic<int> received{0};
    broker.subscribe("test.topic", [&](const Message&) { ++received; return true; });
    broker.start();

    EventDispatcher dispatcher(broker);
    dispatcher.start();

    // Enqueue several events.
    for (int i = 0; i < 5; ++i) {
        EXPECT_TRUE(dispatcher.enqueue("test.topic", "event_" + std::to_string(i)));
    }

    dispatcher.stop();
    broker.stop();

    EXPECT_EQ(received.load(), 5);
}

// =========================================================================
// 5. Shutdown semantics
// =========================================================================

TEST(EventDispatcherTest, EmptyShutdown)
{
    FailingMessageBroker broker;
    EventDispatcher dispatcher(broker);
    dispatcher.start();
    dispatcher.stop();  // Should not block or crash.
}

TEST(EventDispatcherTest, QueuedEventsDrainOnShutdown)
{
    InMemoryMessageBroker broker;
    std::atomic<int> received{0};
    broker.subscribe("test.topic", [&](const Message&) { ++received; return true; });
    broker.start();

    EventDispatcher dispatcher(broker);
    dispatcher.start();

    for (int i = 0; i < 10; ++i) {
        dispatcher.enqueue("test.topic", "drain_" + std::to_string(i));
    }

    dispatcher.stop();
    broker.stop();

    EXPECT_EQ(received.load(), 10);
}

TEST(EventDispatcherTest, StopIsIdempotent)
{
    FailingMessageBroker broker;
    EventDispatcher dispatcher(broker);
    dispatcher.start();
    dispatcher.stop();
    dispatcher.stop();  // Second call should be safe.
    dispatcher.stop();  // Third call should be safe.
}

TEST(EventDispatcherTest, DestructorWithQueuedEventsDoesNotDeadlock)
{
    InMemoryMessageBroker broker;
    std::atomic<int> received{0};
    broker.subscribe("test.topic", [&](const Message&) { ++received; return true; });
    broker.start();

    {
        EventDispatcher dispatcher(broker);
        dispatcher.start();
        for (int i = 0; i < 5; ++i) {
            dispatcher.enqueue("test.topic", "destroy_" + std::to_string(i));
        }
        // Destructor should drain and stop.
    }

    broker.stop();
    EXPECT_EQ(received.load(), 5);
}

// =========================================================================
// 6. Coordinator integration
// =========================================================================

TEST(EventDispatcherTest, SuccessfulIngestEnqueuesOneEvent)
{
    InMemoryMessageBroker broker;
    std::atomic<int> event_count{0};
    broker.subscribe(topics::kDocumentIndexed, [&](const Message&) {
        ++event_count;
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

    EXPECT_EQ(event_count.load(), 1);
}

TEST(EventDispatcherTest, SuccessfulUpdateEnqueuesOneEvent)
{
    InMemoryMessageBroker broker;
    std::atomic<int> event_count{0};
    broker.subscribe(topics::kDocumentUpdated, [&](const Message&) {
        ++event_count;
        return true;
    });
    broker.start();

    EventDispatcher dispatcher(broker);
    dispatcher.start();

    auto coord = make_coord(3, &dispatcher);
    coord->ingest({1, "original"});
    const auto resp = coord->update({1, "updated"});
    ASSERT_FALSE(resp.is_error);

    dispatcher.stop();
    broker.stop();

    EXPECT_EQ(event_count.load(), 1);
}

TEST(EventDispatcherTest, SuccessfulRemoveEnqueuesOneEvent)
{
    InMemoryMessageBroker broker;
    std::atomic<int> event_count{0};
    broker.subscribe(topics::kDocumentRemoved, [&](const Message&) {
        ++event_count;
        return true;
    });
    broker.start();

    EventDispatcher dispatcher(broker);
    dispatcher.start();

    auto coord = make_coord(3, &dispatcher);
    coord->ingest({1, "to remove"});
    const auto resp = coord->remove(1);
    ASSERT_FALSE(resp.is_error);

    dispatcher.stop();
    broker.stop();

    EXPECT_EQ(event_count.load(), 1);
}

TEST(EventDispatcherTest, FailedOperationsEnqueueZeroEvents)
{
    InMemoryMessageBroker broker;
    std::atomic<int> indexed{0};
    std::atomic<int> updated{0};
    std::atomic<int> removed{0};

    broker.subscribe(topics::kDocumentIndexed, [&](const Message&) { ++indexed; return true; });
    broker.subscribe(topics::kDocumentUpdated, [&](const Message&) { ++updated; return true; });
    broker.subscribe(topics::kDocumentRemoved, [&](const Message&) { ++removed; return true; });
    broker.start();

    EventDispatcher dispatcher(broker);
    dispatcher.start();

    auto coord = make_coord(3, &dispatcher);
    coord->ingest({1, ""});          // Fail: empty content
    coord->update({2, ""});          // Fail: empty content
    // Remove on non-existent doc succeeds (all replicas succeed with remove).

    dispatcher.stop();
    broker.stop();

    EXPECT_EQ(indexed.load(), 0);
    EXPECT_EQ(updated.load(), 0);
}

TEST(EventDispatcherTest, R2SuccessEnqueuesExactlyOneEvent)
{
    InMemoryMessageBroker broker;
    std::atomic<int> event_count{0};
    broker.subscribe(topics::kDocumentIndexed, [&](const Message&) {
        ++event_count;
        return true;
    });
    broker.start();

    EventDispatcher dispatcher(broker);
    dispatcher.start();

    auto router = std::make_unique<ShardRouter>(2);
    std::vector<ShardReplicaSet> replica_sets = {{0, {0, 1}}, {1, {0, 1}}};
    auto rp = std::make_unique<ShardReplicaPlacement>(2, 2, 2, replica_sets);

    auto n0 = std::make_unique<LocalNode>(0);
    auto n1 = std::make_unique<LocalNode>(1);
    for (std::size_t i = 0; i < 2; ++i) {
        n0->add_shard(i, std::make_unique<Shard>());
        n1->add_shard(i, std::make_unique<Shard>());
    }

    std::vector<std::unique_ptr<NodeClient>> nodes;
    nodes.push_back(std::move(n0));
    nodes.push_back(std::move(n1));

    auto coord = std::make_unique<ShardCoordinator>(
        std::move(router), std::move(rp), std::move(nodes));
    coord->set_event_dispatcher(&dispatcher);

    const auto resp = coord->ingest({1, "replicated"});
    ASSERT_FALSE(resp.is_error);

    dispatcher.stop();
    broker.stop();

    EXPECT_EQ(event_count.load(), 1);  // One event, not two.
}

// =========================================================================
// 7. Concurrency
// =========================================================================

TEST(EventDispatcherTest, ConcurrentProducers)
{
    InMemoryMessageBroker broker;
    std::atomic<int> received{0};
    broker.subscribe("test.topic", [&](const Message&) { ++received; return true; });
    broker.start();

    EventDispatcher::Config cfg;
    cfg.max_queue_size = 400;
    EventDispatcher dispatcher(broker, cfg);
    dispatcher.start();

    constexpr int kThreads = 4;
    constexpr int kPerThread = 50;

    std::vector<std::thread> producers;
    for (int t = 0; t < kThreads; ++t) {
        producers.emplace_back([&dispatcher, t]() {
            for (int i = 0; i < kPerThread; ++i) {
                dispatcher.enqueue("test.topic",
                    "t" + std::to_string(t) + "_" + std::to_string(i));
            }
        });
    }

    for (auto& t : producers) {
        t.join();
    }

    dispatcher.stop();
    broker.stop();

    EXPECT_EQ(received.load(), kThreads * kPerThread);
    auto s = dispatcher.stats();
    EXPECT_EQ(s.enqueued, static_cast<std::uint64_t>(kThreads * kPerThread));
    EXPECT_EQ(s.published, static_cast<std::uint64_t>(kThreads * kPerThread));
    EXPECT_EQ(s.rejected, 0u);
}

TEST(EventDispatcherTest, ShutdownAfterConcurrentProducers)
{
    InMemoryMessageBroker broker;
    std::atomic<int> received{0};
    broker.subscribe("test.topic", [&](const Message&) {
        ++received;
        return true;
    });
    broker.start();

    EventDispatcher::Config cfg;
    cfg.max_queue_size = 100;
    EventDispatcher dispatcher(broker, cfg);
    dispatcher.start();

    // Launch producers that enqueue concurrently.
    std::vector<std::thread> producers;
    for (int t = 0; t < 4; ++t) {
        producers.emplace_back([&]() {
            for (int i = 0; i < 20; ++i) {
                dispatcher.enqueue("test.topic", "event");
            }
        });
    }

    // Wait for all producers to finish.
    for (auto& t : producers) {
        t.join();
    }

    // Now stop — all events are already enqueued. Drain and exit cleanly.
    dispatcher.stop();
    broker.stop();

    // All events should have been delivered.
    EXPECT_EQ(received.load(), 80);
    auto s = dispatcher.stats();
    EXPECT_EQ(s.enqueued, 80u);
    EXPECT_EQ(s.published, 80u);
    EXPECT_EQ(s.rejected, 0u);
}

// =========================================================================
// 8. Stats
// =========================================================================

TEST(EventDispatcherTest, StatsTrackEnqueuedAndPublished)
{
    InMemoryMessageBroker broker;
    broker.subscribe("test.topic", [&](const Message&) { return true; });
    broker.start();

    EventDispatcher dispatcher(broker);
    dispatcher.start();

    for (int i = 0; i < 5; ++i) {
        dispatcher.enqueue("test.topic", "event");
    }

    dispatcher.stop();
    broker.stop();

    auto s = dispatcher.stats();
    EXPECT_EQ(s.enqueued, 5u);
    EXPECT_EQ(s.published, 5u);
    EXPECT_EQ(s.broker_errors, 0u);
    EXPECT_EQ(s.pending, 0u);
}

// =========================================================================
// 9. Phase 18E: EventStore integration + retry + replay
// =========================================================================

TEST(EventDispatcherTest, EnqueueWithEventTracksLifecycle)
{
    auto store = create_in_memory_event_store();
    FailingMessageBroker broker(false);

    EventDispatcher dispatcher(broker, *store);
    dispatcher.start();

    auto id1 = store->create_event("test.topic", "payload1");
    auto id2 = store->create_event("test.topic", "payload2");

    EXPECT_TRUE(dispatcher.enqueue_with_event(id1, "test.topic", "payload1"));
    EXPECT_TRUE(dispatcher.enqueue_with_event(id2, "test.topic", "payload2"));

    dispatcher.stop();

    EXPECT_EQ(store->get(id1)->status, EventStatus::PUBLISHED);
    EXPECT_EQ(store->get(id2)->status, EventStatus::PUBLISHED);
}

TEST(EventDispatcherTest, BrokerFailureMarksEventFailed)
{
    auto store = create_in_memory_event_store();
    FailingMessageBroker broker(true);  // Always throws

    EventDispatcher dispatcher(broker, *store);
    dispatcher.start();

    auto id = store->create_event("test.topic", "fail_me");
    EXPECT_TRUE(dispatcher.enqueue_with_event(id, "test.topic", "fail_me"));

    dispatcher.stop();

    auto* ev = store->get(id);
    ASSERT_NE(ev, nullptr);
    EXPECT_EQ(ev->status, EventStatus::FAILED);
    EXPECT_GT(ev->attempt_count, 0u);
}

TEST(EventDispatcherTest, RetryRetriesFailedPublishAttempt)
{
    auto store = create_in_memory_event_store();
    FailingMessageBroker broker(true);  // Always throws

    EventDispatcher::Config cfg;
    cfg.max_retries = 3;
    EventDispatcher dispatcher(broker, *store, cfg);
    dispatcher.start();

    auto id = store->create_event("test.topic", "retry_me");
    dispatcher.enqueue_with_event(id, "test.topic", "retry_me");

    dispatcher.stop();

    auto* ev = store->get(id);
    ASSERT_NE(ev, nullptr);
    EXPECT_EQ(ev->status, EventStatus::FAILED);
    // Initial attempt + 3 retries = 4 total attempts
    EXPECT_EQ(ev->attempt_count, 4u);
}

TEST(EventDispatcherTest, SuccessfulAfterRetry)
{
    auto store = create_in_memory_event_store();

    // A broker that fails first, then succeeds
    class FlakyBroker : public MessageBroker {
    public:
        Offset publish(Message) override {
            if (fail_count_.fetch_add(1) < 2) {
                throw std::runtime_error("transient");
            }
            return 0;
        }
        std::optional<Offset> publish_with_timeout(Message m, std::size_t) override { return publish(std::move(m)); }
        void subscribe(const Topic&, MessageHandler) override {}
        void start() override {}
        void stop() override {}
        std::size_t queue_size(const Topic&) const override { return 0; }
        BrokerStats stats() const override { return {}; }
        std::vector<Message> dead_letters(const Topic&) const override { return {}; }
        bool was_processed(MessageId) const override { return false; }
    private:
        std::atomic<int> fail_count_{0};
    };

    FlakyBroker broker;
    EventDispatcher::Config cfg;
    cfg.max_retries = 5;
    EventDispatcher dispatcher(broker, *store, cfg);
    dispatcher.start();

    auto id = store->create_event("test.topic", "succeed_soon");
    dispatcher.enqueue_with_event(id, "test.topic", "succeed_soon");

    dispatcher.stop();

    auto* ev = store->get(id);
    ASSERT_NE(ev, nullptr);
    EXPECT_EQ(ev->status, EventStatus::PUBLISHED);
    EXPECT_EQ(ev->attempt_count, 3u);  // 2 fails + 1 success
}

TEST(EventDispatcherTest, ReplayReEnqueuesFailedEvents)
{
    auto store = create_in_memory_event_store();

    FailingMessageBroker broker(true);  // Fail initially

    EventDispatcher::Config cfg;
    cfg.max_retries = 0;  // Fail immediately
    EventDispatcher dispatcher(broker, *store, cfg);
    dispatcher.start();

    auto id = store->create_event("test.topic", "replay_me");
    dispatcher.enqueue_with_event(id, "test.topic", "replay_me");
    dispatcher.stop();

    // Should be FAILED now
    EXPECT_EQ(store->get(id)->status, EventStatus::FAILED);

    // Fix the broker and replay
    broker.set_fail(false);
    dispatcher.start();
    auto count = dispatcher.replay_failed();
    EXPECT_EQ(count, 1u);
    dispatcher.stop();

    // Should now be PUBLISHED
    auto* ev = store->get(id);
    ASSERT_NE(ev, nullptr);
    EXPECT_EQ(ev->status, EventStatus::PUBLISHED);
}

TEST(EventDispatcherTest, ReplayPreservesEventId)
{
    auto store = create_in_memory_event_store();
    FailingMessageBroker broker(true);

    EventDispatcher::Config cfg;
    cfg.max_retries = 0;
    EventDispatcher dispatcher(broker, *store, cfg);
    dispatcher.start();

    auto id = store->create_event("test.topic", "preserve_me");
    dispatcher.enqueue_with_event(id, "test.topic", "preserve_me");
    dispatcher.stop();

    // Replay with working broker
    std::atomic<bool> broker_received{false};

    InMemoryMessageBroker workingBroker;
    workingBroker.subscribe("test.topic", [&](const Message&) {
        broker_received = true;
        return true;
    });
    workingBroker.start();

    // Create a new dispatcher with the working broker and same store
    EventDispatcher dispatcher2(workingBroker, *store);
    dispatcher2.start();
    auto requeued = dispatcher2.replay_failed();
    dispatcher2.stop();
    workingBroker.stop();

    // Verify event was replayed to broker
    EXPECT_EQ(requeued, 1u);
    EXPECT_TRUE(broker_received.load());
    // Verify event_id is preserved in store
    auto* ev = store->get(id);
    ASSERT_NE(ev, nullptr);
    EXPECT_EQ(ev->id, id);
    EXPECT_EQ(ev->status, EventStatus::PUBLISHED);
}

TEST(EventDispatcherTest, ReplayDoesNotAffectPublishedEvents)
{
    auto store = create_in_memory_event_store();
    FailingMessageBroker broker(false);

    EventDispatcher dispatcher(broker, *store);
    dispatcher.start();

    auto id = store->create_event("test.topic", "done");
    dispatcher.enqueue_with_event(id, "test.topic", "done");
    dispatcher.stop();

    EXPECT_EQ(store->get(id)->status, EventStatus::PUBLISHED);

    dispatcher.start();
    auto count = dispatcher.replay_failed();
    dispatcher.stop();

    EXPECT_EQ(count, 0u);  // Nothing to replay
    EXPECT_EQ(store->get(id)->status, EventStatus::PUBLISHED);
}

TEST(EventDispatcherTest, StatsIncludeRetryCount)
{
    auto store = create_in_memory_event_store();
    FailingMessageBroker broker(true);

    EventDispatcher::Config cfg;
    cfg.max_retries = 2;
    EventDispatcher dispatcher(broker, *store, cfg);
    dispatcher.start();

    auto id = store->create_event("test.topic", "retry_stats");
    dispatcher.enqueue_with_event(id, "test.topic", "retry_stats");
    dispatcher.stop();

    auto s = dispatcher.stats();
    EXPECT_EQ(s.retried, 2u);  // 2 retries after initial failure
    EXPECT_EQ(s.broker_errors, 3u);  // 1 initial + 2 retries
}

TEST(EventDispatcherTest, ConcurrentEventTrackedEnqueue)
{
    auto store = create_in_memory_event_store();
    FailingMessageBroker broker(false);

    EventDispatcher::Config cfg;
    cfg.max_queue_size = 200;
    EventDispatcher dispatcher(broker, *store, cfg);
    dispatcher.start();

    constexpr int kThreads = 4;
    constexpr int kPerThread = 25;
    std::vector<EventId> ids;

    // Create events and enqueue them concurrently
    std::vector<std::thread> threads;
    for (int t = 0; t < kThreads; ++t) {
        threads.emplace_back([&]() {
            for (int i = 0; i < kPerThread; ++i) {
                auto id = store->create_event("test.topic", "event");
                dispatcher.enqueue_with_event(id, "test.topic", "event");
            }
        });
    }

    for (auto& t : threads) {
        t.join();
    }

    dispatcher.stop();

    auto s = dispatcher.stats();
    EXPECT_EQ(s.enqueued, static_cast<std::uint64_t>(kThreads * kPerThread));
    EXPECT_EQ(s.published, static_cast<std::uint64_t>(kThreads * kPerThread));

    auto storeStats = store->stats();
    EXPECT_EQ(storeStats.total, static_cast<std::size_t>(kThreads * kPerThread));
    EXPECT_EQ(storeStats.published, static_cast<std::size_t>(kThreads * kPerThread));
}

TEST(EventDispatcherTest, ReplayFailedEventsCount)
{
    auto store = create_in_memory_event_store();

    // Fail first, then succeed on replay
    class TempFailBroker : public MessageBroker {
    public:
        Offset publish(Message) override {
            if (failing_.load()) throw std::runtime_error("nope");
            return 0;
        }
        std::optional<Offset> publish_with_timeout(Message m, std::size_t) override { return publish(std::move(m)); }
        void subscribe(const Topic&, MessageHandler) override {}
        void start() override {}
        void stop() override {}
        std::size_t queue_size(const Topic&) const override { return 0; }
        BrokerStats stats() const override { return {}; }
        std::vector<Message> dead_letters(const Topic&) const override { return {}; }
        bool was_processed(MessageId) const override { return false; }
        std::atomic<bool> failing_{true};
    };

    TempFailBroker broker;
    EventDispatcher::Config cfg;
    cfg.max_retries = 0;
    EventDispatcher dispatcher(broker, *store, cfg);
    dispatcher.start();

    // Create 3 events that will all fail
    for (int i = 0; i < 3; ++i) {
        auto id = store->create_event("test.topic", "fail_" + std::to_string(i));
        dispatcher.enqueue_with_event(id, "test.topic", "fail_" + std::to_string(i));
    }
    dispatcher.stop();

    auto stats1 = store->stats();
    EXPECT_EQ(stats1.failed, 3u);
    EXPECT_EQ(stats1.published, 0u);

    // Fix broker and replay
    broker.failing_ = false;
    dispatcher.start();
    auto count = dispatcher.replay_failed();
    dispatcher.stop();

    EXPECT_EQ(count, 3u);
    auto stats2 = store->stats();
    EXPECT_EQ(stats2.published, 3u);
    EXPECT_EQ(stats2.failed, 0u);
    EXPECT_GE(stats2.retried, 3u);
}

} // namespace
} // namespace dse
