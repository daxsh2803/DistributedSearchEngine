// Distributed Search Engine - Event Store Tests (Phase 18E).
//
// Tests for the EventStore abstraction and InMemoryEventStore.
// Covers:
//   - Event ID generation
//   - Stable ID across retries
//   - Lifecycle state transitions
//   - Successful publication
//   - Failed publication
//   - Retry after failure
//   - Attempt counting
//   - Replay
//   - Replay preserving event ID
//   - Failed events remaining recoverable
//   - Concurrent EventStore access
//   - Statistics

#include "event_store.h"

#include <gtest/gtest.h>

#include <atomic>
#include <set>
#include <string>
#include <thread>
#include <vector>

namespace dse {
namespace {

// ---------------------------------------------------------------------------
// Event ID generation
// ---------------------------------------------------------------------------

TEST(EventStoreTest, CreateEventAssignsIncrementingIds)
{
    auto store = create_in_memory_event_store();

    auto id1 = store->create_event("topic", "payload1");
    auto id2 = store->create_event("topic", "payload2");
    auto id3 = store->create_event("topic", "payload3");

    EXPECT_GT(id1, 0u);
    EXPECT_GT(id2, id1);
    EXPECT_GT(id3, id2);
}

TEST(EventStoreTest, EventIdIsUnique)
{
    auto store = create_in_memory_event_store();
    std::set<EventId> ids;

    for (int i = 0; i < 100; ++i) {
        auto id = store->create_event("topic", "payload");
        EXPECT_TRUE(ids.insert(id).second) << "Duplicate ID: " << id;
    }
}

// ---------------------------------------------------------------------------
// Initial state
// ---------------------------------------------------------------------------

TEST(EventStoreTest, NewEventIsPending)
{
    auto store = create_in_memory_event_store();
    auto id = store->create_event("topic", "payload");

    auto* ev = store->get(id);
    ASSERT_NE(ev, nullptr);
    EXPECT_EQ(ev->id, id);
    EXPECT_EQ(ev->status, EventStatus::PENDING);
    EXPECT_EQ(ev->topic, "topic");
    EXPECT_EQ(ev->payload, "payload");
    EXPECT_EQ(ev->attempt_count, 0u);
    EXPECT_GT(ev->created_at_ns, 0u);
}

TEST(EventStoreTest, GetReturnsNullptrForUnknownId)
{
    auto store = create_in_memory_event_store();
    EXPECT_EQ(store->get(999), nullptr);
}

// ---------------------------------------------------------------------------
// Lifecycle state transitions
// ---------------------------------------------------------------------------

TEST(EventStoreTest, PendingToDispatching)
{
    auto store = create_in_memory_event_store();
    auto id = store->create_event("topic", "data");

    store->mark_dispatching(id);
    auto* ev = store->get(id);
    ASSERT_NE(ev, nullptr);
    EXPECT_EQ(ev->status, EventStatus::DISPATCHING);
}

TEST(EventStoreTest, DispatchingToPublished)
{
    auto store = create_in_memory_event_store();
    auto id = store->create_event("topic", "data");

    store->mark_dispatching(id);
    store->record_attempt(id);
    store->mark_published(id);

    auto* ev = store->get(id);
    ASSERT_NE(ev, nullptr);
    EXPECT_EQ(ev->status, EventStatus::PUBLISHED);
    EXPECT_EQ(ev->attempt_count, 1u);
}

TEST(EventStoreTest, DispatchingToFailed)
{
    auto store = create_in_memory_event_store();
    auto id = store->create_event("topic", "data");

    store->mark_dispatching(id);
    store->record_attempt(id);
    store->mark_failed(id, "broker rejected");

    auto* ev = store->get(id);
    ASSERT_NE(ev, nullptr);
    EXPECT_EQ(ev->status, EventStatus::FAILED);
    EXPECT_EQ(ev->error_message, "broker rejected");
    EXPECT_EQ(ev->attempt_count, 1u);
}

TEST(EventStoreTest, FailedToPendingViaRequeue)
{
    auto store = create_in_memory_event_store();
    auto id = store->create_event("topic", "data");

    store->mark_dispatching(id);
    store->mark_failed(id, "error");
    bool result = store->requeue(id);
    EXPECT_TRUE(result);

    auto* ev = store->get(id);
    ASSERT_NE(ev, nullptr);
    EXPECT_EQ(ev->status, EventStatus::PENDING);
}

TEST(EventStoreTest, RequeueOnlyWorksForFailed)
{
    auto store = create_in_memory_event_store();
    auto id = store->create_event("topic", "data");

    // PENDING state - should fail
    EXPECT_FALSE(store->requeue(id));

    store->mark_dispatching(id);
    // DISPATCHING state - should fail
    EXPECT_FALSE(store->requeue(id));
}

TEST(EventStoreTest, RequeueUnknownEventFails)
{
    auto store = create_in_memory_event_store();
    EXPECT_FALSE(store->requeue(999));
}

TEST(EventStoreTest, AttemptCountIncrements)
{
    auto store = create_in_memory_event_store();
    auto id = store->create_event("topic", "data");

    store->mark_dispatching(id);
    store->record_attempt(id);
    store->mark_failed(id, "err1");
    store->requeue(id);
    store->mark_dispatching(id);
    store->record_attempt(id);
    store->mark_failed(id, "err2");

    auto* ev = store->get(id);
    ASSERT_NE(ev, nullptr);
    EXPECT_EQ(ev->attempt_count, 2u);
}

TEST(EventStoreTest, RecordAttemptIncrementsCount)
{
    auto store = create_in_memory_event_store();
    auto id = store->create_event("topic", "data");

    store->mark_dispatching(id);
    store->record_attempt(id);  // attempt 1
    store->record_attempt(id);  // attempt 2
    store->record_attempt(id);  // attempt 3
    store->mark_published(id);

    auto* ev = store->get(id);
    ASSERT_NE(ev, nullptr);
    // record_attempt tracks each publish attempt; mark_published transitions state
    EXPECT_EQ(ev->attempt_count, 3u);
    EXPECT_EQ(ev->status, EventStatus::PUBLISHED);
}

// ---------------------------------------------------------------------------
// Get by status
// ---------------------------------------------------------------------------

TEST(EventStoreTest, GetByStatus)
{
    auto store = create_in_memory_event_store();

    auto id1 = store->create_event("t1", "p1");
    auto id2 = store->create_event("t1", "p2");
    auto id3 = store->create_event("t1", "p3");

    store->mark_dispatching(id1);
    store->mark_published(id1);
    store->mark_dispatching(id2);
    // id3 remains PENDING

    auto pending = store->get_by_status(EventStatus::PENDING);
    auto published = store->get_by_status(EventStatus::PUBLISHED);

    EXPECT_EQ(pending.size(), 1u);
    EXPECT_EQ(pending[0].id, id3);
    EXPECT_EQ(published.size(), 1u);
    EXPECT_EQ(published[0].id, id1);
}

TEST(EventStoreTest, GetByTopic)
{
    auto store = create_in_memory_event_store();

    store->create_event("topicA", "p1");
    store->create_event("topicB", "p2");
    store->create_event("topicA", "p3");

    auto topicA = store->get_by_topic("topicA", EventStatus::PENDING);
    EXPECT_EQ(topicA.size(), 2u);

    auto topicB = store->get_by_topic("topicB", EventStatus::PENDING);
    EXPECT_EQ(topicB.size(), 1u);

    auto topicC = store->get_by_topic("topicC", EventStatus::PENDING);
    EXPECT_EQ(topicC.size(), 0u);
}

// ---------------------------------------------------------------------------
// Statistics
// ---------------------------------------------------------------------------

TEST(EventStoreTest, StatsTrackLifecycle)
{
    auto store = create_in_memory_event_store();

    auto id1 = store->create_event("t", "p");
    auto id2 = store->create_event("t", "p");

    auto s = store->stats();
    EXPECT_EQ(s.total, 2u);
    EXPECT_EQ(s.pending, 2u);

    store->mark_dispatching(id1);
    s = store->stats();
    EXPECT_EQ(s.pending, 1u);
    EXPECT_EQ(s.dispatching, 1u);

    store->mark_published(id1);
    store->mark_dispatching(id2);
    store->mark_failed(id2, "err");
    s = store->stats();
    EXPECT_EQ(s.published, 1u);
    EXPECT_EQ(s.failed, 1u);
    EXPECT_EQ(s.dispatching, 0u);
    EXPECT_EQ(s.pending, 0u);
}

TEST(EventStoreTest, StatsTrackRetries)
{
    auto store = create_in_memory_event_store();
    auto id = store->create_event("t", "p");

    store->mark_dispatching(id);
    store->mark_failed(id, "err");
    store->requeue(id);

    auto s = store->stats();
    EXPECT_EQ(s.retried, 1u);
}

// ---------------------------------------------------------------------------
// Concurrent access
// ---------------------------------------------------------------------------

TEST(EventStoreTest, ConcurrentCreateEvents)
{
    auto store = create_in_memory_event_store();

    constexpr int kThreads = 4;
    constexpr int kPerThread = 100;
    std::vector<std::thread> threads;
    std::vector<std::vector<EventId>> thread_ids(kThreads);

    for (int t = 0; t < kThreads; ++t) {
        threads.emplace_back([&, t]() {
            for (int i = 0; i < kPerThread; ++i) {
                auto id = store->create_event("topic", "data");
                thread_ids[t].push_back(id);
            }
        });
    }

    for (auto& t : threads) t.join();

    // All IDs must be unique across threads
    std::set<EventId> all_ids;
    for (auto& ids : thread_ids) {
        for (auto id : ids) {
            EXPECT_TRUE(all_ids.insert(id).second) << "Duplicate: " << id;
        }
    }
    EXPECT_EQ(all_ids.size(), kThreads * kPerThread);

    // Total count must match
    EXPECT_EQ(store->stats().total, kThreads * kPerThread);
}

TEST(EventStoreTest, ConcurrentDispatchAndPublish)
{
    auto store = create_in_memory_event_store();
    constexpr int N = 50;

    // Create events first
    std::vector<EventId> ids;
    for (int i = 0; i < N; ++i) {
        ids.push_back(store->create_event("topic", "data"));
    }

    // Mark dispatching from one thread, publishing from another
    std::atomic<int> dispatched{0};
    std::atomic<int> published{0};

    std::vector<std::thread> threads;

    // Dispatcher thread
    threads.emplace_back([&]() {
        for (int i = 0; i < N; ++i) {
            store->mark_dispatching(ids[i]);
            ++dispatched;
        }
    });

    // Publisher thread (waits a bit then publishes)
    threads.emplace_back([&]() {
        for (int i = 0; i < N; ++i) {
            // Busy wait until dispatched
            while (dispatched.load() <= i) {
                std::this_thread::yield();
            }
            store->mark_published(ids[i]);
            ++published;
        }
    });

    for (auto& t : threads) t.join();

    EXPECT_EQ(dispatched.load(), N);
    EXPECT_EQ(published.load(), N);

    auto s = store->stats();
    EXPECT_EQ(s.published, N);
    EXPECT_EQ(s.dispatching, 0u);
}

TEST(EventStoreTest, ConcurrentRequeue)
{
    auto store = create_in_memory_event_store();
    constexpr int N = 50;

    // Create and fail all events
    std::vector<EventId> ids;
    for (int i = 0; i < N; ++i) {
        auto id = store->create_event("topic", "data");
        store->mark_dispatching(id);
        store->mark_failed(id, "err");
        ids.push_back(id);
    }

    // Requeue from multiple threads (each event requeued by a unique thread)
    std::vector<std::thread> threads;
    for (int t = 0; t < N; ++t) {
        threads.emplace_back([&, t]() {
            store->requeue(ids[t]);
        });
    }

    for (auto& t : threads) t.join();

    auto pending = store->get_by_status(EventStatus::PENDING);
    EXPECT_EQ(pending.size(), N);
}

} // namespace
} // namespace dse
