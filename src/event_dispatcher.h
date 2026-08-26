// Distributed Search Engine - Event Dispatcher (Phase 18D/18E).
//
// Bounded, lifecycle-managed asynchronous dispatch layer between
// ShardCoordinator and MessageBroker.
//
// Architecture:
//
//   ShardCoordinator
//       |
//       | enqueue(topic, payload)  or  enqueue_with_event(id, topic, payload)
//       v
//   EventDispatcher
//       |
//       | worker thread drains queue
//       | EventStore tracks lifecycle (Phase 18E)
//       v
//   MessageBroker::publish()
//
// Properties:
//   - Bounded queue with configurable capacity.
//   - Timed enqueue: returns false if queue remains full after timeout.
//   - Document mutation success/failure is NEVER affected by event dispatch.
//   - FIFO ordering within the single worker thread.
//   - Graceful shutdown: drains pending events before exiting.
//   - Thread-safe for concurrent producers.
//   - RAII lifecycle: destructor stops the dispatcher.
//   - Phase 18E: EventStore integration for reliable delivery tracking.
//
// Delivery guarantee:
//   AT-LEAST-ONCE within process lifetime. A successful enqueue guarantees
//   the event will be published unless the process crashes or the broker
//   rejects the message. Failed events are retried up to the configured
//   maximum. There is no durable outbox — events not yet enqueued are
//   lost on crash.
//
// Backpressure:
//   enqueue() blocks up to timeout_ms waiting for queue space.
//   If the queue remains full, enqueue returns false (event dropped).
//   The document mutation is NOT rolled back.

#pragma once

#include <atomic>
#include <condition_variable>
#include <cstddef>
#include <cstdint>
#include <deque>
#include <mutex>
#include <string>
#include <thread>
#include <vector>

namespace dse {

class EventStore;  // forward declare
class MessageBroker;  // forward declare

// ---------------------------------------------------------------------------
// EventDispatcher
// ---------------------------------------------------------------------------

class EventDispatcher {
public:
    struct Config {
        // Maximum events that can be queued before backpressure activates.
        std::size_t max_queue_size = 4096;

        // Default timeout in ms for enqueue(). 0 = non-blocking.
        std::size_t default_timeout_ms = 100;

        // Maximum retry attempts for failed broker deliveries.
        std::size_t max_retries = 3;

        // Delay between retries in milliseconds (0 = immediate retry).
        std::size_t retry_delay_ms = 0;

        Config() = default;
    };

    // Statistics observable by callers.
    struct Stats {
        std::uint64_t enqueued     = 0;
        std::uint64_t published    = 0;
        std::uint64_t rejected     = 0;  // queue-full timeout
        std::uint64_t broker_errors = 0;
        std::uint64_t retried      = 0;  // Phase 18E: retry attempts
        std::size_t   pending      = 0;  // current queue depth
    };

    // Create a dispatcher backed by the given broker, with optional
    // event store for reliable delivery tracking.
    // Does NOT take ownership of broker or store.
    explicit EventDispatcher(MessageBroker& broker);
    EventDispatcher(MessageBroker& broker, Config config);
    EventDispatcher(MessageBroker& broker, EventStore& store);
    EventDispatcher(MessageBroker& broker, EventStore& store,
                    Config config);
    ~EventDispatcher();

    // Non-copyable, non-movable (owns a worker thread).
    EventDispatcher(const EventDispatcher&) = delete;
    EventDispatcher& operator=(const EventDispatcher&) = delete;
    EventDispatcher(EventDispatcher&&) = delete;
    EventDispatcher& operator=(EventDispatcher&&) = delete;

    // Start the worker thread. Must be called before enqueue().
    // Safe to call multiple times (only starts once).
    void start();

    // Enqueue an event for asynchronous dispatch (no event tracking).
    // Blocks up to timeout_ms waiting for queue space.
    // Returns true if enqueued, false if queue remained full.
    bool enqueue(std::string topic, std::string payload,
                 std::size_t timeout_ms = 0);

    // Phase 18E: Enqueue with event tracking via EventStore.
    // The event_id is used to track delivery lifecycle.
    // The EventStore must be available if this method is used.
    bool enqueue_with_event(std::uint64_t event_id,
                            std::string topic, std::string payload,
                            std::size_t timeout_ms = 0);

    // Graceful shutdown.
    // 1. Stops accepting new events.
    // 2. Drains queued events (publishes them to the broker).
    // 3. Joins the worker thread.
    // Safe to call multiple times. Safe to call without start().
    void stop();

    // Replay failed events from the EventStore.
    // Moves events from FAILED → PENDING and re-enqueues them.
    // Returns the number of events re-enqueued.
    std::size_t replay_failed();

    // Current statistics.
    Stats stats() const;

private:
    // Internal queued event. event_id == 0 means no tracking.
    struct Event {
        std::string topic;
        std::string payload;
        std::uint64_t event_id = 0;
    };

    // Worker thread main loop.
    void worker_loop();

    // Process a single event with retry logic.
    // Returns true if the event was successfully published.
    bool process_event(Event& event);

    // Attempt to publish a single event to the broker.
    bool publish_to_broker(const Event& event);

    // --- Configuration ---
    Config config_;

    // --- Broker reference (not owned) ---
    MessageBroker& broker_;

    // --- Event store (optional, not owned) ---
    EventStore* store_ = nullptr;

    // --- Queue ---
    mutable std::mutex mutex_;
    std::condition_variable enqueue_cv_;   // producers wait here when full
    std::condition_variable dequeue_cv_;   // worker waits here when empty
    std::deque<Event> queue_;

    // --- Lifecycle ---
    std::atomic<bool> running_{false};
    std::atomic<bool> stopping_{false};
    std::thread worker_;

    // --- Statistics (atomic for lock-free reads) ---
    std::atomic<std::uint64_t> enqueued_{0};
    std::atomic<std::uint64_t> published_{0};
    std::atomic<std::uint64_t> rejected_{0};
    std::atomic<std::uint64_t> broker_errors_{0};
    std::atomic<std::uint64_t> retried_{0};
    std::atomic<std::uint64_t> replayed_{0};
};

} // namespace dse
