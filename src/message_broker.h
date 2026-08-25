// Distributed Search Engine - In-Memory Message Broker (Phase 18A).
//
// A thread-safe, in-memory message broker implementing core messaging
// concepts: publish/subscribe, FIFO ordering, backpressure, retry,
// dead-letter queue, idempotent consumers, and graceful shutdown.
//
// This is an educational implementation that demonstrates the abstractions
// found in systems like Kafka. It is NOT a replacement for Kafka.
//
// Threading model:
//   - Multiple producer threads may publish concurrently.
//   - One consumer thread per subscribed topic (configurable).
//   - Consumer threads process messages sequentially within a topic.
//   - A handler is invoked synchronously in the consumer thread.
//   - The handler's return value determines ack/nack.
//
// Backpressure:
//   - publish() blocks when the topic queue is at max_queue_size.
//   - publish_with_timeout() returns nullopt after the timeout.
//   - publish_cv_ is notified when messages are consumed (freeing space).
//
// Graceful shutdown:
//   - stop() drains pending messages before exiting.
//   - Failed messages during shutdown are dead-lettered (not retried).
//   - In-flight messages (currently in handler) complete normally.
//
// Constraints:
//   - subscribe() must be called before start().
//   - start() must be called before publish() has any effect on delivery.
//   - stop() must not be called concurrently with start().

#pragma once

#include <atomic>
#include <condition_variable>
#include <cstddef>
#include <cstdint>
#include <deque>
#include <memory>
#include <mutex>
#include <optional>
#include <thread>
#include <unordered_map>
#include <unordered_set>
#include <vector>

#include "message.h"

namespace dse {

class MessageBroker {
public:
    // Create a message broker with the given configuration.
    explicit MessageBroker(BrokerConfig config = {});

    // Destructor calls stop() if not already stopped.
    ~MessageBroker();

    // Non-copyable, non-movable (owns threads).
    MessageBroker(const MessageBroker&) = delete;
    MessageBroker& operator=(const MessageBroker&) = delete;
    MessageBroker(MessageBroker&&) = delete;
    MessageBroker& operator=(MessageBroker&&) = delete;

    // --- Producer API ---------------------------------------------------

    // Publish a message to its topic.
    // Assigns a unique ID, monotonically increasing offset, and timestamp.
    // Blocks if the topic queue is at max_queue_size (backpressure).
    // The message's topic field must be set before calling.
    Offset publish(Message message);

    // Publish with timeout. Returns nullopt if the queue remains full
    // after timeout_ms milliseconds.
    std::optional<Offset> publish_with_timeout(
        Message message, std::size_t timeout_ms);

    // --- Consumer API ---------------------------------------------------

    // Register a handler for a topic.
    // Must be called before start().
    void subscribe(const Topic& topic, MessageHandler handler);

    // Start consumer threads for all subscribed topics.
    // Must be called after all subscribe() calls.
    void start();

    // Graceful shutdown:
    //   1. Signals consumers to stop accepting new work.
    //   2. Drains remaining pending messages (processes them).
    //   3. Dead-letters any failed messages (no retries during shutdown).
    //   4. Joins all consumer threads.
    //   5. Returns when all threads have exited.
    // Safe to call multiple times. Safe to call without start().
    void stop();

    // --- Inspection -----------------------------------------------------

    // Current pending queue depth for a specific topic.
    std::size_t queue_size(const Topic& topic) const;

    // Aggregate broker statistics across all topics.
    BrokerStats stats() const;

    // Messages in the dead-letter queue for a topic.
    std::vector<Message> dead_letters(const Topic& topic) const;

    // --- Idempotency ----------------------------------------------------

    // Check if a message ID has been successfully processed.
    // Only meaningful when enable_idempotency is true.
    bool was_processed(MessageId id) const;

private:
    // Per-topic state: pending queue, dead-letter queue, handler, offset.
    struct TopicState {
        std::deque<Message> pending;
        std::deque<Message> dead;
        MessageHandler handler;
        Offset next_offset = 0;
    };

    // Consumer thread main loop for a given topic.
    void consumer_loop(TopicState& state);

    // Requeue or dead-letter a failed message.
    void handle_failure(TopicState& state, Message msg);

    // --- Configuration ---
    BrokerConfig config_;

    // --- Synchronization ---
    mutable std::mutex mutex_;
    std::condition_variable publish_cv_;   // wakes producers when space frees
    std::condition_variable consume_cv_;   // wakes consumers when messages arrive

    // --- Topic state ---
    std::unordered_map<Topic, std::unique_ptr<TopicState>> topics_;
    std::vector<std::thread> consumer_threads_;

    // --- Lifecycle ---
    std::atomic<bool> running_{false};
    std::atomic<std::uint64_t> next_message_id_{1};

    // --- Idempotency (protected by mutex_) ---
    std::unordered_set<MessageId> processed_ids_;

    // --- Statistics (atomic for lock-free reads) ---
    std::atomic<std::uint64_t> messages_published_{0};
    std::atomic<std::uint64_t> messages_delivered_{0};
    std::atomic<std::uint64_t> messages_acknowledged_{0};
    std::atomic<std::uint64_t> messages_retried_{0};
    std::atomic<std::uint64_t> messages_dead_lettered_{0};
};

} // namespace dse
