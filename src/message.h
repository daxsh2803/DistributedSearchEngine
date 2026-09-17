// Distributed Search Engine - Message Abstraction (Phase 18A).
//
// Core types for the asynchronous messaging subsystem:
//   - Message: unit of data with identity, topic, payload, offset
//   - BrokerConfig: delivery configuration (backpressure, retries)
//   - BrokerStats: observable broker statistics
//   - MessageHandler: consumer callback type
//
// This phase introduces the conceptual foundation that maps to
// Kafka-like message brokers. It is NOT a Kafka implementation.
// The in-memory broker provides learning-level semantics:
//   - At-least-once delivery
//   - Configurable retry with dead-letter queue
//   - Optional idempotent consumer support
//   - Bounded queue (backpressure)
//   - Graceful shutdown
//
// Delivery semantics:
//   AT-LEAST-ONCE: a message that fails processing is retried up to
//   max_delivery_attempts times. If the consumer crashes before
//   acknowledging, the message is redelivered. This means a handler
//   may see the same message more than once — idempotent consumers
//   must use the message ID to deduplicate.
//
//   This is explicitly NOT exactly-once delivery. Achieving exactly-once
//   requires coordination (e.g., transactional commits) that is out of
//   scope for this in-memory implementation.

#pragma once

#include <chrono>
#include <cstddef>
#include <cstdint>
#include <functional>
#include <string>
#include <vector>

namespace dse {

// ---------------------------------------------------------------------------
// Core identity types
// ---------------------------------------------------------------------------

// Unique message identifier (monotonically increasing within a broker).
using MessageId = std::uint64_t;

// Monotonically increasing offset within a topic.
using Offset = std::uint64_t;

// Topic name.
using Topic = std::string;

// ---------------------------------------------------------------------------
// Delivery state
// ---------------------------------------------------------------------------

// Tracks the lifecycle of a message through the broker.
enum class DeliveryState {
    Pending,        // In queue, not yet consumed
    Processing,     // Being processed by a consumer
    Acknowledged,   // Successfully processed
    DeadLetter      // Exceeded maximum delivery attempts
};

// ---------------------------------------------------------------------------
// Message
// ---------------------------------------------------------------------------

// A single message in the broker.
//
// The caller sets topic and payload. The broker assigns id, offset,
// and published_at on publish().
struct Message {
    MessageId id = 0;
    Topic topic;
    std::string key;
    std::string payload;
    Offset offset = 0;
    std::chrono::steady_clock::time_point published_at;
    std::size_t delivery_attempt = 0;
    DeliveryState state = DeliveryState::Pending;
};

// ---------------------------------------------------------------------------
// Broker configuration
// ---------------------------------------------------------------------------

struct BrokerConfig {
    // Maximum messages per topic queue before backpressure activates.
    // Producers block (or time out) when the queue is at capacity.
    std::size_t max_queue_size = 10000;

    // Maximum delivery attempts before a message is dead-lettered.
    // A value of 3 means: 1 initial attempt + 2 retries.
    std::size_t max_delivery_attempts = 3;

    // Number of consumer threads per subscribed topic.
    std::size_t consumer_threads = 1;

    // Enable idempotent consumer (in-memory deduplication).
    // When enabled, a message whose ID was already successfully processed
    // is silently skipped on redelivery.
    bool enable_idempotency = false;

    // Maximum tracked message IDs for idempotency.
    // When exceeded, the tracking set is cleared (bounded memory).
    // This means very old message IDs may no longer be deduplicated.
    std::size_t idempotency_window = 10000;
};

// ---------------------------------------------------------------------------
// Broker statistics
// ---------------------------------------------------------------------------

// Observable statistics. Counters are cumulative since broker creation.
struct BrokerStats {
    std::uint64_t messages_published = 0;
    std::uint64_t messages_delivered = 0;
    std::uint64_t messages_acknowledged = 0;
    std::uint64_t messages_retried = 0;
    std::uint64_t messages_dead_lettered = 0;
    std::size_t  queue_depth = 0;  // total pending across all topics
};

// ---------------------------------------------------------------------------
// Consumer handler
// ---------------------------------------------------------------------------

// The consumer handler receives a message and returns:
//   true  — message processed successfully (acknowledge)
//   false — processing failed (negative-acknowledge → retry or dead-letter)
//
// The handler runs synchronously in the consumer thread.
// Handlers must not block indefinitely; the broker waits for handlers
// to complete during graceful shutdown.
using MessageHandler = std::function<bool(const Message&)>;

} // namespace dse
