// Distributed Search Engine - Abstract Message Broker Interface (Phase 18B).
//
// Pure virtual interface for message broker implementations.
// Concrete implementations include:
//   - InMemoryMessageBroker (in-memory, for testing and development)
//   - [future] KafkaMessageBroker, etc.
//
// The interface defines the producer/consumer lifecycle:
//   publish -> subscribe -> start -> [produce/consume] -> stop
//
// Delivery semantics are implementation-defined. The in-memory broker
// provides at-least-once delivery with retry and dead-letter support.
//
// Thread safety: implementations must be safe for concurrent publish()
// and concurrent subscribe() (before start()). Multiple consumer threads
// may call the handler concurrently depending on configuration.

#pragma once

#include <cstddef>
#include <cstdint>
#include <optional>
#include <vector>

#include "message.h"

namespace dse {

// Abstract message broker interface.
//
// Defines the contract that all broker implementations must satisfy.
// Callers program against this interface so that the underlying
// broker (in-memory, Kafka, etc.) can be swapped without changing
// producer/consumer code.
class MessageBroker {
public:
    virtual ~MessageBroker() = default;

    // Non-copyable, non-movable (broker owns threads/resources).
    MessageBroker(const MessageBroker&) = delete;
    MessageBroker& operator=(const MessageBroker&) = delete;
    MessageBroker(MessageBroker&&) = delete;
    MessageBroker& operator=(MessageBroker&&) = delete;

    // --- Producer API ---------------------------------------------------

    // Publish a message to its topic.
    // Assigns a unique ID, monotonically increasing offset, and timestamp.
    // Blocks if the topic queue is at capacity (backpressure).
    // Returns the assigned offset.
    virtual Offset publish(Message message) = 0;

    // Publish with timeout. Returns nullopt if the queue remains full
    // after timeout_ms milliseconds.
    virtual std::optional<Offset> publish_with_timeout(
        Message message, std::size_t timeout_ms) = 0;

    // --- Consumer API ---------------------------------------------------

    // Register a handler for a topic.
    // Must be called before start().
    virtual void subscribe(const Topic& topic, MessageHandler handler) = 0;

    // Start consumer threads for all subscribed topics.
    // Must be called after all subscribe() calls.
    virtual void start() = 0;

    // Graceful shutdown.
    // Implementations should:
    //   1. Stop accepting new work.
    //   2. Drain or abandon pending messages per documented semantics.
    //   3. Join all consumer threads.
    //   4. Return when complete.
    // Safe to call multiple times. Safe to call without start().
    virtual void stop() = 0;

    // --- Inspection -----------------------------------------------------

    // Current pending queue depth for a specific topic.
    virtual std::size_t queue_size(const Topic& topic) const = 0;

    // Aggregate broker statistics across all topics.
    virtual BrokerStats stats() const = 0;

    // Messages in the dead-letter queue for a topic.
    virtual std::vector<Message> dead_letters(const Topic& topic) const = 0;

    // --- Idempotency ----------------------------------------------------

    // Check if a message ID has been successfully processed.
    // Only meaningful when enable_idempotency is true in the config.
    virtual bool was_processed(MessageId id) const = 0;

protected:
    MessageBroker() = default;
};

} // namespace dse
