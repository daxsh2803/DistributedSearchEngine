// Distributed Search Engine - Kafka Message Broker (Phase 19C/19D).
//
// Concrete MessageBroker implementation backed by Kafka via KafkaClient
// (producer) and KafkaConsumer (consumer groups).
//
// Architecture:
//
//   EventDispatcher
//       |
//       | publish(Message)
//       v
//   KafkaMessageBroker
//       |
//       ├── KafkaClient (Phase 19B — producer)
//       |       └── librdkafka Producer
//       |
//       └── KafkaConsumer (Phase 19D — consumer groups)
//               └── librdkafka KafkaConsumer
//
// Publish semantics (Phase 19C):
//   publish() maps to KafkaClient::produce_async().
//   "Accepted into librdkafka's producer queue" = success.
//   This is consistent with InMemoryMessageBroker.
//
// Consumer semantics (Phase 19D):
//   subscribe() registers a handler per topic.
//   start() launches a consumer thread that polls KafkaConsumer.
//   Messages are dispatched to the registered handler.
//   Manual offset commits after successful handler execution.
//   At-least-once delivery: failed handlers do NOT commit offsets.
//
// Poll thread (producer, Phase 19C):
//   A dedicated thread calls KafkaClient::poll() periodically to
//   process delivery reports.
//
// Consumer thread (Phase 19D):
//   A dedicated thread calls KafkaConsumer::poll() in a loop,
//   dispatching messages to registered handlers.
//
// Threading:
//   publish() / publish_with_timeout() are safe for concurrent calls.
//   Consumer thread handles consumption sequentially.
//   stop() is idempotent and joins all threads.
//
// Lifecycle:
//   construct -> subscribe() -> start() ->
//   publish/consume ... -> stop() -> destructor
//   stop() is idempotent. Destructor calls stop() if not already stopped.
//
// Ownership:
//   KafkaMessageBroker owns KafkaClient and KafkaConsumer through RAII.
//   Handlers are stored by value (std::function) — no external ownership.

#pragma once

#include <atomic>
#include <cstddef>
#include <cstdint>
#include <functional>
#include <memory>
#include <mutex>
#include <string>
#include <thread>
#include <unordered_map>
#include <vector>

#include "message.h"
#include "message_broker.h"

namespace dse {

class KafkaClient;       // forward declare — Pimpl hides librdkafka producer
class KafkaConsumer;     // forward declare — Pimpl hides librdkafka consumer

// ---------------------------------------------------------------------------
// Configuration
// ---------------------------------------------------------------------------

struct KafkaBrokerConfig {
    // Kafka broker connection (shared by producer and consumer).
    std::string bootstrap_servers = "localhost:9094";
    std::string client_id = "dse-kafka-broker";

    // Producer poll thread interval in milliseconds.
    int poll_interval_ms = 50;

    // --- Consumer group configuration (Phase 19D) ---

    // Consumer group ID. All KafkaMessageBroker instances sharing the
    // same group.id will coordinate partition assignment.
    //
    // For distributed deployments, each node should use a node-specific
    // group ID (e.g. "dse-node-0", "dse-node-1") so that every node
    // receives the full event stream independently.
    //
    // If left as the default, the application must override it before
    // constructing KafkaMessageBroker (typically in main.cpp).
    std::string group_id = "dse-consumer-group";

    // Consumer client ID.
    std::string consumer_client_id = "dse-consumer";

    // Auto offset reset policy when no committed offset exists.
    // "earliest": consume from the beginning.
    // "latest": consume only new messages.
    std::string auto_offset_reset = "earliest";

    // Consumer poll timeout in milliseconds.
    int consumer_poll_timeout_ms = 100;

    KafkaBrokerConfig() = default;
};

// ---------------------------------------------------------------------------
// KafkaMessageBroker
// ---------------------------------------------------------------------------

class KafkaMessageBroker : public MessageBroker {
public:
    // Create a Kafka message broker with the given configuration.
    explicit KafkaMessageBroker(KafkaBrokerConfig config = {});

    // Destructor stops all threads, flushes, and closes handles.
    ~KafkaMessageBroker() override;

    // Non-copyable, non-movable (owns threads and Kafka handles).
    KafkaMessageBroker(const KafkaMessageBroker&) = delete;
    KafkaMessageBroker& operator=(const KafkaMessageBroker&) = delete;
    KafkaMessageBroker(KafkaMessageBroker&&) = delete;
    KafkaMessageBroker& operator=(KafkaMessageBroker&&) = delete;

    // --- MessageBroker interface -----------------------------------------

    // Producer: publish a message to its topic.
    Offset publish(Message message) override;
    std::optional<Offset> publish_with_timeout(
        Message message, std::size_t timeout_ms) override;

    // Consumer: register a handler for a topic. Must be called before start().
    void subscribe(const Topic& topic, MessageHandler handler) override;

    // Start producer poll thread and consumer thread(s).
    void start() override;

    // Graceful shutdown: stop consumer, stop producer, flush, close.
    void stop() override;

    std::size_t queue_size(const Topic& topic) const override;
    BrokerStats stats() const override;
    std::vector<Message> dead_letters(const Topic& topic) const override;
    bool was_processed(MessageId id) const override;

    // --- Kafka-specific inspection --------------------------------------

    // Access the underlying KafkaClient (read-only).
    const KafkaClient& client() const;

    // Number of delivery reports received since construction.
    std::uint64_t delivery_reports_count() const;

    // Number of messages consumed since construction.
    std::uint64_t messages_consumed() const;

    // Number of rebalance events since construction.
    std::uint64_t rebalance_count() const;

private:
    // Producer poll thread loop: calls client_->poll() periodically.
    void poll_loop();

    // Start the producer poll thread (called lazily on first publish).
    void ensure_poll_thread_started();

    // Consumer thread loop: polls KafkaConsumer and dispatches to handlers.
    void consumer_loop();

    // --- Configuration ---
    KafkaBrokerConfig config_;

    // --- Kafka producer (owned) ---
    std::unique_ptr<KafkaClient> client_;

    // --- Kafka consumer (owned, Phase 19D) ---
    std::unique_ptr<KafkaConsumer> consumer_;

    // --- Producer poll thread ---
    std::thread poll_thread_;
    std::atomic<bool> poll_running_{false};

    // --- Consumer thread (Phase 19D) ---
    std::thread consumer_thread_;
    std::atomic<bool> consumer_running_{false};

    // --- Consumer handlers (set before start, read-only after) ---
    std::mutex handler_mutex_;
    std::unordered_map<std::string, MessageHandler> handlers_;

    // --- Offset counter (atomic, monotonic) ---
    std::atomic<std::uint64_t> next_offset_{0};

    // --- Statistics (atomic for lock-free reads) ---
    std::atomic<std::uint64_t> messages_published_{0};
    std::atomic<std::uint64_t> messages_failed_{0};
    std::atomic<std::uint64_t> messages_consumed_{0};
    std::atomic<std::uint64_t> messages_acked_{0};
    std::atomic<std::uint64_t> messages_nacked_{0};
};

} // namespace dse
