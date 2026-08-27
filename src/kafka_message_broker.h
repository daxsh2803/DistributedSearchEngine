// Distributed Search Engine - Kafka Message Broker (Phase 19C).
//
// Concrete MessageBroker implementation backed by Kafka via KafkaClient.
// EventDispatcher programs against the MessageBroker interface; it does
// not know whether it is using InMemoryMessageBroker or KafkaMessageBroker.
//
// Architecture:
//
//   EventDispatcher
//       |
//       | publish(Message)
//       v
//   KafkaMessageBroker
//       |
//       | produce_async(topic, payload, key)
//       v
//   KafkaClient
//       |
//       | librdkafka C++ API
//       v
//   Kafka broker
//
// Publish semantics:
//   publish() maps to KafkaClient::produce_async().
//   "Accepted into librdkafka's producer queue" = success.
//   This is consistent with InMemoryMessageBroker: publish() returns
//   after the message is accepted into a local queue, NOT after the
//   end consumer has processed it.
//
// Poll thread:
//   A dedicated thread calls KafkaClient::poll() periodically to
//   process delivery reports. The thread is started lazily on the
//   first publish() and joined during stop().
//
// Consumer methods:
//   subscribe/start/queue_size/dead_letters/was_processed are minimal
//   stubs. Kafka consumer groups are NOT part of Phase 19C; they
//   belong to Phase 19D.
//
// Thread safety:
//   publish() and publish_with_timeout() are safe for concurrent calls.
//   stop() and the destructor are safe for single-threaded invocation
//   after all publishers have quiesced.
//
// Lifecycle:
//   construct -> start (optional, starts poll thread) ->
//   publish ... -> stop -> destructor
//   stop() is idempotent. Destructor calls stop() if not already stopped.
//
// Ownership:
//   KafkaMessageBroker owns its KafkaClient through RAII.
//   The KafkaClient owns the librdkafka producer handle.

#pragma once

#include <atomic>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <string>
#include <thread>

#include "message.h"
#include "message_broker.h"

namespace dse {

class KafkaClient;  // forward declare — Pimpl hides librdkafka

// ---------------------------------------------------------------------------
// Configuration
// ---------------------------------------------------------------------------

struct KafkaBrokerConfig {
    // Kafka broker connection.
    std::string bootstrap_servers = "localhost:9094";
    std::string client_id = "dse-kafka-broker";

    // Poll thread interval in milliseconds.
    int poll_interval_ms = 50;

    KafkaBrokerConfig() = default;
};

// ---------------------------------------------------------------------------
// KafkaMessageBroker
// ---------------------------------------------------------------------------

class KafkaMessageBroker : public MessageBroker {
public:
    // Create a Kafka message broker with the given configuration.
    // Constructs the KafkaClient internally.
    explicit KafkaMessageBroker(KafkaBrokerConfig config = {});

    // Destructor stops the poll thread, flushes, and closes the client.
    ~KafkaMessageBroker() override;

    // Non-copyable, non-movable (owns threads and Kafka handles).
    KafkaMessageBroker(const KafkaMessageBroker&) = delete;
    KafkaMessageBroker& operator=(const KafkaMessageBroker&) = delete;
    KafkaMessageBroker(KafkaMessageBroker&&) = delete;
    KafkaMessageBroker& operator=(KafkaMessageBroker&&) = delete;

    // --- MessageBroker interface -----------------------------------------

    Offset publish(Message message) override;
    std::optional<Offset> publish_with_timeout(
        Message message, std::size_t timeout_ms) override;

    // Consumer-side: minimal stubs (consumer groups deferred to Phase 19D).
    void subscribe(const Topic& topic, MessageHandler handler) override;
    void start() override;
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

private:
    // Poll thread loop: calls client_->poll() periodically.
    void poll_loop();

    // Start the poll thread (called lazily on first publish).
    void ensure_poll_thread_started();

    // --- Configuration ---
    KafkaBrokerConfig config_;

    // --- Kafka client (owned) ---
    std::unique_ptr<KafkaClient> client_;

    // --- Poll thread ---
    std::thread poll_thread_;
    std::atomic<bool> poll_running_{false};

    // --- Offset counter (atomic, monotonic) ---
    std::atomic<std::uint64_t> next_offset_{0};

    // --- Statistics (atomic for lock-free reads) ---
    std::atomic<std::uint64_t> messages_published_{0};
    std::atomic<std::uint64_t> messages_failed_{0};
};

} // namespace dse
