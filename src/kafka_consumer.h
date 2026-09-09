// Distributed Search Engine - Kafka Consumer (Phase 19D).
//
// RAII wrapper around librdkafka's KafkaConsumer providing consumer-group-based
// message consumption with manual offset commits.
//
// Architecture:
//
//   KafkaMessageBroker
//       |
//       | subscribe() + poll()
//       v
//   KafkaConsumer
//       |
//       | wraps RdKafka::KafkaConsumer
//       v
//   librdkafka (C++ API)
//       |
//       | consumer group protocol
//       v
//   Kafka broker
//
// Properties:
//   - Consumer group support via librdkafka's built-in group coordination.
//   - Manual offset commits (enable.auto.commit=false) for at-least-once delivery.
//   - Rebalance callback for partition assignment/revocation.
//   - Pimpl hides rdkafkacpp.h from consumers.
//   - RAII: constructor creates consumer, destructor leaves group and closes.
//
// Offset semantics:
//   - After successful message processing, call commit() to commit offsets.
//   - If the handler fails, do NOT commit — Kafka will redeliver on restart.
//   - This provides at-least-once delivery, NOT exactly-once.
//
// Threading:
//   - consume() is NOT thread-safe with other KafkaConsumer methods.
//   - Only one thread should call consume() at a time.
//   - commit() is thread-safe.
//   - close() is thread-safe and idempotent.
//
// This is NOT a MessageBroker implementation. KafkaMessageBroker wraps
// KafkaConsumer to implement the MessageBroker interface.

#pragma once

#include <atomic>
#include <cstdint>
#include <memory>
#include <string>
#include <vector>

namespace dse {

// ---------------------------------------------------------------------------
// Configuration
// ---------------------------------------------------------------------------

struct KafkaConsumerConfig {
    // Broker connection.
    std::string bootstrap_servers = "localhost:9094";
    std::string client_id = "dse-consumer";
    std::string group_id = "dse-consumer-group";

    // Topic subscription.
    std::vector<std::string> topics;

    // Consumer behavior.
    std::string auto_offset_reset = "earliest";  // "earliest" or "latest"
    bool enable_auto_commit = false;  // manual commits for reliability
    int poll_timeout_ms = 100;

    KafkaConsumerConfig() = default;
};

// ---------------------------------------------------------------------------
// ConsumedMessage — result of a single poll()
// ---------------------------------------------------------------------------

struct KafkaConsumerMessage {
    std::string topic;
    std::string key;
    std::string payload;
    int32_t partition = 0;
    int64_t offset = -1;

    // Error state.
    bool is_error = false;
    int error_code = 0;
    std::string error;

    // Well-known error codes (from librdkafka, but not leaking the header).
    static constexpr int ERR_TIMEOUT = -185;  // RdKafka::ERR__TIMED_OUT
};

// ---------------------------------------------------------------------------
// KafkaConsumer — RAII consumer-group wrapper around librdkafka
// ---------------------------------------------------------------------------

class KafkaConsumer {
public:
    // Create a Kafka consumer with the given configuration.
    // Subscribes to the configured topics.
    // Throws std::runtime_error if the consumer cannot be created.
    explicit KafkaConsumer(KafkaConsumerConfig config);

    // Destructor leaves the consumer group and closes.
    ~KafkaConsumer();

    // Non-copyable, non-movable.
    KafkaConsumer(const KafkaConsumer&) = delete;
    KafkaConsumer& operator=(const KafkaConsumer&) = delete;
    KafkaConsumer(KafkaConsumer&&) = delete;
    KafkaConsumer& operator=(KafkaConsumer&&) = delete;

    // --- Consumption API --------------------------------------------------

    // Poll for a single message. Blocks up to poll_timeout_ms.
    // Returns a message with is_error=false on success, or is_error=true
    // on error/timeout. Returns ERR__TIMED_OUT on timeout.
    KafkaConsumerMessage poll(int timeout_ms = -1);

    // --- Offset management ------------------------------------------------

    // Store the offset of a consumed message for later commit.
    // Must be called before commit() to ensure the specific offset
    // is included in the commit.
    void store_offset(const KafkaConsumerMessage& msg);

    // Commit stored offsets for all currently assigned partitions.
    // Should be called after successful handler execution.
    void commit();

    // --- Lifecycle --------------------------------------------------------

    // Leave consumer group and close. Safe to call multiple times.
    void close();

    // --- Health / status --------------------------------------------------

    // True if the consumer has not been closed.
    bool is_healthy() const;

    // Last error message, or empty if no error.
    std::string last_error() const;

    // Number of messages consumed since construction.
    std::uint64_t messages_consumed() const;

    // Number of rebalance events since construction.
    std::uint64_t rebalance_count() const;

    // Current number of assigned partitions.
    int assigned_partitions() const;

    // Estimate of consumer lag across all assigned partitions.
    int64_t estimated_lag() const;

private:
    // Pimpl: hides librdkafka headers from consumers.
    struct Impl;
    std::unique_ptr<Impl> impl_;
};

} // namespace dse
