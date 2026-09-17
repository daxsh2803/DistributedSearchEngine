// Distributed Search Engine - Kafka Client (Phase 19B).
//
// RAII wrapper around librdkafka providing the minimum Kafka producer
// foundation needed by the event system.
//
// Architecture:
//
//   EventDispatcher / ShardCoordinator
//          |
//          | produce_async(topic, payload, key)
//          v
//   KafkaClient
//          |
//          | wraps RdKafka::Producer
//          v
//   librdkafka (C++ API)
//
// Properties:
//   - Asynchronous produce is the fundamental operation.
//   - Synchronous convenience via condition_variable on delivery reports.
//   - Delivery reports collected through librdkafka callback.
//   - Pimpl hides rdkafkacpp.h from consumers.
//   - Thread-safe: multiple threads may call produce_async() concurrently.
//   - RAII: constructor creates producer, destructor flushes and closes.
//
// This is NOT a MessageBroker implementation. Phase 19C will create
// KafkaMessageBroker that implements MessageBroker using KafkaClient.
//
// Delivery semantics:
//   librdkafka provides at-least-once delivery by default.
//   The delivery report callback indicates success or failure.
//   Failed deliveries return error information through the
//   DeliveryReport struct.
//
// Thread safety:
//   produce_async() is thread-safe (librdkafka is thread-safe).
//   poll() is thread-safe but should ideally be called from a
//   single thread for deterministic delivery report processing.
//   flush() is thread-safe.
//   close() is NOT thread-safe with concurrent produce_async().

#pragma once

#include <atomic>
#include <cstddef>
#include <cstdint>
#include <functional>
#include <memory>
#include <string>

namespace dse {

// ---------------------------------------------------------------------------
// Configuration
// ---------------------------------------------------------------------------

struct KafkaClientConfig {
    // Broker connection.
    std::string bootstrap_servers = "localhost:9094";
    std::string client_id = "dse";

    // Producer tuning.
    int delivery_timeout_ms = 5000;
    int message_timeout_ms = 5000;
    int queue_buffering_max_ms = 5;    // linger.ms
    int queue_buffering_max_messages = 100000;
    int batch_num_messages = 10000;

    KafkaClientConfig() = default;
};

// ---------------------------------------------------------------------------
// Delivery report
// ---------------------------------------------------------------------------

// Result of a single message delivery (or delivery failure).
// Returned asynchronously via delivery report callback,
// or synchronously via produce().
struct DeliveryReport {
    bool success = false;
    int error_code = 0;              // RdKafka::ErrorCode raw value
    std::string error_message;
    std::string topic;
    int32_t partition = 0;
    int64_t offset = 0;
    void* opaque = nullptr;
};

// ---------------------------------------------------------------------------
// KafkaClient — producer-focused RAII wrapper around librdkafka
// ---------------------------------------------------------------------------

class KafkaClient {
public:
    // Create a Kafka client with the given configuration.
    // The librdkafka producer is created in the constructor.
    // Throws std::runtime_error if the producer cannot be created.
    explicit KafkaClient(KafkaClientConfig config);

    // Destructor flushes pending messages and closes the producer.
    ~KafkaClient();

    // Non-copyable, non-movable (owns librdkafka handles).
    KafkaClient(const KafkaClient&) = delete;
    KafkaClient& operator=(const KafkaClient&) = delete;
    KafkaClient(KafkaClient&&) = delete;
    KafkaClient& operator=(KafkaClient&&) = delete;

    // --- Producer API (async-first) ------------------------------------

    // Asynchronous produce. Returns immediately.
    // The delivery report will be available after poll() or flush().
    // Returns true if the message was accepted into librdkafka's queue.
    // Returns false if the queue is full or the producer is closed.
    bool produce_async(
        const std::string& topic,
        const std::string& payload,
        const std::string& key = "",
        void* opaque = nullptr);

    // Synchronous convenience. Produces and blocks until the delivery
    // report is received (or timeout expires).
    // Internally calls produce_async() + poll() in a loop.
    DeliveryReport produce(
        const std::string& topic,
        const std::string& payload,
        const std::string& key = "",
        int timeout_ms = 5000,
        void* opaque = nullptr);

    // --- Event processing -----------------------------------------------

    // Poll librdkafka for delivery reports and events.
    // Must be called periodically to receive delivery reports.
    // timeout_ms: 0 = non-blocking, -1 = block indefinitely.
    void poll(int timeout_ms = 0);

    // Flush all outstanding produce requests.
    // Blocks until all messages are delivered or timeout expires.
    // Returns true if all messages were flushed.
    bool flush(int timeout_ms = 5000);

    // --- Lifecycle ------------------------------------------------------

    // Graceful shutdown. Flushes pending messages and closes the producer.
    // Safe to call multiple times.
    void close();

    // Set callback for asynchronous delivery reports.
    void set_delivery_report_callback(std::function<void(const DeliveryReport&)> cb);

    // --- Health / status ------------------------------------------------

    // True if the producer has not been closed and is not in error state.
    bool is_healthy() const;

    // Last error message, or empty if no error.
    std::string last_error() const;

    // Number of messages in librdkafka's internal outqueue.
    std::size_t outqueue_length() const;

    // Total delivery reports received since construction.
    std::uint64_t delivery_reports_count() const;

private:
    // Pimpl: hides librdkafka headers from consumers.
    struct Impl;
    std::unique_ptr<Impl> impl_;
};

} // namespace dse
