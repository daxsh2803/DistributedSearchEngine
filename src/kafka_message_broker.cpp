// Distributed Search Engine - Kafka Message Broker (Phase 19C).
//
// Implementation of KafkaMessageBroker using KafkaClient.
// Maps MessageBroker::publish() to KafkaClient::produce_async().
//
// The poll thread processes librdkafka delivery reports. It is started
// lazily on the first publish() call and joined during stop().

#include "kafka_message_broker.h"

#include <chrono>
#include <stdexcept>
#include <utility>

#include "kafka_client.h"

namespace dse {

// ---------------------------------------------------------------------------
// Construction / destruction
// ---------------------------------------------------------------------------

KafkaMessageBroker::KafkaMessageBroker(KafkaBrokerConfig config)
    : config_(std::move(config))
{
    KafkaClientConfig client_cfg;
    client_cfg.bootstrap_servers = config_.bootstrap_servers;
    client_cfg.client_id = config_.client_id;
    client_ = std::make_unique<KafkaClient>(std::move(client_cfg));
}

KafkaMessageBroker::~KafkaMessageBroker()
{
    stop();
}

// ---------------------------------------------------------------------------
// Poll thread
// ---------------------------------------------------------------------------

void KafkaMessageBroker::ensure_poll_thread_started()
{
    bool expected = false;
    if (poll_running_.compare_exchange_strong(expected, true)) {
        poll_thread_ = std::thread(&KafkaMessageBroker::poll_loop, this);
    }
}

void KafkaMessageBroker::poll_loop()
{
    while (poll_running_.load(std::memory_order_relaxed)) {
        if (client_ && client_->is_healthy()) {
            client_->poll(config_.poll_interval_ms);
        } else {
            // Client unhealthy — sleep briefly to avoid busy-wait.
            std::this_thread::sleep_for(
                std::chrono::milliseconds(config_.poll_interval_ms));
        }
    }

    // Final poll to process any remaining delivery reports.
    if (client_ && client_->is_healthy()) {
        client_->poll(0);
    }
}

// ---------------------------------------------------------------------------
// Producer API
// ---------------------------------------------------------------------------

Offset KafkaMessageBroker::publish(Message message)
{
    ensure_poll_thread_started();

    // Try async produce — returns true if accepted into librdkafka queue.
    bool accepted = client_->produce_async(
        message.topic, message.payload, /*key=*/"");

    if (!accepted) {
        // Producer queue full or client closed — throw to let
        // EventDispatcher apply its existing retry policy.
        throw std::runtime_error(
            "KafkaMessageBroker: produce_async rejected message: "
            + client_->last_error());
    }

    ++messages_published_;
    return next_offset_.fetch_add(1, std::memory_order_relaxed);
}

std::optional<Offset> KafkaMessageBroker::publish_with_timeout(
    Message message, std::size_t timeout_ms)
{
    // KafkaClient::produce_async() does not block — it either accepts
    // or rejects immediately. The timeout parameter is therefore used
    // as a total deadline: we try async first, and if that fails, we
    // fall back to the synchronous produce() with the given timeout.
    ensure_poll_thread_started();

    bool accepted = client_->produce_async(
        message.topic, message.payload, /*key=*/"");

    if (accepted) {
        ++messages_published_;
        return next_offset_.fetch_add(1, std::memory_order_relaxed);
    }

    // First attempt failed — try synchronous produce with timeout.
    if (timeout_ms > 0 && client_->is_healthy()) {
        auto report = client_->produce(
            message.topic, message.payload, /*key=*/"",
            static_cast<int>(timeout_ms));

        if (report.success) {
            ++messages_published_;
            return next_offset_.fetch_add(1, std::memory_order_relaxed);
        }
    }

    // Both attempts failed.
    ++messages_failed_;
    return std::nullopt;
}

// ---------------------------------------------------------------------------
// Consumer API — minimal stubs (Phase 19D)
// ---------------------------------------------------------------------------

void KafkaMessageBroker::subscribe(const Topic& /*topic*/,
                                    MessageHandler /*handler*/)
{
    // Consumer groups are deferred to Phase 19D.
    // Intentionally no-op: KafkaMessageBroker is producer-only in 19C.
}

void KafkaMessageBroker::start()
{
    // Start the poll thread for delivery report processing.
    ensure_poll_thread_started();
}

void KafkaMessageBroker::stop()
{
    // Stop the poll thread first (it references client_).
    if (poll_running_.exchange(false)) {
        if (poll_thread_.joinable()) {
            poll_thread_.join();
        }
    }

    // Flush pending messages and close the client.
    if (client_ && client_->is_healthy()) {
        client_->flush(5000);
        client_->close();
    }
}

// ---------------------------------------------------------------------------
// Inspection
// ---------------------------------------------------------------------------

std::size_t KafkaMessageBroker::queue_size(const Topic& /*topic*/) const
{
    // Kafka does not expose per-topic local queue depth through the
    // MessageBroker abstraction. Return the outqueue length as a proxy.
    if (client_) {
        return client_->outqueue_length();
    }
    return 0;
}

BrokerStats KafkaMessageBroker::stats() const
{
    BrokerStats s;
    s.messages_published =
        messages_published_.load(std::memory_order_relaxed);
    s.messages_delivered = 0;   // not tracked at broker level
    s.messages_acknowledged = 0;  // not tracked at broker level
    s.messages_retried = 0;    // retries are EventDispatcher's job
    s.messages_dead_lettered = 0;
    s.queue_depth = queue_size("");
    return s;
}

std::vector<Message> KafkaMessageBroker::dead_letters(
    const Topic& /*topic*/) const
{
    // KafkaMessageBroker does not maintain a dead-letter queue.
    // Failed events are tracked by the EventStore (Phase 18E).
    return {};
}

bool KafkaMessageBroker::was_processed(MessageId /*id*/) const
{
    // Idempotency checking is not implemented in KafkaMessageBroker.
    return false;
}

// ---------------------------------------------------------------------------
// Kafka-specific inspection
// ---------------------------------------------------------------------------

const KafkaClient& KafkaMessageBroker::client() const
{
    return *client_;
}

std::uint64_t KafkaMessageBroker::delivery_reports_count() const
{
    return client_ ? client_->delivery_reports_count() : 0;
}

} // namespace dse
