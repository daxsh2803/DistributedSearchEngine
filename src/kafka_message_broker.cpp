// Distributed Search Engine - Kafka Message Broker (Phase 19C/19D).
//
// Implementation of KafkaMessageBroker using KafkaClient (producer) and
// KafkaConsumer (consumer groups).
//
// Producer: maps MessageBroker::publish() to KafkaClient::produce_async().
// Consumer: polls KafkaConsumer in a dedicated thread, dispatches messages
// to registered handlers, and commits offsets after successful processing.
//
// Manual offset commits provide at-least-once delivery: failed handlers
// do not commit offsets, so Kafka will redeliver on restart.

#include "kafka_message_broker.h"

#include <chrono>
#include <stdexcept>
#include <utility>
#include <vector>

#include "kafka_client.h"
#include "kafka_consumer.h"

namespace dse {

// ---------------------------------------------------------------------------
// Construction / destruction
// ---------------------------------------------------------------------------

KafkaMessageBroker::KafkaMessageBroker(KafkaBrokerConfig config)
    : config_(std::move(config))
{
    // Create the producer.
    KafkaClientConfig client_cfg;
    client_cfg.bootstrap_servers = config_.bootstrap_servers;
    client_cfg.client_id = config_.client_id;
    client_ = std::make_unique<KafkaClient>(std::move(client_cfg));

    client_->set_delivery_report_callback([this](const DeliveryReport& report) {
        if (delivery_callback_ && report.opaque) {
            MessageId id = static_cast<MessageId>(reinterpret_cast<std::uintptr_t>(report.opaque));
            delivery_callback_(id, report.success, report.error_message);
        }
    });
}

KafkaMessageBroker::~KafkaMessageBroker()
{
    stop();
}

// ---------------------------------------------------------------------------
// Producer poll thread
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

    bool accepted = client_->produce_async(
        message.topic, message.payload, message.key,
        reinterpret_cast<void*>(static_cast<std::uintptr_t>(message.id)));

    if (!accepted) {
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
    ensure_poll_thread_started();

    bool accepted = client_->produce_async(
        message.topic, message.payload, message.key,
        reinterpret_cast<void*>(static_cast<std::uintptr_t>(message.id)));

    if (accepted) {
        ++messages_published_;
        return next_offset_.fetch_add(1, std::memory_order_relaxed);
    }

    // First attempt failed — try synchronous produce with timeout.
    if (timeout_ms > 0 && client_->is_healthy()) {
        auto report = client_->produce(
            message.topic, message.payload, message.key,
            static_cast<int>(timeout_ms),
            reinterpret_cast<void*>(static_cast<std::uintptr_t>(message.id)));

        if (report.success) {
            ++messages_published_;
            return next_offset_.fetch_add(1, std::memory_order_relaxed);
        }
    }

    ++messages_failed_;
    return std::nullopt;
}

// ---------------------------------------------------------------------------
// Consumer API (Phase 19D)
// ---------------------------------------------------------------------------

void KafkaMessageBroker::subscribe(const Topic& topic,
                                    MessageHandler handler)
{
    std::lock_guard<std::mutex> lock(handler_mutex_);
    handlers_[topic] = std::move(handler);
}

void KafkaMessageBroker::start()
{
    // Start the producer poll thread.
    ensure_poll_thread_started();

    // Start the consumer thread if there are registered handlers.
    {
        std::lock_guard<std::mutex> lock(handler_mutex_);
        if (handlers_.empty()) {
            return;  // No consumers to start.
        }
    }

    bool expected = false;
    if (!consumer_running_.compare_exchange_strong(expected, true)) {
        return;  // Already running.
    }

    // Build the list of topics to subscribe to.
    std::vector<std::string> topics;
    {
        std::lock_guard<std::mutex> lock(handler_mutex_);
        for (const auto& [topic, handler] : handlers_) {
            topics.push_back(topic);
        }
    }

    // Create the consumer.
    KafkaConsumerConfig consumer_cfg;
    consumer_cfg.bootstrap_servers = config_.bootstrap_servers;
    consumer_cfg.client_id = config_.consumer_client_id;
    consumer_cfg.group_id = config_.group_id;
    consumer_cfg.auto_offset_reset = config_.auto_offset_reset;
    consumer_cfg.poll_timeout_ms = config_.consumer_poll_timeout_ms;
    consumer_cfg.topics = std::move(topics);

    consumer_ = std::make_unique<KafkaConsumer>(std::move(consumer_cfg));

    // Start the consumer thread.
    consumer_thread_ = std::thread(&KafkaMessageBroker::consumer_loop, this);
}

void KafkaMessageBroker::stop()
{
    // Stop the consumer thread first (it references consumer_).
    if (consumer_running_.exchange(false)) {
        if (consumer_thread_.joinable()) {
            consumer_thread_.join();
        }
    }

    // Close the consumer.
    if (consumer_) {
        consumer_->close();
        consumer_.reset();
    }

    // Stop the producer poll thread.
    if (poll_running_.exchange(false)) {
        if (poll_thread_.joinable()) {
            poll_thread_.join();
        }
    }

    // Flush pending messages and close the producer.
    if (client_ && client_->is_healthy()) {
        client_->flush(5000);
        client_->close();
    }
}

// ---------------------------------------------------------------------------
// Consumer loop (Phase 19D)
// ---------------------------------------------------------------------------

void KafkaMessageBroker::consumer_loop()
{
    while (consumer_running_.load(std::memory_order_relaxed)) {
        if (!consumer_ || !consumer_->is_healthy()) {
            std::this_thread::sleep_for(
                std::chrono::milliseconds(config_.consumer_poll_timeout_ms));
            continue;
        }

        auto msg = consumer_->poll(config_.consumer_poll_timeout_ms);

        if (msg.is_error) {
            // ERR__TIMED_OUT is normal — means no message available.
            // Other errors are logged but do not crash the loop.
            if (msg.error_code != KafkaConsumerMessage::ERR_TIMEOUT) {
                // Transient error — continue polling.
            }
            continue;
        }

        // Look up the handler for this topic.
        MessageHandler handler;
        {
            std::lock_guard<std::mutex> lock(handler_mutex_);
            auto it = handlers_.find(msg.topic);
            if (it != handlers_.end()) {
                handler = it->second;
            }
        }

        if (!handler) {
            // No handler for this topic — skip the message.
            // Do not commit offset so Kafka can redeliver if needed.
            continue;
        }

        // Convert to Message for the handler.
        Message broker_msg;
        broker_msg.topic = msg.topic;
        broker_msg.payload = std::move(msg.payload);
        broker_msg.offset = static_cast<std::uint64_t>(msg.offset);

        ++messages_consumed_;

        // Invoke handler synchronously with strict sequential retry and bounded backoff.
        std::size_t backoff_ms = 100;
        const std::size_t max_backoff_ms = 5000;
        bool success = false;
        bool paused = false;

        while (consumer_running_.load(std::memory_order_relaxed)) {
            try {
                success = handler(broker_msg);
            } catch (...) {
                success = false;
            }

            if (success) {
                break;
            }

            // Failure: pause, backoff, and retry exactly this message.
            ++messages_nacked_;

            if (!paused) {
                consumer_->pause_all();
                paused = true;
            }

            std::this_thread::sleep_for(
                std::chrono::milliseconds(backoff_ms));

            backoff_ms = std::min(backoff_ms * 2, max_backoff_ms);

            // DO NOT CALL consumer_->poll() HERE.
        }

        if (paused) {
            consumer_->resume_all();
        }

        if (success) {
            ++messages_acked_;
            // Store this specific offset for commit.
            consumer_->store_offset(msg);
        }
    }

    // Final cleanup: process any remaining messages in the queue.
    if (consumer_ && consumer_->is_healthy()) {
        for (int i = 0; i < 10; ++i) {
            auto msg = consumer_->poll(0);
            if (msg.is_error) break;

            MessageHandler handler;
            {
                std::lock_guard<std::mutex> lock(handler_mutex_);
                auto it = handlers_.find(msg.topic);
                if (it != handlers_.end()) {
                    handler = it->second;
                }
            }

            if (!handler) continue;

            Message broker_msg;
            broker_msg.topic = std::move(msg.topic);
            broker_msg.payload = std::move(msg.payload);
            broker_msg.offset = static_cast<std::uint64_t>(msg.offset);

            ++messages_consumed_;

            bool success = false;
            std::size_t backoff_ms = 100;
            const std::size_t max_backoff_ms = 5000;

            while (consumer_running_.load()) {
                try {
                    success = handler(broker_msg);
                } catch (...) {
                    success = false;
                }

                if (success) {
                    ++messages_acked_;
                    consumer_->commit();
                    break;
                } else {
                    ++messages_nacked_;
                    // Bounded exponential backoff
                    std::this_thread::sleep_for(std::chrono::milliseconds(backoff_ms));
                    backoff_ms = std::min(backoff_ms * 2, max_backoff_ms);
                }
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Inspection
// ---------------------------------------------------------------------------

std::size_t KafkaMessageBroker::queue_size(const Topic& /*topic*/) const
{
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
    s.messages_delivered = messages_consumed_.load(std::memory_order_relaxed);
    s.messages_acknowledged = messages_acked_.load(std::memory_order_relaxed);
    s.messages_retried = 0;  // retries are EventDispatcher's job
    s.messages_dead_lettered = 0;
    s.queue_depth = queue_size("");
    return s;
}

std::vector<Message> KafkaMessageBroker::dead_letters(
    const Topic& /*topic*/) const
{
    return {};
}

bool KafkaMessageBroker::was_processed(MessageId /*id*/) const
{
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

std::uint64_t KafkaMessageBroker::messages_consumed() const
{
    return messages_consumed_.load(std::memory_order_relaxed);
}

std::uint64_t KafkaMessageBroker::rebalance_count() const
{
    return consumer_ ? consumer_->rebalance_count() : 0;
}

std::uint64_t KafkaMessageBroker::consumer_lag() const
{
    return consumer_ ? static_cast<std::uint64_t>(std::max<int64_t>(0, consumer_->estimated_lag())) : 0;
}

} // namespace dse
