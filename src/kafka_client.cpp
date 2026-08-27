// Distributed Search Engine - Kafka Client (Phase 19B).
//
// Implementation of KafkaClient using librdkafka's C++ API.
// All librdkafka headers are contained in this file (Pimpl pattern).

#include "kafka_client.h"

#include <condition_variable>
#include <cstring>
#include <mutex>
#include <stdexcept>
#include <string>

// --- librdkafka C++ API --------------------------------------------------
// This is the ONLY file in the project that includes librdkafka headers.
#include <rdkafkacpp.h>

namespace dse {

// ---------------------------------------------------------------------------
// Delivery report callback — collects results from librdkafka
// ---------------------------------------------------------------------------

class DeliveryReportCallback : public RdKafka::DeliveryReportCb {
public:
    void dr_cb(RdKafka::Message& message) override {
        DeliveryReport report;
        report.success = (message.err() == RdKafka::ERR_NO_ERROR);
        report.error_code = static_cast<int>(message.err());
        report.error_message = RdKafka::err2str(message.err());
        report.topic = message.topic_name();
        report.partition = message.partition();
        report.offset = message.offset();

        {
            std::lock_guard lock(mutex_);
            reports_.push_back(std::move(report));
            total_.fetch_add(1, std::memory_order_relaxed);
        }
        cv_.notify_all();
    }

    // Wait for at least `count` delivery reports.
    // Returns all accumulated reports.
    std::vector<DeliveryReport> wait_for(std::size_t count,
                                          int timeout_ms) {
        std::unique_lock lock(mutex_);
        cv_.wait_for(lock,
                     std::chrono::milliseconds(timeout_ms),
                     [this, count] {
                         return reports_.size() >= count;
                     });
        std::vector<DeliveryReport> result;
        result.swap(reports_);
        return result;
    }

    // Non-blocking drain of all available reports.
    std::vector<DeliveryReport> drain() {
        std::lock_guard lock(mutex_);
        std::vector<DeliveryReport> result;
        result.swap(reports_);
        return result;
    }

    std::uint64_t total() const {
        return total_.load(std::memory_order_relaxed);
    }

private:
    mutable std::mutex mutex_;
    std::condition_variable cv_;
    std::vector<DeliveryReport> reports_;
    std::atomic<std::uint64_t> total_{0};
};

// ---------------------------------------------------------------------------
// KafkaClient::Impl — holds all librdkafka state
// ---------------------------------------------------------------------------

struct KafkaClient::Impl {
    KafkaClientConfig config;
    RdKafka::Producer* producer = nullptr;
    DeliveryReportCallback dr_cb;
    std::atomic<bool> closed{false};
    std::string error_str;

    explicit Impl(KafkaClientConfig cfg) : config(std::move(cfg)) {
        create_producer();
    }

    ~Impl() {
        close_producer();
    }

    void create_producer() {
        std::string errstr;
        auto* conf = RdKafka::Conf::create(RdKafka::Conf::CONF_GLOBAL);

        if (conf->set("bootstrap.servers", config.bootstrap_servers,
                       errstr) != RdKafka::Conf::CONF_OK) {
            delete conf;
            throw std::runtime_error(
                "KafkaClient: failed to set bootstrap.servers: " + errstr);
        }

        if (conf->set("client.id", config.client_id,
                       errstr) != RdKafka::Conf::CONF_OK) {
            delete conf;
            throw std::runtime_error(
                "KafkaClient: failed to set client.id: " + errstr);
        }

        // Delivery timeout.
        if (conf->set("delivery.timeout.ms",
                       std::to_string(config.delivery_timeout_ms),
                       errstr) != RdKafka::Conf::CONF_OK) {
            delete conf;
            throw std::runtime_error(
                "KafkaClient: failed to set delivery.timeout.ms: " + errstr);
        }

        // Message timeout.
        if (conf->set("message.timeout.ms",
                       std::to_string(config.message_timeout_ms),
                       errstr) != RdKafka::Conf::CONF_OK) {
            delete conf;
            throw std::runtime_error(
                "KafkaClient: failed to set message.timeout.ms: " + errstr);
        }

        // Linger (batching delay).
        if (conf->set("linger.ms",
                       std::to_string(config.queue_buffering_max_ms),
                       errstr) != RdKafka::Conf::CONF_OK) {
            delete conf;
            throw std::runtime_error(
                "KafkaClient: failed to set linger.ms: " + errstr);
        }

        // Queue buffering.
        if (conf->set("queue.buffering.max.messages",
                       std::to_string(config.queue_buffering_max_messages),
                       errstr) != RdKafka::Conf::CONF_OK) {
            delete conf;
            throw std::runtime_error(
                "KafkaClient: failed to set queue.buffering.max.messages: "
                + errstr);
        }

        // Batch size.
        if (conf->set("batch.num.messages",
                       std::to_string(config.batch_num_messages),
                       errstr) != RdKafka::Conf::CONF_OK) {
            delete conf;
            throw std::runtime_error(
                "KafkaClient: failed to set batch.num.messages: " + errstr);
        }

        // Delivery report callback.
        if (conf->set("dr_cb", &dr_cb,
                       errstr) != RdKafka::Conf::CONF_OK) {
            delete conf;
            throw std::runtime_error(
                "KafkaClient: failed to set dr_cb: " + errstr);
        }

        // Create the producer.
        producer = RdKafka::Producer::create(conf, errstr);
        delete conf;

        if (!producer) {
            throw std::runtime_error(
                "KafkaClient: failed to create producer: " + errstr);
        }
    }

    void close_producer() {
        if (closed.exchange(true)) {
            return;  // already closed
        }
        if (producer) {
            // Flush with a generous timeout to deliver pending messages.
            producer->flush(5000);
            // Final poll to receive remaining delivery reports.
            producer->poll(0);
            delete producer;
            producer = nullptr;
        }
    }

    bool is_healthy() const {
        return !closed.load() && producer != nullptr;
    }
};

// ---------------------------------------------------------------------------
// KafkaClient construction / destruction
// ---------------------------------------------------------------------------

KafkaClient::KafkaClient(KafkaClientConfig config)
    : impl_(std::make_unique<Impl>(std::move(config))) {}

KafkaClient::~KafkaClient() {
    // Impl destructor handles flush + close.
}

// ---------------------------------------------------------------------------
// Producer API
// ---------------------------------------------------------------------------

bool KafkaClient::produce_async(
    const std::string& topic,
    const std::string& payload,
    const std::string& key)
{
    if (!impl_ || impl_->closed.load() || !impl_->producer) {
        return false;
    }

    RdKafka::ErrorCode err = impl_->producer->produce(
        topic,
        RdKafka::Topic::PARTITION_UA,   // let librdkafka choose partition
        RdKafka::Producer::RK_MSG_COPY,
        const_cast<char*>(payload.data()),
        payload.size(),
        key.empty() ? nullptr : key.data(),
        key.size(),
        0,                                // timestamp (auto)
        nullptr);                         // opaque

    if (err != RdKafka::ERR_NO_ERROR) {
        impl_->error_str = RdKafka::err2str(err);
        return false;
    }

    // Trigger background poll to keep librdkafka processing.
    impl_->producer->poll(0);
    return true;
}

DeliveryReport KafkaClient::produce(
    const std::string& topic,
    const std::string& payload,
    const std::string& key,
    int timeout_ms)
{
    // Record the expected delivery report count before producing.
    const auto before = impl_->dr_cb.total();

    if (!produce_async(topic, payload, key)) {
        DeliveryReport report;
        report.success = false;
        report.error_message = impl_->error_str;
        report.topic = topic;
        return report;
    }

    // Poll until we receive the delivery report for this message.
    const auto deadline = std::chrono::steady_clock::now()
                        + std::chrono::milliseconds(timeout_ms);

    while (impl_->dr_cb.total() <= before) {
        auto remaining = std::chrono::duration_cast<std::chrono::milliseconds>(
            deadline - std::chrono::steady_clock::now()).count();
        if (remaining <= 0) {
            // Timeout — return a timeout error.
            DeliveryReport report;
            report.success = false;
            report.error_code = -1;  // custom: timeout
            report.error_message = "produce() timed out after "
                                 + std::to_string(timeout_ms) + "ms";
            report.topic = topic;
            return report;
        }
        impl_->producer->poll(static_cast<int>(
            std::min(remaining, static_cast<long long>(100))));
    }

    // Drain all available reports; the last one for our topic is ours.
    auto reports = impl_->dr_cb.drain();
    if (!reports.empty()) {
        return std::move(reports.back());
    }

    // Should not reach here, but handle defensively.
    DeliveryReport report;
    report.success = false;
    report.error_message = "no delivery report received";
    report.topic = topic;
    return report;
}

// ---------------------------------------------------------------------------
// Event processing
// ---------------------------------------------------------------------------

void KafkaClient::poll(int timeout_ms) {
    if (impl_ && impl_->producer && !impl_->closed.load()) {
        impl_->producer->poll(timeout_ms);
    }
}

bool KafkaClient::flush(int timeout_ms) {
    if (!impl_ || !impl_->producer || impl_->closed.load()) {
        return false;
    }
    RdKafka::ErrorCode err = impl_->producer->flush(timeout_ms);
    return (err == RdKafka::ERR_NO_ERROR);
}

// ---------------------------------------------------------------------------
// Lifecycle
// ---------------------------------------------------------------------------

void KafkaClient::close() {
    if (impl_) {
        impl_->close_producer();
    }
}

// ---------------------------------------------------------------------------
// Health / status
// ---------------------------------------------------------------------------

bool KafkaClient::is_healthy() const {
    return impl_ && impl_->is_healthy();
}

std::string KafkaClient::last_error() const {
    return impl_ ? impl_->error_str : "no client";
}

std::size_t KafkaClient::outqueue_length() const {
    if (impl_ && impl_->producer && !impl_->closed.load()) {
        return static_cast<std::size_t>(impl_->producer->outq_len());
    }
    return 0;
}

std::uint64_t KafkaClient::delivery_reports_count() const {
    return impl_ ? impl_->dr_cb.total() : 0;
}

} // namespace dse
