// Distributed Search Engine - Kafka Consumer (Phase 19D).
//
// Implementation of KafkaConsumer using librdkafka's KafkaConsumer API.
// All librdkafka headers are contained in this file (Pimpl pattern).
//
// Uses manual offset commits (enable.auto.commit=false) for at-least-once
// delivery semantics. The caller is responsible for calling commit() after
// successful message processing.

#include "kafka_consumer.h"

#include <chrono>
#include <condition_variable>
#include <cstring>
#include <mutex>
#include <stdexcept>
#include <string>
#include <vector>

// --- librdkafka C++ API --------------------------------------------------
// This is the ONLY file that includes librdkafka consumer headers.
#include <rdkafkacpp.h>

namespace dse {

// ---------------------------------------------------------------------------
// Rebalance callback — handles partition assignment/revocation
// ---------------------------------------------------------------------------

class ConsumerRebalanceCb : public RdKafka::RebalanceCb {
public:
    explicit ConsumerRebalanceCb(std::atomic<bool>& running)
        : running_(running) {}

    void rebalance_cb(RdKafka::KafkaConsumer* consumer,
                      RdKafka::ErrorCode err,
                      std::vector<RdKafka::TopicPartition*>& partitions) override
    {
        if (err == RdKafka::ERR__ASSIGN_PARTITIONS) {
            // Eager assignor: assign all partitions.
            consumer->assign(partitions);
            assigned_count_ = static_cast<int>(partitions.size());
            ++rebalance_count_;
        } else if (err == RdKafka::ERR__REVOKE_PARTITIONS) {
            // Commit any final offsets before revocation.
            consumer->unassign();
            assigned_count_ = 0;
            ++rebalance_count_;
        } else {
            // Unexpected error — unassign to synchronize state.
            consumer->unassign();
            assigned_count_ = 0;
            ++rebalance_count_;
        }
    }

    int assigned_count() const { return assigned_count_; }
    std::uint64_t rebalance_count() const { return rebalance_count_; }

private:
    std::atomic<bool>& running_;
    int assigned_count_ = 0;
    std::uint64_t rebalance_count_ = 0;
};

// ---------------------------------------------------------------------------
// KafkaConsumer::Impl — holds all librdkafka consumer state
// ---------------------------------------------------------------------------

struct KafkaConsumer::Impl {
    KafkaConsumerConfig config;
    RdKafka::KafkaConsumer* consumer = nullptr;
    ConsumerRebalanceCb rebalance_cb;
    std::atomic<bool> closed{false};
    std::atomic<bool> running{true};
    std::string error_str;
    std::uint64_t messages_consumed_ = 0;

    explicit Impl(KafkaConsumerConfig cfg)
        : config(std::move(cfg))
        , rebalance_cb(running)
    {
        create_consumer();
    }

    ~Impl() {
        close_consumer();
    }

    void create_consumer() {
        std::string errstr;
        auto* conf = RdKafka::Conf::create(RdKafka::Conf::CONF_GLOBAL);

        // Bootstrap servers.
        if (conf->set("bootstrap.servers", config.bootstrap_servers,
                       errstr) != RdKafka::Conf::CONF_OK) {
            delete conf;
            throw std::runtime_error(
                "KafkaConsumer: failed to set bootstrap.servers: " + errstr);
        }

        // Client ID.
        if (conf->set("client.id", config.client_id,
                       errstr) != RdKafka::Conf::CONF_OK) {
            delete conf;
            throw std::runtime_error(
                "KafkaConsumer: failed to set client.id: " + errstr);
        }

        // Group ID.
        if (conf->set("group.id", config.group_id,
                       errstr) != RdKafka::Conf::CONF_OK) {
            delete conf;
            throw std::runtime_error(
                "KafkaConsumer: failed to set group.id: " + errstr);
        }

        // Auto offset reset.
        if (conf->set("auto.offset.reset", config.auto_offset_reset,
                       errstr) != RdKafka::Conf::CONF_OK) {
            delete conf;
            throw std::runtime_error(
                "KafkaConsumer: failed to set auto.offset.reset: " + errstr);
        }

        // Disable auto commit (manual commits for reliability).
        if (conf->set("enable.auto.commit", "false",
                       errstr) != RdKafka::Conf::CONF_OK) {
            delete conf;
            throw std::runtime_error(
                "KafkaConsumer: failed to set enable.auto.commit: " + errstr);
        }

        // Disable auto offset store (we store after commit).
        if (conf->set("enable.auto.offset.store", "false",
                       errstr) != RdKafka::Conf::CONF_OK) {
            delete conf;
            throw std::runtime_error(
                "KafkaConsumer: failed to set enable.auto.offset.store: "
                + errstr);
        }

        // Session timeout.
        if (conf->set("session.timeout.ms", "30000",
                       errstr) != RdKafka::Conf::CONF_OK) {
            delete conf;
            throw std::runtime_error(
                "KafkaConsumer: failed to set session.timeout.ms: " + errstr);
        }

        // Rebalance callback.
        if (conf->set("rebalance_cb", &rebalance_cb,
                       errstr) != RdKafka::Conf::CONF_OK) {
            delete conf;
            throw std::runtime_error(
                "KafkaConsumer: failed to set rebalance_cb: " + errstr);
        }

        // Create the consumer.
        consumer = RdKafka::KafkaConsumer::create(conf, errstr);
        delete conf;

        if (!consumer) {
            throw std::runtime_error(
                "KafkaConsumer: failed to create consumer: " + errstr);
        }

        // Subscribe to configured topics.
        if (!config.topics.empty()) {
            RdKafka::ErrorCode err = consumer->subscribe(config.topics);
            if (err != RdKafka::ERR_NO_ERROR) {
                std::string topic_err = RdKafka::err2str(err);
                close_consumer();
                throw std::runtime_error(
                    "KafkaConsumer: failed to subscribe: " + topic_err);
            }
        }
    }

    void close_consumer() {
        if (closed.exchange(true)) {
            return;  // already closed
        }
        running.store(false);
        if (consumer) {
            consumer->close();
            delete consumer;
            consumer = nullptr;
        }
    }
};

// ---------------------------------------------------------------------------
// Construction / destruction
// ---------------------------------------------------------------------------

KafkaConsumer::KafkaConsumer(KafkaConsumerConfig config)
    : impl_(std::make_unique<Impl>(std::move(config))) {}

KafkaConsumer::~KafkaConsumer() {
    close();
}

// ---------------------------------------------------------------------------
// Poll
// ---------------------------------------------------------------------------

KafkaConsumerMessage KafkaConsumer::poll(int timeout_ms) {
    KafkaConsumerMessage result;

    if (!impl_ || impl_->closed.load() || !impl_->consumer) {
        result.is_error = true;
        result.error_code = -1;
        result.error = "consumer not available";
        return result;
    }

    RdKafka::Message* msg = impl_->consumer->consume(timeout_ms);

    if (!msg) {
        result.is_error = true;
        result.error_code = -1;
        result.error = "consume() returned null";
        return result;
    }

    if (msg->err() != RdKafka::ERR_NO_ERROR) {
        result.is_error = true;
        result.error_code = static_cast<int>(msg->err());
        result.error = RdKafka::err2str(msg->err());
        delete msg;
        return result;
    }

    // Valid message.
    result.topic = msg->topic_name();
    result.partition = msg->partition();
    result.offset = msg->offset();

    if (msg->key()) {
        result.key = *msg->key();
    }

    if (msg->payload()) {
        result.payload.assign(
            static_cast<const char*>(msg->payload()), msg->len());
    }

    ++impl_->messages_consumed_;
    delete msg;
    return result;
}

// ---------------------------------------------------------------------------
// Offset management
// ---------------------------------------------------------------------------

void KafkaConsumer::store_offset(const KafkaConsumerMessage& msg) {
    if (!impl_ || impl_->closed.load() || !impl_->consumer) {
        return;
    }
    // Create a TopicPartition with the specific offset to store.
    auto* tp = RdKafka::TopicPartition::create(
        msg.topic, msg.partition, msg.offset + 1);  // commit offset+1
    if (tp) {
        std::vector<RdKafka::TopicPartition*> offsets = {tp};
        impl_->consumer->commitAsync(offsets);
        delete tp;
    }
}

void KafkaConsumer::commit() {
    if (!impl_ || impl_->closed.load() || !impl_->consumer) {
        return;
    }
    impl_->consumer->commitAsync();
}

// ---------------------------------------------------------------------------
// Partition control
// ---------------------------------------------------------------------------

void KafkaConsumer::pause_all() {
    if (!impl_ || impl_->closed.load() || !impl_->consumer) {
        return;
    }
    std::vector<RdKafka::TopicPartition*> assignment;
    RdKafka::ErrorCode err = impl_->consumer->assignment(assignment);
    if (err == RdKafka::ERR_NO_ERROR && !assignment.empty()) {
        impl_->consumer->pause(assignment);
    }
    RdKafka::TopicPartition::destroy(assignment);
}

void KafkaConsumer::resume_all() {
    if (!impl_ || impl_->closed.load() || !impl_->consumer) {
        return;
    }
    std::vector<RdKafka::TopicPartition*> assignment;
    RdKafka::ErrorCode err = impl_->consumer->assignment(assignment);
    if (err == RdKafka::ERR_NO_ERROR && !assignment.empty()) {
        impl_->consumer->resume(assignment);
    }
    RdKafka::TopicPartition::destroy(assignment);
}

// ---------------------------------------------------------------------------
// Lifecycle
// ---------------------------------------------------------------------------

void KafkaConsumer::close() {
    if (impl_) {
        impl_->close_consumer();
    }
}

// ---------------------------------------------------------------------------
// Health / status
// ---------------------------------------------------------------------------

bool KafkaConsumer::is_healthy() const {
    return impl_ && !impl_->closed.load() && impl_->consumer != nullptr;
}

std::string KafkaConsumer::last_error() const {
    return impl_ ? impl_->error_str : "no consumer";
}

std::uint64_t KafkaConsumer::messages_consumed() const {
    return impl_ ? impl_->messages_consumed_ : 0;
}

std::uint64_t KafkaConsumer::rebalance_count() const {
    return impl_ ? impl_->rebalance_cb.rebalance_count() : 0;
}

int KafkaConsumer::assigned_partitions() const {
    return impl_ ? impl_->rebalance_cb.assigned_count() : 0;
}

int64_t KafkaConsumer::estimated_lag() const {
    if (!impl_ || impl_->closed.load() || !impl_->consumer) {
        return 0;
    }

    std::vector<RdKafka::TopicPartition*> assignment;
    if (impl_->consumer->assignment(assignment) != RdKafka::ERR_NO_ERROR) {
        return 0;
    }

    if (impl_->consumer->position(assignment) != RdKafka::ERR_NO_ERROR) {
        RdKafka::TopicPartition::destroy(assignment);
        return 0;
    }

    int64_t total_lag = 0;
    for (auto* tp : assignment) {
        int64_t low = 0, high = 0;
        if (impl_->consumer->query_watermark_offsets(
                tp->topic(), tp->partition(), &low, &high, 1000) == RdKafka::ERR_NO_ERROR) {

            // tp->offset() gives the current logical position.
            // If valid, lag = high - offset.
            if (tp->offset() >= 0 && high >= tp->offset()) {
                total_lag += (high - tp->offset());
            } else if (tp->offset() < 0 && high > low) {
                // If offset is invalid (e.g. newly assigned without commit), lag is high - low
                total_lag += (high - low);
            }
        }
    }

    RdKafka::TopicPartition::destroy(assignment);
    return total_lag;
}

} // namespace dse
