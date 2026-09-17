// Distributed Search Engine - In-Memory Message Broker (Phase 18B).
//
// Implementation of InMemoryMessageBroker: thread-safe publish/subscribe
// with backpressure, retry, dead-letter, idempotency, and graceful shutdown.

#include "in_memory_message_broker.h"

#include <chrono>
#include <stdexcept>

namespace dse {

// ---------------------------------------------------------------------------
// Construction / destruction
// ---------------------------------------------------------------------------

InMemoryMessageBroker::InMemoryMessageBroker(BrokerConfig config)
    : config_(std::move(config)) {}

InMemoryMessageBroker::~InMemoryMessageBroker() {
    stop();
}

// ---------------------------------------------------------------------------
// Producer API
// ---------------------------------------------------------------------------

Offset InMemoryMessageBroker::publish(Message message) {
    std::unique_lock lock(mutex_);

    // Backpressure: block until the queue has space.
    publish_cv_.wait(lock, [this, &message] {
        auto it = topics_.find(message.topic);
        if (it == topics_.end()) return true;
        return it->second->pending.size() < config_.max_queue_size;
    });

    // Create topic state if needed.
    if (topics_.find(message.topic) == topics_.end()) {
        topics_[message.topic] = std::make_unique<TopicState>();
    }
    auto& state = *topics_[message.topic];

    // Assign identity.
    if (message.id == 0) {
        message.id = next_message_id_.fetch_add(1, std::memory_order_relaxed);
    }
    message.offset = state.next_offset++;
    message.published_at = std::chrono::steady_clock::now();
    message.state = DeliveryState::Pending;
    message.delivery_attempt = 0;

    const MessageId generated_id = message.id;
    const Offset assigned_offset = message.offset;
    state.pending.push_back(std::move(message));
    messages_published_.fetch_add(1, std::memory_order_relaxed);

    if (delivery_callback_) {
        delivery_callback_(generated_id, true, "");
    }

    // Wake a consumer thread.
    consume_cv_.notify_one();

    return assigned_offset;
}

std::optional<Offset> InMemoryMessageBroker::publish_with_timeout(
    Message message, std::size_t timeout_ms) {
    std::unique_lock lock(mutex_);

    // Backpressure with timeout.
    auto deadline =
        std::chrono::steady_clock::now() + std::chrono::milliseconds(timeout_ms);

    auto has_space = [this, &message] {
        auto it = topics_.find(message.topic);
        if (it == topics_.end()) return true;
        return it->second->pending.size() < config_.max_queue_size;
    };

    if (!publish_cv_.wait_until(lock, deadline, has_space)) {
        return std::nullopt;  // timed out
    }

    // Create topic state if needed.
    if (topics_.find(message.topic) == topics_.end()) {
        topics_[message.topic] = std::make_unique<TopicState>();
    }
    auto& state = *topics_[message.topic];

    // Assign identity.
    if (message.id == 0) {
        message.id = next_message_id_.fetch_add(1, std::memory_order_relaxed);
    }
    message.offset = state.next_offset++;
    message.published_at = std::chrono::steady_clock::now();
    message.state = DeliveryState::Pending;
    message.delivery_attempt = 0;

    const MessageId generated_id = message.id;
    const Offset assigned_offset = message.offset;
    state.pending.push_back(std::move(message));
    messages_published_.fetch_add(1, std::memory_order_relaxed);

    if (delivery_callback_) {
        delivery_callback_(generated_id, true, "");
    }

    consume_cv_.notify_one();
    return assigned_offset;
}

// ---------------------------------------------------------------------------
// Consumer API
// ---------------------------------------------------------------------------

void InMemoryMessageBroker::subscribe(const Topic& topic, MessageHandler handler) {
    std::lock_guard lock(mutex_);
    if (running_.load(std::memory_order_relaxed)) {
        throw std::logic_error(
            "Cannot subscribe after start()");
    }
    if (topics_.find(topic) == topics_.end()) {
        topics_[topic] = std::make_unique<TopicState>();
    }
    topics_[topic]->handler = std::move(handler);
}

void InMemoryMessageBroker::start() {
    if (running_.exchange(true)) return;  // already running

    std::lock_guard lock(mutex_);
    for (auto& [topic, state] : topics_) {
        if (!state->handler) continue;  // no handler, skip
        for (std::size_t i = 0; i < config_.consumer_threads; ++i) {
            consumer_threads_.emplace_back(
                &InMemoryMessageBroker::consumer_loop, this, std::ref(*state));
        }
    }
}

void InMemoryMessageBroker::stop() {
    if (!running_.exchange(false)) return;  // already stopped

    // Wake all threads so they can observe running_ == false.
    {
        std::lock_guard lock(mutex_);
        publish_cv_.notify_all();
        consume_cv_.notify_all();
    }

    // Join all consumer threads.
    for (auto& t : consumer_threads_) {
        if (t.joinable()) t.join();
    }
    consumer_threads_.clear();
}

// ---------------------------------------------------------------------------
// Consumer loop
// ---------------------------------------------------------------------------

void InMemoryMessageBroker::consumer_loop(TopicState& state) {
    while (true) {
        std::unique_lock lock(mutex_);

        // Wait until there are messages or we are shutting down.
        consume_cv_.wait(lock, [this, &state] {
            return !state.pending.empty() ||
                   !running_.load(std::memory_order_relaxed);
        });

        // Shutdown + queue empty -> exit.
        if (!running_.load(std::memory_order_relaxed) && state.pending.empty()) {
            break;
        }

        if (state.pending.empty()) continue;

        // Dequeue the front message.
        Message msg = std::move(state.pending.front());
        state.pending.pop_front();
        msg.state = DeliveryState::Processing;
        ++msg.delivery_attempt;
        messages_delivered_.fetch_add(1, std::memory_order_relaxed);

        // Idempotency check.
        if (config_.enable_idempotency && processed_ids_.count(msg.id)) {
            // Already processed — skip and acknowledge.
            msg.state = DeliveryState::Acknowledged;
            messages_acknowledged_.fetch_add(1, std::memory_order_relaxed);
            publish_cv_.notify_all();
            continue;
        }

        // Release lock during handler execution.
        lock.unlock();

        bool success = false;
        try {
            success = state.handler(msg);
        } catch (...) {
            success = false;  // exceptions treated as failure
        }

        lock.lock();

        if (success) {
            msg.state = DeliveryState::Acknowledged;
            messages_acknowledged_.fetch_add(1, std::memory_order_relaxed);

            // Track for idempotency.
            if (config_.enable_idempotency) {
                if (processed_ids_.size() >= config_.idempotency_window) {
                    processed_ids_.clear();  // bounded eviction
                }
                processed_ids_.insert(msg.id);
            }

            // Notify producers that space may be available.
            publish_cv_.notify_all();
        } else {
            handle_failure(state, std::move(msg));
        }
    }
}

// ---------------------------------------------------------------------------
// Failure handling
// ---------------------------------------------------------------------------

void InMemoryMessageBroker::handle_failure(TopicState& state, Message msg) {
    // During shutdown, dead-letter immediately to prevent infinite retry loops.
    const bool shutting_down =
        !running_.load(std::memory_order_relaxed);

    if (msg.delivery_attempt >= config_.max_delivery_attempts || shutting_down) {
        msg.state = DeliveryState::DeadLetter;
        state.dead.push_back(std::move(msg));
        messages_dead_lettered_.fetch_add(1, std::memory_order_relaxed);
    } else {
        msg.state = DeliveryState::Pending;
        state.pending.push_back(std::move(msg));
        messages_retried_.fetch_add(1, std::memory_order_relaxed);
        consume_cv_.notify_one();
    }

    // Notify producers that space may be available.
    publish_cv_.notify_all();
}

// ---------------------------------------------------------------------------
// Inspection
// ---------------------------------------------------------------------------

std::size_t InMemoryMessageBroker::queue_size(const Topic& topic) const {
    std::lock_guard lock(mutex_);
    auto it = topics_.find(topic);
    if (it == topics_.end()) return 0;
    return it->second->pending.size();
}

BrokerStats InMemoryMessageBroker::stats() const {
    std::lock_guard lock(mutex_);
    BrokerStats s;
    s.messages_published =
        messages_published_.load(std::memory_order_relaxed);
    s.messages_delivered =
        messages_delivered_.load(std::memory_order_relaxed);
    s.messages_acknowledged =
        messages_acknowledged_.load(std::memory_order_relaxed);
    s.messages_retried =
        messages_retried_.load(std::memory_order_relaxed);
    s.messages_dead_lettered =
        messages_dead_lettered_.load(std::memory_order_relaxed);
    s.queue_depth = 0;
    for (const auto& [topic, state] : topics_) {
        s.queue_depth += state->pending.size();
    }
    return s;
}

std::vector<Message> InMemoryMessageBroker::dead_letters(const Topic& topic) const {
    std::lock_guard lock(mutex_);
    auto it = topics_.find(topic);
    if (it == topics_.end()) return {};
    return std::vector<Message>(it->second->dead.begin(),
                                it->second->dead.end());
}

// ---------------------------------------------------------------------------
// Idempotency
// ---------------------------------------------------------------------------

bool InMemoryMessageBroker::was_processed(MessageId id) const {
    std::lock_guard lock(mutex_);
    return processed_ids_.count(id) > 0;
}

} // namespace dse
