// Distributed Search Engine - Event Dispatcher (Phase 18D/18E).
//
// Bounded asynchronous dispatch layer with EventStore integration
// for reliable delivery tracking. A single worker thread drains the
// queue and publishes events to the MessageBroker, with retry support
// for failed deliveries.

#include "event_dispatcher.h"

#include <chrono>
#include <cstddef>
#include <utility>
#include <vector>

#include "event_store.h"
#include "message.h"
#include "message_broker.h"

namespace dse {

// ---------------------------------------------------------------------------
// Construction
// ---------------------------------------------------------------------------

EventDispatcher::EventDispatcher(MessageBroker& broker)
    : broker_(broker)
{
}

EventDispatcher::EventDispatcher(MessageBroker& broker, Config config)
    : config_(std::move(config))
    , broker_(broker)
{
}

EventDispatcher::EventDispatcher(MessageBroker& broker, EventStore& store)
    : broker_(broker)
    , store_(&store)
{
}

EventDispatcher::EventDispatcher(MessageBroker& broker, EventStore& store,
                                 Config config)
    : config_(std::move(config))
    , broker_(broker)
    , store_(&store)
{
}

EventDispatcher::~EventDispatcher()
{
    stop();
}

// ---------------------------------------------------------------------------
// Lifecycle
// ---------------------------------------------------------------------------

void EventDispatcher::start()
{
    bool expected = false;
    if (!running_.compare_exchange_strong(expected, true)) {
        return;  // Already started.
    }
    stopping_ = false;
    worker_ = std::thread(&EventDispatcher::worker_loop, this);
}

// ---------------------------------------------------------------------------
// Enqueue
// ---------------------------------------------------------------------------

bool EventDispatcher::enqueue(std::string topic, std::string payload,
                              std::size_t timeout_ms)
{
    if (!running_.load() || stopping_.load()) {
        ++rejected_;
        return false;
    }

    Event event;
    event.topic = std::move(topic);
    event.payload = std::move(payload);
    event.event_id = 0;  // No tracking.

    std::unique_lock<std::mutex> lock(mutex_);

    if (timeout_ms == 0) {
        if (queue_.size() >= config_.max_queue_size) {
            ++rejected_;
            return false;
        }
        queue_.push_back(std::move(event));
        ++enqueued_;
        lock.unlock();
        dequeue_cv_.notify_one();
        return true;
    }

    const auto deadline = std::chrono::steady_clock::now()
                        + std::chrono::milliseconds(timeout_ms);

    if (!enqueue_cv_.wait_until(lock, deadline, [this]() {
        return queue_.size() < config_.max_queue_size || stopping_.load();
    })) {
        ++rejected_;
        return false;
    }

    if (stopping_.load()) {
        ++rejected_;
        return false;
    }

    queue_.push_back(std::move(event));
    ++enqueued_;
    lock.unlock();
    dequeue_cv_.notify_one();
    return true;
}

bool EventDispatcher::enqueue_with_event(std::uint64_t event_id,
                                          std::string topic,
                                          std::string payload,
                                          std::size_t timeout_ms)
{
    // Mark the event as dispatching in the store.
    if (store_) {
        store_->mark_dispatching(event_id);
    }

    if (!running_.load() || stopping_.load()) {
        if (store_) {
            store_->mark_failed(event_id, "dispatcher not running");
        }
        ++rejected_;
        return false;
    }

    Event event;
    event.topic = std::move(topic);
    event.payload = std::move(payload);
    event.event_id = event_id;

    std::unique_lock<std::mutex> lock(mutex_);

    if (timeout_ms == 0) {
        if (queue_.size() >= config_.max_queue_size) {
            if (store_) {
                store_->mark_failed(event_id, "queue full");
            }
            ++rejected_;
            return false;
        }
        queue_.push_back(std::move(event));
        ++enqueued_;
        lock.unlock();
        dequeue_cv_.notify_one();
        return true;
    }

    const auto deadline = std::chrono::steady_clock::now()
                        + std::chrono::milliseconds(timeout_ms);

    if (!enqueue_cv_.wait_until(lock, deadline, [this]() {
        return queue_.size() < config_.max_queue_size || stopping_.load();
    })) {
        if (store_) {
            store_->mark_failed(event_id, "queue full (timeout)");
        }
        ++rejected_;
        return false;
    }

    if (stopping_.load()) {
        if (store_) {
            store_->mark_failed(event_id, "dispatcher stopping");
        }
        ++rejected_;
        return false;
    }

    queue_.push_back(std::move(event));
    ++enqueued_;
    lock.unlock();
    dequeue_cv_.notify_one();
    return true;
}

// ---------------------------------------------------------------------------
// Shutdown
// ---------------------------------------------------------------------------

void EventDispatcher::stop()
{
    {
        std::lock_guard<std::mutex> lock(mutex_);
        if (!running_.load()) {
            return;
        }
        stopping_ = true;
    }

    dequeue_cv_.notify_one();
    enqueue_cv_.notify_all();

    if (worker_.joinable()) {
        worker_.join();
    }

    running_ = false;
}

// ---------------------------------------------------------------------------
// Replay
// ---------------------------------------------------------------------------

std::size_t EventDispatcher::replay_failed()
{
    if (!store_) return 0;

    auto failed = store_->get_by_status(EventStatus::FAILED);
    std::size_t requeued = 0;

    for (auto& ev : failed) {
        if (store_->requeue(ev.id)) {
            ++requeued;
            ++replayed_;
            // Re-enqueue into the dispatcher.
            enqueue_with_event(ev.id, std::move(ev.topic),
                               std::move(ev.payload), 0);
        }
    }

    return requeued;
}

// ---------------------------------------------------------------------------
// Statistics
// ---------------------------------------------------------------------------

EventDispatcher::Stats EventDispatcher::stats() const
{
    std::lock_guard<std::mutex> lock(mutex_);
    Stats s;
    s.enqueued      = enqueued_.load();
    s.published     = published_.load();
    s.rejected      = rejected_.load();
    s.broker_errors = broker_errors_.load();
    s.retried       = retried_.load();
    s.pending       = queue_.size();
    return s;
}

// ---------------------------------------------------------------------------
// Worker loop
// ---------------------------------------------------------------------------

void EventDispatcher::worker_loop()
{
    while (true) {
        Event event;

        {
            std::unique_lock<std::mutex> lock(mutex_);

            dequeue_cv_.wait(lock, [this]() {
                return !queue_.empty() || stopping_.load();
            });

            if (stopping_.load() && queue_.empty()) {
                break;
            }

            if (queue_.empty()) {
                break;
            }

            event = std::move(queue_.front());
            queue_.pop_front();
            enqueue_cv_.notify_one();
        }

        // Process the event with retry logic.
        process_event(event);
    }
}

// ---------------------------------------------------------------------------
// Event processing with retry
// ---------------------------------------------------------------------------

bool EventDispatcher::process_event(Event& event)
{
    const std::size_t max_attempts = config_.max_retries + 1;
    const bool tracked = store_ && event.event_id != 0;

    for (std::size_t attempt = 0; attempt < max_attempts; ++attempt) {
        // NOTE: We intentionally do NOT check stopping_ here.
        // Once an event is dequeued from the queue, it must be fully
        // processed (published or retried to failure). The worker_loop
        // handles the queue-level shutdown gate: no new events are
        // dequeued after stop() is called.

        // Record each delivery attempt in the event store.
        if (tracked) {
            store_->record_attempt(event.event_id);
        }

        // Retry delay (only for retries, not the initial attempt).
        if (attempt > 0) {
            ++retried_;
            if (config_.retry_delay_ms > 0) {
                std::this_thread::sleep_for(
                    std::chrono::milliseconds(config_.retry_delay_ms));
            }
        }

        if (publish_to_broker(event)) {
            ++published_;
            if (tracked) {
                store_->mark_published(event.event_id);
            }
            return true;
        }

        ++broker_errors_;
    }

    // All attempts failed.
    if (tracked) {
        store_->mark_failed(event.event_id, "broker rejected");
    }
    return false;
}

// ---------------------------------------------------------------------------
// Broker publication
// ---------------------------------------------------------------------------

bool EventDispatcher::publish_to_broker(const Event& event)
{
    Message msg;
    msg.topic = event.topic;
    msg.payload = event.payload;

    try {
        broker_.publish(std::move(msg));
        return true;
    } catch (...) {
        return false;
    }
}

} // namespace dse
