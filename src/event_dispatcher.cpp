// Distributed Search Engine - Event Dispatcher (Phase 18D).
//
// Bounded asynchronous dispatch layer. A single worker thread drains the
// queue and publishes events to the MessageBroker.

#include "event_dispatcher.h"

#include <chrono>
#include <cstddef>
#include <utility>

#include "message.h"
#include "message_broker.h"

namespace dse {

EventDispatcher::EventDispatcher(MessageBroker& broker)
    : broker_(broker)
{
    // Use default Config values.
}

EventDispatcher::EventDispatcher(MessageBroker& broker, Config config)
    : config_(config)
    , broker_(broker)
{
}

EventDispatcher::~EventDispatcher()
{
    stop();
}

void EventDispatcher::start()
{
    bool expected = false;
    if (!running_.compare_exchange_strong(expected, true)) {
        return;  // Already started.
    }
    stopping_ = false;
    worker_ = std::thread(&EventDispatcher::worker_loop, this);
}

bool EventDispatcher::enqueue(std::string topic, std::string payload,
                              std::size_t timeout_ms)
{
    if (!running_.load() || stopping_.load()) {
        ++rejected_;
        return false;  // Not running or stopping — drop event.
    }

    Event event{std::move(topic), std::move(payload)};

    std::unique_lock<std::mutex> lock(mutex_);

    if (timeout_ms == 0) {
        // Non-blocking: try to enqueue only if there is space.
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

    // Timed wait: block until space is available or timeout expires.
    const auto deadline = std::chrono::steady_clock::now()
                        + std::chrono::milliseconds(timeout_ms);

    if (!enqueue_cv_.wait_until(lock, deadline, [this]() {
        return queue_.size() < config_.max_queue_size || stopping_.load();
    })) {
        // Timed out — queue still full.
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

void EventDispatcher::stop()
{
    {
        std::lock_guard<std::mutex> lock(mutex_);
        if (!running_.load()) {
            return;  // Already stopped or never started.
        }
        stopping_ = true;
    }

    // Wake the worker to drain and exit.
    dequeue_cv_.notify_one();
    enqueue_cv_.notify_all();

    if (worker_.joinable()) {
        worker_.join();
    }

    running_ = false;
}

EventDispatcher::Stats EventDispatcher::stats() const
{
    std::lock_guard<std::mutex> lock(mutex_);
    Stats s;
    s.enqueued      = enqueued_.load();
    s.published     = published_.load();
    s.rejected      = rejected_.load();
    s.broker_errors = broker_errors_.load();
    s.pending       = queue_.size();
    return s;
}

void EventDispatcher::worker_loop()
{
    while (true) {
        Event event;

        {
            std::unique_lock<std::mutex> lock(mutex_);

            // Wait for events or stop signal.
            dequeue_cv_.wait(lock, [this]() {
                return !queue_.empty() || stopping_.load();
            });

            // If stopping and queue is empty, exit.
            if (stopping_.load() && queue_.empty()) {
                break;
            }

            // If stopping but queue has events, drain them.
            if (queue_.empty()) {
                break;
            }

            event = std::move(queue_.front());
            queue_.pop_front();

            // Notify producers that space freed up.
            lock.unlock();
            enqueue_cv_.notify_one();
        }

        // Publish to broker (outside the lock).
        Message msg;
        msg.topic = std::move(event.topic);
        msg.payload = std::move(event.payload);

        try {
            broker_.publish(std::move(msg));
            ++published_;
        } catch (...) {
            ++broker_errors_;
        }
    }
}

} // namespace dse
