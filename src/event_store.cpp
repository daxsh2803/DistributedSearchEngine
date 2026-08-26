// Distributed Search Engine - In-Memory Event Store (Phase 18E).
//
// Thread-safe in-memory implementation of the EventStore interface.
// Tracks event lifecycle through PENDING → DISPATCHING → PUBLISHED/FAILED
// with support for retry/replay via FAILED → PENDING requeue.
//
// This is an in-memory implementation. Events are lost on process restart.
// The abstraction is designed so a future persistent outbox can replace
// this implementation without modifying ShardCoordinator or EventDispatcher.

#include "event_store.h"

#include <chrono>
#include <mutex>
#include <unordered_map>
#include <vector>

namespace dse {

class InMemoryEventStore final : public EventStore {
public:
    InMemoryEventStore() = default;

    // --- Lifecycle ---

    EventId create_event(std::string topic, std::string payload) override
    {
        std::lock_guard lock(mutex_);
        EventId id = next_id_++;
        const auto now = now_ns();
        StoredEvent event;
        event.id = id;
        event.topic = std::move(topic);
        event.payload = std::move(payload);
        event.status = EventStatus::PENDING;
        event.attempt_count = 0;
        event.created_at_ns = now;
        event.updated_at_ns = now;
        events_.emplace(id, std::move(event));
        ++stats_.total;
        ++stats_.pending;
        return id;
    }

    void mark_dispatching(EventId id) override
    {
        std::lock_guard lock(mutex_);
        auto it = events_.find(id);
        if (it == events_.end()) return;
        auto& ev = it->second;
        if (ev.status != EventStatus::PENDING) return;
        ev.status = EventStatus::DISPATCHING;
        ev.updated_at_ns = now_ns();
        --stats_.pending;
        ++stats_.dispatching;
    }

    void record_attempt(EventId id) override
    {
        std::lock_guard lock(mutex_);
        auto it = events_.find(id);
        if (it == events_.end()) return;
        ++it->second.attempt_count;
        it->second.updated_at_ns = now_ns();
    }

    void mark_published(EventId id) override
    {
        std::lock_guard lock(mutex_);
        auto it = events_.find(id);
        if (it == events_.end()) return;
        auto& ev = it->second;
        if (ev.status != EventStatus::DISPATCHING) return;
        ev.status = EventStatus::PUBLISHED;
        ev.updated_at_ns = now_ns();
        --stats_.dispatching;
        ++stats_.published;
    }

    void mark_failed(EventId id, const std::string& error) override
    {
        std::lock_guard lock(mutex_);
        auto it = events_.find(id);
        if (it == events_.end()) return;
        auto& ev = it->second;
        if (ev.status != EventStatus::DISPATCHING) return;
        ev.status = EventStatus::FAILED;
        ev.error_message = error;
        ev.updated_at_ns = now_ns();
        --stats_.dispatching;
        ++stats_.failed;
    }

    bool requeue(EventId id) override
    {
        std::lock_guard lock(mutex_);
        auto it = events_.find(id);
        if (it == events_.end()) return false;
        auto& ev = it->second;
        if (ev.status != EventStatus::FAILED) return false;
        ev.status = EventStatus::PENDING;
        ev.updated_at_ns = now_ns();
        --stats_.failed;
        ++stats_.pending;
        ++stats_.retried;
        return true;
    }

    // --- Queries ---

    const StoredEvent* get(EventId id) const override
    {
        std::lock_guard lock(mutex_);
        auto it = events_.find(id);
        if (it == events_.end()) return nullptr;
        return &it->second;
    }

    std::vector<StoredEvent> get_by_status(
        EventStatus status) const override
    {
        std::lock_guard lock(mutex_);
        std::vector<StoredEvent> result;
        for (const auto& [id, ev] : events_) {
            if (ev.status == status) {
                result.push_back(ev);
            }
        }
        return result;
    }

    std::vector<StoredEvent> get_by_topic(
        const std::string& topic, EventStatus status) const override
    {
        std::lock_guard lock(mutex_);
        std::vector<StoredEvent> result;
        for (const auto& [id, ev] : events_) {
            if (ev.status == status && ev.topic == topic) {
                result.push_back(ev);
            }
        }
        return result;
    }

    DeliveryStats stats() const override
    {
        std::lock_guard lock(mutex_);
        return stats_;
    }

private:
    static std::uint64_t now_ns()
    {
        return static_cast<std::uint64_t>(
            std::chrono::steady_clock::now().time_since_epoch().count());
    }

    mutable std::mutex mutex_;
    std::unordered_map<EventId, StoredEvent> events_;
    EventId next_id_ = 1;
    DeliveryStats stats_;
};

} // namespace dse

// Provide the factory function so that callers can create an
// InMemoryEventStore through the EventStore interface.
namespace dse {
std::unique_ptr<EventStore> create_in_memory_event_store()
{
    return std::make_unique<InMemoryEventStore>();
}
} // namespace dse
