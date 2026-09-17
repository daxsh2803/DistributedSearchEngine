// Distributed Search Engine - Event Store (Phase 18E).
//
// Reliable event delivery abstraction between document mutations and
// asynchronous dispatch. The EventStore tracks the lifecycle of every
// domain event from creation through delivery.
//
// Architecture:
//
//   ShardCoordinator
//         |
//         | create_event()  ← stable event_id assigned here
//         v
//   EventStore (tracks lifecycle)
//         |
//         | pending / retry / failed queries
//         v
//   EventDispatcher
//         |
//         | publish to MessageBroker
//         v
//   MessageBroker
//
// Lifecycle states:
//   PENDING     → Event created, awaiting dispatch
//   DISPATCHING → Event handed to dispatcher, delivery in progress
//   PUBLISHED   → Broker confirmed delivery (acknowledged)
//   FAILED      → Delivery failed after all retry attempts
//
// Key properties:
//   - Stable event_id: assigned once, never changes across retries/replay.
//   - Thread-safe: concurrent producers and consumers are safe.
//   - In-memory only: events are lost on process restart.
//     This is a documented limitation; a future persistent outbox
//     implementation can replace InMemoryEventStore without changing
//     ShardCoordinator or EventDispatcher.
//
// Delivery guarantee:
//   AT-LEAST-ONCE within process lifetime. A failed dispatch can be
//   retried with the SAME event ID, preserving identity semantics.

#pragma once

#include <cstddef>
#include <cstdint>
#include <memory>
#include <string>
#include <vector>

namespace dse {

// Unique event identifier. Stable across retries and replay.
using EventId = std::uint64_t;

// -----------------------------------------------------------------------
// Event lifecycle states
// -----------------------------------------------------------------------

enum class EventStatus {
    PENDING,      // Created, not yet dispatched
    DISPATCHING,  // Handed to dispatcher, delivery in progress
    PUBLISHED,    // Successfully delivered to broker
    FAILED        // All retry attempts exhausted
};

// -----------------------------------------------------------------------
// StoredEvent — full event record tracked by the EventStore
// -----------------------------------------------------------------------

struct StoredEvent {
    EventId id = 0;
    std::string topic;
    std::string key;
    std::string payload;
    EventStatus status = EventStatus::PENDING;
    std::size_t attempt_count = 0;
    std::uint64_t created_at_ns = 0;
    std::uint64_t updated_at_ns = 0;
    std::string error_message;
};

// -----------------------------------------------------------------------
// Delivery statistics
// -----------------------------------------------------------------------

struct DeliveryStats {
    std::size_t total       = 0;  // all events ever created
    std::size_t pending     = 0;
    std::size_t dispatching = 0;
    std::size_t published   = 0;
    std::size_t failed      = 0;
    std::size_t retried     = 0;
    std::size_t replayed    = 0;
};

// -----------------------------------------------------------------------
// EventStore — abstract interface
// -----------------------------------------------------------------------

class EventStore {
public:
    virtual ~EventStore() = default;

    // Non-copyable, non-movable.
    EventStore(const EventStore&) = delete;
    EventStore& operator=(const EventStore&) = delete;
    EventStore(EventStore&&) = delete;
    EventStore& operator=(EventStore&&) = delete;

    // --- Lifecycle ---

    // Create a new event and assign it a stable ID. The event starts
    // in PENDING state. Returns the assigned ID.
    virtual EventId create_event(std::string topic,
                                 std::string key,
                                 std::string payload) = 0;

    // Mark an event as being dispatched (PENDING → DISPATCHING).
    virtual void mark_dispatching(EventId id) = 0;

    // Record a delivery attempt without changing lifecycle state.
    // Called by the dispatcher on each publish attempt (including retries).
    virtual void record_attempt(EventId id) = 0;

    // Mark an event as successfully published (DISPATCHING → PUBLISHED).
    virtual void mark_published(EventId id) = 0;

    // Mark an event as failed after retry exhaustion
    // (DISPATCHING → FAILED). Includes the last error message.
    virtual void mark_failed(EventId id, const std::string& error) = 0;

    // Re-enqueue a failed event for retry (FAILED → PENDING).
    // The event ID is preserved. Returns true if requeued, false if
    // the event does not exist or is not in FAILED state.
    virtual bool requeue(EventId id) = 0;

    // --- Queries ---

    // Retrieve a stored event by ID. Returns nullptr if not found.
    virtual const StoredEvent* get(EventId id) const = 0;

    // Retrieve all events in a given status.
    virtual std::vector<StoredEvent> get_by_status(
        EventStatus status) const = 0;

    // Retrieve all events for a given topic in a given status.
    virtual std::vector<StoredEvent> get_by_topic(
        const std::string& topic, EventStatus status) const = 0;

    // Current delivery statistics.
    virtual DeliveryStats stats() const = 0;

protected:
    EventStore() = default;
};

// Factory function for the in-memory event store implementation.
std::unique_ptr<EventStore> create_in_memory_event_store();

} // namespace dse
