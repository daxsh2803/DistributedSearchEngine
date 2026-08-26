// Distributed Search Engine - Persistent Event Store (Phase 18F).
//
// Durable outbox implementation of the EventStore interface.
// Persists event lifecycle to JSONL files so that events survive
// process restart. Designed as a drop-in replacement for
// InMemoryEventStore — ShardCoordinator and EventDispatcher
// require no code changes.
//
// Architecture:
//
//   ShardCoordinator
//         |
//         | create_event()  <- stable event_id assigned here
//         v
//   PersistentEventStore  (implements EventStore)
//         |
//         | data/events/events.jsonl  <- append log
//         | data/events/events.meta   <- metadata (next_id, stats)
//         |
//         v
//   EventDispatcher
//         |
//         v
//   MessageBroker
//
// Recovery semantics:
//   PENDING     -> PENDING     (retry on next dispatch)
//   DISPATCHING -> PENDING     (was in-flight at crash, retry)
//   PUBLISHED   -> PUBLISHED   (don't redeliver)
//   FAILED      -> FAILED      (available for replay)
//
// Durability:
//   - flush() persists to disk via write-temp-then-rename
//   - Constructor loads existing events from disk
//   - Destructor calls flush() for graceful shutdown
//   - At-least-once delivery guarantee (NOT exactly-once)
//
// Thread safety:
//   All public methods are safe for concurrent use.
//   Uses std::mutex to protect all shared state.
//   File I/O is performed under lock to ensure consistency
//   between in-memory state and on-disk state.

#pragma once

#include <cstdint>
#include <memory>
#include <mutex>
#include <string>
#include <unordered_map>
#include <vector>

#include "event_store.h"

namespace dse {

// Persistent implementation of EventStore backed by JSONL files.
//
// The store maintains an in-memory cache of all events and persists
// them to disk via flush(). On construction, it loads any previously
// persisted events and applies recovery state transitions.
//
// Usage:
//   PersistentEventStore store("data/events");
//   EventDispatcher dispatcher(broker, store);
//   dispatcher.start();
//   // ... use store through ShardCoordinator ...
//   dispatcher.stop();
//   store.flush();  // or rely on destructor
//
// The constructor creates the directory if it doesn't exist.
// Events are persisted to <directory>/events.jsonl.
// Metadata is persisted to <directory>/events.meta.
class PersistentEventStore final : public EventStore {
public:
    // Create a persistent event store backed by the given directory.
    // Loads existing events from disk. Creates the directory if needed.
    // Throws std::runtime_error if the directory cannot be created.
    explicit PersistentEventStore(std::string directory);

    // Destructor calls flush() to persist any unsaved changes.
    ~PersistentEventStore() override;

    // Non-copyable, non-movable (owns file handles during flush).
    PersistentEventStore(const PersistentEventStore&) = delete;
    PersistentEventStore& operator=(const PersistentEventStore&) = delete;
    PersistentEventStore(PersistentEventStore&&) = delete;
    PersistentEventStore& operator=(PersistentEventStore&&) = delete;

    // --- EventStore interface ---

    EventId create_event(std::string topic, std::string payload) override;
    void mark_dispatching(EventId id) override;
    void record_attempt(EventId id) override;
    void mark_published(EventId id) override;
    void mark_failed(EventId id, const std::string& error) override;
    bool requeue(EventId id) override;

    const StoredEvent* get(EventId id) const override;
    std::vector<StoredEvent> get_by_status(EventStatus status) const override;
    std::vector<StoredEvent> get_by_topic(
        const std::string& topic, EventStatus status) const override;
    DeliveryStats stats() const override;

    // --- Persistence ---

    // Persist all events to disk. Thread-safe.
    // Uses write-temp-then-rename for crash safety.
    // No-op if no changes have been made since the last flush.
    void flush();

    // Number of events currently in the store.
    std::size_t size() const;

private:
    // --- File I/O helpers ---

    // Load events from disk. Called by constructor.
    // Applies recovery state transitions (DISPATCHING -> PENDING).
    // Returns true on success, false if no data to load.
    bool load_from_disk();

    // Persist events and metadata to disk.
    // Writes to a temporary file, then renames for crash safety.
    bool save_to_disk() const;

    // Create the persistence directory if it doesn't exist.
    void ensure_directory() const;

    // --- In-memory state ---

    mutable std::mutex mutex_;
    std::unordered_map<EventId, StoredEvent> events_;
    EventId next_id_ = 1;
    DeliveryStats stats_;
    bool dirty_ = false;  // true if in-memory state differs from disk

    // --- Persistence configuration ---

    std::string directory_;
    std::string events_path_;
    std::string meta_path_;
};

// Factory function for creating a persistent event store.
std::unique_ptr<EventStore> create_persistent_event_store(
    const std::string& directory);

} // namespace dse
