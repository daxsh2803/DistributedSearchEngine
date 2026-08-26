// Distributed Search Engine - Persistent Event Store (Phase 18F).
//
// JSONL-backed implementation of the EventStore interface.
// Follows the same persistence pattern as DocumentStore (ADR-006):
//   - One JSON object per line
//   - Corrupt lines are skipped on load
//   - Deterministic output (sorted by event ID)
//   - Safe file replacement via write-temp-then-rename

#include "persistent_event_store.h"

#include <algorithm>
#include <chrono>
#include <cstdio>
#include <cstdlib>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <stdexcept>
#include <string>
#include <utility>
#include <vector>

#include <nlohmann/json.hpp>

#include "event_store.h"

namespace dse {

// ---------------------------------------------------------------------------
// EventStatus <-> string conversion helpers
// ---------------------------------------------------------------------------

namespace {

const char* status_to_string(EventStatus s)
{
    switch (s) {
        case EventStatus::PENDING:     return "PENDING";
        case EventStatus::DISPATCHING: return "DISPATCHING";
        case EventStatus::PUBLISHED:   return "PUBLISHED";
        case EventStatus::FAILED:      return "FAILED";
    }
    return "UNKNOWN";
}

EventStatus status_from_string(const std::string& s)
{
    if (s == "PENDING")     return EventStatus::PENDING;
    if (s == "DISPATCHING") return EventStatus::DISPATCHING;
    if (s == "PUBLISHED")   return EventStatus::PUBLISHED;
    if (s == "FAILED")      return EventStatus::FAILED;
    return EventStatus::PENDING;  // default to PENDING for safety
}

std::uint64_t now_ns()
{
    return static_cast<std::uint64_t>(
        std::chrono::steady_clock::now().time_since_epoch().count());
}

} // anonymous namespace

// ---------------------------------------------------------------------------
// Construction / destruction
// ---------------------------------------------------------------------------

PersistentEventStore::PersistentEventStore(std::string directory)
    : directory_(std::move(directory))
    , events_path_(directory_ + "/events.jsonl")
    , meta_path_(directory_ + "/events.meta")
{
    ensure_directory();
    load_from_disk();
}

PersistentEventStore::~PersistentEventStore()
{
    flush();
}

// ---------------------------------------------------------------------------
// EventStore interface
// ---------------------------------------------------------------------------

EventId PersistentEventStore::create_event(std::string topic,
                                            std::string payload)
{
    std::lock_guard<std::mutex> lock(mutex_);
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
    dirty_ = true;
    return id;
}

void PersistentEventStore::mark_dispatching(EventId id)
{
    std::lock_guard<std::mutex> lock(mutex_);
    auto it = events_.find(id);
    if (it == events_.end()) return;
    auto& ev = it->second;
    if (ev.status != EventStatus::PENDING) return;
    ev.status = EventStatus::DISPATCHING;
    ev.updated_at_ns = now_ns();
    --stats_.pending;
    ++stats_.dispatching;
    dirty_ = true;
}

void PersistentEventStore::record_attempt(EventId id)
{
    std::lock_guard<std::mutex> lock(mutex_);
    auto it = events_.find(id);
    if (it == events_.end()) return;
    ++it->second.attempt_count;
    it->second.updated_at_ns = now_ns();
    dirty_ = true;
}

void PersistentEventStore::mark_published(EventId id)
{
    std::lock_guard<std::mutex> lock(mutex_);
    auto it = events_.find(id);
    if (it == events_.end()) return;
    auto& ev = it->second;
    if (ev.status != EventStatus::DISPATCHING) return;
    ev.status = EventStatus::PUBLISHED;
    ev.updated_at_ns = now_ns();
    --stats_.dispatching;
    ++stats_.published;
    dirty_ = true;
}

void PersistentEventStore::mark_failed(EventId id, const std::string& error)
{
    std::lock_guard<std::mutex> lock(mutex_);
    auto it = events_.find(id);
    if (it == events_.end()) return;
    auto& ev = it->second;
    if (ev.status != EventStatus::DISPATCHING) return;
    ev.status = EventStatus::FAILED;
    ev.error_message = error;
    ev.updated_at_ns = now_ns();
    --stats_.dispatching;
    ++stats_.failed;
    dirty_ = true;
}

bool PersistentEventStore::requeue(EventId id)
{
    std::lock_guard<std::mutex> lock(mutex_);
    auto it = events_.find(id);
    if (it == events_.end()) return false;
    auto& ev = it->second;
    if (ev.status != EventStatus::FAILED) return false;
    ev.status = EventStatus::PENDING;
    ev.updated_at_ns = now_ns();
    --stats_.failed;
    ++stats_.pending;
    ++stats_.retried;
    dirty_ = true;
    return true;
}

const StoredEvent* PersistentEventStore::get(EventId id) const
{
    std::lock_guard<std::mutex> lock(mutex_);
    auto it = events_.find(id);
    if (it == events_.end()) return nullptr;
    return &it->second;
}

std::vector<StoredEvent> PersistentEventStore::get_by_status(
    EventStatus status) const
{
    std::lock_guard<std::mutex> lock(mutex_);
    std::vector<StoredEvent> result;
    for (const auto& [id, ev] : events_) {
        if (ev.status == status) {
            result.push_back(ev);
        }
    }
    return result;
}

std::vector<StoredEvent> PersistentEventStore::get_by_topic(
    const std::string& topic, EventStatus status) const
{
    std::lock_guard<std::mutex> lock(mutex_);
    std::vector<StoredEvent> result;
    for (const auto& [id, ev] : events_) {
        if (ev.status == status && ev.topic == topic) {
            result.push_back(ev);
        }
    }
    return result;
}

DeliveryStats PersistentEventStore::stats() const
{
    std::lock_guard<std::mutex> lock(mutex_);
    return stats_;
}

// ---------------------------------------------------------------------------
// Persistence
// ---------------------------------------------------------------------------

void PersistentEventStore::flush()
{
    std::lock_guard<std::mutex> lock(mutex_);
    if (!dirty_) return;
    if (save_to_disk()) {
        dirty_ = false;
    }
}

std::size_t PersistentEventStore::size() const
{
    std::lock_guard<std::mutex> lock(mutex_);
    return events_.size();
}

// ---------------------------------------------------------------------------
// Internal: Load from disk
// ---------------------------------------------------------------------------

bool PersistentEventStore::load_from_disk()
{
    // Step 1: Load metadata if it exists.
    {
        std::ifstream meta_in(meta_path_);
        if (meta_in.is_open()) {
            std::string content(
                (std::istreambuf_iterator<char>(meta_in)),
                std::istreambuf_iterator<char>());

            // Handle empty file (crash during write).
            if (!content.empty()) {
                try {
                    auto j = nlohmann::json::parse(content);
                    if (j.contains("next_event_id")) {
                        next_id_ = j["next_event_id"].get<EventId>();
                    }
                    if (j.contains("total")) {
                        stats_.total = j["total"].get<std::size_t>();
                    }
                    if (j.contains("published")) {
                        stats_.published = j["published"].get<std::size_t>();
                    }
                    if (j.contains("failed")) {
                        stats_.failed = j["failed"].get<std::size_t>();
                    }
                    if (j.contains("retried")) {
                        stats_.retried = j["retried"].get<std::size_t>();
                    }
                } catch (const nlohmann::json::exception& e) {
                    std::cerr << "PersistentEventStore: corrupt metadata, "
                              << "using defaults: " << e.what() << "\n";
                }
            }
        }
    }

    // Step 2: Load events from JSONL.
    std::ifstream events_in(events_path_);
    if (!events_in.is_open()) {
        return false;  // No events file — first run.
    }

    std::string line;
    std::size_t line_number = 0;
    std::size_t loaded_count = 0;
    std::size_t skipped_count = 0;
    std::size_t restored_pending = 0;
    std::size_t restored_dispatching = 0;
    std::size_t restored_published = 0;
    std::size_t restored_failed = 0;

    while (std::getline(events_in, line)) {
        ++line_number;

        // Skip empty lines (trailing newline, blank lines).
        if (line.empty()) {
            continue;
        }

        // Parse the JSON line.
        nlohmann::json j;
        try {
            j = nlohmann::json::parse(line);
        } catch (const nlohmann::json::parse_error& e) {
            std::cerr << "PersistentEventStore: skipping corrupt line "
                      << line_number << ": " << e.what() << "\n";
            ++skipped_count;
            continue;
        }

        // Validate required fields.
        if (!j.contains("id") || !j["id"].is_number_unsigned()) {
            std::cerr << "PersistentEventStore: skipping line "
                      << line_number
                      << ": missing or invalid 'id' field\n";
            ++skipped_count;
            continue;
        }

        if (!j.contains("topic") || !j["topic"].is_string()) {
            std::cerr << "PersistentEventStore: skipping line "
                      << line_number
                      << ": missing or invalid 'topic' field\n";
            ++skipped_count;
            continue;
        }

        if (!j.contains("status") || !j["status"].is_string()) {
            std::cerr << "PersistentEventStore: skipping line "
                      << line_number
                      << ": missing or invalid 'status' field\n";
            ++skipped_count;
            continue;
        }

        const auto id = j["id"].get<EventId>();

        // Reject duplicate IDs in the file (first wins).
        if (events_.contains(id)) {
            std::cerr << "PersistentEventStore: skipping line "
                      << line_number
                      << ": duplicate event ID " << id << "\n";
            ++skipped_count;
            continue;
        }

        // Reconstruct the StoredEvent.
        StoredEvent ev;
        ev.id = id;
        ev.topic = j["topic"].get<std::string>();
        ev.payload = j.contains("payload")
            ? j["payload"].get<std::string>() : std::string();
        ev.status = status_from_string(j["status"].get<std::string>());
        ev.attempt_count = j.contains("attempt_count")
            ? j["attempt_count"].get<std::size_t>() : 0;
        ev.created_at_ns = j.contains("created_at_ns")
            ? j["created_at_ns"].get<std::uint64_t>() : 0;
        ev.updated_at_ns = j.contains("updated_at_ns")
            ? j["updated_at_ns"].get<std::uint64_t>() : 0;
        ev.error_message = j.contains("error_message")
            ? j["error_message"].get<std::string>() : std::string();

        // --- Recovery state transitions ---
        // DISPATCHING means the process crashed while the broker
        // publish was in flight. Reset to PENDING so the dispatcher
        // retries on the next startup.
        if (ev.status == EventStatus::DISPATCHING) {
            ev.status = EventStatus::PENDING;
            ev.updated_at_ns = now_ns();
            ++restored_dispatching;
        }

        // Track status counts for stats restoration.
        switch (ev.status) {
            case EventStatus::PENDING:
                ++restored_pending;
                break;
            case EventStatus::PUBLISHED:
                ++restored_published;
                break;
            case EventStatus::FAILED:
                ++restored_failed;
                break;
            default:
                break;
        }

        // Ensure next_id_ is greater than any loaded ID.
        if (id >= next_id_) {
            next_id_ = id + 1;
        }

        events_.emplace(id, std::move(ev));
        ++loaded_count;
    }

    // Step 3: Restore stats from loaded events (authoritative).
    stats_.pending = restored_pending;
    stats_.dispatching = 0;  // All DISPATCHING reset to PENDING
    stats_.published = restored_published;
    stats_.failed = restored_failed;

    if (skipped_count > 0) {
        std::cerr << "PersistentEventStore: loaded " << loaded_count
                  << " events, skipped " << skipped_count
                  << " invalid lines from " << events_path_ << "\n";
    }

    if (loaded_count > 0) {
        std::cerr << "PersistentEventStore: recovered " << loaded_count
                  << " events (" << restored_pending << " pending, "
                  << restored_dispatching
                  << " dispatching reset to pending, "
                  << restored_published << " published, "
                  << restored_failed << " failed)\n";
    }

    return loaded_count > 0;
}

// ---------------------------------------------------------------------------
// Internal: Save to disk
// ---------------------------------------------------------------------------

bool PersistentEventStore::save_to_disk() const
{
    // Step 1: Take a snapshot of events under lock (already held by caller).
    // Sort by event ID for deterministic output.
    std::vector<std::pair<EventId, StoredEvent>> snapshot;
    snapshot.reserve(events_.size());
    for (const auto& [id, ev] : events_) {
        snapshot.emplace_back(id, ev);
    }
    std::sort(snapshot.begin(), snapshot.end(),
              [](const auto& a, const auto& b) {
                  return a.first < b.first;
              });

    // Step 2: Write events to a temporary file.
    // This prevents data loss if the process crashes mid-write.
    const std::string temp_path = events_path_ + ".tmp";
    {
        std::ofstream ofs(temp_path);
        if (!ofs.is_open()) {
            std::cerr << "PersistentEventStore: failed to open "
                      << temp_path << " for writing\n";
            return false;
        }

        for (const auto& [id, ev] : snapshot) {
            nlohmann::json j;
            j["id"] = ev.id;
            j["topic"] = ev.topic;
            j["payload"] = ev.payload;
            j["status"] = status_to_string(ev.status);
            j["attempt_count"] = ev.attempt_count;
            j["created_at_ns"] = ev.created_at_ns;
            j["updated_at_ns"] = ev.updated_at_ns;
            j["error_message"] = ev.error_message;
            ofs << j.dump() << "\n";
        }

        ofs.flush();
        if (!ofs.good()) {
            std::cerr << "PersistentEventStore: failed to write "
                      << temp_path << "\n";
            return false;
        }
    }

    // Step 3: Atomically replace the events file.
    // On POSIX, rename() is atomic for same-filesystem moves.
    // On Windows, std::filesystem::rename may not be atomic if the
    // destination exists. We remove the destination first for safety.
    std::error_code ec;
    std::filesystem::remove(events_path_, ec);  // ignore error
    std::filesystem::rename(temp_path, events_path_, ec);
    if (ec) {
        std::cerr << "PersistentEventStore: failed to rename "
                  << temp_path << " to " << events_path_
                  << ": " << ec.message() << "\n";
        return false;
    }

    // Step 4: Write metadata (non-critical, best-effort).
    {
        std::ofstream meta_ofs(meta_path_);
        if (meta_ofs.is_open()) {
            nlohmann::json meta;
            meta["version"] = 1;
            meta["next_event_id"] = next_id_;
            meta["total"] = stats_.total;
            meta["pending"] = stats_.pending;
            meta["dispatching"] = stats_.dispatching;
            meta["published"] = stats_.published;
            meta["failed"] = stats_.failed;
            meta["retried"] = stats_.retried;
            meta_ofs << meta.dump(2) << "\n";
            meta_ofs.flush();
        }
    }

    return true;
}

// ---------------------------------------------------------------------------
// Internal: Ensure directory exists
// ---------------------------------------------------------------------------

void PersistentEventStore::ensure_directory() const
{
    std::error_code ec;
    std::filesystem::create_directories(directory_, ec);
    if (ec) {
        throw std::runtime_error(
            "PersistentEventStore: failed to create directory '"
            + directory_ + "': " + ec.message());
    }
}

// ---------------------------------------------------------------------------
// Factory function
// ---------------------------------------------------------------------------

std::unique_ptr<EventStore> create_persistent_event_store(
    const std::string& directory)
{
    return std::make_unique<PersistentEventStore>(directory);
}

} // namespace dse
