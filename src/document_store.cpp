// Distributed Search Engine - Document Store (Phase 6B-1, 7A-1, 8A-1).
//
// Implementation of the contract in src/document_store.h:
//   - add() inserts a new document; rejects duplicate IDs;
//   - get() returns an owning copy or std::nullopt;
//   - contains() is a simple map lookup;
//   - size() returns the map size;
//   - all() returns a COPY of the document map (snapshot);
//   - save() writes all documents as JSONL, sorted by doc_id;
//   - load() reads JSONL, skips corrupt lines, rejects duplicate IDs.
//
// Thread-safety: all public methods are safe to call from multiple threads.
// Read operations acquire a shared lock. Write operations acquire an
// exclusive lock. The all() method acquires a shared lock, copies the
// data, then releases the lock — the caller owns the snapshot.

#include "document_store.h"

#include <algorithm>
#include <cstddef>
#include <cstdlib>
#include <fstream>
#include <iostream>
#include <memory>
#include <mutex>
#include <optional>
#include <shared_mutex>
#include "shared_mutex.h"
#include <string>
#include <utility>
#include <vector>

#include <nlohmann/json.hpp>

namespace dse {

// Constructor: initialize the mutex.
DocumentStore::DocumentStore()
    : mutex_(std::make_unique<SharedMutex>())
{
}

bool DocumentStore::add(Document document)
{
    // Exclusive lock: only one thread can add at a time.
    // This also prevents concurrent reads during mutation.
    std::unique_lock lock(*mutex_);

    // Try to insert. If the ID already exists, insert() returns false
    // and leaves the existing entry untouched.
    const auto [it, inserted] = documents_.try_emplace(
        document.id, std::move(document));
    return inserted;
}

bool DocumentStore::update(Document document)
{
    // Exclusive lock: only one thread can update at a time.
    std::unique_lock lock(*mutex_);

    // Find the existing document. If it doesn't exist, return false.
    auto it = documents_.find(document.id);
    if (it == documents_.end()) {
        return false;
    }

    // Replace the content of the existing document.
    it->second.content = std::move(document.content);
    return true;
}

bool DocumentStore::remove(doc_id id)
{
    // Exclusive lock: only one thread can remove at a time.
    std::unique_lock lock(*mutex_);

    // Erase returns the number of elements removed (0 or 1).
    return documents_.erase(id) > 0;
}

std::optional<Document> DocumentStore::get(doc_id id) const
{
    // Shared lock: multiple threads can read concurrently.
    std::shared_lock lock(*mutex_);

    const auto it = documents_.find(id);
    if (it == documents_.end()) {
        return std::nullopt;
    }
    return it->second;  // Copy the document (returns owning optional)
}

bool DocumentStore::contains(doc_id id) const
{
    // Shared lock: multiple threads can read concurrently.
    std::shared_lock lock(*mutex_);

    return documents_.contains(id);
}

std::size_t DocumentStore::size() const
{
    // Shared lock: multiple threads can read concurrently.
    std::shared_lock lock(*mutex_);

    return documents_.size();
}

std::unordered_map<doc_id, Document> DocumentStore::all() const
{
    // Shared lock: multiple threads can read concurrently.
    // Copy the entire map while holding the lock, then release the lock.
    // The caller owns the returned snapshot and can iterate it safely
    // without holding any lock on the DocumentStore.
    std::shared_lock lock(*mutex_);

    // Return a copy (not a reference) to avoid lifetime issues.
    // A reference would be unsafe because the lock is released when
    // all() returns, leaving the caller with an unprotected reference
    // that could be invalidated by a concurrent add() (rehash).
    return documents_;
}

// ---------------------------------------------------------------------------
// Persistence
// ---------------------------------------------------------------------------

bool DocumentStore::save(const std::string& path) const
{
    // Step 1: Take a snapshot under shared lock.
    // This ensures we write a consistent set of documents.
    std::vector<std::pair<doc_id, Document>> snapshot;
    {
        std::shared_lock lock(*mutex_);
        snapshot.reserve(documents_.size());
        for (const auto& [id, doc] : documents_) {
            snapshot.emplace_back(id, doc);
        }
    }
    // Lock released — file I/O happens without holding any lock.

    // Step 2: Sort by doc_id for deterministic output.
    std::sort(snapshot.begin(), snapshot.end(),
              [](const auto& a, const auto& b) { return a.first < b.first; });

    // Step 3: Write to file.
    std::ofstream ofs(path);
    if (!ofs.is_open()) {
        return false;
    }

    for (const auto& [id, doc] : snapshot) {
        nlohmann::json j;
        j["id"] = doc.id;
        j["content"] = doc.content;
        ofs << j.dump() << "\n";
    }

    ofs.flush();
    return ofs.good();
}

bool DocumentStore::load(const std::string& path)
{
    // Step 1: Try to open the file without holding any lock.
    // If the file doesn't exist, return false quickly.
    std::ifstream ifs(path);
    if (!ifs.is_open()) {
        return false;
    }

    // Step 2: Parse into a temporary container (no lock held).
    // This is safe because we haven't modified the store yet.
    std::unordered_map<doc_id, Document> loaded;
    std::string line;
    std::size_t line_number = 0;
    std::size_t loaded_count = 0;
    std::size_t skipped_count = 0;

    while (std::getline(ifs, line)) {
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
            std::cerr << "DocumentStore::load: skipping corrupt line "
                      << line_number << ": " << e.what() << "\n";
            ++skipped_count;
            continue;
        }

        // Validate required fields and types.
        if (!j.contains("id") || !j["id"].is_number_unsigned()) {
            std::cerr << "DocumentStore::load: skipping line " << line_number
                      << ": missing or invalid 'id' field\n";
            ++skipped_count;
            continue;
        }

        if (!j.contains("content") || !j["content"].is_string()) {
            std::cerr << "DocumentStore::load: skipping line " << line_number
                      << ": missing or invalid 'content' field\n";
            ++skipped_count;
            continue;
        }

        const auto id = j["id"].get<doc_id>();

        // Reject duplicate IDs in the file (first wins).
        if (loaded.contains(id)) {
            std::cerr << "DocumentStore::load: skipping line " << line_number
                      << ": duplicate document ID " << id << "\n";
            ++skipped_count;
            continue;
        }

        loaded.emplace(id, Document{id, j["content"].get<std::string>()});
        ++loaded_count;
    }

    // Step 3: Atomically replace the current store under exclusive lock.
    {
        std::unique_lock lock(*mutex_);
        documents_ = std::move(loaded);
    }

    if (skipped_count > 0) {
        std::cerr << "DocumentStore::load: loaded " << loaded_count
                  << " documents, skipped " << skipped_count
                  << " invalid lines from " << path << "\n";
    }

    return true;
}

} // namespace dse
