// Distributed Search Engine - Document Store (Phase 6B-1, 7A-1, 8A-1).
//
// In-memory storage for raw document content, keyed by document ID.
// This component is the SOURCE OF TRUTH for document data. The
// InvertedIndex is derived state that can be rebuilt from these documents.
//
// DocumentStore is a pure storage component. Whether empty or
// whitespace-only content is valid is an IngestionService concern;
// DocumentStore stores whatever it receives.
//
// Phase 7A-1 adds JSONL persistence: save() writes all documents to a
// file, load() reads them back, and all() exposes a snapshot of all
// documents for index rebuilding.
//
// Phase 8A-1 adds thread safety: all public methods are safe to call
// from multiple threads concurrently. Read operations (get, contains,
// size, all, save) acquire a shared lock. Write operations (add, load)
// acquire an exclusive lock.

#pragma once

#include <cstddef>
#include <cstdint>
#include <memory>
#include <optional>
#include <string>
#include <unordered_map>

#include "inverted_index.h"
#include "shared_mutex.h"

namespace dse {

// A document with its raw text content. The InvertedIndex never sees this
// struct — it only processes tokenized text via add_document().
struct Document {
    doc_id id;
    std::string content;

    friend bool operator==(const Document&, const Document&) = default;
};

// Thread-safe in-memory document storage with JSONL persistence.
//
// Contract (Phase 6B-1 + 7A-1 + 8A-1 + 9):
//   - documents are keyed by doc_id (reuses the same type as InvertedIndex);
//   - add() stores a new document; returns false and does NOT overwrite if
//     the ID already exists;
//   - update() replaces an existing document's content; returns true if the
//     document existed, false if the ID does not exist (never creates);
//   - remove() deletes an existing document; returns true if removed,
//     false if the ID did not exist;
//   - get() returns std::nullopt for unknown IDs;
//   - contains() and size() are O(1) average;
//   - no validation is performed on content (empty, whitespace, etc.);
//   - identical insertion sequences produce identical state (deterministic);
//   - save() writes all documents to a JSONL file, sorted by doc_id;
//   - load() reads documents from a JSONL file; corrupt lines are skipped
//     with a warning; duplicate IDs in the file are rejected (first wins);
//     the existing store is NOT modified if the file cannot be opened;
//   - all() returns a COPY of the complete document map (snapshot);
//     the caller owns the returned data and can safely iterate it without
//     holding any lock on the DocumentStore;
//   - all public methods are thread-safe: multiple threads may call any
//     combination of methods concurrently without data races.
//
// Complexity: O(1) average for add/update/remove/get/contains; O(D) space
// where D is the number of stored documents. Save is O(D log D + total
// content size).
class DocumentStore {
public:
    // Default constructor: initializes the internal mutex.
    DocumentStore();

    // Add a new document. Returns true on success. If a document with the
    // same ID already exists, returns false and leaves the existing document
    // unchanged.
    // Thread-safe: acquires exclusive lock.
    bool add(Document document);

    // Update an existing document's content. Returns true if the document
    // existed and was updated, false if the ID does not exist (never creates
    // a new document).
    // Thread-safe: acquires exclusive lock.
    bool update(Document document);

    // Remove an existing document. Returns true if the document was removed,
    // false if the ID did not exist.
    // Thread-safe: acquires exclusive lock.
    bool remove(doc_id id);

    // Return the stored document if present, or std::nullopt.
    // Thread-safe: acquires shared lock.
    std::optional<Document> get(doc_id id) const;

    // Whether a document with this ID exists. O(1) average.
    // Thread-safe: acquires shared lock.
    bool contains(doc_id id) const;

    // Number of stored documents. O(1).
    // Thread-safe: acquires shared lock.
    std::size_t size() const;

    // Return a COPY of the complete document map (snapshot).
    // Used for iterating all documents (e.g., rebuilding the inverted index).
    //
    // Thread-safe: acquires shared lock, copies data, releases lock.
    // The caller owns the returned map and can iterate it without holding
    // any lock on the DocumentStore. This is safe even if another thread
    // calls add() after all() returns.
    //
    // Note: this returns by value (not by reference) to avoid lifetime
    // issues. A reference would be unsafe because the lock is released
    // when all() returns, leaving the caller with an unprotected reference
    // that could be invalidated by a concurrent add() (rehash).
    std::unordered_map<doc_id, Document> all() const;

    // Save all documents to a JSONL file (complete rewrite).
    // Documents are written sorted by doc_id for deterministic output.
    // Returns true on success, false if the file cannot be opened/written.
    // Thread-safe: acquires shared lock, takes snapshot, releases lock,
    // then performs file I/O without holding any lock.
    bool save(const std::string& path) const;

    // Load documents from a JSONL file.
    // - If the file does not exist: returns false, store unchanged.
    // - If the file exists: parses line-by-line. Valid documents are added.
    //   Corrupt/malformed lines are skipped (warning printed to stderr).
    //   Duplicate IDs in the file are rejected (first wins).
    //   Returns true if the file was opened successfully (even if some
    //   lines were corrupt), false only if the file could not be opened.
    // Thread-safe: acquires exclusive lock. Loads into a temporary
    // container first, then replaces the current map atomically under
    // the lock.
    bool load(const std::string& path);

private:
    // Wrapped in unique_ptr so DocumentStore remains movable.
    // dse::SharedMutex is non-movable and non-copyable.
    mutable std::unique_ptr<SharedMutex> mutex_;
    std::unordered_map<doc_id, Document> documents_;
};

} // namespace dse
