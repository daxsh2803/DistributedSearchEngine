// Distributed Search Engine - Shard (Phase 10).
//
// A Shard is the ownership boundary for a partition of documents.
// Each Shard owns one DocumentStore and one InvertedIndex, protected
// by its own mutation mutex. Multiple shards can operate independently.
//
// Lifecycle invariant (preserved from Phase 9):
//   DocumentStore state == InvertedIndex state
//   for every shard.
//
// Thread safety:
//   - Write operations (add/update/remove) acquire the shard mutation mutex.
//   - Read operations acquire shared locks on internal components.
//   - The coordinator must NOT acquire shard locks manually.

#pragma once

#include <cstddef>
#include <memory>
#include <optional>
#include <string>

#include "document_store.h"
#include "inverted_index.h"
#include "shared_mutex.h"

namespace dse {

class Shard {
public:
    // Construct a shard with an optional persistence path.
    // If data_path is empty, persistence is disabled.
    explicit Shard(std::string data_path = "");

    // --- Write operations ---
    // These operations maintain the lifecycle invariant: the document
    // is added to both DocumentStore and InvertedIndex atomically.

    // Add a new document. Returns true on success, false if the document
    // ID already exists or content validation fails.
    bool add_document(doc_id id, const std::string& content);

    // Update an existing document's content. Returns true if the document
    // existed and was updated, false if it did not exist.
    bool update_document(doc_id id, const std::string& content);

    // Remove an existing document. Returns true if removed, false if
    // the document did not exist.
    bool remove_document(doc_id id);

    // --- Read operations ---

    // Get a document by ID. Returns std::nullopt if not found.
    std::optional<Document> get_document(doc_id id) const;

    // Whether a document exists in this shard.
    bool contains_document(doc_id id) const;

    // Number of documents in this shard.
    std::size_t document_count() const;

    // --- Persistence ---

    // Save all documents to the shard's persistence path.
    // Returns true on success or if persistence is not configured.
    bool save() const;

    // Load documents from the shard's persistence path and rebuild
    // the inverted index. Returns true on success, false if the file
    // cannot be opened or no path is configured.
    bool load();

    // --- Accessors for coordinator (read-only) ---

    // Const reference to the document store. The coordinator must not
    // acquire the shard's mutation lock directly.
    const DocumentStore& store() const;

    // Const reference to the inverted index. The coordinator must not
    // acquire the shard's mutation lock directly.
    const InvertedIndex& index() const;

    // The persistence path for this shard.
    const std::string& data_path() const;

private:
    // Content validation: reject empty or whitespace-only content.
    static bool is_valid_content(const std::string& content);

    DocumentStore store_;
    InvertedIndex index_;
    std::string data_path_;

    // Per-shard mutation mutex. Serializes create/update/delete operations.
    // Search (read-only) operations do NOT acquire this mutex.
    mutable std::unique_ptr<SharedMutex> mutation_mutex_;
};

} // namespace dse
