// Distributed Search Engine - Shard (Phase 10).
//
// Implementation of the contract in src/shard.h.
// Each shard owns a DocumentStore + InvertedIndex + mutation mutex.
// Lifecycle operations maintain the invariant that DocumentStore state
// equals InvertedIndex state within the shard.

#include "shard.h"

#include <algorithm>
#include <cstddef>
#include <string>
#include <utility>

#include "document_store.h"
#include "inverted_index.h"
#include "shared_mutex.h"
#include "tokenizer.h"

namespace dse {

Shard::Shard(std::string data_path)
    : data_path_(std::move(data_path))
    , mutation_mutex_(std::make_unique<SharedMutex>())
{
}

bool Shard::is_valid_content(const std::string& content)
{
    if (content.empty()) {
        return false;
    }
    for (const char c : content) {
        if (c != ' ' && c != '\t' && c != '\n' && c != '\r') {
            return true;
        }
    }
    return false;
}

// ---------------------------------------------------------------------------
// Write operations
// ---------------------------------------------------------------------------

bool Shard::add_document(doc_id id, const std::string& content)
{
    if (!is_valid_content(content)) {
        return false;
    }

    std::unique_lock lock(*mutation_mutex_);

    // Atomic check-and-claim: store_.add() returns false if ID already exists.
    if (!store_.add(Document{id, content})) {
        return false;
    }

    // Index the document.
    index_.add_document(id, content);

    return true;
}

bool Shard::update_document(doc_id id, const std::string& content)
{
    if (!is_valid_content(content)) {
        return false;
    }

    std::unique_lock lock(*mutation_mutex_);

    // Verify the document exists.
    if (!store_.contains(id)) {
        return false;
    }

    // Remove old index representation.
    index_.remove_document(id);

    // Replace the content in DocumentStore.
    store_.update(Document{id, content});

    // Add the new index representation.
    index_.add_document(id, content);

    return true;
}

bool Shard::remove_document(doc_id id)
{
    std::unique_lock lock(*mutation_mutex_);

    // Verify the document exists.
    if (!store_.contains(id)) {
        return false;
    }

    // Remove from index.
    index_.remove_document(id);

    // Remove from store.
    store_.remove(id);

    return true;
}

// ---------------------------------------------------------------------------
// Read operations
// ---------------------------------------------------------------------------

std::optional<Document> Shard::get_document(doc_id id) const
{
    return store_.get(id);
}

bool Shard::contains_document(doc_id id) const
{
    return store_.contains(id);
}

std::size_t Shard::document_count() const
{
    return store_.size();
}

// ---------------------------------------------------------------------------
// Persistence
// ---------------------------------------------------------------------------

bool Shard::save() const
{
    if (data_path_.empty()) {
        return true;  // No persistence configured.
    }
    return store_.save(data_path_);
}

bool Shard::load()
{
    if (data_path_.empty()) {
        return false;  // No persistence configured.
    }

    // Load documents from the persistence file.
    if (!store_.load(data_path_)) {
        return false;
    }

    // Rebuild the inverted index from DocumentStore.
    for (const auto& [id, doc] : store_.all()) {
        index_.add_document(id, doc.content);
    }

    return true;
}

// ---------------------------------------------------------------------------
// Accessors
// ---------------------------------------------------------------------------

const DocumentStore& Shard::store() const
{
    return store_;
}

const InvertedIndex& Shard::index() const
{
    return index_;
}

const std::string& Shard::data_path() const
{
    return data_path_;
}

} // namespace dse
