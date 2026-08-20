// Distributed Search Engine - Document Store (Phase 6B-1).
//
// Implementation of the contract in src/document_store.h:
//   - add() inserts a new document; rejects duplicate IDs;
//   - get() returns an owning copy or std::nullopt;
//   - contains() is a simple map lookup;
//   - size() returns the map size.

#include "document_store.h"

#include <cstddef>
#include <optional>
#include <unordered_map>
#include <utility>

namespace dse {

bool DocumentStore::add(Document document)
{
    // Try to insert. If the ID already exists, insert() returns false
    // and leaves the existing entry untouched.
    const auto [it, inserted] = documents_.try_emplace(
        document.id, std::move(document));
    return inserted;
}

std::optional<Document> DocumentStore::get(doc_id id) const
{
    const auto it = documents_.find(id);
    if (it == documents_.end()) {
        return std::nullopt;
    }
    return it->second;
}

bool DocumentStore::contains(doc_id id) const
{
    return documents_.contains(id);
}

std::size_t DocumentStore::size() const
{
    return documents_.size();
}

} // namespace dse
