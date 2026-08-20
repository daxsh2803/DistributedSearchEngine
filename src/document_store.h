// Distributed Search Engine - Document Store (Phase 6B-1).
//
// In-memory storage for raw document content, keyed by document ID.
// This component is independent of the InvertedIndex: it owns the raw
// text that the index never stores. Together they form the write-path
// storage layer used by IngestionService (Phase 6B-2).
//
// DocumentStore is a pure storage component. Whether empty or
// whitespace-only content is valid is an IngestionService concern;
// DocumentStore stores whatever it receives.

#pragma once

#include <cstddef>
#include <cstdint>
#include <optional>
#include <string>
#include <unordered_map>

#include "inverted_index.h"

namespace dse {

// A document with its raw text content. The InvertedIndex never sees this
// struct — it only processes tokenized text via add_document().
struct Document {
    doc_id id;
    std::string content;

    friend bool operator==(const Document&, const Document&) = default;
};

// In-memory document storage.
//
// Contract (Phase 6B-1):
//   - documents are keyed by doc_id (reuses the same type as InvertedIndex);
//   - add() stores a new document; returns false and does NOT overwrite if
//     the ID already exists;
//   - get() returns std::nullopt for unknown IDs;
//   - contains() and size() are O(1) average;
//   - no validation is performed on content (empty, whitespace, etc.);
//   - identical insertion sequences produce identical state (deterministic).
//
// Complexity: O(1) average for add/get/contains; O(D) space where D is the
// number of stored documents.
class DocumentStore {
public:
    // Add a new document. Returns true on success. If a document with the
    // same ID already exists, returns false and leaves the existing document
    // unchanged.
    bool add(Document document);

    // Return the stored document if present, or std::nullopt.
    std::optional<Document> get(doc_id id) const;

    // Whether a document with this ID exists. O(1) average.
    bool contains(doc_id id) const;

    // Number of stored documents. O(1).
    std::size_t size() const;

private:
    std::unordered_map<doc_id, Document> documents_;
};

} // namespace dse
