// Distributed Search Engine - Document Store (Phase 6B-1, 7A-1).
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
// file, load() reads them back, and all() exposes the full map for
// index rebuilding.

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

// In-memory document storage with JSONL persistence.
//
// Contract (Phase 6B-1 + 7A-1):
//   - documents are keyed by doc_id (reuses the same type as InvertedIndex);
//   - add() stores a new document; returns false and does NOT overwrite if
//     the ID already exists;
//   - get() returns std::nullopt for unknown IDs;
//   - contains() and size() are O(1) average;
//   - no validation is performed on content (empty, whitespace, etc.);
//   - identical insertion sequences produce identical state (deterministic);
//   - save() writes all documents to a JSONL file, sorted by doc_id;
//   - load() reads documents from a JSONL file; corrupt lines are skipped
//     with a warning; duplicate IDs in the file are rejected (first wins);
//     the existing store is NOT modified if the file cannot be opened;
//   - all() returns a const reference to the internal map.
//
// Complexity: O(1) average for add/get/contains; O(D) space where D is the
// number of stored documents. Save is O(D log D + total content size).
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

    // Return a const reference to the complete document map.
    // Used for iterating all documents (e.g., rebuilding the inverted index).
    const std::unordered_map<doc_id, Document>& all() const;

    // Save all documents to a JSONL file (complete rewrite).
    // Documents are written sorted by doc_id for deterministic output.
    // Returns true on success, false if the file cannot be opened/written.
    bool save(const std::string& path) const;

    // Load documents from a JSONL file.
    // - If the file does not exist: returns false, store unchanged.
    // - If the file exists: parses line-by-line. Valid documents are added.
    //   Corrupt/malformed lines are skipped (warning printed to stderr).
    //   Duplicate IDs in the file are rejected (first wins).
    //   Returns true if the file was opened successfully (even if some
    //   lines were corrupt), false only if the file could not be opened.
    bool load(const std::string& path);

private:
    std::unordered_map<doc_id, Document> documents_;
};

} // namespace dse
