// Distributed Search Engine - Document Store (Phase 6B-1, 7A-1).
//
// Implementation of the contract in src/document_store.h:
//   - add() inserts a new document; rejects duplicate IDs;
//   - get() returns an owning copy or std::nullopt;
//   - contains() is a simple map lookup;
//   - size() returns the map size;
//   - all() returns a const reference to the internal map;
//   - save() writes all documents as JSONL, sorted by doc_id;
//   - load() reads JSONL, skips corrupt lines, rejects duplicate IDs.

#include "document_store.h"

#include <algorithm>
#include <cstddef>
#include <cstdlib>
#include <fstream>
#include <iostream>
#include <optional>
#include <string>
#include <utility>
#include <vector>

#include <nlohmann/json.hpp>

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

const std::unordered_map<doc_id, Document>& DocumentStore::all() const
{
    return documents_;
}

// ---------------------------------------------------------------------------
// Persistence
// ---------------------------------------------------------------------------

bool DocumentStore::save(const std::string& path) const
{
    // Collect documents into a vector and sort by doc_id for deterministic
    // output. The unordered_map iteration order is unspecified.
    std::vector<std::pair<doc_id, const Document*>> sorted;
    sorted.reserve(documents_.size());
    for (const auto& [id, doc] : documents_) {
        sorted.emplace_back(id, &doc);
    }
    std::sort(sorted.begin(), sorted.end(),
              [](const auto& a, const auto& b) { return a.first < b.first; });

    // Open the file for writing (truncates existing content).
    std::ofstream ofs(path);
    if (!ofs.is_open()) {
        return false;
    }

    // Write each document as one JSON object per line.
    for (const auto& [id, doc] : sorted) {
        nlohmann::json j;
        j["id"] = doc->id;
        j["content"] = doc->content;
        ofs << j.dump() << "\n";
    }

    ofs.flush();
    return ofs.good();
}

bool DocumentStore::load(const std::string& path)
{
    // If the file does not exist, return false and leave the store unchanged.
    // This is the normal first-run condition.
    std::ifstream ifs(path);
    if (!ifs.is_open()) {
        return false;
    }

    // Load into a temporary container first. If loading fails midway,
    // the existing store is not partially destroyed.
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

    // Replace the current store with the loaded data.
    // This is the point of no return — the old data is discarded.
    documents_ = std::move(loaded);

    if (skipped_count > 0) {
        std::cerr << "DocumentStore::load: loaded " << loaded_count
                  << " documents, skipped " << skipped_count
                  << " invalid lines from " << path << "\n";
    }

    return true;
}

} // namespace dse
