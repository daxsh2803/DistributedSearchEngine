# Phase 7 — Document Persistence

## 1. Why Persistence Matters

After Phase 6, the search engine can ingest and search documents. But all data lives in memory. The moment the process exits, everything is lost.

Persistence means saving data to durable storage (disk) so it survives process restarts.

**The fundamental question**: What should we persist, and how?

## 2. Source of Truth vs Derived State

This is the most important concept in Phase 7.

### Source of Truth

The **source of truth** is the authoritative data that everything else depends on. In our search engine:

- **DocumentStore** (raw document text) is the source of truth
- If we lose the documents, we lose everything

### Derived State

**Derived state** can be recomputed from the source of truth. In our search engine:

- **InvertedIndex** (term → postings) is derived state
- It can be rebuilt from raw documents via `InvertedIndex::add_document()`

### Why This Distinction Matters

If you persist derived state without its source:
- Corruption is unrecoverable
- Schema changes require migration
- You can't verify correctness

If you persist the source of truth:
- Derived state can always be rebuilt
- Corruption is recoverable
- Schema changes just mean rebuilding

**Production search engines use this pattern:**
- Elasticsearch stores documents, rebuilds segments (its indexes)
- Apache Lucene stores documents, builds inverted indexes
- Meilisearch stores documents, rebuilds its LMDB index

## 3. JSONL Format

Phase 7A-1 uses **JSON Lines (JSONL)** — one JSON object per line:

```json
{"id":1,"content":"The quick brown fox jumps over the lazy dog"}
{"id":2,"content":"A fast red fox leaps over a sleeping hound"}
```

### Why JSONL?

1. **Human-readable** — open the file and see what's stored
2. **Simple to parse** — read line by line, parse each as JSON
3. **Append-friendly** — new documents are new lines
4. **Corruption-tolerant** — bad lines can be skipped
5. **Already available** — we have nlohmann/json as a dependency

### Why Not Binary?

For an educational project with thousands of documents, JSONL's simplicity far outweighs binary's performance. Binary formats are an optimization, not a requirement.

## 4. DocumentStore API Changes

Phase 7A-1 adds three methods to DocumentStore:

### `all()` — Access All Documents

```cpp
const std::unordered_map<doc_id, Document>& all() const;
```

Returns a const reference to the complete document map. Used for:
- Iterating all documents (e.g., rebuilding the inverted index)
- Counting documents
- Checking what's stored

### `save(path)` — Persist to Disk

```cpp
bool save(const std::string& path) const;
```

Writes all documents to a JSONL file:
1. Collects documents into a vector
2. Sorts by doc_id (deterministic output)
3. Truncates the file
4. Writes each document as one JSON line

Returns `true` on success, `false` if the file cannot be opened/written.

### `load(path)` — Restore from Disk

```cpp
bool load(const std::string& path);
```

Reads documents from a JSONL file:
- If file doesn't exist → returns `false`, store unchanged
- If file exists → parses line-by-line
- Corrupt lines are skipped (warning printed)
- Duplicate IDs are rejected (first wins)

Returns `true` if the file was opened successfully.

## 5. Save Semantics

### Complete File Rewrite

`save()` does a complete rewrite, not an append:

```
save("data.jsonl")
  ↓
open/truncate file
  ↓
write every document as one JSON object per line
  ↓
return true on success
```

**Why full rewrite?**
- Simple to implement
- Self-healing (corruption is overwritten on next save)
- No need for append-specific corruption handling

**Performance note**: Full rewrite is O(D) per save, where D is the number of documents. For thousands of documents, this is fine. For millions, you'd use append + compaction.

### Deterministic Ordering

Documents are sorted by `doc_id` before serialization. This ensures:
- Same documents → same file content (bit-for-bit)
- Easy to diff between saves
- No dependence on `unordered_map` iteration order

## 6. Load Semantics

### Line-by-Line Parsing

```cpp
while (std::getline(ifs, line)) {
    // Parse JSON
    // Validate fields
    // Add to temporary container
}
// Replace current store
documents_ = std::move(loaded);
```

### Corruption Handling

| Condition | Behavior |
|-----------|----------|
| Empty line | Skip |
| Invalid JSON | Skip, print warning |
| Missing `id` field | Skip, print warning |
| `id` is not unsigned int | Skip, print warning |
| Missing `content` field | Skip, print warning |
| `content` is not string | Skip, print warning |
| Duplicate `id` in file | Skip, print warning (first wins) |

### Load Atomicity

The implementation loads into a **temporary container** first. If loading fails midway (e.g., disk error), the existing store is NOT partially destroyed.

```
load("data.jsonl")
  ↓
create temporary map
  ↓
parse each line → add to temporary map
  ↓
replace current map with temporary map
  ↓
return true
```

This is a critical safety property. Without it, a failed load could destroy existing data.

## 7. File Missing vs File Corrupt

These are two different conditions:

### File Missing (Normal)

```
load("nonexistent.jsonl")
  → return false
  → store unchanged
  → This is the normal first-run condition
```

### File Corrupt (Abnormal)

```
load("corrupt.jsonl")
  → parse valid lines, skip corrupt lines
  → print warnings to stderr
  → return true (file was opened)
  → valid documents loaded
```

Don't treat these as identical. Missing files are expected; corrupt files are errors that should be reported.

## 8. Persistence Architecture

```
POST /documents {"id": 42, "content": "..."}
  ↓
IngestionService::ingest()
  ↓
1. Validate request
2. Check duplicate → DocumentStore::contains()
3. Store in memory → DocumentStore::add()
4. Index tokens → InvertedIndex::add_document()
5. Persist to disk → DocumentStore::save(path)  ← NEW
  ↓
HTTP 201

Startup (future phase):
  ↓
1. Create DocumentStore
2. DocumentStore::load(path)  ← NEW
3. For each document in store.all():
     InvertedIndex::add_document(id, content)
4. Ready to serve
```

## 9. Complexity Analysis

### Save

```
save(path):
  1. Collect documents: O(D)
  2. Sort by doc_id: O(D log D)
  3. Write to file: O(total content size)
  
Total: O(D log D + total content size)
```

Where D = number of documents.

### Load

```
load(path):
  1. Read file: O(file size)
  2. Parse each line: O(total content size)
  3. Insert into map: O(D) average
  
Total: O(file size)
```

### all()

```
all():
  Return reference to map: O(1)
```

### Memory

```
Store: O(D × average content size)
Temporary during load: O(D × average content size) — freed after move
```

## 10. Startup Recovery (Phase 7A-2)

Phase 7A-2 connects persistence to the application lifecycle.

### Startup Sequence

```
main():
  1. Resolve data path (--data / DSE_DATA / default)
  2. Create InvertedIndex
  3. Create DocumentStore
  4. Create IngestionService(index, store, data_path)
  5. store.load(data_path)
     ├── success → rebuild_index(index, store)
     └── file missing → load_seed_corpus(ingestion)
  6. Start HTTP server
```

### Rebuild Index from DocumentStore

```cpp
void rebuild_index(InvertedIndex& index, const DocumentStore& store) {
    for (const auto& [id, doc] : store.all()) {
        index.add_document(id, doc.content);
    }
}
```

This demonstrates that the InvertedIndex is **derived state** — it can always be rebuilt from the source of truth.

### Seed Corpus Behavior

The seed corpus is only loaded when the persistence file does not exist:
- **First run**: No persistence file → load seed corpus → persist to disk
- **Subsequent runs**: Persistence file exists → load from disk → skip seed corpus

This avoids duplicate document IDs and provides a clean first-run experience.

### IngestionService Persistence

IngestionService now accepts an optional `data_path` parameter:

```cpp
// Without persistence (tests, backward compatible)
IngestionService ingestion(index, store);

// With persistence (application)
IngestionService ingestion(index, store, "data/documents.jsonl");
```

If `data_path` is non-empty, each successful `ingest()` call calls `store_.save(data_path_)`. If persistence fails, the response reports an error.

### Restart Test

The key Phase 7 learning outcome:

```
Process 1:
  POST /documents {"id": 42, "content": "..."}
  → document persisted to disk
  Process 1 exits

Process 2:
  starts
  → loads DocumentStore from disk
  → rebuilds InvertedIndex
  GET /search?q=...
  → document is found
```

## 11. Future Extensions

Phase 7A-1 + 7A-2 establish the persistence foundation. Future phases can build on it:

### Phase 7B+: Advanced Persistence

- **Append-only writes**: More efficient than full rewrite
- **Atomic file operations**: temp file + rename for crash safety
- **Compression**: Reduce file size for large datasets
- **Binary format**: Faster parsing than JSONL
- **Concurrent access**: Thread-safe save/load
- **Write-ahead log**: Durability guarantees for critical applications

### Distributed Phases

- **Sharding**: Each shard owns a subset of documents
- **Replication**: Replicate document files to other nodes
- **Index rebuild**: Always possible from documents

## 12. Testing Strategy

### Phase 7A-1: DocumentStore Persistence (24 tests)

1. Save/load round trip
2. Multiple documents
3. doc_id = 0
4. Non-contiguous IDs
5. Empty content
6. Special characters / JSON escaping
7. Long content
8. Save creates file
9. Save overwrites existing file
10. Missing file (returns false, store unchanged)
11. Malformed JSON line
12. Missing id field
13. Missing content field
14. Wrong id type
15. Wrong content type
16. Duplicate IDs in file
17. Deterministic save ordering
18. all() exposes complete map
19. Existing store unchanged on load failure
20. Mixed valid + malformed records
21. Negative id (signed) rejected
22. Content with newlines
23. Empty store save/load

### Phase 7A-2: Startup Recovery (7 tests)

1. Loaded document becomes searchable after restart
2. First run with missing file loads seed corpus
3. Newly ingested document survives restart
4. Malformed records follow documented policy
5. Persistence failure reports error
6. Existing search behavior remains intact
7. Index rebuilt from DocumentStore

## 13. Key Takeaways

1. **Source of truth matters** — persist documents, not derived indexes
2. **Rebuild from source** — always possible, always correct
3. **JSONL is simple** — human-readable, easy to parse, corruption-tolerant
4. **Load into temporary** — never partially destroy existing data
5. **Report corruption** — don't silently ignore bad data
6. **Deterministic output** — sort by doc_id for reproducible files
7. **Full rewrite is fine** — for educational scale, simplicity beats performance
8. **Rebuild from source** — the index is always rebuilt from documents
9. **Seed corpus is fallback** — only loaded when no persistence file exists
10. **Persistence failures are reported** — never silently claim success
