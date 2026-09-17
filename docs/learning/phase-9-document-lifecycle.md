# Phase 9: Document Lifecycle

## Overview

Phase 9 adds **update** and **delete** operations to the document lifecycle. Before this phase, the search engine could only create and search documents.

## Why Update/Delete Are Different from Insertion

Insertion is purely additive — you add data to both DocumentStore and InvertedIndex. Update and delete are **subtractive-then-additive** or **subtractive**:

- **Delete** must remove data from both components
- **Update** must remove old data and add new data, all atomically

This requires coordination between the two storage layers.

## Source of Truth vs Derived State

```
DocumentStore   = source of truth (raw document content)
InvertedIndex   = derived state (term → postings mapping)
```

The InvertedIndex can be rebuilt from DocumentStore. This separation means:

- Delete must remove from DocumentStore first (or atomically with index)
- Update replaces DocumentStore content, then updates the index
- If the process crashes mid-operation, the index can be rebuilt from DocumentStore

## Reverse Mapping

The InvertedIndex introduces a reverse mapping:

```
Forward:   term      → [(docID, tf), (docID, tf), ...]
Reverse:   docID     → {term1, term2, term3, ...}
```

Without this, `remove_document()` would need to scan every posting list in the vocabulary. With the reverse mapping, we look up the document's terms and only modify those posting lists.

### Implementation

```cpp
std::unordered_map<doc_id, std::unordered_set<std::string>> doc_terms_;
```

Populated by `add_document()`, read by `remove_document()`.

## Update Flow

```
validate request
    ↓
verify document exists in DocumentStore
    ↓
index_.remove_document(id)     // remove old postings
    ↓
store_.update(document)        // replace content
    ↓
index_.add_document(id, text)  // add new postings
    ↓
persist if configured
    ↓
return success
```

Key detail: we use `remove_document()` followed by `add_document()` rather than modifying individual postings. This reuses existing, tested code paths.

## Delete Flow

```
verify document exists in DocumentStore
    ↓
index_.remove_document(id)     // remove all postings
    ↓
store_.remove(id)              // remove from store
    ↓
persist if configured
    ↓
return success
```

## Service-Level Mutation Synchronization

DocumentStore and InvertedIndex each have their own internal locks. These protect individual operations but don't make multi-component operations atomic.

`IngestionService` adds a `mutation_mutex_` that serializes create/update/delete:

```cpp
std::unique_lock lock(*mutation_mutex_);
// ... multi-component operation ...
```

Search operations do NOT acquire this mutex, so concurrent reads remain unblocked.

```
Create:   acquire mutex → modify both → release
Update:   acquire mutex → modify both → release
Delete:   acquire mutex → modify both → release
Search:   no mutex needed → reads from InvertedIndex (thread-safe)
```

## Concurrency Guarantees

- Individual DocumentStore operations are thread-safe (SharedMutex)
- Individual InvertedIndex operations are thread-safe (SharedMutex)
- Multi-component mutations are serialized (service mutation_mutex_)
- Searches remain fully concurrent
- No deadlocks: search path never acquires mutation_mutex_

## Persistence

Continues using JSONL persistence. After successful update/delete:

```cpp
store_.save(data_path_);  // full rewrite, deterministic doc_id order
```

No WAL, no crash recovery, no atomic file operations. If persistence fails, the in-memory mutation may have already succeeded.

## HTTP APIs

### PUT /documents/:id

```json
Request:  { "content": "new content" }
Response: { "document_id": 1, "terms_indexed": 3 }    // 200
Error:    { "error": "Document not found" }            // 404
Error:    { "error": "Missing content field" }         // 400
```

### DELETE /documents/:id

```
204 No Content     — successful deletion
404 Not Found      — document doesn't exist
```

Uses httplib's `PathParamsMatcher` (`:id` syntax) for cross-platform compatibility.

## Invariants

After any successful mutation:

```
DocumentStore::size() == InvertedIndex::document_count()
```

For application-managed documents.

## Testing

### DocumentStore tests
- update existing / missing / preserves count
- delete existing / missing / changes count
- double remove is safe
- persistence after update/delete

### InvertedIndex tests
- remove existing / missing / updates count
- removes postings and cleans empty terms
- preserves other documents
- sorted posting invariant maintained
- empty-token document removal

### IngestionService tests
- update/delete success and failure cases
- update/delete then search verification
- persistence after update/delete

### HTTP tests
- PUT update success / missing → 404 / invalid → 400
- DELETE success → 204 / missing → 404
- update then search / delete then search
- preserve unrelated documents

## Phase 9 Non-Goals

- WAL / crash recovery
- Replication / sharding
- Distributed transactions
- Distributed coordinator
- Inter-node communication
- Consensus protocols
- Ranking changes
- Query language changes
- Tokenizer changes

## Test Results

- 419/419 tests passed
- Zero compiler warnings
- All Phase 8 concurrency guarantees preserved
