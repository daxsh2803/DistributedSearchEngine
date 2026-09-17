# ADR-008: Document Lifecycle (Update and Delete)

**Status:** Accepted

**Date:** 2026-08-22

## Context

Phase 8 established thread-safe DocumentStore and InvertedIndex, with service-level mutation coordination in Phase 8B. The search engine could create and search documents but had no way to update or delete them.

Document lifecycle operations (update, delete) require coordinated mutations across two storage components:

- **DocumentStore** — source of truth for raw document content
- **InvertedIndex** — derived searchable state (term → postings)

Unlike create (which only appends), update and delete must modify both components atomically and preserve consistency between them.

## Decision

### DocumentStore API additions

```cpp
bool update(Document document);  // replace content, returns false if missing
bool remove(doc_id id);          // delete document, returns false if missing
```

Both acquire the existing exclusive lock. `update()` never creates a missing document.

### InvertedIndex API additions

```cpp
bool remove_document(doc_id id);  // remove all postings for a document
```

#### Reverse mapping

A reverse mapping `doc_terms_` maps each document ID to the set of terms it contains:

```cpp
std::unordered_map<doc_id, std::unordered_set<std::string>> doc_terms_;
```

This avoids scanning the entire vocabulary during removal. `add_document()` populates this mapping; `remove_document()` reads it to find which posting lists to update.

`remove_document()` preserves all invariants:
- sorted posting lists
- empty-term cleanup
- document_count_ decrement
- no modification if document not found

### Service-level mutation coordination

`IngestionService` gains a `mutation_mutex_` (via `dse::SharedMutex`) that serializes create/update/delete operations. Search operations do NOT acquire this mutex, so concurrent reads remain unblocked.

```
Create:  validate → store_.add() → index_.add_document() → persist
Update:  validate → store_.contains() → index_.remove_document() → store_.update() → index_.add_document() → persist
Delete:  validate → store_.contains() → index_.remove_document() → store_.remove() → persist
```

### Update semantics

The logical update is:

1. Validate request content
2. Verify document exists in DocumentStore
3. Remove old index representation (`remove_document`)
4. Replace DocumentStore content (`update`)
5. Add new index representation (`add_document`)
6. Persist if configured

### Delete semantics

1. Verify document exists in DocumentStore
2. Remove from InvertedIndex (`remove_document`)
3. Remove from DocumentStore (`remove`)
4. Persist if configured

### HTTP API

```
PUT    /documents/:id   { "content": "..." }   → 200 OK | 400 | 404
DELETE /documents/:id                           → 204 No Content | 404
```

Uses httplib's `PathParamsMatcher` (`:id` syntax) for portability across platforms including MinGW/MSYS2.

### Persistence

Continues using existing JSONL persistence. After successful update/delete, `DocumentStore::save()` is called if a persistence path is configured.

## Invariants

After any successful lifecycle mutation:

```
DocumentStore::size() == InvertedIndex::document_count()
```

For application-managed documents (those added through IngestionService).

## Non-goals

- WAL / crash recovery
- Replication / sharding
- Distributed transactions
- Atomic filesystem transactions
- Update-document on InvertedIndex (service coordinates remove + add)

## Consequences

- The reverse mapping adds O(T) space per document (T = distinct terms in document)
- Service-level mutex serializes all mutations but allows concurrent searches
- The existing `add_document()` contract (unique doc_id) is preserved; updates achieve "modification" via remove + re-add within a single locked section
