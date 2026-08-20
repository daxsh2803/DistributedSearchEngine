# ADR-006: Document Persistence Design

## Status

Accepted (Phase 7A-1 + 7A-2)

## Context

After Phase 6, the search engine can ingest and search documents, but all data is lost when the process exits. Phase 7 adds persistence so that documents survive restarts.

The key architectural question is: **what should be persisted?**

Two candidates:

1. **DocumentStore** (raw documents) — the source of truth
2. **InvertedIndex** (term → postings) — derived search structure

## Decision

**Persist raw documents only. Rebuild the inverted index on startup.**

### Source of Truth

DocumentStore is the source of truth. It owns the raw document text that the InvertedIndex never stores.

InvertedIndex is derived state. It can always be rebuilt from raw documents via `InvertedIndex::add_document()`.

### Why Not Persist the Index?

1. The index is derived — persisting derived state violates the source-of-truth principle
2. If the index file is corrupted, recovery is impossible without raw documents
3. Schema evolution (changing tokenization, TF calculation) requires index migration
4. The index is a performance optimization, not data

This matches how production search engines work:
- Elasticsearch stores documents, rebuilds segments
- Apache Lucene stores documents, builds inverted indexes
- Meilisearch stores documents, rebuilds its LMDB index

### Serialization Format

**JSONL (JSON Lines)** — one JSON object per line:

```
{"id":1,"content":"The quick brown fox"}
{"id":2,"content":"Another document"}
```

Why JSONL:
- Human-readable (can inspect the file)
- Already have nlohmann/json as a dependency
- Simple to parse (read line by line)
- Append-friendly (new documents are new lines)
- Corruption-tolerant (bad lines can be skipped)

### Deterministic Output

Documents are sorted by `doc_id` before serialization. The `unordered_map` iteration order is unspecified, so sorting ensures identical input produces identical output.

### Save Semantics

`save(path)` performs a **complete file rewrite**:
1. Collect all documents into a vector
2. Sort by doc_id
3. Truncate the file
4. Write each document as one JSON line

This is simple, self-healing (corruption is overwritten), and correct for the educational phase.

### Load Semantics

`load(path)` parses line-by-line:
1. If file does not exist → return false, store unchanged (normal first-run)
2. If file exists → parse each line as JSON
3. Validate required fields (`id` as unsigned int, `content` as string)
4. Skip corrupt lines with warning to stderr
5. Reject duplicate IDs (first wins)
6. Load into temporary container, then replace current store

### Corruption Policy

- **Corrupt JSON**: Skip line, print warning, continue
- **Missing fields**: Skip line, print warning, continue
- **Wrong types**: Skip line, print warning, continue
- **Duplicate IDs**: Skip line, print warning, continue (first wins)

The load never partially destroys the existing store. It loads into a temporary container and replaces atomically.

### Atomicity

The current implementation does NOT use atomic file operations (temp file + rename). Full rewrite is sufficient for the educational phase. If the process crashes during save, the file may be partially written. On load, valid lines are processed and corrupt lines are skipped.

## Alternatives Considered

### Persist Inverted Index Only

Rejected. Derived state without source of truth. If the index is corrupted, documents are lost.

### Persist Both Documents and Index

Rejected. Adds complexity (two files to keep in sync) without benefit (index is rebuildable).

### External Database (SQLite, PostgreSQL)

Rejected for Phase 7. Learn file-based persistence fundamentals first. Databases can be introduced later.

### Append-Only Persistence

Deferred to a future phase. Full rewrite is simpler and correct for the current scale.

## Consequences

### Positive
- Simple implementation (one file format, one load function, one save function)
- Index is always consistent with documents (rebuilt from source of truth)
- Easy to understand and debug (human-readable JSONL)
- Natural fit for future sharding (each shard owns documents)
- Natural fit for future replication (replicate documents, rebuild index)

### Negative
- Startup time scales with number of documents (must re-index everything)
- Full rewrite on each save is O(D) per ingestion (not efficient for large datasets)
- No atomic file operations (partial writes possible on crash)

## Phase 7A-2: Startup Recovery

### Startup Behavior

On startup, the application:

1. Attempts to load documents from the persistence file
2. If successful: rebuilds the InvertedIndex from loaded documents
3. If file missing (first run): loads the deterministic seed corpus
4. Creates services and starts the HTTP server

This demonstrates the source-of-truth pattern: the index is always rebuilt from documents.

### Persistence Path Configuration

Configuration (first match wins):
- `--data <path>` command-line argument
- `DSE_DATA=<path>` environment variable
- Default: `data/documents.jsonl`

### Seed Corpus Decision

The seed corpus is only loaded when the persistence file does not exist (first run). This avoids duplicate document IDs and provides a clean first-run experience.

### IngestionService Persistence

IngestionService now accepts an optional `data_path` parameter. If set, each successful `ingest()` call persists the DocumentStore to disk. If persistence fails, the response reports an error, but the in-memory state is unchanged.

### Guarantees

- **Durability**: If `ingest()` returns success AND persistence path is configured, the document is on disk
- **Recovery**: On restart, all persisted documents are loaded and the index is rebuilt
- **Consistency**: The index is always consistent with the DocumentStore (rebuilt from source)

### Deferred
- Index persistence (optimization, not needed)
- Async persistence (adds complexity)
- Write-ahead log (overkill for single-process)
- Binary serialization (optimization)
- Compression (not needed at educational scale)
- Concurrent access (single-threaded HTTP server)
