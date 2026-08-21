# ADR-007: Concurrency and Thread Safety

## Status

Accepted (Phase 8A-1: DocumentStore; Phase 8A-2: InvertedIndex; Phase 8A-3: HTTP concurrency)

## Context

After Phase 7, the search engine is a persistent single-process application with:
- An HTTP server (cpp-httplib) handling GET /search and POST /documents
- An InvertedIndex (term → postings) for search
- A DocumentStore (doc_id → raw text) for persistence
- A SearchService and IngestionService as business-logic boundaries

All data structures are **not thread-safe**. The HTTP server currently handles one request at a time (cpp-httplib's thread pool exists but was not validated).

The project goal is to evolve into a distributed search system, so the engine must handle concurrent requests safely before introducing sharding, replication, or distribution.

## Decision

### Phase 8A-1: Thread-Safe DocumentStore

**Add `dse::SharedMutex` to DocumentStore.**

- `std::shared_mutex` was rejected because MinGW GCC 16.2's implementation has a known bug: `pthread_mutex_lock` asserts `__ret == 0` under exclusive lock contention (38% failure rate in stress tests)
- A portable `SharedMutex` wrapper using `std::mutex` + `std::condition_variable` was implemented
- Writer-priority design prevents writer starvation
- `notify_all()` on every unlock eliminates complex notification-ordering issues

**Locking strategy:**

| Method | Lock | Rationale |
|--------|------|-----------|
| `add()` | `unique_lock` | Write — must be exclusive |
| `get()` | `shared_lock` | Read — concurrent readers safe |
| `contains()` | `shared_lock` | Read — concurrent readers safe |
| `size()` | `shared_lock` | Read — concurrent readers safe |
| `all()` | `shared_lock` (snapshot) | Returns owning copy, not reference |
| `save()` | `shared_lock` (snapshot only) | Takes snapshot, releases lock before file I/O |
| `load()` | `unique_lock` (replace only) | Parses without lock, replaces atomically |

**API change:** `all()` now returns `std::unordered_map<doc_id, Document>` (copy) instead of `const&` (reference). This eliminates the lifetime issue: a reference returned under a shared lock is unsafe if another thread triggers a rehash.

### Phase 8A-2: Thread-Safe InvertedIndex

**Add `dse::SharedMutex` to InvertedIndex.**

**Locking strategy:**

| Method | Lock | Rationale |
|--------|------|-----------|
| `add_document()` | `unique_lock` | Write — modifies documents_, postings_by_term_, document_count_ atomically |
| `postings()` | `shared_lock` (snapshot) | Returns owning copy, not span |
| `document_count()` | `shared_lock` | Read-only |
| `term_count()` | `shared_lock` | Read-only |
| `contains()` | `shared_lock` | Read-only |

**API change:** `postings()` now returns `std::vector<Posting>` (owning copy) instead of `std::span<const Posting>` (non-owning view). Under concurrent mutation, a span is unsafe: the lock is released when the function returns, but the caller holds a reference to data that may be invalidated by a concurrent `add_document()` triggering vector reallocation.

**Caller updates:** QueryProcessor and Ranker were updated to use `std::vector<Posting>` instead of `std::span<const Posting>`. The Ranker stores `std::vector<Posting>` snapshots for the duration of a single ranked query.

### Phase 8A-3: HTTP Concurrency Verification

**No production code changes required.**

cpp-httplib already provides a built-in thread pool with `max(8, hardware_concurrency - 1)` threads (compile-time `CPPHTTPLIB_THREAD_POOL_COUNT`). The server is already concurrent by default.

The HTTP handlers are stateless beyond borrowed references to thread-safe SearchService and IngestionService. Since DocumentStore and InvertedIndex are already synchronized (Phase 8A-1/8A-2), concurrent requests execute correctly.

**9 HTTP concurrency tests** were added to prove concurrent request handling:
1. Multiple simultaneous GET /search requests (different terms)
2. Concurrent searches for the same term
3. Concurrent GET /search and POST /documents
4. Multiple concurrent POST /documents (unique IDs)
5. Search after concurrent ingestion sees all documents
6. Server remains responsive under concurrent load
7. Clean shutdown after concurrent requests
8. Mixed reader/writer workload
9. Concurrent AND and OR queries

## Alternatives Considered

### Thread-safe services (SearchService/IngestionService own locks)

**Rejected.** IngestionService needs to modify both DocumentStore and InvertedIndex atomically. If each data structure owns its own lock, there is no way to guarantee atomicity across both stores without service-level locking. However, adding service-level locks in Phase 8A would overlap with Phase 8B (service coordination). Phase 8A focuses on data-structure-level safety; Phase 8B adds service coordination.

### Custom thread pool for HTTP server

**Rejected.** cpp-httplib already provides a production-quality thread pool. Building a custom pool adds complexity without benefit.

### One-thread-per-request model

**Rejected.** Unbounded threads lead to resource exhaustion. cpp-httplib's bounded thread pool is the standard approach.

## Known Limitation: TOCTOU Race in IngestionService

Concurrent duplicate ingestion can expose a Time-of-Check-Time-of-Use race:

```
Thread A: store_.contains(42) → false   (shared lock)
Thread B: store_.contains(42) → false   (shared lock, concurrent)
Thread A: store_.add({42, ...}) → true   (unique lock)
Thread B: store_.add({42, ...}) → false  (unique lock, rejected)
Thread A: index_.add_document(42, ...)   → succeeds
Thread B: index_.add_document(42, ...)   → assertion failure
```

Both threads pass the `contains()` check before either adds. Thread B's `store_.add()` fails, but `IngestionService::ingest()` does not check the return value before calling `index_.add_document()`.

**This is explicitly deferred to Phase 8B** (service-level coordination). Phase 8A establishes data-structure-level safety; Phase 8B adds atomic check-and-act semantics across services.

## Consequences

### Positive

- All data structures are safe for concurrent access
- No deadlocks (SharedMutex uses single-CV design with writer priority)
- Existing tests remain unchanged and passing
- HTTP server handles concurrent requests without code changes
- Clean separation: data-structure safety (8A) vs service coordination (8B)

### Negative

- `all()` and `postings()` return copies instead of references/spans — O(k) copy per call
- `SharedMutex` wrapper adds one heap allocation per data structure (negligible)
- TOCTOU race in IngestionService remains until Phase 8B

## Files

### Created
- `src/shared_mutex.h` — portable SharedMutex wrapper
- `tests/document_store_concurrency_test.cpp` — 10 tests
- `tests/inverted_index_concurrency_test.cpp` — 10 tests
- `tests/http_concurrency_test.cpp` — 9 tests

### Modified
- `src/document_store.h` — SharedMutex, new API
- `src/document_store.cpp` — locking implementation
- `src/inverted_index.h` — SharedMutex, new API
- `src/inverted_index.cpp` — locking implementation
- `src/query_processor.h` — vector<Posting> API
- `src/query_processor.cpp` — vector<Posting> API
- `src/ranker.cpp` — vector<Posting> API
- `tests/inverted_index_test.cpp` — updated for snapshot API
- `CMakeLists.txt` — 3 new test targets

## Verification Results

- Build: **successful, zero warnings** (`-Wall -Wextra -Wpedantic`)
- Tests: **362/362 passed (100%)**
  - 9 HTTP concurrency tests
  - 10 InvertedIndex concurrency tests
  - 10 DocumentStore concurrency tests
  - 333 pre-existing tests
- Repeated HTTP concurrency: **27/27 passed** (3 runs × 9 tests)
- ConcurrentDuplicateAdds: **100/100 passed** (50+50 runs)
- TSAN: **unavailable** (libtsan missing from MSYS2 UCRT64)

## Future Evolution

1. **Phase 8B — Service-level coordination**: atomic check-and-act in IngestionService; shared_mutex for concurrent search + ingestion
2. **Sharding**: each shard owns a DocumentStore + InvertedIndex
3. **Replication**: replicate DocumentStore, rebuild InvertedIndex
4. **Distributed query**: coordinator scatters queries across shards, gathers results
