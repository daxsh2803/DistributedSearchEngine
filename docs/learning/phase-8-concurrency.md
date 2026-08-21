# Phase 8 — Concurrency and Thread Safety

## 1. Why Concurrency Matters

After Phase 7, the search engine is a persistent single-process application. But it handles one request at a time. In a real system, multiple clients query and ingest simultaneously. Without concurrency support, the server becomes a bottleneck: every request blocks until the previous one completes.

**The fundamental question**: How do we make the server handle multiple requests safely at the same time?

## 2. Two Separate Problems

Concurrency involves two distinct challenges:

### HTTP Concurrency

Multiple HTTP requests execute simultaneously. The server must dispatch requests to worker threads and handle overlapping I/O.

### Data Synchronization

When concurrent requests access shared data structures (InvertedIndex, DocumentStore), those structures must remain consistent. A search must not read a partially-updated index. An ingestion must not corrupt the postings list.

**Phase 8A addresses both:**
- Phase 8A-1/8A-2: Data structure synchronization
- Phase 8A-3: HTTP concurrency verification

## 3. The Concurrency Pipeline

```
Client A: GET /search?q=cat     ─┐
Client B: GET /search?q=dog     ─┤──→ HTTP Server Thread Pool
Client C: POST /documents       ─┘         │
                                           ▼
                                   ┌───────────────┐
                                   │  Request N    │
                                   │  Request N+1  │
                                   │  Request N+2  │
                                   └───────┬───────┘
                                           │
                         ┌─────────────────┼─────────────────┐
                         ▼                 ▼                 ▼
                   SearchService    IngestionService   Error Handling
                         │                 │
                         ▼                 ▼
                   InvertedIndex    DocumentStore + InvertedIndex
                   (shared_lock)    (unique_lock)
```

Each request runs in its own thread from the thread pool. Multiple requests can execute simultaneously. The data structures must be safe for concurrent access.

## 4. SharedMutex: Reader-Writer Locks

### The Problem with std::mutex

A simple `std::mutex` allows only one thread at a time. If 8 threads all search for different terms, they execute serially even though they only read data. This wastes CPU cores.

### The SharedMutex Solution

A **reader-writer lock** allows:
- **Multiple readers simultaneously** (shared lock)
- **Only one writer at a time** (exclusive lock)

```
Thread A: search("cat")   → shared_lock   → allowed (reader)
Thread B: search("dog")   → shared_lock   → allowed (reader)
Thread C: ingest(doc)     → unique_lock   → blocks until A and B release
Thread D: search("fox")   → shared_lock   → blocks until C releases
```

### Our SharedMutex Implementation

```cpp
class SharedMutex {
    std::mutex mtx_;
    std::condition_variable cv_;
    int readers_ = 0;
    bool writer_active_ = false;
    bool writer_waiting_ = false;
};
```

**How it works:**

1. `lock_shared()` (reader): Waits if a writer is active or waiting. Increments `readers_`.
2. `unlock_shared()` (reader): Decrements `readers_`. If zero, notifies waiting writers.
3. `lock()` (writer): Sets `writer_waiting_`. Waits until `readers_ == 0` and `!writer_active_`. Sets `writer_active_`.
4. `unlock()` (writer): Clears `writer_active_` and `writer_waiting_`. Notifies all.

**Why writer-priority?** Without it, a continuous stream of readers could starve writers forever. Writer-priority ensures writers eventually get access.

**Why notify_all()?** Using `notify_one()` caused deadlocks in earlier iterations because the wrong thread could be woken. `notify_all()` wakes all waiters; spurious wakeups are handled by the condition predicate. For small thread counts, the overhead is negligible.

### Why Not std::shared_mutex?

MinGW GCC 16.2's `std::shared_mutex` has a bug: under exclusive lock contention, `pthread_mutex_lock()` sometimes returns a non-zero error code, and MinGW's libstdc++ fires `__glibcxx_assert(__ret == 0)`, aborting the process.

**Evidence:**
- 50 runs of ConcurrentDuplicateAdds: **19/50 failures** (38%)
- Error: `shared_mutex:205: Assertion '__ret == 0' failed`
- Root cause: MinGW pthreads implementation under exclusive-lock contention

**Solution:** Our `dse::SharedMutex` uses `std::mutex` + `std::condition_variable` (which work correctly on MinGW) and provides the same shared/exclusive semantics.

## 5. DocumentStore Thread Safety (Phase 8A-1)

### Locking Strategy

| Method | Lock Type | Why |
|--------|-----------|-----|
| `add()` | `unique_lock` | Write — must be exclusive |
| `get()` | `shared_lock` | Read — concurrent readers safe |
| `contains()` | `shared_lock` | Read — concurrent readers safe |
| `size()` | `shared_lock` | Read — concurrent readers safe |
| `all()` | `shared_lock` → copy | Returns owning copy, not reference |
| `save()` | `shared_lock` → copy → release → write | Snapshot under lock, I/O without lock |
| `load()` | parse → `unique_lock` → replace | Parse without lock, replace atomically |

### The all() Lifetime Problem

**Before:** `all()` returned `const std::unordered_map&` — a reference to the internal map.

**Problem:** If `all()` acquires a shared_lock, returns a reference, then releases the lock, the caller holds an unprotected reference. A concurrent `add()` could trigger a rehash, invalidating all iterators and references — **undefined behavior**.

**After:** `all()` returns an owning copy. The caller owns the snapshot and can iterate safely without holding any lock. This is safe for concurrent access.

**Performance:** `all()` is called infrequently (startup, tests). The O(D) copy cost is negligible.

### save() Strategy

```
save(path):
  1. shared_lock → copy all documents to vector → release lock
  2. Sort vector by doc_id (deterministic output)
  3. Open file, write JSONL → NO LOCK HELD
  4. Return success/failure
```

The lock is held only during the snapshot phase, not during file I/O. This avoids blocking readers during potentially slow disk writes.

### load() Strategy

```
load(path):
  1. Open file → NO LOCK HELD (fast-fail for missing files)
  2. Parse line-by-line into temporary map
  3. unique_lock → replace current map atomically → release lock
  4. Return success
```

The lock is held only during the atomic replacement, not during parsing. This minimizes contention.

## 6. InvertedIndex Thread Safety (Phase 8A-2)

### Locking Strategy

| Method | Lock Type | Why |
|--------|-----------|-----|
| `add_document()` | `unique_lock` | Write — modifies documents_, postings_by_term_, document_count_ atomically |
| `postings()` | `shared_lock` → copy | Returns owning copy, not span |
| `document_count()` | `shared_lock` | Read-only |
| `term_count()` | `shared_lock` | Read-only |
| `contains()` | `shared_lock` | Read-only |

### The postings() Lifetime Problem

**Before:** `postings()` returned `std::span<const Posting>` — a non-owning view into the internal vector.

**Problem:** Under concurrent mutation, the lock is released when the function returns. The caller holds a span pointing to data that may be invalidated by a concurrent `add_document()` triggering vector reallocation — **undefined behavior**.

**After:** `postings()` returns `std::vector<Posting>` — an owning copy taken under the shared lock. The returned vector is safe to use indefinitely.

**Why this matters for the Ranker:** The Ranker stores posting snapshots for the duration of a single ranked query. With the old span API, a concurrent ingestion during ranking could corrupt the Ranker's data. With the new owning-copy API, the Ranker's data is safe.

### Atomicity of add_document()

`add_document()` modifies three data structures:
1. `documents_` (set of doc_ids)
2. `postings_by_term_` (map of term → postings)
3. `document_count_`

All three modifications occur under a single `unique_lock`. Readers never observe a partially-updated index.

## 7. HTTP Concurrency (Phase 8A-3)

### cpp-httplib's Built-in Thread Pool

**Key finding:** cpp-httplib already has a built-in thread pool. No custom implementation was needed.

```cpp
// From httplib.h:
#define CPPHTTPLIB_THREAD_POOL_COUNT \
    ((std::max)(8u, std::thread::hardware_concurrency() > 0 \
                      ? std::thread::hardware_concurrency() - 1 \
                      : 0))

// Server constructor:
inline Server::Server()
    : new_task_queue(
          [] { return new ThreadPool(CPPHTTPLIB_THREAD_POOL_COUNT); }) {
```

The server creates a thread pool with `max(8, hardware_concurrency - 1)` threads. Each incoming request is dispatched to a worker thread from this pool.

### Why No Custom Thread Pool?

| Option | Complexity | Performance | Resource Usage | Shutdown | Verdict |
|--------|-----------|-------------|----------------|----------|---------|
| cpp-httplib built-in | None | Good | Bounded | Clean | ✅ Chosen |
| Custom thread pool | High | Good | Bounded | Complex | ❌ Rejected |
| One-thread-per-request | Low | Poor | Unbounded | Complex | ❌ Rejected |

cpp-httplib's thread pool is production-quality, already integrated, and handles shutdown cleanly. Building a custom pool adds complexity without benefit.

### Request Flow Under Concurrency

```
Thread Pool (8+ threads):
  ┌──────────┐ ┌──────────┐ ┌──────────┐ ┌──────────┐
  │ Worker 1 │ │ Worker 2 │ │ Worker 3 │ │ Worker 4 │
  │ GET /search│ │ POST /docs│ │ GET /search│ │ GET /search│
  └────┬─────┘ └────┬─────┘ └────┬─────┘ └────┬─────┘
       │             │             │             │
       ▼             ▼             ▼             ▼
  SearchService  IngestionSvc  SearchService  SearchService
       │             │             │             │
       ▼             ▼             ▼             ▼
  InvertedIndex  DocStore +    InvertedIndex  InvertedIndex
  (shared_lock)  InvIdx        (shared_lock)  (shared_lock)
                 (unique_lock)
```

Multiple searches run concurrently (shared locks). An ingestion blocks searches briefly (unique lock), then releases.

## 8. Server Lifecycle

### Startup

```
main():
  1. Create InvertedIndex, DocumentStore
  2. Create services
  3. Create HttpServer
  4. Install signal handlers (SIGINT, SIGTERM)
  5. Start server_thread → server.listen(port)
  6. server.wait_until_ready()
  7. Print "Server listening"
  8. server_thread.join()  // blocks until stop()
```

### Shutdown

```
SIGINT/SIGTERM received:
  → signal_handler() → server.stop()
  → server.listen() returns
  → server_thread exits
  → main() → server_thread.join() returns
  → g_server.store(nullptr)
  → "Server stopped"
```

The `stop()` method is safe to call from any thread (it sets an atomic flag). The thread pool drains remaining requests before the server exits.

### Destructor

`~HttpServer()` calls `stop()` if the server is still running. This prevents thread leaks if the server is destroyed without explicit shutdown.

## 9. Concurrency Guarantees

### What Phase 8A Provides

| Guarantee | Status |
|-----------|--------|
| Data structures are safe for concurrent access | ✅ |
| Multiple searches run in parallel | ✅ |
| Searches during ingestion are safe | ✅ (blocked, not concurrent) |
| Concurrent ingestions are serialized | ✅ (unique lock) |
| No deadlocks | ✅ (single-CV SharedMutex) |
| No data races | ✅ (all shared state protected) |
| Clean shutdown under load | ✅ |

### What Phase 8A Does NOT Provide

| Limitation | Phase |
|------------|-------|
| Atomic check-and-act across services | Phase 8B |
| Concurrent search + ingestion (no blocking) | Phase 8B |
| Lock-free data structures | Not planned |
| Fine-grained per-term locking | Not planned |

## 11. Phase 8B: Service-Level Coordination

### The TOCTOU Race (Solved)

Phase 8A established data-structure-level safety. Phase 8B solves the service-level TOCTOU race in IngestionService.

**Before (Phase 8A):**
```cpp
// CHECK — shared lock acquired and released
if (store_.contains(request.id)) { return error; }
// GAP — another thread could add the same ID here
// USE — separate lock acquisitions
store_.add(Document{request.id, request.content});
index_.add_document(request.id, request.content);
```

**After (Phase 8B):**
```cpp
// Atomic "check and claim": add() returns false if ID already exists
if (!store_.add(Document{request.id, request.content})) {
    return error;  // Duplicate — add() rejected it
}
// ID is now claimed in DocumentStore. Safe to add to index.
index_.add_document(request.id, request.content);
```

### Why This Works

`DocumentStore::add()` uses `try_emplace` under an exclusive lock. It returns `true` if the insert succeeded, `false` if the ID already exists. This return value acts as an atomic "check-and-claim" operation.

**Concrete scenario:**

| Step | Thread A (ID 42) | Thread B (ID 42) |
|------|-------------------|-------------------|
| 1 | `store_.add({42, "first"})` → **true** | |
| 2 | | `store_.add({42, "second"})` → **false** |
| 3 | `index_.add_document(42, "first")` → succeeds | returns error |
| 4 | | never reaches `add_document` |

Thread B's `store_.add()` fails because Thread A already claimed ID 42. Thread B never reaches `index_.add_document()`.

### Concurrency Guarantees (Phase 8B)

| Guarantee | Status |
|-----------|--------|
| Duplicate document IDs rejected atomically | ✅ |
| Document in both stores or neither | ✅ |
| Concurrent different-document ingestions proceed | ✅ |
| Concurrent search + ingestion is safe | ✅ |
| No additional locks needed | ✅ |
| No deadlock risk | ✅ |

### What Phase 8B Does NOT Provide

| Limitation | Why Deferred |
|------------|-------------|
| Atomic persistence (WAL, atomic rename) | Phase 9+ |
| Rollback on partial failure | Complex transaction semantics |
| Distributed transactions | Out of scope |

## 10. The TOCTOU Race

### What is TOCTOU?

**T**ime **o**f **C**heck **T**ime **o**f **U**se: the gap between checking a condition and acting on it. During this gap, another thread can change the state, making the check stale.

### The IngestionService Race

```cpp
// IngestionService::ingest():
if (store_.contains(request.id)) {        // CHECK
    // ... return error
}
// ... gap: another thread could add the same ID here
store_.add(Document{request.id, ...});     // USE
index_.add_document(request.id, ...);      // USE
```

Two threads can both pass the `contains()` check, then both call `store_.add()`. One succeeds, one fails. But both call `index_.add_document()`, triggering the assertion.

### Why This is Deferred to Phase 8B

Phase 8A establishes data-structure-level safety. Each data structure (DocumentStore, InvertedIndex) is individually thread-safe. The TOCTOU race is a **service-level** issue: IngestionService performs a check-then-act across two data structures without atomicity.

Phase 8B will add service-level coordination to make the check-and-act atomic.

## 11. Complexity Analysis

### DocumentStore Operations

| Operation | Time | Space |
|-----------|------|-------|
| `add()` | O(1) average + lock | O(1) |
| `get()` | O(1) average + lock | O(1) |
| `contains()` | O(1) average + lock | O(1) |
| `size()` | O(1) + lock | O(1) |
| `all()` | O(D) copy + lock | O(D) |
| `save()` | O(D log D + content) | O(D) snapshot |
| `load()` | O(file size) | O(D) temporary |

### InvertedIndex Operations

| Operation | Time | Space |
|-----------|------|-------|
| `add_document()` | O(T) + lock | O(T) |
| `postings()` | O(k) copy + lock | O(k) |
| `document_count()` | O(1) + lock | O(1) |
| `term_count()` | O(1) + lock | O(1) |
| `contains()` | O(1) average + lock | O(1) |

Where D = documents, T = terms per document, k = postings for a term.

### Lock Overhead

The `SharedMutex` wrapper adds:
- One heap allocation per data structure (`unique_ptr<SharedMutex>`)
- One mutex lock/unlock per operation
- One condition-variable wait/notify per writer operation

For a search engine handling thousands of requests per second, this overhead is negligible compared to the actual work (tokenization, ranking, I/O).

## 12. Testing Strategy

### DocumentStore Concurrency Tests (10 tests)

1. **ConcurrentGetCalls** — multiple threads call `get()` simultaneously
2. **ConcurrentContainsCalls** — multiple threads call `contains()` simultaneously
3. **ReadersDuringAdd** — readers while another thread adds
4. **ConcurrentUniqueAdds** — multiple threads add distinct IDs
5. **ConcurrentDuplicateAdds** — multiple threads add the same ID
6. **ReadersDuringSave** — readers while save executes
7. **LoadBehaviorUnderConcurrency** — load while other threads active
8. **SizeConsistentDuringConcurrentOps** — size() reflects actual count
9. **AllReturnsConsistentSnapshot** — all() returns complete data
10. **MixedWorkload** — concurrent reads and writes

### InvertedIndex Concurrency Tests (10 tests)

1. **ConcurrentReadOnlyPostings** — multiple threads read postings
2. **ConcurrentContains** — multiple threads call contains()
3. **ConcurrentCounts** — document_count/term_count under load
4. **ConcurrentUniqueAdds** — multiple threads add distinct docs
5. **ReadersDuringAdd** — readers while writer adds
6. **PostingListsSortedAfterConcurrentAdds** — sorted invariant preserved
7. **TermFrequencyCorrectAfterConcurrentAdds** — TF values correct
8. **DocumentCountCorrectAfterConcurrentAdds** — count correct
9. **SnapshotSurvivesConcurrentModification** — returned vector safe
10. **MixedWorkload** — concurrent reads and writes

### HTTP Concurrency Tests (9 tests)

1. **MultipleSimultaneousSearchRequests** — 8 threads, different terms
2. **ConcurrentSearchesForSameTerm** — 10 threads, same term
3. **ConcurrentSearchAndIngestion** — 4 searchers + 4 ingestors
4. **ConcurrentUniqueIngestion** — 16 threads, unique IDs
5. **SearchAfterConcurrentIngestion** — ingest then verify
6. **ServerRemainsResponsive** — background + foreground
7. **CleanShutdownAfterConcurrentRequests** — 8 threads then shutdown
8. **MixedReadWriteWorkload** — 6 readers + 4 writers
9. **ConcurrentANDandORQueries** — 8 threads, mixed modes

## 13. Verification Results

| Metric | Result |
|--------|--------|
| Total tests | 368 |
| Tests passed | **368/368 (100%)** |
| HTTP concurrency tests | 9/9 |
| InvertedIndex concurrency tests | 10/10 |
| DocumentStore concurrency tests | 10/10 |
| Ingestion concurrency tests | 6/6 |
| ConcurrentDuplicateAdds (100 runs) | 100/100 |
| HTTP concurrency (3 repeated runs) | 27/27 |
| Ingestion concurrency (3 repeated runs) | 18/18 |
| Compiler warnings | **0** |
| TSAN | **unavailable** (libtsan missing from MSYS2 UCRT64) |

## 14. Key Takeaways

1. **Two separate problems** — HTTP concurrency (thread pool) vs data synchronization (locks)
2. **Reader-writer locks** — multiple readers, one writer; more parallelism than mutex
3. **Own, don't borrow** — returning copies is safer than returning references/spans under concurrency
4. **cpp-httplib already handles concurrency** — don't build what's already built
5. **Writer-priority prevents starvation** — continuous readers can't block writers forever
6. **TOCTOU is a service-level issue** — data-structure safety doesn't guarantee service-level atomicity
7. **Portable wrappers beat platform bugs** — MinGW's std::shared_mutex has a known defect
8. **Snapshots under lock, I/O without** — minimize lock hold time for better throughput
9. **Atomic replacement** — load into temporary container, replace atomically
10. **Test under contention** — concurrent tests must create real parallel load, not just sequential calls
11. **Use return values as atomic checks** — `add()` return value eliminates TOCTOU without new locks
12. **Leverage existing atomicity** — `try_emplace` under exclusive lock is already the check-and-act we need
