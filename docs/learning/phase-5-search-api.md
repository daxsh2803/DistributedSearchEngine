# Phase 5 — Search API

## 1. Where the Search API Fits

```
External Client (curl, browser, mobile app)
    ↓ HTTP Request
HTTP Handler (Phase 5B-2)
    ↓ Structured request
SearchService (Phase 5B-1)  ← we are here
    ↓ Business logic
Ranker (Phase 4)
    ↓ TF-IDF scoring
QueryProcessor (Phase 3)
    ↓ Boolean retrieval
InvertedIndex (Phase 2)
    ↓ Term lookup
Tokenizer (Phase 1)
    ↓ Normalization
Raw Text
```

The SearchAPI is the **outermost layer** of the search engine. It is the
boundary between the internal search pipeline and external clients.

## 2. Why a Service Layer?

### The Problem Without SearchService

If the HTTP handler directly owns the Index and Ranker, two problems arise:

1. **Untestable without HTTP**: to test the search logic, you must start
   an HTTP server and send real requests. Unit tests become integration tests.

2. **Coupled to transport**: if you later want a CLI tool, a gRPC endpoint,
   or a test harness, you must duplicate the business logic in each transport.

### The Solution: SearchService

```
HTTP Handler          SearchService          Ranker
(parse params)  →     (validate, execute)  → (score)
(format JSON)   ←     (return results)     ←
```

The SearchService owns no state beyond a borrowed reference to the index.
It is a pure function: request in, response out. This means:

- **Unit tests** call `SearchService::search()` directly — fast, no network
- **Multiple transports** (HTTP, CLI, gRPC) can share the same SearchService
- **Business logic** is isolated from serialization and transport concerns

## 3. The SearchService API

### Data Structures

```cpp
enum class SearchMode { And, Or };

struct SearchResult {
    doc_id document_id;
    double score;
};

struct SearchRequest {
    std::string query;
    SearchMode mode = SearchMode::Or;
    std::size_t limit = 10;
};

struct SearchResponse {
    std::string query;
    std::string mode;
    std::size_t total;       // matches before limit
    std::size_t limit;
    std::vector<SearchResult> results;
    bool is_error = false;
    std::string error_message;
};
```

### Key Design Decisions

**SearchMode as an enum, not a string**: type-safe, no string comparison
at the business logic level. The HTTP handler converts "and"/"or" strings
to the enum.

**Default mode is OR**: OR is more permissive and returns more results,
which is the expected default for search engines.

**Default limit is 10**: a reasonable default that prevents accidentally
returning thousands of results.

**is_error in SearchResponse**: validation errors are returned as part
of the response, not thrown as exceptions. This makes the API predictable
and easy to test.

**total field**: the total number of matches before applying the limit.
This is essential for pagination (showing "Page 1 of 5" or "Showing 1-10
of 47 results").

## 4. Request Validation

The SearchService validates every request before execution:

| Field | Rule | Error |
|-------|------|-------|
| query | Non-empty after trimming whitespace | "Invalid request: empty query or limit < 1" |
| limit | ≥ 1 | "Invalid request: empty query or limit < 1" |
| mode | Always valid (enum by construction) | N/A |

Validation is a static method (`validate_request`) so callers can check
without executing.

## 5. How the Ranker Is Invoked

```
SearchService::search(request)
    ↓
if mode == And: ranker.ranked_and(query)
if mode == Or:  ranker.ranked_or(query)
    ↓
vector<RankedResult> (sorted by score desc)
    ↓
Apply limit: take first N results
    ↓
Convert RankedResult → SearchResult (drop unused fields)
    ↓
Return SearchResponse
```

The Ranker is created inside `search()` (it borrows the index, does not
own it). This is safe because the index outlives the SearchService.

## 6. Result Limiting

The limit is applied **after** ranking, not before. This ensures:
- The top-N results are always the highest-scored ones
- Limit does not affect which documents are scored
- Total reflects the full match count, not the truncated count

Example: 47 documents match, limit=10 → total=47, results.size()=10.

## 7. Complexity

| Operation | Time | Space |
|-----------|------|-------|
| Validate request | O(Q) | O(1) |
| Ranker invocation | O(Q + V log V + Σ Lᵢ + C log C) | O(C) |
| Apply limit | O(min(limit, C)) | O(min(limit, C)) |
| **Total** | Same as Ranker | Same as Ranker |

The SearchService adds negligible overhead beyond the Ranker.

## 8. Corpus Loading

In Phase 5, the search engine is **read-only**. The index is populated
programmatically at application startup:

```cpp
int main() {
    InvertedIndex index;
    // Hardcoded sample data (Phase 6 will add POST /documents)
    index.add_document(1, "the cat sat on the mat");
    index.add_document(2, "the dog chased the cat");
    // ...

    SearchService svc(index);
    // Start HTTP server (Phase 5B-2)
}
```

Document ingestion via API is deferred to Phase 6.

## 9. Testing Strategy

### Unit Tests (SearchService)

Tests call `SearchService::search()` directly — no HTTP, no JSON.

| Group | Tests | What It Proves |
|-------|-------|----------------|
| Basic search | 4 | AND/OR modes return correct documents |
| Result limiting | 4 | Limit truncates correctly, defaults work |
| Request validation | 7 | Empty query, whitespace, zero limit → errors |
| Ranking order | 2 | Results sorted by score desc, limited results are top-scored |
| Empty index | 2 | No documents → empty results |
| Edge cases | 5 | Single doc, punctuation, case, response fields |
| Determinism | 2 | Same query → same result; insertion order irrelevant |
| Integration | 2 | TF-IDF scores in results, AND misses partial matches |

**Total: 28 tests**

### Integration Tests (HTTP, Phase 5B-2)

Tests send real HTTP requests to a localhost server:
- Valid requests → 200 + JSON
- Missing/invalid params → 400 + error JSON
- JSON structure matches contract
- AND/OR modes work end-to-end
- Limit behavior verified

## 10. System Design Concepts

### API Contracts

The `GET /search` endpoint has a defined request format and response
format. Both client and server agree on this contract. Breaking the
contract (e.g., changing the response format) is a breaking change.

### Service Boundaries

The SearchService is the **service boundary**. Everything inside
(SearchService, Ranker, QueryProcessor, Index, Tokenizer) is the
search engine's internal implementation. Everything outside (HTTP
handler, JSON serialization) is the transport layer.

This boundary is critical for future distribution: each service
instance can be independently deployed, scaled, and tested.

### Stateless Request Handling

Every call to `SearchService::search()` is independent. There is no
session state, no request accumulation, no shared mutable state.
This means:
- Requests can be handled in any order
- Multiple threads can call search() simultaneously (if the index
  is read-only)
- No cleanup is needed between requests

### Validation

Input validation happens at the boundary (SearchService), not deep
in the pipeline. This fails fast: invalid requests never reach the
Ranker or Index.

### Error Handling

Validation errors return `is_error = true` in the response. The HTTP
layer converts this to a 400 status code. Business logic errors
(empty results) are not errors — they return `is_error = false` with
an empty results vector.

### Serialization (Phase 5B-2)

The HTTP handler converts `SearchResponse` → JSON. This is a pure
function: structured data in, JSON string out. The SearchService
never knows about JSON.

## 11. What Is Deferred

- **HTTP server** (Phase 5B-2): cpp-httplib, JSON serialization
- **Document ingestion API** (Phase 6): `POST /documents`
- **Authentication** — later phase
- **Rate limiting** — later phase
- **Caching** — later phase
- **Persistent storage** — later phase
- **HTTPS/TLS** — later phase
- **Request logging** — later phase
- **Health check endpoint** — optional
- **Metrics/monitoring** — later phase
- **Docker/deployment** — later phase
- **Distributed querying** — much later phase

---

## 12. Implementation Results — Phase 5B-2

### HTTP Server Architecture

The HTTP transport layer uses two header-only libraries fetched via CMake FetchContent:

- **cpp-httplib v0.23.0** — synchronous HTTP server and client (MIT license)
- **nlohmann/json v3.12.0** — JSON serialization (MIT license)

Both are linked as PUBLIC dependencies of `dse_core` so all targets can use them.

### Server Lifecycle

```
HttpServer(service)
  | owns httplib::Server
  | registers GET /search
  |
  start_server() in thread:
    server_->listen(0)  // OS-assigned port
    server_->wait_until_ready()  // block until accepting
  |
  httplib::Client sends GET /search?q=...
  |
  stop() + thread.join()
```

Key design decisions:
- `bind_to_any_port()` returns the actual port (not `bind_to_port()` which returns bool)
- `wait_until_ready()` blocks until the server is accepting connections (replaces fragile sleep)
- `stop()` is safe to call from any thread
- Destructor calls `stop()` + `join()` if the thread is still running

### JSON Serialization

Uses `nlohmann::json` for proper serialization rather than manual string construction:

```cpp
nlohmann::json j;
j["query"] = resp.query;
j["mode"]  = resp.mode;
j["total"] = resp.total;
j["limit"] = resp.limit;
j["results"] = results;  // nlohmann::json::array()
return j.dump();
```

### MSYS2/MinGW Compatibility

cpp-httplib auto-detects system features (OpenSSL, Brotli, Zlib, Zstd) which
conflict with MSYS2 headers. Disabled via CMake options before FetchContent:

```cmake
set(HTTPLIB_USE_OPENSSL_IF_AVAILABLE OFF CACHE BOOL "" FORCE)
set(HTTPLIB_USE_ZLIB_IF_AVAILABLE OFF CACHE BOOL "" FORCE)
set(HTTPLIB_USE_BROTLI_IF_AVAILABLE OFF CACHE BOOL "" FORCE)
set(HTTPLIB_USE_ZSTD_IF_AVAILABLE OFF CACHE BOOL "" FORCE)
set(HTTPLIB_USE_NON_BLOCKING_GETADDRINFO OFF CACHE BOOL "" FORCE)
```

The `HTTPLIB_USE_NON_BLOCKING_GETADDRINFO` option triggers `GetAddrInfoExCancel`
(a Windows 8+ API not available in MinGW headers). Disabling it is essential.

### Testing Approach

19 HTTP integration tests use real HTTP requests to localhost:

1. Test fixture creates an `HttpServer` per test
2. Server starts on OS-assigned port (port 0) in a background thread
3. `wait_until_ready()` blocks until the server is accepting connections
4. `httplib::Client` sends real HTTP GET /search requests
5. Responses are parsed with `nlohmann::json`
6. TearDown calls `stop()` + `thread.join()` for clean shutdown

Each test takes ~2s due to server startup/teardown overhead.

### Test Results

- 19 HTTP API tests: all passing
- 220 existing tests: all passing
- Total: **239/239 passed (100%)**
- Build: zero warnings under `-Wall -Wextra -Wpedantic`
- Total test time: ~51 seconds (HTTP tests dominate due to lifecycle overhead)

---

## 13. Implementation Results — Phase 5B-3

### Application Entry Point

`src/main.cpp` was rewritten to create a complete working application:

1. Create `InvertedIndex` and load 20 seed documents
2. Create `SearchService` borrowing the index
3. Create `HttpServer` borrowing the service
4. Parse port from `--port` CLI arg, `DSE_PORT` env var, or default 8080
5. Start server in a thread, print address, wait for shutdown signal

### Seed Corpus

20 documents covering:
- Animal text (fox, dog, hound) — demonstrates ranking differences
- Programming languages (C++, Rust, Python)
- Infrastructure (Linux, Git, Docker, PostgreSQL, Redis, Kafka, Kubernetes)
- Search concepts (inverted maps, TF-IDF, retrieval)
- Web/ML (browsers, HTTP, neural networks, machine learning)

### Port Configuration

Priority: `--port <N>` > `DSE_PORT=<N>` > default 8080.

Chosen because:
- CLI arg is explicit and standard for servers
- Env var allows Docker/deployment without code changes
- Default lets users run without configuration

### Shutdown

Signal-based: `SIGINT` (Ctrl+C) and `SIGTERM` call `server.stop()` via a
global `std::atomic<HttpServer*>`. The server thread exits cleanly, the
main thread joins, and destructors clean up in order. No infinite loop.

### MSYS2 Compatibility Fix

`HttpServer::listen(port)` now correctly uses:
- `bind_to_port(host, port)` when port > 0 (specific port)
- `bind_to_any_port(host)` when port == 0 (OS-assigned, for tests)

### Tests Added

5 application integration tests:
1. Single-term search returns results from seed corpus
2. Multi-term AND search
3. Multi-term OR search with ranking verification
4. Response is valid JSON with correct structure
5. Application starts and serves correctly

### Test Results

- **244/244 tests passed (100%)**
  - 5 app integration tests + 19 HTTP API tests + 28 SearchService tests
    + 34 Ranker tests + 60 QueryProcessor tests + 52 InvertedIndex tests
    + 45 Tokenizer tests + 2 smoke tests
- Build: zero warnings
- Application: starts, serves, responds to curl, shuts down cleanly
