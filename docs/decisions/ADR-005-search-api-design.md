# ADR-005: Search API Design

## Status

Accepted (Phase 5B-1: SearchService implemented; Phase 5B-2: HTTP layer implemented; Phase 5B-3: Application integration implemented)

## Context

Phases 1–4 build a complete single-node search pipeline:
Tokenizer → InvertedIndex → QueryProcessor → Ranker → Ranked Results

Every component is tested (220/220) but the only consumer is `main.cpp`,
which prints a banner and exits. There is no way for an external client
to query the search engine.

The project goal is to evolve into a distributed search system, so the
API design must create a clean boundary between clients and the search core.

## Decision

Introduce a `SearchService` class as the business-logic boundary, with an
HTTP REST layer to be added in Phase 5B-2.

### Architecture

```
HTTP Client → HTTP Handler → SearchService → Ranker → InvertedIndex
```

The SearchService:
- Owns no state beyond a borrowed `const InvertedIndex&`
- Composes the Ranker for query execution
- Validates requests (empty query, invalid mode/limit)
- Applies result limits after ranking
- Returns structured `SearchResponse` objects

### Data Model

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
    std::size_t total;
    std::size_t limit;
    std::vector<SearchResult> results;
    bool is_error = false;
    std::string error_message;
};
```

### API Contract (HTTP, Phase 5B-2)

```
GET /search?q={query}&mode={and|or}&limit={1-100}

Response 200:
{
  "query": "string",
  "mode": "and" | "or",
  "total": <number>,
  "limit": <number>,
  "results": [
    { "document_id": <number>, "score": <number> },
    ...
  ]
}

Response 400:
{ "error": "description" }
```

### Corpus Loading

The search engine is read-only in Phase 5. The index is populated
programmatically at application startup (hardcoded sample data in
`main.cpp`). Document ingestion via API is deferred to Phase 6.

## Alternatives Considered

1. **Direct Ranker access in HTTP handler** — rejected: couples
   transport with business logic; harder to test without HTTP

2. **CLI-only interface** — rejected: no service boundary;
   not composable; not distributable

3. **gRPC** — rejected: heavier dependency; overkill for Phase 5

4. **Separate service in another language** — rejected: premature
   microservices; loses type safety

## Trade-offs

- SearchService is stateless and thin; future phases may add caching,
  rate limiting, or request logging here
- The `is_error` field in SearchResponse avoids exceptions for
  validation errors, keeping the API predictable
- JSON serialization is deferred to Phase 5B-2 (HTTP layer)

## Consequences

- New files: `src/search_service.h`, `src/search_service.cpp`,
  `tests/search_service_test.cpp`
- Modified: `CMakeLists.txt` (add search_service.cpp to dse_core,
  add search_service_test target)
- No changes to Phase 1–4 code
- The SearchService is testable without HTTP (pure C++ unit tests)

## Future Evolution

1. **HTTP layer** (Phase 5B-2): cpp-httplib + JSON serialization
2. **Application integration** (Phase 5B-3): seed corpus, configurable port, signal-based shutdown
3. **Document ingestion API** (Phase 6): `POST /documents`
3. **Authentication** — later phase
4. **Rate limiting** — later phase
5. **Caching** — later phase
6. **Request logging** — later phase
