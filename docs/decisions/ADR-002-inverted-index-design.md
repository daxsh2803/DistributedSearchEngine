# ADR-002: Inverted Index Design

- **Status:** Accepted
- **Date:** 2026-08-16 (Phase 2A — design; implementation in Phase 2B)
- **Deciders:** DistributedSearchEngine project (per AGENTS.md methodology:
  explain, design, trade-offs, complexity, implement, test, build, report)

## Context

The pipeline is `Raw Document -> Tokenizer -> Normalized Tokens -> Inverted
Index -> Query Processing -> Ranking -> Results`. Phase 1 delivered the
tokenizer (`dse::tokenize`, ADR-001): a pure, deterministic, ASCII-oriented
function that preserves order and duplicates, so term-frequency information
survives to downstream stages. Phase 2 must build the component that consumes
those tokens and makes documents findable by term.

Constraints:

- C++20, no third-party data structures; the core engine is implemented by us
  (no Lucene/Solr/Elasticsearch — AGENTS.md);
- keep components simple, deterministic, and thoroughly testable;
- keep the project buildable and testable after every phase;
- later phases add query processing and BM25 ranking, which require per-term
  postings (docID + term frequency), document frequency, and collection size;
- avoid over-engineering: do not build update/delete, positions, persistence,
  or distribution before they are needed (AGENTS.md: do not implement future
  phases prematurely);
- dependency direction stays one-way: the index may depend on the tokenizer;
  nothing downstream is built yet.

## Decision

Adopt a **single, in-memory, deterministic `InvertedIndex` class**:

- `using doc_id = std::uint32_t;`
- `struct Posting { doc_id document_id; std::uint32_t term_frequency; };`
- storage: `std::unordered_map<std::string, std::vector<Posting>>`
  (term -> postings list);
- **postings lists are always sorted by document ID** (append for in-order
  arrivals, binary-search + insert otherwise);
- term frequencies are aggregated from the tokenizer's duplicate-preserving
  output: a document's tokens are counted into a local
  `{term -> count}` map, then merged as postings `(id, count)`;
- **document frequency is derived**, not stored: `postings(term).size()`;
- **unique docID precondition**: each document ID may be added at most once;
  re-adding is a contract violation (asserted in debug builds);
- **lookup returns `std::span<const Posting>`**: empty for unknown terms (a
  postings list is never empty for an existing term), valid until the next
  modification of the index;
- `document_count()`, `term_count()`, and `contains(term)` as O(1)
  convenience queries;
- no options, no configuration, no class hierarchy; a stateful class (unlike
  the stateless tokenizer) because the index accumulates documents across
  calls.

**Proposed API:**

```cpp
namespace dse {

using doc_id = std::uint32_t;

struct Posting {
    doc_id document_id;
    std::uint32_t term_frequency;
};

class InvertedIndex {
public:
    void add_document(doc_id id, std::string_view text);
    std::span<const Posting> postings(std::string_view term) const;
    std::size_t document_count() const;
    std::size_t term_count() const;
    bool contains(std::string_view term) const;

private:
    std::unordered_map<std::string, std::vector<Posting>> postings_by_term_;
    std::size_t document_count_ = 0;
};

} // namespace dse
```

## Key rules

| Rule | Rationale |
|---|---|
| Term = tokenizer token, stored lowercase | consistency by construction; the tokenizer (ADR-001) is the sole authority on terms |
| Postings sorted by docID | enables O(a + b) merge/intersection and O(log k) binary search in later phases |
| TF per posting; DF = list size | BM25 needs both; DF falls out of the structure in O(1) |
| Duplicates aggregated to TF | preserves the term-frequency signal Phase 1 deliberately kept |
| Unique docID per insertion | append corrupts TF/DF; replace needs a forward map — premature |
| `span<const Posting>` lookups | zero-copy reads for query processing; empty span == missing term |
| `unordered_map` storage | O(1) average lookups/inserts dominate the workload; no ordering requirement |

## Alternatives considered

1. **`std::map<std::string, std::vector<Posting>>`** — sorted-term iteration
   and O(log n) operations. We never iterate the vocabulary in order, and
   O(1) average lookup is what query processing will demand. Rejected.
2. **Return `const std::vector<Posting>&`** — a reference into the map node
   dangles on rehash (nodes move); a span into the vector's heap buffer does
   not, and a span is honest about borrowing. Rejected in favor of `span`.
3. **Return `std::vector<Posting>` by value** — safe but copies the whole
   list per lookup; every query would pay O(list length) for no reason.
   Rejected.
4. **`std::optional<std::span<const Posting>>`** — distinguishes "missing"
   from "empty"; unnecessary because an existing term's list is never empty.
   Rejected for simplicity.
5. **Replace semantics via a forward map (`doc_id -> terms`)** — correct for
   updates, but roughly doubles per-pair storage and adds a second structure
   before any consumer needs it. Deferred to the update/delete phase.
6. **Append on duplicate docID** — silently corrupts TF and DF. Rejected.
7. **Store positions per posting** — phrase queries are a later phase; TF-only
   postings are all BM25 needs. Deferred (additive: a positional structure
   alongside).
8. **Persistent/on-disk index now** — serialization format is a later phase;
   the in-memory structure is the right first step and unchanged by that work.
   Rejected for Phase 2.
9. **Sorted `std::vector` of (term, postings) pairs instead of a hash map** —
   compact and cache-friendly, but O(V) binary-search lookups per term and
   O(V) insertion shifts; wrong profile for a lookup-heavy index. Rejected.
10. **A `Tokenizer`-style free function over tokens** — indexing is
    stateful by nature (accumulation across calls); a class with private
    state and invariants is the honest model. The stateless-vs-stateful
    distinction from Phase 1B-1 applies here.

## Trade-offs

Accepted costs of the chosen design:

- **Out-of-order docID insertion** costs O(log k + k) per affected posting
  (binary-search insert); the common in-order corpus case is amortized O(1)
  append.
- **`unordered_map` overhead** — bucket array plus per-entry node overhead,
  and unspecified vocabulary iteration order (callers must not depend on
  it). A compact sorted-array layout may replace it in the persistence
  phase.
- **No updates/deletes** — re-indexing a changed document is impossible
  without violating the unique-docID precondition; accepted until the
  update phase adds a forward map.
- **Plain 4-byte docIDs and TFs** — no compression; a known later-phase
  optimization (delta encoding, varint).
- **Lookup key copy** — `unordered_map` lookup from a `string_view`
  constructs a temporary `std::string` (heterogeneous lookup needs a
  transparent hash); cost is O(|term|), the same order as hashing, so it is
  not a real penalty.

Benefits:

- **O(1) average term lookup** and **O(T) amortized** indexing — the
  performance envelope every later phase builds on;
- **everything BM25 needs is already present**: TF in postings, DF from list
  sizes, N from `document_count()`;
- **sorted postings lists** make AND/OR merging a textbook O(a + b) walk in
  the next phase;
- **deterministic and pure-in-effect**: identical insertion sequences
  produce identical indices; no I/O, no randomness, no global state;
- **small, honest contract** — every future feature (positions, updates,
  persistence, distribution) is additive rather than breaking.

## Consequences

Positive:

- The index is the first *consumer* of the tokenizer; dependency direction
  stays one-way (`inverted_index` -> `tokenizer.h`), and both live in the
  existing `dse_core` library with the Phase 1B CMake pattern.
- The vocabulary is defined by the tokenizer alone; case-insensitivity,
  ASCII scope, and duplicate preservation carry through by construction.
- Tests can be written purely against the public contract (text in,
  postings out), including the tokenizer-integration property, without any
  mocks or I/O.
- Sorted postings and derived DF remove whole categories of future
  bookkeeping (no separate DF table, no sort step before merging).

Negative (accepted):

- A document cannot be re-indexed or removed once added (unique-docID
  precondition); genuine update workloads must wait for the forward-map
  phase.
- The index is in-memory and single-threaded; scale, durability, and
  distribution are explicitly out of Phase 2 scope.
- Vocabulary iteration order is unspecified, so any future "list all terms"
  feature must define its own ordering.

## Future evolution

All extensions are additive and do not change the Phase 2 contract:

- **Query processing** — AND/OR merge over sorted postings lists;
  `postings()` and `contains()` are the only lookups it needs.
- **BM25 ranking** — consumes TF (in postings), DF (list size), and N
  (`document_count()`); no index changes required.
- **Document updates/deletes** — add a forward map `doc_id -> terms` and
  relax the unique-docID precondition to replace semantics, with its own
  tests.
- **Positions** — a separate positional structure or extended posting
  (docID, TF, positions) for phrase queries; `postings()` remains TF-focused.
- **Persistence** — serialization of the vocabulary and postings; internal
  layout (e.g., sorted arrays, compressed docIDs) can change behind the
  public API.
- **Concurrency / sharding / replication** — distribution phases; the index
  design (immutable-ish, deterministic, per-term lists) is a good base for
  sharding by term hash.
- **Transparent-hash lookups** — a custom hash enables true `string_view`
  heterogeneous lookup, removing the temporary-key copy.

Reference: full learning material in
`docs/learning/phase-2-inverted-index.md`.
