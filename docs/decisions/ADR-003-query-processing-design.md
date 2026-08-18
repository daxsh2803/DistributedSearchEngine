# ADR-003: Query Processing Design

- **Status:** Accepted
- **Date:** 2026-08-17 (Phase 3A — design; implementation in Phase 3B)
- **Deciders:** DistributedSearchEngine project (per AGENTS.md methodology:
  explain, design, trade-offs, complexity, implement, test, build, report)

## Context

The pipeline is `Raw Document -> Tokenizer -> Normalized Tokens -> Inverted
Index -> Query Processing -> Ranking -> Results`. Phase 1 delivered the
tokenizer (`dse::tokenize`, ADR-001): pure, deterministic, ASCII-oriented,
duplicate-preserving. Phase 2 delivered the inverted index (ADR-002): a
stateful `dse::InvertedIndex` mapping terms to **postings lists that are
always sorted by document ID**, with `postings(term)` returning a borrowed
`std::span<const Posting>` (empty for missing terms), and TF stored per
posting for future ranking.

Phase 3 must build the component that turns a user's raw query into a
document-ID result list, using the index as the read-only source of truth.
Constraints:

- C++20, no third-party text-processing or retrieval library (the engine is
  ours — AGENTS.md forbids Lucene/Solr/Elasticsearch);
- deterministic, pure-in-effect behavior and exhaustive testability;
- keep the project buildable and testable after every phase;
- later phases add BM25 ranking (which consumes the result list and needs
  TF/DF/N from the index), phrase queries, and a query language — the Phase 3
  contract must make these additive, not breaking;
- avoid over-engineering: no scoring, no query parser, no negation, no
  pagination, no distribution yet (AGENTS.md: do not implement future phases
  prematurely);
- dependency direction stays one-way: query processing may depend on the
  tokenizer and the index; nothing upstream changes.

## Decision

Adopt a two-layer query-processing design:

1. **Two pure two-pointer merge primitives** over sorted postings lists,
   hand-rolled (not `std::set_intersection`/`std::set_union`):

   ```cpp
   std::vector<doc_id> intersect(std::span<const Posting> lhs,
                                 std::span<const Posting> rhs);
   std::vector<doc_id> merge_union(std::span<const Posting> lhs,
                                   std::span<const Posting> rhs);
   ```

   - precondition: both inputs sorted by `document_id` (the Phase 2
     invariant);
   - output: owning `std::vector<doc_id>`, sorted ascending, deduplicated;
   - complexity: O(lhs.size() + rhs.size()) each.

2. **A `QueryProcessor` class** that borrows a `const InvertedIndex&` and
   composes the pipeline:

   ```cpp
   class QueryProcessor {
   public:
       explicit QueryProcessor(const InvertedIndex& index);
       std::vector<doc_id> and_query(std::string_view query) const;
       std::vector<doc_id> or_query(std::string_view query) const;
   private:
       const InvertedIndex* index_;  // borrowed; must outlive the processor
   };
   ```

   Pipeline per query: `dse::tokenize(query)` -> deduplicate terms
   (`std::sort` + `std::unique`, deterministic) -> `index.postings(term)`
   per distinct term -> fold the postings lists with the primitives.

**Semantics (the contract):**

- **AND** = intersection of all distinct query terms' postings lists,
  folded smallest-list-first (same answer regardless of order; the choice
  only affects cost);
- **OR** = union of all distinct query terms' postings lists;
- **missing term:** AND -> empty result (no document can contain an
  unindexed term); OR -> the missing term contributes nothing;
- **zero query terms** (empty / whitespace / punctuation-only / non-ASCII
  input): empty result for both AND and OR (match-all semantics deferred);
- **results:** owning, sorted ascending, deduplicated, deterministic —
  identical queries against identical indices yield identical vectors;
- **no scoring:** results are docID lists in canonical order; relevance
  ordering is the ranking phase's job;
- **no query parser:** AND and OR are explicit methods, not parsed syntax.

## Key rules

| Rule | Rationale |
|---|---|
| Reuse `dse::tokenize` unchanged for queries | documents and queries are normalized identically by construction; one authority on terms |
| Merge sorted spans, never copy postings | O(a + b) merges; zero-copy reads of index storage |
| Hand-rolled two-pointer merges | the merge is the pedagogical core of the phase; matches the learning material exactly; trivially swappable for the STL later |
| Deduplicate query terms before merging | OR would otherwise emit duplicate docIDs; AND would wastefully self-intersect; duplicates carry no Boolean signal |
| Missing term: AND -> empty, OR -> ignored | matches the semantics of "all terms" vs "at least one term"; falls out of empty-span convention |
| Zero query terms -> empty result | honest answer to "query asks for nothing"; match-all is a separate deferred feature |
| Result = owning `vector<doc_id>`, sorted, deduplicated | deterministic, exact-testable, canonical input for ranking |
| Smallest-list-first AND fold | commutative semantics; keeps the accumulator small and work proportional to the smallest lists |
| Borrowed index (must outlive processor) | query processing reads only; sharing by borrowing is the honest model, like `string_view` one level up |
| No scoring, no parser, no negation, no pagination | each is a later phase; the Boolean core is the smallest useful retrieval layer |

## Alternatives considered

1. **Use `std::set_intersection` / `std::set_union` from `<algorithm>`** —
   battle-tested and concise, but they hide the algorithm and make the
   Posting->docID projection less obvious. The project's core principle is
   understanding the architecture; a hand-written merge is ~15 lines and
   matches the step-by-step learning material exactly. Rejected for Phase 3B
   (noted as a trivial future refactor).
2. **A single monolithic query function** (e.g. one free function taking the
   index and a query) — less testable: the merge logic could not be verified
   in isolation against hand-built vectors. Chosen: primitives + composing
   class.
3. **Return `std::span<const doc_id>` borrowed from a processor-held
   cache** — would add state and a lifetime minefield for no benefit at this
   scale. Chosen: owning vector.
4. **Return `std::vector<Posting>`** (keep TF in results) — doubles result
   size; Boolean answers need document identity only. Ranking can re-fetch
   scores from the index. Rejected.
5. **AND-only retrieval** — simpler, but OR is nearly free with the same
   machinery and gives ranking a complete Boolean baseline. Chosen: both.
6. **No query-term dedup** — OR would produce duplicate docIDs, violating
   the sorted-deduplicated contract. Rejected.
7. **Fold AND in query-term order** — correct but wasteful; the accumulator
   can stay large while intersecting with huge lists. Chosen:
   smallest-list-first (identical semantics, better typical cost,
   deterministic via (size, term) ordering).
8. **Empty query returns all documents (match-all)** — a real feature but a
   different one; accidentally treating "empty" as "everything" is how
   engines leak entire corpora. Deferred explicitly.
9. **A query-language parser now** (`"cat AND dog"`, parentheses, NOT) —
   defines syntax before semantics are settled; negation requires the full
   docID universe and belongs with a later phase. Rejected for Phase 3.
10. **Streaming / paginated results** — real engines cap results, but full
    materialization is the simplest correct contract and exactly what
    ranking needs as input. Deferred to the ranking/pagination phase.

## Trade-offs

Accepted costs of the chosen design:

- **Hand-rolled merges** duplicate what `<algorithm>` already provides;
  risk of subtle bugs is mitigated by exhaustive primitive-level tests and
  the step-by-step traces in the learning document.
- **Full result materialization** — an OR query matching a million documents
  produces a 4 MB vector; no limits, no top-k, no streaming. Inherent to the
  "return the complete answer" contract; deferred features will cap it.
- **Borrowed-index lifetime contract** — the processor must not outlive the
  index; a caller error is a dangling reference, not a compile error. The
  constructor-takes-reference pattern and documentation mitigate this.
- **No relevance ordering** — Boolean results are canonical, not ranked;
  users wanting "best first" must wait for ranking.
- **Sort+unique dedup is O(V log V)** rather than hash-based O(V) — fine at
  query scale and fully deterministic.

Benefits:

- **O(a + b) merges** over sorted postings — the Phase 2 invariant pays off
  exactly as designed; quadratic naive intersection is avoided entirely.
- **Deterministic, sorted, deduplicated results** — exact unit testing and
  reproducible behavior; a canonical baseline ranking can reorder.
- **Pure, isolated testable primitives** plus a two-method public contract —
  the entire phase is testable through the public API with no mocks or I/O.
- **Purely additive** — no Phase 1 or Phase 2 contract changes; tokenizer,
  index, and processor all live in `dse_core` with one-way dependencies.
- **Everything ranking needs is already in place** — the result list is the
  ranker's input; TF/DF/N remain queryable in the index untouched.

## Consequences

Positive:

- The engine can now answer queries end to end: raw query text ->
  tokenizer -> index lookups -> Boolean merge -> sorted docID list.
- End-to-end integration tests (tokenizer -> index -> processor) prove the
  three components agree with hand-computed expectations, without
  duplicating any component's own tests.
- The sorted-result contract is exactly what cross-shard merging and ranking
  will build on in later phases.

Negative (accepted):

- Boolean-only retrieval: no relevance order, no phrase, no negation, no
  query syntax — each deferred.
- Results are materialized in full; large result sets are the caller's
  problem until pagination/top-k arrives.
- The processor's validity is tied to the index's lifetime; misuse (index
  destroyed first) is undefined behavior, as with any borrowed reference.

## Future evolution

All extensions are additive and do not change the Phase 3 contract:

- **Ranking (BM25)** — consumes the Boolean result list, re-fetching TF/DF/N
  from the index; `QueryProcessor` is the natural home for a `rank()`
  layer, or a new `Ranker` consuming the same pipeline.
- **Phrase queries** — needs Phase 2's deferred positional postings; a
  positional AND over positions, layered on the same result contract.
- **Negation and a query language** — a parser producing an expression tree
  over AND/OR/NOT, executed with the same merges (NOT requires the full
  docID universe from `document_count()`).
- **Top-k and pagination** — cap materialization by scoring during the
  merge (priority queue), a later ranking-phase feature.
- **Fuzzy search** — separate n-gram/edit-distance component feeding the
  same processor with expanded term sets.
- **Persistence** — the processor's public API survives unchanged behind an
  on-disk index.
- **Distributed querying** — fan-out to shards and merge of sorted partial
  results; the sorted, deduplicated contract makes the merge straightforward.
- **Query caching** — memoize identical queries; an optimization, not a
  correctness change.

Reference: full learning material in
`docs/learning/phase-3-query-processing.md`.
