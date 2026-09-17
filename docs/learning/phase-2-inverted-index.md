# Phase 2 - Inverted Index: Design and Learning Notes (Phase 2A)

Phase 1 gave us the first real component: the **tokenizer** (`dse::tokenize`),
a pure function that turns raw text into normalized tokens. Phase 2 builds the
component that actually *stores* those tokens so they can be searched: the
**inverted index**. This document is the Phase 2A design specification and
learning material, written for a C++ student learning system design. It
contains **no implementation** — the code comes in Phase 2B.

The companion decision record is
`docs/decisions/ADR-002-inverted-index-design.md`, which captures *why* these
choices were made.

---

## 1. Where the inverted index fits in the search engine

Our pipeline, with the Phase 2 component highlighted:

```
Raw Document
    |
    v
Tokenizer          <- Phase 1 (COMPLETE: dse::tokenize)
    |
    v
Normalized Tokens
    |
    v
Inverted Index     <- Phase 2 (THIS DOCUMENT)
    |
    v
Query Processing   <- later phase
    |
    v
Ranking            <- later phase (BM25)
    |
    v
Results
```

The tokenizer converts one piece of text into a *list of tokens*. The
inverted index answers the question the tokenizer cannot: **"which documents
contain this term, and how often?"** It is the memory of the engine.

To appreciate what "inverted" means, consider the naive way you might store
documents:

```
Document 1: "the cat sat"     -> tokens: [the, cat, sat]
Document 2: "the dog ran"     -> tokens: [the, dog, ran]
Document 3: "cat and dog"     -> tokens: [cat, and, dog]
```

A **forward index** is organized *by document*: for each document, list its
terms. That is exactly what the tokenizer output gives us per document. It is
great for "show me document 1's words" but terrible for "which documents
contain `cat`?" — you would have to scan every document.

An **inverted index** flips the organization *by term*:

```
cat -> [ (1, 1), (3, 1) ]      term "cat" appears in docs 1 and 3
dog -> [ (2, 1), (3, 1) ]      term "dog" appears in docs 2 and 3
the -> [ (1, 1), (2, 1) ]
sat -> [ (1, 1) ]
ran -> [ (2, 1) ]
and -> [ (3, 1) ]
```

Now "which documents contain `cat`?" is a single lookup. This is the central
data structure of essentially every real search engine (Lucene, Solr,
Elasticsearch all build one — the rule in AGENTS.md forbids *using* them, so
we build ours ourselves).

### Why the inverted index is foundational

1. **It is where "can this be found?" is decided.** If a term is not in the
   index, no query can ever retrieve it. Everything downstream (query
   processing, ranking) only ever sees what the index returns.
2. **It is where tokenizer decisions become permanent.** The vocabulary — the
   set of terms the engine knows — is defined by whatever the tokenizer
   emitted and the index stored. Lowercasing in Phase 1 means one entry per
   word form; preserving duplicates means the index can count term
   frequencies.
3. **It determines query speed.** A lookup must be effectively O(1) per term;
   ranking later iterates the returned postings lists. The data structure
   chosen here sets the performance envelope for every later phase.

---

## 2. The index's responsibility

A single, clearly stated job:

> **Maintain a mapping from terms to the documents that contain them, with
> enough information for later ranking — and nothing else.**

### The index SHOULD

- accept a document (an ID + raw text), tokenize it with `dse::tokenize`,
  and record which terms occur in it and how often;
- answer "given a term, which documents contain it, and with what term
  frequency?" via postings-list lookup;
- count documents and vocabulary size;
- keep postings lists sorted by document ID so later phases can merge them
  efficiently.

### The index should NOT

- rank documents or score relevance (Phase for BM25);
- process queries (later phase — it only *serves* lookups);
- remove stop words, stem, or lemmatize (language-policy stages, deferred);
- persist to disk (later phase — it is an in-memory structure);
- handle concurrency, sharding, replication, or networking (later phases);
- store the documents themselves (only term → document-ID relationships; the
  document store is a separate concern).

### Why these responsibilities stay outside

Each excluded job is a separate concern with its own correctness criteria:
ranking is a *scoring* policy that needs collection statistics and tuning;
query processing is an *algorithm* over postings lists; persistence is about
format and durability; distribution is about machines, not data structures.
If the index did any of these, we could not test, change, or benchmark one
without the others. The index stays a pure, deterministic in-memory data
structure — the same philosophy that kept the tokenizer pure in Phase 1.

---

## 3. The pieces: terms, document IDs, postings lists

### Terms

A **term** is a canonical token — the unit of vocabulary. In our engine, a
term is exactly what `dse::tokenize` produces: lowercased maximal runs of
ASCII letters and digits. `"Cat"`, `"CAT"`, and `"cat"` all tokenize to the
term `cat`; that is *one* entry in the index, not three. The tokenizer is the
sole authority on what a term is — the index never re-tokenizes or
second-guesses it, it just stores terms as `std::string`.

### Document IDs

A **document ID** (docID) is an integer that uniquely identifies a document:
`using doc_id = std::uint32_t;`. Why integers instead of the document's title
or path?

- **Compact.** 4 bytes vs. a variable-length string in every posting.
- **Comparable.** Sorting, merging, and intersecting postings lists all rely
  on "is this docID before that one?" — trivial with integers.
- **Abstract.** The index does not care what a document *is* (a file, a web
  page, a log line); it only tracks relationships between integer IDs. The
  actual content store (docID → text) is a later concern.

This is a classic separation: the *index* maps terms to IDs; a *document
store* maps IDs to content. Neither needs to know about the other.

### Postings lists

A **posting** records that a term occurs in one document, plus the **term
frequency** (how many times). A **postings list** is all postings for one
term:

```cpp
struct Posting {
    doc_id document_id;
    std::uint32_t term_frequency;
};
// postings list for a term: std::vector<Posting>, sorted by document_id
```

Two design choices deserve explanation:

1. **Sorted by docID.** When query processing arrives (a later phase), the
   most common operation is *intersection*: "which documents contain both
   `search` and `engine`?" With two sorted lists you walk both with two
   pointers in O(a + b) — the classic merge. Sorted lists also make
   lookups-within-a-list (is doc 42 in this list?) binary-searchable in
   O(log k).
2. **Term frequency stored per posting.** Ranking (TF-IDF, BM25) scores a
   document by how often a term appears *in it* — that number must live
   somewhere, and the posting is its natural home.

---

## 4. Term frequency vs document frequency

Two quantities sound similar and are easy to confuse:

| | Term frequency (TF) | Document frequency (DF) |
|---|---|---|
| What it counts | occurrences of the term *within one document* | number of documents *containing* the term |
| Scope | one (term, document) pair | the whole collection |
| Where it lives | inside a posting (`term_frequency`) | derived from the postings list (`postings.size()`) |
| Why it matters later | BM25 weights how strongly a term represents *this* document | IDF = inverse document frequency weights how *rare* (informative) the term is collection-wide |

In our design, **DF needs no separate storage**: the postings list for a term
has one entry per document, so `postings(term).size()` *is* the document
frequency, in O(1). That is a nice property of the inverted structure — the
most collection-level statistic a ranker needs falls out of the structure
itself.

Example: index the three documents from Section 1. For `cat`: postings =
`[(1, 1), (3, 1)]`, so TF in doc 1 is 1 and DF is 2. If document 1 were
"the cat cat sat", the posting for (cat, doc 1) would be `(1, 2)` — TF 2,
DF still 2.

---

## 5. Duplicate-token handling

Phase 1 deliberately preserved duplicates (`"cat cat dog" ->
["cat", "cat", "dog"]`). This is the phase that *uses* them: the index counts
duplicate tokens into the term frequency.

The pipeline is:

```
raw text -> dse::tokenize -> tokens (duplicates preserved)
        -> count per term  -> "cat": 2, "dog": 1
        -> merge into index -> cat postings gain (id, 2), dog gain (id, 1)
```

The tokenizer never deduplicates; the index never re-tokenizes. Each stage
does one job: tokenization produces the raw occurrence stream, indexing
*aggregates* it. If the tokenizer had collapsed duplicates, every document
would look as if each word appeared once, and BM25 later would be blind to
emphasis and repetition. The duplicate count is information, and this is
where it is captured.

---

## 6. Document insertion semantics

`add_document(id, text)` must turn raw text into index entries. Steps:

1. Tokenize: `auto tokens = dse::tokenize(text);`
2. Count: build a local frequency map from tokens — `{term -> count}`. A
   document's tokens are a *multiset*; the map collapses duplicates *within
   the document* into counts (this is exactly the aggregation from
   Section 5).
3. Merge: for each (term, count), add posting `(id, count)` to that term's
   postings list, keeping the list sorted by docID.

### What if the same docID is added twice?

This is the one semantic decision the ADR must make. Three options:

- **Append** — just add another posting. Corrupts the structure: the same
  (term, doc) pair appears twice, TF is wrong, DF is wrong. Bad.
- **Replace** — remove the document's old postings first, then add the new
  ones. Correct for re-indexing (e.g., after editing a document), but
  requires remembering each document's old terms — a *forward* map
  (docID → terms), which doubles the per-pair storage and complicates the
  design.
- **Reject** — declare that each docID is added exactly once; adding it again
  is a contract violation (asserted in debug builds, undefined otherwise).

**Phase 2 chooses reject (unique docIDs).** Rationale: search-engine indexing
is naturally *append-only* — you index a corpus once, in docID order. Update
and delete semantics are a later phase's problem (a real engine needs them,
and that phase will introduce the forward map deliberately, with tests).
Locking in append or replace now would either corrupt data silently or force
the forward-map complexity before it is needed. The invariant is stated
plainly: *"each document ID may be added at most once; re-adding is a
precondition violation."*

### Insertion order and the sorted invariant

To keep postings sorted by docID, merging must handle out-of-order arrivals:

- if the new docID is larger than the last posting's docID, **append** —
  O(1) amortized, the common case for a corpus indexed in order;
- otherwise, **binary-search + insert** at the sorted position — O(log k + k)
  for a list of length k, the rare case.

The invariant ("postings are always sorted by docID") is maintained by the
class on every insertion, so consumers and later merge algorithms never have
to sort.

---

## 7. Lookup semantics

```cpp
std::span<const Posting> postings(std::string_view term) const;
```

- **Returns a view, not a copy.** Query processing (later) will read postings
  lists repeatedly; copying each list per lookup would make every query O(list
  length) *extra* work. A `std::span<const Posting>` is a (pointer, length)
  pair borrowed from the index — zero-copy.
- **Missing terms return an empty span.** A postings list is *never* empty
  for a term that exists (a term exists only because some document contained
  it), so an empty span unambiguously means "term not in the vocabulary."
  This avoids `std::optional` wrapping and keeps call sites simple.
- **Lifetime contract (important):** the returned span is valid *until the
  next modification of the index* (any `add_document` call, or the index's
  destruction). Reading a span after adding documents that touch the same
  term is a dangling-buffer risk — the contract must be documented and
  respected. This is the same ownership lesson as `string_view` in Phase 1:
  *borrowed data is only valid while the owner lives and is unchanged.*

Why `span` and not `const std::vector<Posting>&`? A reference to the vector
*dangles on rehash*: `unordered_map` moves its nodes when it grows, so the
vector object itself moves. The vector's *heap buffer* does not move, so a
span into that buffer survives rehash — but it does not survive a
`push_back` into that same term's list (reallocation). The documented
contract covers both cases honestly.

---

## 8. unordered_map vs map

The central container holds `term -> postings list`. Two STL candidates:

| | `std::unordered_map` | `std::map` |
|---|---|---|
| Lookup / insert | O(1) average (hash) | O(log n) (tree) |
| Order of iteration | unspecified | sorted by key |
| Node layout | buckets + nodes | parent/left/right pointers |
| Memory per entry | higher (bucket array + node overhead) | moderate (3 pointers per node) |
| Hash cost | must hash the string (reads all bytes) | must compare, usually short-circuiting |
| Deterministic iteration | no | yes |

**Decision: `std::unordered_map<std::string, std::vector<Posting>>`.**

Why:

- The index's workload is *point lookups* ("term X's postings") and
  *insertions* — exactly what hashing makes O(1) average. Tree ordering buys
  nothing in Phase 2: we do not iterate the vocabulary, and we never need
  terms in sorted order (postings are sorted by *docID*, which is independent
  of term order).
- Consequence to document: **iteration order over the vocabulary is
  unspecified** and must not be a contract. The class exposes lookups and
  counts, not "give me terms in order." Tests must assert on lookups, never
  on map iteration order.

A subtle note on lookup keys: the map's key type is `std::string`, and our
lookup takes `std::string_view`. Heterogeneous lookup on `unordered_map`
requires a transparent hash, which `std::hash<std::string>` does not provide;
the simple Phase 2 implementation constructs a temporary `std::string` for
the lookup. This is not a performance lie — hashing already reads every byte
of the term, so the copy is the same O(|term|) cost. (A transparent-hash
optimization is noted in the ADR's future evolution.)

---

## 9. Postings-list representation

- `std::vector<Posting>` — contiguous, cache-friendly, supports binary search
  (`std::lower_bound`), merge, and indexing. The natural choice; the same
  reasoning that made `std::vector<std::string>` right for tokenizer output
  applies, *plus* postings lists get *sorted* and *merged* in later phases,
  which vectors do best.
- Not `std::list` (no contiguity, no binary search), not `std::set` (no
  duplicate postings needed, and we need per-posting TF).

Each posting is 8 bytes (4-byte docID + 4-byte TF) with default alignment —
compact enough that a term appearing in a million documents costs ~8 MB
before any compression. Compression (delta-encoding docIDs, varint TF) is a
later-phase optimization; Phase 2 stores plain integers.

---

## 10. Ownership and lifetime

- **The index owns everything it stores**: each term string (the map key) and
  each postings vector. RAII: destroying the index frees all of it; no manual
  memory management anywhere.
- **`add_document`'s input is borrowed**: `std::string_view text` is read
  during the call (the tokenizer already guarantees it never stores the
  view), then the view is dead — the index keeps only *owning* tokens it
  produced itself.
- **Lookup output is borrowed**: `std::span<const Posting>` borrows the
  index's storage, valid until the next modification (Section 7). The
  direction of ownership is always clear: *in* = borrowed, *out* = borrowed
  for lookups, *stored* = owned.

This mirrors the Phase 1 model exactly: `tokenize` borrowed a view and
returned owning tokens; the index borrows text and returns borrowed views of
its own owned storage. Getting the borrowing rules right is what makes the
API safe.

---

## 11. Proposed public API (proposal only, not implemented in Phase 2A)

```cpp
// src/inverted_index.h (Phase 2B)
#pragma once

#include <cstdint>
#include <span>
#include <string>
#include <string_view>
#include <unordered_map>
#include <vector>

namespace dse {

using doc_id = std::uint32_t;

// One (document, term-frequency) pair inside a postings list.
struct Posting {
    doc_id document_id;
    std::uint32_t term_frequency;
};

// In-memory inverted index.
//
// Contract:
//   - each document ID may be added at most once (precondition; re-adding is
//     a contract violation);
//   - postings lists are always sorted by document_id;
//   - postings(term) returns an empty span for unknown terms (a postings
//     list is never empty for a term that exists);
//   - a span returned by postings() is valid until the next modification
//     of the index (or its destruction);
//   - identical insertion sequences produce identical indices (deterministic);
//   - vocabulary iteration order is unspecified (do not depend on it).
class InvertedIndex {
public:
    // Tokenizes `text` with dse::tokenize, counts term frequencies, and
    // merges (id, count) postings into the index. O(T) amortized for a
    // document with T tokens.
    void add_document(doc_id id, std::string_view text);

    // Postings list for `term`, sorted by document_id, or empty if the term
    // is unknown. O(1) average lookup.
    std::span<const Posting> postings(std::string_view term) const;

    // Number of documents added so far.
    std::size_t document_count() const;

    // Number of distinct terms in the vocabulary.
    std::size_t term_count() const;

    // Whether `term` is in the vocabulary. O(1) average.
    bool contains(std::string_view term) const;

private:
    std::unordered_map<std::string, std::vector<Posting>> postings_by_term_;
    std::size_t document_count_ = 0;
};

} // namespace dse
```

Design notes:

- **A class, not a free function.** Unlike the stateless tokenizer, the index
  is inherently *stateful* — it accumulates documents across calls. A class
  with private state and a public contract is the honest shape. (Phase 1B-1
  deferred a class because the tokenizer had no state; now there is state.)
- **`doc_id` as a named alias** communicates intent and lets us change the
  underlying type in one place.
- **No options, no configuration** — same philosophy as Phase 1: the minimal
  contract now, additive extensions later.
- **`document_count()` and `term_count()`** are cheap (a counter and
  `map.size()`); ranking later needs N (collection size) for IDF, so exposing
  it now is free.

---

## 12. Time and space complexity

Let `D` = number of documents, `T_d` = tokens in document `d`, `T = Σ T_d`
total tokens, and `V` = vocabulary size.

### Time

- **`add_document(id, text)`**: tokenize O(T_d) + count O(T_d) + merge. Merge
  is amortized O(1) per distinct term when appending (the normal in-order
  case), so the whole call is **O(T_d) amortized**. The pathological
  out-of-order insertion into a list of length `k` costs O(log k + k); in the
  worst case that is O(T_d · k_max). We document the amortized bound as the
  design target; a later phase can revisit with bulk-append-then-sort.
- **`postings(term)`**: **O(1) average** — one hash lookup, then a span over
  existing storage. (Plus O(|term|) hashing of the term, unavoidable.)
- **`contains(term)`**: O(1) average. **`document_count`** / **`term_count`**:
  O(1).
- **Whole-corpus indexing**: O(T) amortized total — every token is processed
  once, exactly like the tokenizer's O(N) per text.

### Space

The index stores one posting per *distinct (term, doc) pair* — duplicates
within a document collapse into the TF counter, so storage is
**O(number of distinct (term, doc) pairs)**, which is ≤ O(T). Additionally
each distinct term is stored once as a map key, so vocabulary storage is
O(total term characters) ≤ O(T). Total: **O(T)** in the worst case, and in
practice less than the tokenizer output itself, because the TF aggregation
removes within-document duplicates.

Why is this better than a forward index? A forward index stores every token
occurrence (T strings); an inverted index stores one (term, doc) pair with a
count per document per term. For a corpus where the same terms recur in many
documents, the inverted index is dramatically smaller *and* directly answers
the "which documents contain term X" question.

---

## 13. Memory trade-offs

- **Term strings are stored once per distinct term**, not once per
  occurrence — this is the core win of inversion. `"the"` in 10,000
  documents costs one `"the"` key, not 10,000 copies.
- **`unordered_map` overhead**: a bucket array plus one node per distinct
  term. Memory per entry is higher than `std::map`'s tree nodes, and far
  higher than a sorted `std::vector` of (term, postings) pairs would be. For
  Phase 2 scale this is fine; a later phase can move to a compact on-disk or
  sorted-array layout when the vocabulary grows (and when persistence
  arrives, the format will be decided by disk, not memory).
- **TF aggregation saves memory**: `"cat cat cat"` costs one posting
  `(id, 3)`, not three postings — the exact place duplicates from Phase 1 are
  *collapsed into information*.
- **No document store**: the index deliberately does not keep document text,
  so memory is proportional to vocabulary + postings, not to raw corpus size.

---

## 14. Edge cases

| # | Case | Expected behavior |
|---|---|---|
| 1 | Empty document text `""` | No postings added; `document_count` still increments |
| 2 | Whitespace-only text | No postings (tokenizer yields `[]`); doc counted |
| 3 | Punctuation-only text | No postings; doc counted |
| 4 | Non-ASCII-only text (`"你好"`) | No postings (Phase 1 documented limitation) |
| 5 | One term in one document | Single posting `(id, tf)` |
| 6 | Term repeated in one document | One posting with TF = count (`"cat cat"` → `(id, 2)`) |
| 7 | Same term in many documents | Postings list grows; DF = list size |
| 8 | Term with mixed case (`"Cat CAT cat"`) | One term `cat`, TF = 3 (tokenizer lowercases) |
| 9 | Digits / alphanumeric terms (`"abc123"`, `"42"`) | Indexed as single terms (tokenizer contract) |
| 10 | Lookup of a term never added | Empty span |
| 11 | Lookup of an empty term `""` | Empty span (empty string never a token) |
| 12 | Out-of-order docID insertion (5, then 2) | Postings stay sorted by docID (binary-search insert) |
| 13 | Re-adding the same docID | Contract violation (assert in debug builds) |
| 14 | Very long document | Correct counts, O(T) behavior, no crash |
| 15 | Many documents | `document_count` / `term_count` accurate; lookups still O(1) average |
| 16 | Two documents sharing no terms | Disjoint postings lists |
| 17 | Document with no shared terms vs one shared term | Vocabulary grows by the unique terms only |
| 18 | Lookup before any insertion | Empty span; counts zero |

---

## 15. Testing strategy (designed before implementation)

Phase 2B will write tests **first**, grouped by the property each group
proves. Each assertion must compare **the full returned postings** (docID and
TF, in order) against an expected value — never a partial check.

| Group | What it proves | Example cases |
|---|---|---|
| Basic insertion & lookup | text in, correct postings out; single-doc and multi-doc indices | `"cat sat"` → term `cat` postings `[(1, 1)]` |
| Term frequency counting | duplicates within a document collapse into TF | `"cat cat dog"` → `cat: (id, 2)`, `dog: (id, 1)` |
| Document frequency | DF equals postings-list size; grows across docs | `cat` in 3 docs → `postings.size() == 3` |
| Normalization | case-insensitive vocabulary via tokenizer | `"Cat CAT"` → term `cat`, TF 2 |
| Empty documents / no tokens | no postings; doc still counted | `""`, `"   "`, `"!!!"`, `"你好"` |
| Missing terms | empty span; `contains` false | lookup `"zzz"` after indexing |
| Sorted invariant | postings always sorted by docID, even for out-of-order insertion | add doc 5 then doc 2; both lists sorted |
| Multiple terms & shared terms | vocabulary size; overlapping postings | docs sharing `the`, `and`, `dog` |
| Counts | `document_count` and `term_count` accurate | mixed additions |
| Determinism | identical insertion sequences → identical indices | build twice, compare lookups |
| Edge cases | robustness; nothing crashes, nothing dangles | long input, re-adding docID (assert), lookup of empty term |
| Span lifetime | documented contract is enforceable in practice | copy or re-lookup after modification rather than holding stale spans |

A dedicated integration-style test should also assert the **index ↔ tokenizer
connection**: a known text's postings equal hand-computed values from
`dse::tokenize` — proving the two components agree end to end without
duplicating tokenizer tests.

---

## 16. Phase 1 → Phase 2 integration

- **Dependency direction stays clean**: `inverted_index` depends on the
  tokenizer (`#include "tokenizer.h"`, calls `dse::tokenize`); nothing in the
  tokenizer depends on the index. Consumers (app, tests) depend on both. This
  is the same one-way dependency rule established in Phase 1.
- **Library**: both components live in the existing `dse_core` static
  library. Phase 2B adds `src/inverted_index.h` + `src/inverted_index.cpp` to
  the `dse_core` sources and a `tests/inverted_index_test.cpp` target linked
  against `GTest::gtest_main` and `dse_core`, registered with CTest via
  `gtest_discover_tests` — exactly the Phase 1B-1 pattern.
- **Terminology alignment**: the index's "term" is precisely the tokenizer's
  token. No re-normalization happens inside the index; consistency is by
  construction (same tokenizer, same rule, documented in ADR-001).
- **No API change to the tokenizer**: Phase 2 is purely additive. This is the
  payoff of Phase 1's minimal, honest contract — consumers can be built
  without disturbing it.

---

## 17. What is explicitly deferred to later phases

The ADR's "future evolution" section expands on these; the point of listing
them here is that **none of them changes the Phase 2 contract**:

- **Positions / phrase queries** — a posting today is (docID, TF). Phrase
  support needs per-position postings `(docID, [positions])` or a separate
  positional index. Deferred: TF-only postings are all BM25 needs.
- **Document updates / deletes** — re-adding a docID is currently a
  precondition violation. A forward map (docID → terms) enables true replace
  semantics; that phase adds it with its own tests.
- **Query processing** — AND/OR merging over postings lists; the sorted
  invariant is designed for it, but the merge itself is a later phase.
- **Ranking (BM25)** — consumes TF (in postings) and DF/N (derivable from
  list sizes and `document_count`); the index already stores everything a
  ranker needs.
- **Persistence** — the index is in-memory; serialization format and segments
  are later phases.
- **Stop words, stemming, lemmatization** — language-policy stages that run
  *before* indexing (filtering tokens) or are entirely query-side; the index
  contract is unchanged either way.
- **Concurrency, sharding, replication, networking** — distribution phases;
  the index is currently single-threaded and local by design.
- **Compression** — delta-encoded docIDs, varint TF; an optimization that
  changes the internal layout, not the public API.

The rule, repeated from Phase 1: *make the initial API small and honest, and
every future feature becomes an additive extension instead of a breaking
change.*

---

## 18. C++ concepts you will need for the implementation

### `std::span<T>`

A **non-owning view of a contiguous sequence** — a (pointer, length) pair,
like `string_view` but for arbitrary element types. It can be constructed
from a `std::vector` without copying, iterates like a container, and has no
ownership:

```cpp
std::vector<Posting> ps = ...;
std::span<const Posting> view = ps;   // borrows ps's buffer, no copy
```

`span<const T>` promises read-only access — the "const" applies to the
elements, mirroring `const T&` for a whole sequence.

### `std::unordered_map<K, V>`

A **hash table**: average O(1) insert and lookup. Key insight for our use —
the key *is* the term string; lookups hash the key and compare only on hash
collision:

```cpp
std::unordered_map<std::string, std::vector<Posting>> m;
m["cat"].push_back({1, 2});           // operator[] default-constructs if missing
auto it = m.find("cat");              // O(1) average; it == end() if absent
```

Remember: iteration order is unspecified, and references into the map can be
invalidated by rehashing (one reason lookups return spans into the vectors'
heap buffers, not references to the vector objects).

### `std::uint32_t` and fixed-width integers

From `<cstdint>`: an integer that is *exactly* 32 bits on every platform
(unlike `int`, which is "at least 16"). The right type for docIDs and term
frequencies — compact, portable, and self-documenting ("docID needs 4 bytes").

### Structured bindings

Decompose a pair/tuple into named variables:

```cpp
for (const auto& [term, postings] : some_map) { ... }
```

Convenient for iterating the count map during `add_document`. (Not used for
vocabulary iteration in the public contract — order is unspecified.)

### `const` correctness and `const` references

Same lesson as Phase 1, now for class members: `postings()` is a `const`
member function (it does not modify the index), so it can be called on
`const InvertedIndex&`. `std::string_view` parameters are read-only by
nature.

### Classes and invariants

The index is our first real class. The key idea: **the class is responsible
for maintaining its invariants** — "postings are sorted", "docIDs are
unique", "no empty postings lists" — and the private members are the only way
those invariants can be broken. Private state + public contract = the
encapsulation that Phase 1's design notes promised a class would bring.

### RAII, one more time

`std::unordered_map` and `std::vector` own their memory and free it in their
destructors. The index contains them by value; destroying the index destroys
the whole structure. No `new`/`delete`, no leak paths, no manual cleanup —
the same reason the tokenizer needed none.

---

## 19. Design trade-offs (summary)

| We choose | We give up |
|---|---|
| `unordered_map` (O(1) lookups) | deterministic vocabulary iteration; sorted terms |
| Sorted postings lists | O(log k + k) worst-case insert for out-of-order docIDs |
| TF stored per posting, DF derived | a stored DF counter (unneeded — it is `size()`) |
| `span` returns (zero-copy) | a caller-held copy that stays valid forever |
| Unique-docID precondition | built-in update/replace semantics |
| Plain 4-byte docIDs/TF | compressed postings (later phase) |
| In-memory only | persistence (later phase) |

### Recommendation

The design above: a single class `dse::InvertedIndex` backed by
`std::unordered_map<std::string, std::vector<Posting>>`, with `Posting
{doc_id, term_frequency}`, postings always sorted by docID, TF aggregated
from the tokenizer's duplicate-preserving output, DF derived from list
lengths, `std::span<const Posting>` lookups with a documented lifetime, and
unique docIDs as the insertion precondition. It is the smallest design that
stores everything BM25 will need, keeps every later phase additive, and
stays deterministic and fully testable.

---

## 20. The Phase 2 contract (summary)

- **Responsibility:** maintain `term -> sorted postings list`; answer "which
  documents contain this term, and how often?" Nothing else.
- **Input:** `add_document(doc_id, std::string_view text)` — tokenized by
  `dse::tokenize`, counts aggregated into TF.
- **Lookup:** `postings(std::string_view) -> std::span<const Posting>`,
  empty for unknown terms, valid until the next modification.
- **Data model:** `doc_id = std::uint32_t`; `Posting { doc_id,
  term_frequency }`; `unordered_map<std::string, std::vector<Posting>>`;
  postings sorted by docID; DF = list size.
- **Preconditions:** each docID added at most once; identical insertion
  sequences yield identical indices.
- **Complexity:** O(1) average lookup; O(T) amortized per document indexed;
  O(distinct term–doc pairs) space.
- **Known limitations:** TF-only postings (no positions); no updates/deletes;
  no persistence; in-memory, single-threaded, ASCII vocabulary (Phase 1
  contract) — each explicitly deferred, none blocking BM25 later.
