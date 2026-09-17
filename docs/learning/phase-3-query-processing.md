# Phase 3 - Query Processing: Design and Learning Notes (Phase 3A)

Phase 1 gave us the **tokenizer** (`dse::tokenize`): a pure function that
turns raw text into normalized, duplicate-preserving tokens. Phase 2 gave us
the **inverted index** (`dse::InvertedIndex`): a stateful structure that
stores `term -> sorted postings list`, so we can ask "which documents contain
this term, and how often?" Phase 3 builds the component that finally answers
a *question*: **"which documents match this query?"**

Query processing sits between the index and ranking in our pipeline. It is
the first component that *reads* the index, the first component that consumes
a user's input, and the first component that produces an actual **result
list**. This document is the Phase 3A design specification and learning
material — it contains **no implementation** (the code comes in Phase 3B).

The companion decision record is
`docs/decisions/ADR-003-query-processing-design.md`, which captures *why*
these choices were made.

---

## 1. Where query processing fits in the complete search engine

Our pipeline, with the Phase 3 component highlighted:

```
Raw Document                      Raw Query
    |                                  |
    v                                  v
Tokenizer         <- Phase 1     Tokenizer        (the SAME function)
    |                                  |
    v                                  v
Normalized Tokens                Normalized Query Terms
    |                                  |
    v                                  v
Inverted Index   <- Phase 2            |
    |                                  |
    +-------------+--------------------+
                  v
        Query Processing   <- Phase 3 (THIS DOCUMENT)
                  |
                  v
              Ranking      <- later phase (BM25)
                  |
                  v
              Results
```

The pipeline is symmetric in an elegant way: documents flow down the left
side into the index; queries flow down the right side; query processing is
where the two sides meet.

Think about the division of labor:

- The **tokenizer** decides what counts as a word. It is used twice — once
  for documents (Phase 1) and once for queries (Phase 3) — because both
  sides must speak the same language.
- The **inverted index** remembers which documents contain which terms. It
  is built once, from documents, and then *read* many times by queries.
- **Query processing** walks the index's postings lists to combine the
  answers for each query term into one result list. It is the "answering
  organ" of the engine.
- **Ranking** (a later phase) will take the same result list and put it in
  relevance order. Query processing does not rank; it *retrieves*.

This phase implements **Boolean retrieval**: AND (documents must contain all
terms) and OR (documents must contain at least one). Boolean retrieval is
the foundation every real search engine builds on — it is what makes the
index useful at all — and it is a clean, fully testable step before ranking
adds scoring.

---

## 2. How a raw query flows through the Phase 1 tokenizer

A query is just text, and we already have a function for turning text into
terms:

```
"Search ENGINE"  --dse::tokenize-->  ["search", "engine"]
"search-engine"  --dse::tokenize-->  ["search", "engine"]
"Search, engine!" --dse::tokenize--> ["search", "engine"]
```

All three queries are the *same query* after tokenization. That is the point
of reusing `dse::tokenize` unchanged: **queries are normalized with exactly
the same rules as documents**, so a document containing "Search" and a query
containing "search" agree on the term `search`.

Why is this critical? The index's vocabulary is defined by the tokenizer
(Phase 1 was the sole authority on terms, and Phase 2 respected that). If
queries were normalized differently — say, case-sensitive, or with different
punctuation rules — then a query term would silently never match a document
term that a human considers identical. Reusing the same function makes
document-query agreement true **by construction**: there is only one
tokenizer, and both sides use it.

Consequences that fall out of Phase 1's rules, applied to queries:

- `"Hello, World!"` -> `["hello", "world"]` (punctuation is a separator);
- `"C++"` -> `["c"]` (symbols split; the letters survive);
- `"don't"` -> `["don", "t"]` (apostrophes split);
- `"user@example.com"` -> `["user", "example", "com"]` (email-like text
  fragments);
- `"你好"` -> `[]` (non-ASCII bytes are separators — Phase 1's documented
  ASCII limitation applies to queries too);
- `"42"` -> `["42"]` (digits are token characters).

None of this is new behavior — it is the Phase 1 contract, applied a second
time. Query processing adds **zero** new text-processing rules, which keeps
the tokenizer the single source of truth for "what is a term."

---

## 3. How tokenized query terms interact with the Phase 2 inverted index

Once we have the query's terms, each one is looked up in the index:

```
query: "cat dog"
         |      |
   tokenize    tokenize
         |      |
       "cat"  "dog"
         |      |
   index.postings("cat") -> [(1,1), (3,1)]     span<const Posting>
   index.postings("dog") -> [(2,1), (3,1)]     span<const Posting>
```

This is the meeting point of the two pipeline sides: **document vocabulary**
(lowercased tokens stored by the index) meets **query vocabulary** (the same
tokens produced by the same function). A term matches if and only if the
index has a postings list for it.

Important properties of this interaction:

1. **Query processing only reads.** It calls `postings(term)` and
   `document_count()`; it never calls `add_document`. The index is
   immutable during a query, which is what makes it safe to hold spans into
   the index for the duration of the query.
2. **Everything needed is already in the index.** Postings sorted by docID
   (for merging), TF per posting (for future ranking), DF via list size, and
   collection size via `document_count()`. Phase 2 stored exactly what Phase 3
   needs — nothing more, nothing less.
3. **Missing terms are visible.** `postings(term)` returns an *empty span*
   for an unknown term, which unambiguously means "this term does not exist
   in the vocabulary." Section 11 shows how AND and OR treat that case
   differently (and correctly).

Notice that the postings lists carry term frequencies, but Boolean queries
ignore them for now: `"cat"` matches document 1 whether it appears once or
five times. The TF numbers stay safe in the index, untouched, until the
ranking phase needs them. Query processing *drops information it does not
need* and returns only document IDs.

---

## 4. Single-term query processing

The simplest possible query: one term.

```
query: "search"
terms: ["search"]
postings("search") -> [(2,1), (5,3), (9,1)]
result: [2, 5, 9]
```

The result of a single-term query is exactly the docIDs of that term's
postings list, in the order the list is stored — ascending by docID, thanks
to the Phase 2 sorted invariant.

Three things worth noting:

1. **The result is a list of docIDs, not postings.** Term frequencies are
   dropped. For Boolean retrieval, "document 2 contains `search`" is a
   yes/no fact; the count is irrelevant until ranking.
2. **A missing term gives an empty result.** `postings("zzz")` is an empty
   span, so the result is `[]` — no document contains a term that does not
   exist.
3. **Single-term queries need no merging.** The two-pointer algorithms from
   Sections 9-10 only kick in when there are at least two terms. A single
   term is the base case: the postings list *is* the answer (its docIDs,
   anyway).

Single-term queries are also the natural unit test: they verify the
tokenizer -> index -> result path end to end before any merge logic is
involved.

---

## 5. Multi-term queries

As soon as a query has two or more terms, we must decide how to combine the
per-term answers:

```
query: "cat dog"
term "cat" matches docs {1, 3}
term "dog" matches docs {2, 3}
```

Two natural interpretations:

- **AND** — the user wants documents about both: `{3}`.
- **OR** — the user wants documents about either: `{1, 2, 3}`.

Both are standard in every search engine (web search engines use an
implicit OR with ranking on top; library catalogs and database queries use
explicit AND/NOT; both appear in advanced query syntax). Phase 3 implements
**both**, as two explicit methods, because:

- they are the two fundamental set operations over postings lists;
- they share the same machinery (the two-pointer merge family);
- the OR path is nearly free once AND exists, and both give ranking (Phase 4)
  a meaningful baseline to reorder.

Formally, with the vocabulary of sets of docIDs:

- AND = **intersection** of the per-term document sets;
- OR = **union** of the per-term document sets.

The next sections show how the sorted postings lists make both operations
linear-time walks.

---

## 6. AND semantics and postings-list intersection

"`cat` AND `dog`" should return documents that contain *both* terms — the
**intersection** of the two postings lists (projected to docIDs):

```
cat -> [(1,1), (3,1)]
dog -> [(2,1), (3,1)]
intersection by docID -> [3]
```

Why is intersection the right semantics for AND? A document is in the answer
if and only if it appears in *every* term's postings list. That is precisely
the mathematical definition of set intersection.

For **k terms** (k >= 2), we intersect the lists one pair at a time:

```
result = postings(term_1)
for each remaining term t:
    result = intersect(result, postings(t))
```

Because intersection is associative and commutative, the order in which we
fold doesn't change the answer — which is convenient, because it lets us
choose an order that minimizes work (Section 17). The key property of the
fold: the result can only shrink. The final answer is never larger than the
smallest input list, so AND queries get *faster* as they get more specific.

---

## 7. OR semantics and postings-list union

"`cat` OR `dog`" should return documents that contain *at least one* of the
terms — the **union** of the postings lists:

```
cat -> [(1,1), (3,1)]
dog -> [(2,1), (3,1)]
union by docID -> [1, 2, 3]
```

Note that document 3 appears in both lists but is emitted **once** in the
union. Union is a set operation: no duplicates.

For **k terms**, we fold pairwise just like AND:

```
result = postings(term_1)
for each remaining term t:
    result = merge_union(result, postings(t))
```

The union result can only grow — every document matching any term ends up in
the answer. OR is the "forgiving" retrieval: it maximizes recall (finding
everything possibly relevant) at the cost of precision (returning documents
that match only one marginal term). Ranking later exists precisely to put the
best of an OR result first. And because OR queries return more, their
results can be large — a fact that matters for the memory discussion in
Section 18.

---

## 8. Why Phase 2's sorted postings lists are important

Everything in this phase is easy because of one invariant Phase 2 chose to
maintain: **postings lists are always sorted by document ID**. Here is what
that buys us.

### Without sorting, intersection is quadratic

If a term's postings list were unsorted, the naive way to intersect two
lists of length `a` and `b` is a nested scan: for each posting in list A,
scan all of list B looking for the same docID. That is **O(a * b)** — for
two lists of 10,000 postings, 100 million comparisons. Real engines intersect
lists with millions of entries; quadratic is not viable.

### With sorting, intersection is linear

Two sorted lists can be merged with two moving pointers in a single walk:
each pointer only moves forward, every comparison either emits a match or
advances one pointer, and the walk ends when either list is exhausted.
That is **O(a + b)** — for two lists of 10,000 postings, at most 20,000
comparisons. Five orders of magnitude better, for zero extra storage,
because Phase 2 paid a tiny insertion-time cost to *keep* the lists sorted.

### Sorting also enables everything else

- **Binary search within a list** — "is doc 42 in this list?" in O(log k).
- **Union with dedup** — identical machinery to intersection, also O(a + b).
- **Deterministic output** — the merges emit ascending docIDs, which is a
  stable, reproducible contract regardless of the hash map's unspecified
  iteration order.
- **Future optimizations** — skip lists, block/roaring bitmaps, and on-disk
  segment merges (all later phases) all assume sorted postings.

This is the payoff moment for ADR-002's "postings sorted by docID" rule: the
invariant was chosen in Phase 2 *because* Phase 3 would need exactly this
merge, and the cost was deferred to where it belongs (insertion time), not
where it would hurt (every single query).

---

## 9. Two-pointer intersection algorithm, step by step

The intersection of two sorted lists, walked with two indices:

```
intersect(A, B):
    result = []
    i = 0, j = 0
    while i < A.size() and j < B.size():
        if A[i].docID == B[j].docID:
            result.push_back(A[i].docID)
            i++, j++
        else if A[i].docID < B[j].docID:
            i++
        else:
            j++
    return result
```

The invariant is simple: at every step, anything *before* `i` in A and
anything *before* `j` in B has already been decided; nothing we skip can ever
appear in the other list later, because both lists are sorted.

Let's trace it on concrete numbers. Take `A = [1, 3, 5, 7, 9]` and
`B = [2, 3, 5, 8]`:

| Step | i | A[i] | j | B[j] | Action | result |
|---|---|---|---|---|---|---|
| 1 | 0 | 1 | 0 | 2 | 1 < 2, skip A's 1 | [] |
| 2 | 1 | 3 | 0 | 2 | 2 < 3, skip B's 2 | [] |
| 3 | 1 | 3 | 1 | 3 | **equal, emit 3** | [3] |
| 4 | 2 | 5 | 2 | 5 | **equal, emit 5** | [3, 5] |
| 5 | 3 | 7 | 3 | 8 | 7 < 8, skip A's 7 | [3, 5] |
| 6 | 4 | 9 | 3 | 8 | 8 < 9, skip B's 8 | [3, 5] |
| 7 | 5 | end | 4 | end | i exhausted, stop | [3, 5] |

Result: `[3, 5]` — both lists visited exactly once, six comparisons total,
no element touched twice. If the two lists share nothing (e.g. `A = [1, 2]`,
`B = [3, 4]`), the loop still terminates in O(a + b) with an empty result.

Termination: the loop condition checks both pointers, so the walk always ends
when the shorter side is exhausted — and by then, nothing after the surviving
pointer can match (any remaining A element is larger than everything in B),
so there is no drain step for intersection.

---

## 10. Two-pointer union algorithm, step by step

Union is the sibling of intersection: walk both sorted lists, emit the
smaller docID when they differ, emit one copy when they are equal:

```
merge_union(A, B):
    result = []
    i = 0, j = 0
    while i < A.size() and j < B.size():
        if A[i].docID == B[j].docID:
            result.push_back(A[i].docID)     // emit ONCE
            i++, j++
        else if A[i].docID < B[j].docID:
            result.push_back(A[i].docID)
            i++
        else:
            result.push_back(B[j].docID)
            j++
    // drain whichever list still has elements
    while i < A.size(): result.push_back(A[i].docID); i++
    while j < B.size(): result.push_back(B[j].docID); j++
    return result
```

The only structural difference from intersection is that we *emit* on every
step (not just on equality) and we add a **drain** at the end: once one list
is exhausted, the remainder of the other list is entirely greater than
everything emitted so far, so it appends in order.

Same lists as before, `A = [1, 3, 5, 7, 9]`, `B = [2, 3, 5, 8]`:

| Step | i | A[i] | j | B[j] | Action | result |
|---|---|---|---|---|---|---|
| 1 | 0 | 1 | 0 | 2 | emit 1 (A smaller), i++ | [1] |
| 2 | 1 | 3 | 0 | 2 | emit 2 (B smaller), j++ | [1, 2] |
| 3 | 1 | 3 | 1 | 3 | **equal, emit 3 once**, both ++ | [1, 2, 3] |
| 4 | 2 | 5 | 2 | 5 | **equal, emit 5 once**, both ++ | [1, 2, 3, 5] |
| 5 | 3 | 7 | 3 | 8 | emit 7 (A smaller), i++ | [1, 2, 3, 5, 7] |
| 6 | 4 | 9 | 3 | 8 | emit 8 (B smaller), j++ | [1, 2, 3, 5, 7, 8] |
| 7 | 5 | end | 4 | 9 | B not exhausted — drain B: emit 9 | [1, 2, 3, 5, 7, 8, 9] |

Result: `[1, 2, 3, 5, 7, 8, 9]` — the full sorted union, with document 3
(shared) appearing exactly once. Both lists visited once: O(a + b).

These two algorithms are the entire core of Phase 3. Everything else —
tokenizing the query, deduplicating terms, looking up postings, folding —
is plumbing around them.

---

## 11. Handling missing terms

A "missing term" is a query term with no postings list in the index
(`postings(term)` returns an empty span). AND and OR must treat it
differently, and each treatment is the *correct* one:

### AND: a missing term makes the query empty

```
"cat AND zzz"   (zzz never indexed)
postings("cat") -> [(1,1), (3,1)]
postings("zzz") -> []                    // empty span
intersect([1,3], []) -> []               // empty result
```

The reasoning is airtight: AND requires the document to contain *every*
term, and no document can contain a term that was never indexed. One missing
term poisons the whole AND. The empty-span convention from ADR-002 makes
this fall out automatically — intersecting anything with an empty list yields
an empty list, no special-casing required.

### OR: a missing term is simply ignored

```
"cat OR zzz"
merge_union([1,3], []) -> [1, 3]
```

OR requires *at least one* term to match. A term that matches nothing
contributes no documents and removes none; the result is the union of the
terms that do exist. Again, no special-casing: the empty span is just a list
with nothing in it.

This asymmetry is a nice property to test explicitly: the *same* query with
AND and OR can give wildly different results when a term is missing, and
that is correct behavior, not a bug.

---

## 12. Handling duplicate query terms

What about the query `"cat cat dog"`? Tokenization preserves duplicates
(Phase 1's contract), so the terms are `["cat", "cat", "dog"]`. What happens
if we feed both `cat` postings lists into the merge?

- **AND:** `intersect(cat_list, cat_list)` is the identity — a set
  intersected with itself is itself. Harmless but wasteful: we'd look up the
  same term twice and merge a list with itself.
- **OR:** `merge_union(cat_list, cat_list)` would emit every `cat` docID
  **twice** — a duplicate in the result, which violates our
  deduplicated-result contract.

The clean solution: **deduplicate the query terms once, immediately after
tokenization, before any index lookup.**

```
"cat cat dog" -> ["cat", "cat", "dog"] -> sort+unique -> ["cat", "dog"]
```

Two standard implementations, both deterministic:

- `std::sort` + `std::unique` on the token vector — simple, deterministic,
  O(V log V);
- an insertion-ordered set (or a small `unordered_set` probe while keeping
  first occurrence) — O(V) average, preserves the query's term order.

Either is fine, because AND and OR are commutative and associative — the
order in which we process distinct terms does not affect the result. Phase 3
uses sort + unique for its simplicity and determinism.

A note for later: duplicate *occurrences* in a query are information for
ranking (query term frequency — a term the user typed twice is probably more
important). Boolean retrieval has no use for that signal, so Phase 3
discards it; ranking will re-derive it from the raw query if it ever needs
it. De-duplicating at the Boolean layer changes nothing the ranker needs.

---

## 13. Empty and punctuation-only queries

Tokenization of `""`, `"   "`, `"!!!"`, or `"你好"` produces **zero terms**
(Phase 1's contract: empty and whitespace/punctuation-only inputs yield an
empty token list). Query processing must define what "zero terms" means:

> **A query with zero terms returns an empty result for both AND and OR.**

The reasoning: there is nothing to look up, so there is nothing to merge.
This is the *empty result*, not "all documents." Returning every document
for an empty query would be a different feature entirely — match-all
semantics — which real engines expose deliberately (e.g. browsing all
documents) and which we defer to a later phase rather than baking it in by
accident.

Consequences worth stating explicitly:

- `""` -> `[]`
- `"   "` -> `[]`
- `"..."` -> `[]`
- `"你好世界"` -> `[]` (Phase 1 ASCII limitation)
- `", ? !"` -> `[]`

A user typing a query like `"!!!"` gets no results — which is honest:
under our tokenizer's rules, that query asks for nothing.

---

## 14. Result ordering and determinism

Both merges emit docIDs in ascending order *by construction*: the two-pointer
walk always appends the smaller docID next, and the drain appends in sorted
order. So:

> **Every query result is a `std::vector<doc_id>`, sorted ascending, with no
> duplicates.**

Two properties follow, and both are contracts:

1. **Determinism.** The same query against the same index gives the exact
   same vector, every time. This is true even though the underlying
   `unordered_map` iterates in unspecified order, because query processing
   never iterates the map — it only does point lookups (`postings(term)`)
   and merges sorted spans. Determinism is what makes unit testing exact
   (compare full vectors, not fuzzy checks) and behavior reproducible.
2. **DocID order is not relevance order.** The result is ordered by document
   number, not by how well each document matches. That is the ranker's job
   (Phase 4): it will take this same list, score each document, and reorder.
   Query processing deliberately does not guess at relevance — it produces
   the *candidate set* in a canonical order and stops.

Keeping the output sorted also gives later phases a stable baseline: a
ranker can reorder from a known canonical order, and debugging is trivial
("the answer set is the same, only the order changed").

---

## 15. Proposed query-processing API

Following the established pattern — pure leaf functions where possible, a
small class where state or composition is needed:

```cpp
// src/query_processor.h (Phase 3B)
#pragma once

#include <cstdint>
#include <span>
#include <string_view>
#include <vector>

#include "inverted_index.h"

namespace dse {

// Pure two-pointer merge primitives over sorted postings lists.
//
// Precondition: both inputs sorted by document_id (the index invariant).
// Output: an owning vector of docIDs, sorted ascending, deduplicated.
// Complexity: O(lhs.size() + rhs.size()).
std::vector<doc_id> intersect(std::span<const Posting> lhs,
                              std::span<const Posting> rhs);

std::vector<doc_id> merge_union(std::span<const Posting> lhs,
                                std::span<const Posting> rhs);

// Boolean query processor.
//
// Tokenizes a query with dse::tokenize (Phase 1), looks up each distinct
// term in the borrowed index (Phase 2), and combines the postings lists
// with the primitives above.
//
// Contract:
//   - the index is borrowed and must outlive this processor;
//   - queries are read during the call only and never stored;
//   - results are owning, sorted ascending, deduplicated;
//   - an empty query (zero terms) returns an empty result;
//   - a missing term makes AND empty and is ignored by OR.
class QueryProcessor {
public:
    explicit QueryProcessor(const InvertedIndex& index);

    // Documents containing ALL query terms, sorted by docID. O(total
    // postings visited), see design doc Section 17.
    std::vector<doc_id> and_query(std::string_view query) const;

    // Documents containing AT LEAST ONE query term, sorted by docID.
    std::vector<doc_id> or_query(std::string_view query) const;

private:
    const InvertedIndex* index_;  // borrowed; non-null, see constructor
};

} // namespace dse
```

Why this shape?

- **The two primitives are pure functions.** They take borrowed spans and
  return owning vectors; they have no state, no I/O, and no dependency on
  the index. That makes them independently testable in isolation (feed
  hand-built postings vectors, check the exact output) and trivially
  verifiable against the step-by-step traces in Sections 9-10.
- **`QueryProcessor` is the composition point.** It owns the *pipeline*:
  tokenize -> dedup -> look up -> fold. It holds the only state — a borrowed
  reference to the index — and exposes the two user-facing entry points.
  This mirrors the Phase 1/2 split: the tokenizer is a pure function, the
  index is a stateful class, and the processor is a stateful *service* that
  composes both. It is also the natural extension point for ranking in
  Phase 4 (a `rank()` method consuming the same pipeline).
- **Borrowed index as a pointer member.** The constructor takes a reference
  (callers cannot accidentally pass null), stored as a pointer so the class
  stays trivially copyable and assignable. The lifetime contract — "the
  index must outlive the processor" — is documented; Section 16 explains it.
- **`and_query` / `or_query` are `const`.** Querying does not modify the
  processor or the index, so the methods can be called on a `const`
  processor and from multiple query operations safely.

No options, no configuration, no query-language parser: Phase 3 exposes
exactly two Boolean operations. A parser for `AND`/`OR`/`NOT` syntax, scores,
limits, and pagination are all later phases (Section 24).

---

## 16. Input/output ownership and lifetime

Phase 1 taught the ownership rule with `string_view`; Phase 2 extended it to
spans; Phase 3 uses both and adds one borrowed object. The rule is always
the same: **borrowed data is only valid while the owner lives and is
unchanged.**

### Input: the query string is borrowed

```cpp
std::vector<doc_id> and_query(std::string_view query) const;
```

- The `string_view` is read during the call only; it is passed to
  `dse::tokenize` (which never stores it) and is never kept.
- Callers may pass string literals, `std::string`, or temporaries — the
  view dies at the end of the call, and nothing outlives it.
- This is the identical contract as Phase 1's `tokenize(string_view)`: *in*
  is borrowed, consumed synchronously.

### Borrowed dependency: the index must outlive the processor

```cpp
QueryProcessor::QueryProcessor(const InvertedIndex& index) : index_(&index) {}
```

- The processor holds a **borrowed** reference; it does not copy or own the
  index. The caller guarantees the index outlives the processor.
- The typical usage pattern makes this trivial to uphold:

```cpp
dse::InvertedIndex index;            // owner lives first
// ... add documents ...
dse::QueryProcessor processor(index); // borrower lives inside the owner's scope
processor.and_query("cat dog");       // safe: index is alive
```

- This is the same relationship as `std::string_view` -> `std::string`, one
  level up: a *view of an object* instead of a view of a buffer. The
  processor is a "view" of the index's query interface.
- Why not copy the index? It can be large; query processing should be cheap
  and not perturb the index. Why not own it? The index is built once and
  queried many times by (in a real system) many processors — sharing by
  borrowing is the honest model.

### Internal spans are safe by construction

During a query, `postings(term)` returns spans into the index. They are
safe because (a) the index is not modified during the query — `add_document`
is never called from query processing — and (b) each span is consumed within
the call, merged into the result vector. No span escapes the function.

### Output: the result is owned

`std::vector<doc_id>` is **owned by the caller**: it can outlive the index,
be stored, reordered, paginated, or passed elsewhere. Ownership is explicit
and one-directional at every step: *in* = borrowed, *stored* = owned by the
index, *out* = owned by the caller.

---

## 17. Complexity analysis

Let `Q` = query length in characters, `V` = number of distinct query terms,
`L_i` = length of term `i`'s postings list, `L_max` = the largest `L_i`, and
`R` = result size.

### Tokenization and term preparation

- `dse::tokenize(query)`: **O(Q)** — one linear pass (Phase 1's bound).
- Deduplicate terms (sort + unique): **O(V log V)**; with `V <= Q` this is
  bounded by O(Q log Q). (A hash-based dedup is O(V) average; the doc's
  sort+unique choice is fine at this scale and fully deterministic.)
- Index lookups: **O(V)** average, each O(1) plus O(|term|) for hashing the
  term string — total O(Q) in the worst case (the terms' characters sum to
  at most Q).

### Two-list merges (the core)

- `intersect(lhs, rhs)`: **O(a + b)** where `a`, `b` are the list lengths.
  Each step advances at least one pointer and does O(1) work.
- `merge_union(lhs, rhs)`: **O(a + b)**, same argument; every element is
  visited exactly once and emitted at most once.

### k-way queries

- **AND (fold of pairwise intersections):** each step costs
  O(|result_so_far| + |next list|). The result only shrinks, so the *total*
  work is bounded by O(V * L_max) in the worst case — but with the
  **smallest-list-first fold** (intersect the two shortest lists first,
  then fold the rest in), the effective work is dominated by the smallest
  lists, and the final result `R <= min(L_i)`. Since `R` never exceeds the
  smallest list, the worst case is still bounded by the smallest lists'
  sizes in practice.
- **OR (fold of pairwise unions):** **O(Σ L_i)** — every posting across all
  query terms is visited once and emitted at most once. This is optimal:
  the output itself has size up to Σ L_i, so you cannot do better than
  linear in the input plus output.
- Compare with the naive alternative: intersecting unsorted lists is
  O(L_i * L_j) per pair — the entire reason the sorted invariant exists.

### Summary table

| Operation | Time |
|---|---|
| Tokenize query | O(Q) |
| Dedup terms | O(V log V) |
| Per-term lookup | O(1) average |
| Two-list intersect / union | O(a + b) |
| AND (k terms, smallest-first fold) | O(sum of visited postings), result <= min(L_i) |
| OR (k terms) | O(Σ L_i), optimal |

### Space

- Result vectors: AND `O(R) <= O(min(L_i))`; OR `O(Σ L_i)` worst case (the
  output itself).
- The AND fold reuses one accumulator vector, so peak temporary storage is
  one result, not V results.
- O(Q) for the token vector and O(V) for the dedup set (bounded by Q).
- **No copies of postings lists**: merges read spans directly from the
  index, so the index's postings memory is never duplicated during a query.

---

## 18. Memory requirements and temporary result handling

Query processing is deliberately lean about memory:

1. **Postings lists are never copied.** The merges consume
   `std::span<const Posting>` — borrowed views of the index's storage. The
   cost of a query is the cost of the *result* plus small temporaries, not
   the cost of the lists being searched.
2. **AND folds into one accumulator.** Instead of materializing the result
   of every pairwise intersection, we intersect into the same vector,
   overwriting it each step. Peak temporary = one result-sized vector.
3. **OR materializes its output once.** The union result grows to its final
   size as it is built; `std::vector`'s growth policy makes the total
   allocation cost amortized linear.
4. **Term frequencies are dropped from results.** A result is
   `vector<doc_id>` — 4 bytes per match — not `vector<Posting>`. The TF
   numbers remain untouched inside the index for the ranking phase.
5. **Full materialization is a documented choice.** Boolean results are
   returned as complete lists. For a query matching a million documents,
   that is a 4 MB vector. Real engines cap results (top-k, pagination) or
   stream them; both are later-phase features (Section 24). For Phase 3's
   scale and testability, materializing the full answer is the honest,
   simplest contract — and it is what ranking needs as its input anyway.

Memory, summarized: **O(Q) for query preparation + O(result) for output,
with no duplication of the index.** The dominant memory consumer in the
system remains the index itself (Phase 2's design), not the query path.

---

## 19. Edge cases

| # | Case | Expected behavior |
|---|---|---|
| 1 | Empty query `""` | `[]` for both AND and OR |
| 2 | Whitespace-only query `"   "` | `[]` (zero tokens) |
| 3 | Punctuation-only query `"!!!"` | `[]` (zero tokens) |
| 4 | Non-ASCII-only query `"你好"` | `[]` (Phase 1 ASCII limitation) |
| 5 | Single term, present in index | That term's docIDs, ascending |
| 6 | Single term, absent | `[]` |
| 7 | AND with one missing term | `[]` (missing term poisons the AND) |
| 8 | OR with one missing term | Union of the existing terms' docIDs |
| 9 | AND, all terms present, no doc has all | `[]` |
| 10 | AND, terms co-occur in some docs | Only the shared docIDs, ascending |
| 11 | OR, disjoint terms | Concatenation of the lists, ascending, deduplicated |
| 12 | OR, terms overlapping | Union with each docID exactly once |
| 13 | Duplicate query terms `"cat cat dog"` | Same as `"cat dog"` (deduped) |
| 14 | Identical lists, AND | The list itself |
| 15 | Identical lists, OR | The list itself (no duplicate docIDs) |
| 16 | Empty index (no documents) | `[]` for every query |
| 17 | Document 0 exists | docID 0 appears in results like any other |
| 18 | First/last elements shared | Boundary docIDs (min and max) correctly included |
| 19 | Adjacent docIDs (e.g. 1 and 2) | Both handled; merge never skips or duplicates |
| 20 | Very long postings lists | Correct, linear behavior; no crash, no quadratic blowup |
| 21 | Query longer than any term | Works; tokenization bounded by Q |
| 22 | Same query twice | Identical result vectors (determinism) |

Every row is testable through the public contract (query string in, exact
vector out) — no mocks, no I/O.

---

## 20. Testing strategy (designed before implementation)

Phase 3B will write tests **first**, grouped by the property each group
proves. Following the Phase 2 pattern, assertions compare **the full
returned vector** (contents, order, duplicates) — never a partial check.

| Group | What it proves | Example cases |
|---|---|---|
| Merge primitive — intersect | The two-pointer intersection is correct in isolation; matches the Section 9 trace | `[1,3,5] ∩ [2,3,5]` -> `[3,5]`; empty inputs; no overlap; one list empty |
| Merge primitive — union | The two-pointer union is correct; dedup on shared docIDs; drain step | `[1,3,5] ∪ [2,3,5]` -> `[1,2,3,5]`; disjoint lists; one list empty; identical lists |
| Single-term queries | Full path tokenize -> lookup -> result; base case needs no merging | `"cat"` -> cat's docIDs; unknown term -> `[]` |
| AND multi-term | Intersection over real index data; shared and disjoint docs | `"cat dog"` -> shared docIDs only; three terms; no co-occurrence -> `[]` |
| OR multi-term | Union over real index data; dedup across terms | `"cat dog"` -> union docIDs; overlapping terms emit once |
| Missing terms | AND/OR asymmetry (Section 11) | `"cat zzz"` AND -> `[]`; OR -> cat's docIDs |
| Duplicate query terms | Dedup before merging (Section 12) | `"cat cat dog"` AND/OR == `"cat dog"` results |
| Empty / punctuation-only queries | Zero-term rule (Section 13) | `""`, `"   "`, `"!!!"` -> `[]` for both operators |
| Result ordering & determinism | Sorted ascending; no duplicates; identical on re-run | Same query twice -> equal vectors; every result sorted |
| Edge cases | Boundary correctness, robustness | docID 0; shared first/last elements; adjacent docIDs; long lists |
| Long-list sanity | Linear behavior on realistic sizes | 10k-postings lists; AND/OR complete correctly and fast |
| End-to-end integration | Tokenizer -> index -> processor agree end to end | Build a known corpus, hand-compute expected results, compare exactly |

The integration group deserves emphasis: it builds a small corpus of known
documents, indexes them, and checks queries against **hand-computed**
answers — proving that all three components (tokenizer, index, processor)
agree without duplicating either component's own test suite.

---

## 21. Integration: tokenizer -> inverted index -> query processor

The dependency graph stays strictly one-way:

```
tokenizer.h   (Phase 1, pure function)
    ^
    |  #include
    |
inverted_index.h   (Phase 2, stateful class; calls dse::tokenize)
    ^
    |
query_processor.h  (Phase 3, composes both; calls dse::tokenize
                     and index.postings())
```

- `query_processor` includes `tokenizer.h` (to tokenize the query) and
  `inverted_index.h` (to look up postings). Nothing in Phase 1 or Phase 2
  knows the processor exists.
- All three components live in the same `dse_core` static library, so the
  app and every test binary link the exact same compiled artifacts — the
  Phase 1B-1 pattern, extended twice.
- Phase 3 is **purely additive**: no Phase 1 or Phase 2 contract changes,
  no re-tokenization logic, no duplicated merging code. This is the payoff
  of keeping every previous phase's API small and honest.

The end-to-end data flow for one query:

```
raw query text
  -> dse::tokenize(query)          ["cat", "dog"]
  -> dedup (sort + unique)         ["cat", "dog"]
  -> index.postings("cat")         span [(1,1),(3,1)]
  -> index.postings("dog")         span [(2,1),(3,1)]
  -> intersect / merge_union       [3]  or  [1,2,3]
  -> owning vector<doc_id>         caller's result
```

Consistency is by construction: the same tokenizer defines terms on both
sides, and the same index invariant (sorted postings) powers every merge.

---

## 22. Important C++ concepts you will use

### The two-pointer idiom (the heart of Phase 3)

Walking two sorted ranges with two indices, advancing one per step, is one
of the classic algorithm patterns:

```cpp
size_t i = 0, j = 0;
while (i < a.size() && j < b.size()) {
    if (a[i] < b[j]) ++i;
    else if (b[j] < a[i]) ++j;
    else { result.push_back(a[i]); ++i; ++j; }
}
```

It is the same idea as merging two sorted arrays (the merge step of merge
sort) — if you know merge sort's merge, you already know this.

### `std::span<const Posting>` (review)

A borrowed (pointer, length) view. The primitives take spans so they can
read the index's vectors without copying. `span<const T>` means read-only —
the merge never modifies the lists it reads.

### `std::vector<doc_id>` as the owning result

The output is a plain owning vector: sorted, deduplicated, and owned by the
caller. `reserve()` can preallocate when the result size is known (OR's
upper bound is the sum of the list lengths), but it is an optimization, not
a requirement.

### `const` correctness

`and_query`/`or_query`/`intersect`/`merge_union` are all `const`-correct:
the processor's methods are `const` member functions, the index is held as
`const InvertedIndex&`, and the spans are `span<const Posting>`. This lets
you query a `const QueryProcessor` and guarantees no query can mutate state.

### `std::string_view` (review)

The query input is a borrowed view — zero-copy, never stored. Same rule as
Phase 1.

### `std::sort` + `std::unique` for dedup

```cpp
std::vector<std::string> terms = dse::tokenize(query);
std::sort(terms.begin(), terms.end());
terms.erase(std::unique(terms.begin(), terms.end()), terms.end());
```

`std::unique` removes *consecutive* duplicates, so sorting first turns
"remove all duplicates" into a one-liner. The `erase`-`unique` idiom is the
standard way to shrink a vector to its unique elements.

### A class with a borrowed dependency

`QueryProcessor` holds a pointer to an object it does not own. The
constructor takes a reference (no nulls at the call site); the lifetime
contract is documented. This is the first time we borrow a *whole object*
rather than a buffer — the same mental model as `string_view`, one level up.

### Fixed-width integers and `<cstdint>` (review)

`doc_id` is `std::uint32_t` (from Phase 2); results are vectors of it.

### RAII (review)

The result vector frees its own memory; the processor holds no resources.
Nothing to clean up — RAII means the query path has no manual memory
management at all.

### STL alternatives worth knowing (and why we don't use them yet)

`<algorithm>` provides `std::set_intersection` and `std::set_union`, which
implement exactly the two-pointer merges on sorted ranges. We deliberately
hand-roll ours — see ADR-003 — because the merge is the pedagogical core of
this phase, the projection (Posting -> docID) is clearer written out, and a
15-line loop is trivially verifiable against the step-by-step traces in
Sections 9-10. Once the algorithm is understood, swapping in the STL
versions is a trivial future refactor.

---

## 23. Design alternatives and trade-offs

| We choose | We give up |
|---|---|
| Hand-rolled two-pointer merges | battle-tested `std::set_intersection` / `std::set_union` (swap-in later) |
| Pure primitives + composing `QueryProcessor` | a single monolithic query function |
| Results as `vector<doc_id>` (owning) | borrowed spans / streaming / top-k results |
| Both AND and OR | AND-only (simpler, but half the value) |
| Deduplicated query terms before merging | query term frequency signal (deferred to ranking) |
| Smallest-list-first AND fold | naive query-order fold (same answer, worse typical cost) |
| Empty query -> empty result | match-all semantics (deferred feature) |
| Full result materialization | pagination / limits (deferred) |
| Two explicit methods, no parser | a query-language parser with AND/OR/NOT syntax (deferred) |
| Boolean only, no scoring | relevance ordering (deferred to ranking) |

Key trade-offs, in more detail:

1. **Hand-rolled vs STL algorithms.** `std::set_intersection` is correct
   and concise, but it hides the algorithm behind a template signature, and
   its projection story (Posting objects vs docID output) requires a
   comparator that makes the code less obvious. The project's core principle
   is that the developer understands the architecture; a hand-written merge
   is 15 lines, matches the learning material exactly, and is trivially
   swappable later. Chosen: hand-rolled.
2. **docIDs vs Postings in results.** Boolean answers only need document
   identity; carrying TF would double the result size for no Boolean
   benefit. Ranking can re-fetch scores later. Chosen: `vector<doc_id>`.
3. **AND+OR vs AND only.** Both use the same machinery and OR is almost
   free; together they give ranking a complete Boolean foundation. Chosen:
   both.
4. **Smallest-first vs query-order folding.** Intersection is commutative,
   so the fold order never changes the answer — only the cost. Folding
   smallest lists first keeps the accumulator small and the work proportional
   to the smallest lists. A one-line sort by (list size, term) makes this
   deterministic. Chosen: smallest-first, as a documented optimization.
5. **Materialize-all vs streaming/limits.** Returning the full vector is
   the simplest, most testable contract and is exactly what ranking needs.
   Real systems cap results; that is a later phase. Chosen: materialize all.
6. **No parser.** `and_query` / `or_query` are explicit methods. A parser
   for `"cat AND dog"` syntax, negation (NOT), and nested expressions is a
   later phase — Phase 3 defines the *semantics* first.

### Recommendation

The design above: two pure merge primitives (`dse::intersect`,
`dse::merge_union`) plus a `QueryProcessor` that borrows a `const
InvertedIndex&` and exposes `and_query` / `or_query`, with tokenization,
dedup, smallest-first AND folding, sorted deduplicated output, and
deterministic behavior throughout. It is the smallest design that turns the
Phase 2 index into an answering machine, keeps every later phase (ranking,
phrases, query languages, distribution) additive, and is exhaustively
testable through a two-method public contract.

---

## 24. What is explicitly deferred to later phases

None of the following changes the Phase 3 contract; each is listed here so
it is clear what Phase 3 deliberately does *not* do:

- **Ranking / BM25.** Boolean results are docID lists in canonical order,
  not relevance-ranked. Ranking scores each result (using TF from postings,
  DF from list sizes, N from `document_count()`) and reorders. The Phase 3
  result list is the ranker's input.
- **Phrase queries** (`"exact phrase"`). Require positional information
  (Phase 2 deliberately deferred positions); a phrase is a positional AND.
- **Positional indexes.** A separate structure mapping terms to positions
  within documents; prerequisite for phrases, snippets, and proximity
  search. Postings stay TF-only in Phase 3.
- **Fuzzy / typo-tolerant search.** Edit distance, n-gram indexes, or
  suggestion dictionaries — a separate component with its own data
  structures.
- **Negation (NOT) and query languages.** Phase 3 exposes two explicit
  methods. `"cat AND NOT dog"`, parentheses, and field queries need a query
  parser — a later phase built on the same merge machinery (NOT is
  complement-against-all, which needs the full docID universe).
- **Pagination, limits, top-k.** Results are returned in full. Real engines
  cap and paginate; that layer belongs with ranking (top-k scoring) and is
  deferred.
- **Snippets / highlighting.** Needs the document store and positions —
  both later phases.
- **Caching.** Repeated queries re-walk the postings; a query cache is an
  optimization phase, not a correctness one.
- **Stop-word / stemming / synonym handling on queries.** Language policy
  (deferred from Phase 1) applies to the query side too; the processor
  contract is unchanged by adding or removing such filters upstream.
- **Persistence.** Query processing reads the in-memory index; when the
  index moves to disk, the processor's public API survives unchanged behind
  it.
- **Distributed querying.** Fan-out, aggregation, and partial-result merge
  across shards — a distribution phase. The deterministic, sorted result
  contract is precisely what makes cross-shard merging clean later.
- **Networking.** Remote query APIs and transports — a deployment phase.

The rule, repeated from Phases 1 and 2: *make the initial API small and
honest, and every future feature becomes an additive extension instead of a
breaking change.*

---

## 25. The Phase 3 contract (summary)

- **Responsibility:** turn raw query text into sorted, deduplicated docID
  lists via Boolean AND/OR over the inverted index. Nothing else — no
  scoring, no parsing, no persistence, no networking.
- **Input:** a borrowed `std::string_view` query, tokenized with
  `dse::tokenize` (the same Phase 1 function that processed documents); a
  borrowed `const InvertedIndex&` that must outlive the processor.
- **Output:** an owning `std::vector<doc_id>`, sorted ascending,
  deduplicated, deterministic — the same query always returns the same
  vector.
- **Semantics:** AND returns documents containing all distinct query terms
  (missing term -> empty); OR returns documents containing at least one
  (missing term ignored); zero query terms -> empty result for both.
- **Core algorithms:** two-pointer merge — intersection O(a + b), union
  O(a + b) — over the sorted postings lists that Phase 2's invariant
  guarantees; k-way queries fold pairwise (smallest-first for AND).
- **Complexity:** O(Q + V log V) query preparation; O(a + b) per two-list
  merge; O(Σ L_i) for k-way OR; result-size-bounded for AND; O(Q + result)
  memory, no postings copies.
- **Known limitations:** Boolean only (no relevance order); no phrases,
  negation, or query syntax; results fully materialized; in-memory,
  single-machine, ASCII vocabulary (inherited from Phase 1) — each
  explicitly deferred, none blocking ranking next.
