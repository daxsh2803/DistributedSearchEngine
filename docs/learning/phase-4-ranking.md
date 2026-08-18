# Phase 4 — Ranking with TF-IDF

## 1. Where Ranking Fits in the Search-Engine Pipeline

```
Raw Document
    ↓
Tokenizer (Phase 1)
    ↓
Normalized Tokens
    ↓
Inverted Index (Phase 2)
    ↓
term → postings lists
    ↓
Query Processing (Phase 3)
    ↓
Boolean candidate set (AND/OR)
    ↓
Ranking (Phase 4)        ← we are here
    ↓
Scored, sorted results
```

Phases 1–3 give us **Boolean retrieval**: given a query, return the set of
documents that match. Every matching document is treated identically — there
is no notion of "better" or "worse."

Phase 4 introduces **relevance scoring**: each matching document receives a
numeric score that estimates how relevant it is to the query. Results are
returned sorted by score, highest first.

This is the transition from "which documents match?" to "which documents
matter most?"

## 2. The Intuition

Consider a corpus of 100 documents and the query "cat dog."

- "cat" appears in 50 documents. It is common — not very informative.
- "dog" appears in 5 documents. It is rare — very informative.
- A document containing "dog" five times is more relevant to "dog" than one
  containing it once.

A good ranking function should capture both:
1. **Term frequency**: how often does the term appear in this document?
2. **Inverse document frequency**: how rare is this term across the corpus?

TF-IDF is the classic function that combines both.

## 3. Term Frequency (TF)

TF measures how frequently a term appears in a specific document.

```
tf(t, d) = number of times term t appears in document d
```

We already have this: `Posting.term_frequency` stores exactly this value,
computed by `InvertedIndex::add_document` during Phase 2.

### Why TF matters

A document mentioning "cat" 10 times is probably more about cats than one
mentioning it once. TF captures this signal.

### Limitations of raw TF

Raw TF has diminishing returns. A document with "cat" 100 times is not
necessarily 100× more relevant than one with "cat" once. In practice,
logarithmic TF (`1 + log(tf)`) or BM25's saturated TF often performs better.
For Phase 4, we use raw TF for simplicity — this is a deliberate simplification
that BM25 (a future phase) will improve upon.

## 4. Document Frequency (DF)

DF measures how many documents contain a given term.

```
df(t) = number of documents in which term t appears
       = postings(t).size()
```

### Why DF matters

"The" appears in nearly every English document. If you search for "the cat,"
matching "the" tells you almost nothing — nearly every document matches. But
matching "cat" is much more selective. DF captures this: common terms have
high DF; rare terms have low DF.

## 5. Inverse Document Frequency (IDF)

IDF inverts DF so that rare terms get high weights and common terms get low
weights.

```
idf(t) = log(N / df(t))
```

Where:
- N = total number of documents in the index
- df(t) = number of documents containing term t
- log = natural logarithm (ln)

### Examples

| Term    | df  | N    | N/df  | idf = ln(N/df) |
|---------|-----|------|-------|-----------------|
| "the"   | 95  | 100  | 1.05  | 0.05            |
| "cat"   | 20  | 100  | 5.00  | 1.61            |
| "axolotl"| 1  | 100  | 100.0 | 4.61            |

"The" barely registers. "Axolotl" is extremely informative.

### Edge case: df = N

If a term appears in every document, idf = log(N/N) = log(1) = 0. The term
contributes nothing to any document's score. This is correct: a universal
term cannot help distinguish between documents.

### Edge case: df = 0

If a term does not appear in any document, it cannot be looked up in the
index. The query processor handles this: for AND queries, a missing term
makes the result empty; for OR queries, it is ignored. The ranker never
needs to compute IDF for a missing term.

## 6. TF-IDF Score

The TF-IDF score for a single (term, document) pair is:

```
tfidf(t, d) = tf(t, d) × idf(t)
```

This captures: "how important is term t to document d?" — combining how
often it appears locally (TF) with how distinctive it is globally (IDF).

### Document Score for a Query

For a multi-term query, the document's total score is the sum of TF-IDF
scores for each query term:

```
score(d, q) = Σ tfidf(t, d)  for each distinct query term t
```

Terms are deduplicated before scoring (matching Phase 3's dedup behavior).

## 7. Worked Example

Corpus (10 documents):

| doc_id | text                |
|--------|---------------------|
| 1      | "the cat sat"       |
| 2      | "the dog ran"       |
| 3      | "the cat and dog"   |
| 4      | "a bird sang"       |
| 5      | "the cat and bird"  |
| 6      | "dog dog dog"       |
| 7      | "cat cat cat cat"   |
| 8      | "the fish swam"     |
| 9      | "cat and fish"      |
| 10     | "bird bird bird"    |

N = 10

Query: "cat dog"

Term stats:
- "cat": df = 5 (docs 1, 3, 5, 7, 9), idf = ln(10/5) = 0.693
- "dog": df = 4 (docs 2, 3, 6, 9 — wait, doc 9 has "cat and fish", not dog)

Let me recalculate:
- "cat" appears in: 1, 3, 5, 7, 9 → df = 5, idf = ln(10/5) = 0.693
- "dog" appears in: 2, 3, 6 → df = 3, idf = ln(10/3) = 1.204

TF-IDF scores for "cat dog" AND query (only docs with both):
- Doc 3 ("the cat and dog"): tf(cat)=1, tf(dog)=1
  score = 1×0.693 + 1×1.204 = 1.897

TF-IDF scores for "cat dog" OR query:
- Doc 1: tf(cat)=1 → 0.693
- Doc 2: tf(dog)=1 → 1.204
- Doc 3: tf(cat)=1, tf(dog)=1 → 0.693 + 1.204 = 1.897
- Doc 5: tf(cat)=1 → 0.693
- Doc 6: tf(dog)=3 → 3×1.204 = 3.612
- Doc 7: tf(cat)=4 → 4×0.693 = 2.772
- Doc 9: tf(cat)=1 → 0.693

OR results sorted by score descending:
1. Doc 6: 3.612  (three "dog"s, and "dog" is rarer)
2. Doc 7: 2.772  (four "cat"s)
3. Doc 3: 1.897  (both terms)
4. Doc 2: 1.204  (one "dog")
5. Doc 1: 0.693  (one "cat")
6. Doc 5: 0.693  (one "cat")
7. Doc 9: 0.693  (one "cat")

Note: Doc 6 ranks highest despite having only "dog" because "dog" is rarer
(DF=3 vs DF=5 for "cat") AND it appears 3 times.

## 8. Why Use Natural Logarithm?

The base of the logarithm affects IDF scaling but not the relative ordering
of documents. Natural log (base e) is standard in information retrieval
literature. Log base 2 would scale differently but produce the same ranking.
We use `std::log` (natural log) for consistency with academic TF-IDF.

## 9. The Ranker Class

### Why a Separate Class?

The `QueryProcessor` from Phase 3 handles Boolean logic (AND/OR intersection
and union). Ranking is a different concern:

- **QueryProcessor**: "which documents match?" → `vector<doc_id>`
- **Ranker**: "how well does each document match?" → `vector<RankedResult>`

Separating them follows the Single Responsibility Principle and means:
- Boolean queries still work without ranking overhead
- Ranking can be tested independently
- Future changes to one don't break the other

### Architecture

```
Ranker
  ├── borrows InvertedIndex (for IDF computation + postings lookup)
  ├── uses dse::tokenize (Phase 1) for query normalization
  └── produces vector<RankedResult> sorted by score descending
```

The Ranker does NOT modify the index. It reads postings, computes scores,
and returns sorted results.

### RankedResult

```cpp
struct RankedResult {
    doc_id document_id;
    double score;
};
```

Two fields: the document identifier and its TF-IDF score. The `operator==`
is defaulted for testing convenience.

### API

```cpp
class Ranker {
public:
    explicit Ranker(const InvertedIndex& index);
    
    // Documents matching ALL query terms, scored and sorted by TF-IDF.
    std::vector<RankedResult> ranked_and(std::string_view query) const;
    
    // Documents matching ANY query term, scored and sorted by TF-IDF.
    std::vector<RankedResult> ranked_or(std::string_view query) const;
};
```

### Lifetime Contract

The Ranker borrows the InvertedIndex (same as QueryProcessor). The index
must outlive the ranker. The ranker never stores query strings.

## 10. Implementation Algorithm

### ranked_or(query)

```
1. tokens = tokenize(query)
2. if tokens.empty() → return {}
3. dedup tokens → distinct_terms
4. candidate_map = {}  // doc_id → cumulative score
5. for each term in distinct_terms:
     span = index.postings(term)
     if span.empty() → continue  (term not in index)
     df = span.size()
     idf = log(N / df)
     for each posting in span:
       candidate_map[posting.document_id] += posting.term_frequency × idf
6. Convert candidate_map to vector<RankedResult>
7. Sort by score descending, then by doc_id ascending (for determinism)
8. Return
```

### ranked_and(query)

Same as ranked_or, but only documents that appear in ALL term posting lists
are included. Implementation: compute the AND candidate set first (using
Phase 3's intersection logic or a direct check), then score only those.

## 11. Complexity Analysis

| Operation | Time | Space |
|-----------|------|-------|
| Tokenize + dedup | O(Q + V log V) | O(Q + V) |
| IDF computation per term | O(1) | O(1) |
| Score accumulation | O(Σ posting list lengths) | O(C) where C = candidates |
| Sort results | O(C log C) | O(C) |
| **Total** | O(Q + V log V + Σ Lᵢ + C log C) | O(C) |

Where Q = query length, V = unique query terms, Lᵢ = posting list length
for term i, C = number of candidate documents.

In practice, C is typically much smaller than N, and the dominant cost is
the posting list traversal.

## 12. Edge Cases

| Case | Behavior |
|------|----------|
| Empty query | Return empty vector |
| Query term not in index | Skip it (OR); poison the AND |
| Single document in corpus | All IDF = log(1) = 0; all scores 0; tie-break by doc_id |
| Document matches multiple terms | Scores summed across terms |
| Tie in scores | Break by doc_id ascending (deterministic) |
| Very large corpus | IDF values stable; no overflow with double precision |
| TF = 0 | Should not occur (postings only exist for terms with TF ≥ 1) |
| All documents match | All scored; full corpus returned |

## 13. Why Not BM25 Yet?

BM25 (Okapi BM25) is the industry-standard improvement over TF-IDF:

- Saturates TF so that 100 mentions ≠ 100× more relevant
- Normalizes by document length (longer documents naturally have more term occurrences)
- Uses a probabilistic framework rather than a heuristic

BM25 is strictly better than raw TF-IDF, but TF-IDF is:
- Simpler to implement and understand
- The foundation that BM25 builds upon
- A necessary stepping stone for understanding why BM25's modifications help

Phase 4 intentionally uses raw TF-IDF. BM25 is a natural Phase 5 extension.

## 14. Testing Strategy

Tests should verify:

1. **Basic scoring**: single term, single document — score = tf × idf
2. **IDF computation**: verify IDF values against hand-calculated expected values
3. **Multi-term scoring**: document matching multiple terms gets summed score
4. **Ranking order**: higher-scored documents come first
5. **AND vs OR**: AND excludes partial matches; OR includes them
6. **Missing terms**: AND with missing term → empty; OR with missing term → remaining terms scored
7. **Empty queries**: return empty
8. **Single document corpus**: IDF = 0 for all terms, scores all 0
9. **Determinism**: same query, same results every time
10. **Tie-breaking**: equal scores sorted by doc_id
11. **Integration with tokenizer and index**: end-to-end pipeline
12. **Edge cases**: empty index, punctuation queries, large result sets

## 15. Important C++ Concepts

- `std::log()` — natural logarithm from `<cmath>`
- `std::map` / `std::unordered_map` — for accumulating scores per document
- `std::sort` with custom comparator — for ordering results by score
- `struct` with defaulted `operator==` — for test assertions
- `const` correctness — ranker never modifies the index
- `double` for scores — sufficient precision for TF-IDF values
- Move semantics — returning vectors by value

## 16. What Is Deferred

- **BM25 scoring** — replaces TF-IDF with saturation + length normalization
- **Query expansion** — synonyms, related terms
- **Field weighting** — title vs body vs metadata
- **Learning-to-rank** — ML-based ranking
- **Click models** — using user behavior to improve rankings
- **Freshness scoring** — newer documents ranked higher
- **Personalization** — user-specific ranking
